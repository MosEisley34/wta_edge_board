function stageMatchEvents(runId, config, oddsEvents, scheduleEvents) {
  const start = Date.now();
  const toleranceMin = config.MATCH_TIME_TOLERANCE_MIN;
  const fallbackMin = config.MATCH_FALLBACK_EXPANSION_MIN;
  const aliasMap = buildPlayerAliasMap_(config.PLAYER_ALIAS_MAP_JSON);
  const reasonCounts = {};
  const rows = [];
  const unmatched = [];
  let matchedCount = 0;
  const canonicalizationExamples = [];

  const primary = oddsEvents.map((odds) => matchSingleOddsEvent_(odds, scheduleEvents, toleranceMin, aliasMap, canonicalizationExamples));
  const unmatchedPrimary = primary.filter((res) => !res.matched);

  if (unmatchedPrimary.length === 0) reasonCounts.fallback_short_circuit = (reasonCounts.fallback_short_circuit || 0) + 1;

  const finalResults = unmatchedPrimary.length === 0
    ? primary
    : primary.map((res) => {
      if (res.matched) return res;
      const fallback = matchSingleOddsEvent_(res.odds, scheduleEvents, toleranceMin + fallbackMin, aliasMap, canonicalizationExamples);
      if (fallback.matched) {
        fallback.match_type = 'fallback_match';
        return fallback;
      }
      fallback.rejection_code = fallback.rejection_code === 'outside_time_tolerance' ? 'fallback_exhausted' : fallback.rejection_code;
      return fallback;
    });

  finalResults.forEach((result) => {
    if (result.matched) {
      rows.push({
        key: result.odds.event_id,
        odds_event_id: result.odds.event_id,
        schedule_event_id: result.schedule_event_id,
        match_type: result.match_type,
        rejection_code: '',
        time_diff_min: result.time_diff_min,
        competition_tier: result.competition_tier,
        updated_at: new Date().toISOString(),
      });
      matchedCount += 1;
      reasonCounts[result.match_type] = (reasonCounts[result.match_type] || 0) + 1;
      return;
    }

    rows.push({
      key: result.odds.event_id,
      odds_event_id: result.odds.event_id,
      schedule_event_id: '',
      match_type: '',
      rejection_code: result.rejection_code,
      time_diff_min: '',
      competition_tier: '',
      updated_at: new Date().toISOString(),
    });
    reasonCounts[result.rejection_code] = (reasonCounts[result.rejection_code] || 0) + 1;
    unmatched.push({
      odds_event_id: result.odds.event_id,
      competition: result.odds.competition,
      player_1: result.odds.player_1,
      player_2: result.odds.player_2,
      commence_time: result.odds.commence_time.toISOString(),
      rejection_code: result.rejection_code,
    });
  });

  const summary = buildStageSummary_(runId, 'stageMatchEvents', start, {
    input_count: oddsEvents.length,
    output_count: rows.length,
    provider: 'internal_matcher',
    api_credit_usage: 0,
    reason_codes: reasonCounts,
  });

  return {
    rows,
    summary,
    matchedCount,
    unmatchedCount: oddsEvents.length - matchedCount,
    unmatched,
    canonicalizationExamples,
  };
}

function canonicalizeCompetition(name) {
  const resolved = resolveCompetitionTier_({ competition: name }, buildCompetitionTierResolverConfig_({
    GRAND_SLAM_ALIASES_JSON: DEFAULT_CONFIG.GRAND_SLAM_ALIASES_JSON,
    WTA_1000_ALIASES_JSON: DEFAULT_CONFIG.WTA_1000_ALIASES_JSON,
    WTA_500_ALIASES_JSON: DEFAULT_CONFIG.WTA_500_ALIASES_JSON,
  }));
  return resolved.canonical_tier;
}

function buildCompetitionTierResolverConfig_(config) {
  return {
    grandSlamAliases: parseAliasListJson_(config.GRAND_SLAM_ALIASES_JSON, DEFAULT_CONFIG.GRAND_SLAM_ALIASES_JSON),
    wta1000Aliases: parseAliasListJson_(config.WTA_1000_ALIASES_JSON, DEFAULT_CONFIG.WTA_1000_ALIASES_JSON),
    wta500Aliases: parseAliasListJson_(config.WTA_500_ALIASES_JSON, DEFAULT_CONFIG.WTA_500_ALIASES_JSON),
  };
}

function resolveCompetitionTier_(event, resolverConfig) {
  const sourceFields = buildCompetitionSourceFields_(event);

  for (let i = 0; i < sourceFields.length; i += 1) {
    const source = sourceFields[i];
    const canonical = detectTierByValue_(source.value, resolverConfig);
    if (canonical !== 'UNKNOWN') {
      return {
        canonical_tier: canonical,
        matched_by: source.rule,
        matched_field: source.field,
        raw_fields: sourceFields,
      };
    }
  }

  return {
    canonical_tier: 'UNKNOWN',
    matched_by: 'none',
    matched_field: '',
    raw_fields: sourceFields,
  };
}

function buildCompetitionSourceFields_(event) {
  return [
    { field: 'competition', value: event.competition || '', rule: 'direct_competition' },
    { field: 'tournament', value: event.tournament || '', rule: 'tournament' },
    { field: 'event_name', value: event.event_name || '', rule: 'event_name' },
    { field: 'sport_title', value: event.sport_title || '', rule: 'sport_title' },
    { field: 'home_team', value: event.home_team || '', rule: 'home_team' },
    { field: 'away_team', value: event.away_team || '', rule: 'away_team' },
  ];
}

function detectTierByValue_(rawValue, resolverConfig) {
  const norm = normalizeCompetitionValue_(rawValue);
  if (!norm) return 'UNKNOWN';

  if (/wta\s*125/.test(norm)) return 'WTA_125';
  if (containsAlias_(norm, resolverConfig.grandSlamAliases)) return 'GRAND_SLAM';
  if (containsAlias_(norm, resolverConfig.wta1000Aliases)) return 'WTA_1000';
  if (containsAlias_(norm, resolverConfig.wta500Aliases)) return 'WTA_500';
  if (/wta/.test(norm)) return 'OTHER';
  return 'UNKNOWN';
}

function containsAlias_(normalizedSource, aliases) {
  for (let i = 0; i < aliases.length; i += 1) {
    if (normalizedSource.indexOf(aliases[i]) !== -1) return true;
  }
  return false;
}

function normalizeCompetitionValue_(value) {
  return String(value || '').toLowerCase().replace(/[^a-z0-9\s]/g, ' ').replace(/\s+/g, ' ').trim();
}

function parseAliasListJson_(jsonText, fallbackJsonText) {
  let parsed = [];
  try {
    parsed = JSON.parse(jsonText || fallbackJsonText || '[]');
  } catch (e) {
    try {
      parsed = JSON.parse(fallbackJsonText || '[]');
    } catch (ignored) {
      parsed = [];
    }
  }

  if (!Array.isArray(parsed)) return [];

  const aliasMap = {};
  parsed.forEach((value) => {
    const normalized = normalizeCompetitionValue_(value);
    if (normalized) aliasMap[normalized] = true;
  });

  return Object.keys(aliasMap);
}

function isAllowedTournament(canonical, config) {
  if (canonical === 'WTA_500') return { allowed: true, reason_code: 'allowed_wta500' };
  if (canonical === 'WTA_1000') return { allowed: true, reason_code: 'allowed_wta1000' };
  if (canonical === 'GRAND_SLAM') return { allowed: true, reason_code: 'allowed_grand_slam' };
  if (canonical === 'WTA_125') {
    return config.ALLOW_WTA_125
      ? { allowed: true, reason_code: 'allowed_wta125' }
      : { allowed: false, reason_code: 'rejected_wta125' };
  }
  if (canonical === 'OTHER') return { allowed: false, reason_code: 'rejected_other_tier' };
  return { allowed: false, reason_code: 'rejected_unknown_competition' };
}

function matchSingleOddsEvent_(odds, scheduleEvents, maxToleranceMin, aliasMap, canonicalizationExamples) {
  const oddsPlayers = normalizePlayers_(odds.player_1, odds.player_2, aliasMap);
  canonicalizationExamples.push({
    sample_type: 'odds',
    raw_players: [odds.player_1, odds.player_2],
    canonical_players: oddsPlayers,
  });

  const samePlayers = [];
  scheduleEvents.forEach((sched) => {
    const schedPlayers = normalizePlayers_(sched.player_1, sched.player_2, aliasMap);
    if (canonicalizationExamples.length < 25) {
      canonicalizationExamples.push({
        sample_type: 'schedule',
        raw_players: [sched.player_1, sched.player_2],
        canonical_players: schedPlayers,
      });
    }
    if (oddsPlayers === schedPlayers) samePlayers.push(sched);
  });

  if (!samePlayers.length) return { odds, matched: false, rejection_code: 'no_player_match' };

  const inTolerance = samePlayers
    .map((sched) => ({
      sched,
      diffMin: Math.abs(odds.commence_time.getTime() - sched.start_time.getTime()) / 60000,
    }))
    .filter((candidate) => candidate.diffMin <= maxToleranceMin)
    .sort((a, b) => a.diffMin - b.diffMin);

  if (!inTolerance.length) return { odds, matched: false, rejection_code: 'outside_time_tolerance' };
  if (inTolerance.length > 1 && inTolerance[0].diffMin === inTolerance[1].diffMin) {
    return { odds, matched: false, rejection_code: 'ambiguous_candidate' };
  }

  const winner = inTolerance[0];
  return {
    odds,
    matched: true,
    match_type: 'primary_match',
    schedule_event_id: winner.sched.event_id,
    competition_tier: winner.sched.canonical_tier,
    time_diff_min: Math.round(winner.diffMin),
  };
}

function normalizePlayers_(a, b, aliasMap) {
  return [canonicalizePlayerName_(a, aliasMap), canonicalizePlayerName_(b, aliasMap)].sort().join('|');
}

function canonicalizePlayerName_(name, aliasMap) {
  let normalized = String(name || '').toLowerCase();
  normalized = normalized.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  normalized = normalized.replace(/[^a-z0-9\s]/g, ' ').replace(/\s+/g, ' ').trim();
  return aliasMap[normalized] || normalized;
}

function buildPlayerAliasMap_(json) {
  try {
    const parsed = JSON.parse(json || '{}');
    const alias = {};
    Object.keys(parsed || {}).forEach((key) => {
      alias[canonicalizePlayerName_(key, {})] = canonicalizePlayerName_(parsed[key], {});
    });
    return alias;
  } catch (e) {
    return {};
  }
}
