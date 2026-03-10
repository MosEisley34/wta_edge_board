function stageMatchEvents(runId, config, oddsEvents, scheduleEvents) {
  const start = Date.now();
  const toleranceMin = config.MATCH_TIME_TOLERANCE_MIN;
  const fallbackMin = config.MATCH_FALLBACK_EXPANSION_MIN;
  const aliasMap = buildPlayerAliasMap_(config.PLAYER_ALIAS_MAP_JSON);
  const reasonCounts = {};
  const rows = [];
  const unmatched = [];
  let matchedCount = 0;
  let rejectedCount = 0;
  let diagnosticRecordsWritten = 0;
  const canonicalizationExamples = [];

  if ((!oddsEvents || !oddsEvents.length) && scheduleEvents && scheduleEvents.length) {
    scheduleEvents.forEach((event) => {
      rows.push({
        key: ['schedule_seed_no_odds', event.event_id].join('|'),
        odds_event_id: event.event_id,
        schedule_event_id: event.event_id,
        match_type: 'schedule_seed_no_odds',
        rejection_code: '',
        time_diff_min: '',
        competition_tier: event.canonical_tier || 'UNKNOWN',
        updated_at: new Date().toISOString(),
      });
    });

    reasonCounts.schedule_seed_no_odds = (reasonCounts.schedule_seed_no_odds || 0) + scheduleEvents.length;
    diagnosticRecordsWritten += scheduleEvents.length;

    const summaryNoOdds = buildStageSummary_(runId, 'stageMatchEvents', start, {
      input_count: 0,
      output_count: 0,
      provider: 'internal_matcher',
      api_credit_usage: 0,
      reason_codes: Object.assign({}, reasonCounts, {
        matched_count: 0,
        rejected_count: 0,
        diagnostic_records_written: diagnosticRecordsWritten,
      }),
    });

    return {
      rows,
      summary: summaryNoOdds,
      matchedCount: 0,
      rejectedCount: 0,
      diagnosticRecordsWritten,
      unmatchedCount: 0,
      unmatched,
      canonicalizationExamples,
    };
  }

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
    rejectedCount += 1;
    diagnosticRecordsWritten += 1;
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
    output_count: matchedCount,
    provider: 'internal_matcher',
    api_credit_usage: 0,
    reason_codes: Object.assign({}, reasonCounts, {
      matched_count: matchedCount,
      rejected_count: rejectedCount,
      diagnostic_records_written: diagnosticRecordsWritten,
    }),
  });

  return {
    rows,
    summary,
    matchedCount,
    rejectedCount,
    diagnosticRecordsWritten,
    unmatchedCount: rejectedCount,
    unmatched,
    canonicalizationExamples,
  };
}

function canonicalizeCompetition(name) {
  const resolved = resolveCompetitionTier_({ competition: name }, buildCompetitionTierResolverConfig_({
    COMPETITION_SOURCE_FIELDS_JSON: DEFAULT_CONFIG.COMPETITION_SOURCE_FIELDS_JSON,
    GRAND_SLAM_ALIASES_JSON: DEFAULT_CONFIG.GRAND_SLAM_ALIASES_JSON,
    WTA_1000_ALIASES_JSON: DEFAULT_CONFIG.WTA_1000_ALIASES_JSON,
    WTA_500_ALIASES_JSON: DEFAULT_CONFIG.WTA_500_ALIASES_JSON,
    COMPETITION_DENY_ALIASES_JSON: DEFAULT_CONFIG.COMPETITION_DENY_ALIASES_JSON,
    GRAND_SLAM_ALIAS_MAP_JSON: DEFAULT_CONFIG.GRAND_SLAM_ALIAS_MAP_JSON,
    WTA_500_ALIAS_MAP_JSON: DEFAULT_CONFIG.WTA_500_ALIAS_MAP_JSON,
    WTA_1000_ALIAS_MAP_JSON: DEFAULT_CONFIG.WTA_1000_ALIAS_MAP_JSON,
    COMPETITION_DENY_ALIAS_MAP_JSON: DEFAULT_CONFIG.COMPETITION_DENY_ALIAS_MAP_JSON,
  }));
  return resolved.canonical_tier;
}

function buildCompetitionTierResolverConfig_(config) {
  const denyAliases = parseAliasListJson_(config.COMPETITION_DENY_ALIASES_JSON, DEFAULT_CONFIG.COMPETITION_DENY_ALIASES_JSON);
  const denyAliasMap = parseAliasMapJson_(config.COMPETITION_DENY_ALIAS_MAP_JSON, DEFAULT_CONFIG.COMPETITION_DENY_ALIAS_MAP_JSON);

  if (config.ALLOW_WTA_250) {
    delete denyAliasMap.WTA_250;
    return {
      sourceFields: parseCompetitionSourceFieldsJson_(config.COMPETITION_SOURCE_FIELDS_JSON, DEFAULT_CONFIG.COMPETITION_SOURCE_FIELDS_JSON),
      grandSlamAliases: parseAliasListJson_(config.GRAND_SLAM_ALIASES_JSON, DEFAULT_CONFIG.GRAND_SLAM_ALIASES_JSON),
      wta1000Aliases: parseAliasListJson_(config.WTA_1000_ALIASES_JSON, DEFAULT_CONFIG.WTA_1000_ALIASES_JSON),
      wta500Aliases: parseAliasListJson_(config.WTA_500_ALIASES_JSON, DEFAULT_CONFIG.WTA_500_ALIASES_JSON),
      denyAliases: denyAliases.filter((alias) => !isWta250Competition_(alias)),
      grandSlamAliasMap: parseAliasMapJson_(config.GRAND_SLAM_ALIAS_MAP_JSON, DEFAULT_CONFIG.GRAND_SLAM_ALIAS_MAP_JSON),
      wta500AliasMap: parseAliasMapJson_(config.WTA_500_ALIAS_MAP_JSON, DEFAULT_CONFIG.WTA_500_ALIAS_MAP_JSON),
      wta1000AliasMap: parseAliasMapJson_(config.WTA_1000_ALIAS_MAP_JSON, DEFAULT_CONFIG.WTA_1000_ALIAS_MAP_JSON),
      denyAliasMap,
    };
  }

  return {
    sourceFields: parseCompetitionSourceFieldsJson_(config.COMPETITION_SOURCE_FIELDS_JSON, DEFAULT_CONFIG.COMPETITION_SOURCE_FIELDS_JSON),
    grandSlamAliases: parseAliasListJson_(config.GRAND_SLAM_ALIASES_JSON, DEFAULT_CONFIG.GRAND_SLAM_ALIASES_JSON),
    wta1000Aliases: parseAliasListJson_(config.WTA_1000_ALIASES_JSON, DEFAULT_CONFIG.WTA_1000_ALIASES_JSON),
    wta500Aliases: parseAliasListJson_(config.WTA_500_ALIASES_JSON, DEFAULT_CONFIG.WTA_500_ALIASES_JSON),
    denyAliases,
    grandSlamAliasMap: parseAliasMapJson_(config.GRAND_SLAM_ALIAS_MAP_JSON, DEFAULT_CONFIG.GRAND_SLAM_ALIAS_MAP_JSON),
    wta500AliasMap: parseAliasMapJson_(config.WTA_500_ALIAS_MAP_JSON, DEFAULT_CONFIG.WTA_500_ALIAS_MAP_JSON),
    wta1000AliasMap: parseAliasMapJson_(config.WTA_1000_ALIAS_MAP_JSON, DEFAULT_CONFIG.WTA_1000_ALIAS_MAP_JSON),
    denyAliasMap,
  };
}

function resolveCompetitionTier_(event, resolverConfig) {
  const sourceFields = buildCompetitionSourceFields_(event, resolverConfig);
  let fallbackOther = null;

  for (let i = 0; i < sourceFields.length; i += 1) {
    const source = sourceFields[i];
    const canonical = detectTierByValue_(source.value, resolverConfig);
    if (canonical === 'OTHER') {
      if (!fallbackOther) {
        fallbackOther = {
          canonical_tier: canonical,
          matched_by: source.rule,
          matched_field: source.field,
          matched_value: source.value,
          raw_fields: sourceFields,
        };
      }
      continue;
    }

    if (canonical !== 'UNKNOWN') {
      return {
        canonical_tier: canonical,
        matched_by: source.rule,
        matched_field: source.field,
        matched_value: source.value,
        raw_fields: sourceFields,
      };
    }
  }

  if (fallbackOther) return fallbackOther;

  return {
    canonical_tier: 'UNKNOWN',
    matched_by: 'none',
    matched_field: '',
    matched_value: '',
    raw_fields: sourceFields,
  };
}

function describeCompetitionDecision_(resolved, decision) {
  const source = resolveRejectionSource_(resolved);
  return {
    raw_competition: source.value || '',
    canonical_competition: normalizeCompetitionValue_(source.value || ''),
    resolved_tier: resolved && resolved.canonical_tier ? resolved.canonical_tier : 'UNKNOWN',
    allow_decision: decision && decision.allowed ? 'allow' : 'deny',
    decision_reason: decision && decision.reason_code ? decision.reason_code : '',
    source_field: source.field || '',
  };
}

function resolveRejectionSource_(resolved) {
  const fields = (resolved && resolved.raw_fields) ? resolved.raw_fields : [];
  const fallback = fields.find((field) => normalizeCompetitionValue_(field.value));
  if (resolved && resolved.matched_field) {
    return {
      field: resolved.matched_field,
      value: resolved.matched_value || '',
    };
  }
  return {
    field: fallback ? fallback.field : '',
    value: fallback ? fallback.value : '',
  };
}

function buildCompetitionSourceFields_(event, resolverConfig) {
  const sourceFields = resolverConfig && resolverConfig.sourceFields && resolverConfig.sourceFields.length
    ? resolverConfig.sourceFields
    : ['competition', 'tournament', 'event_name', 'sport_title', 'home_team', 'away_team'];

  return sourceFields.map((fieldName) => ({
    field: fieldName,
    value: event[fieldName] || '',
    rule: 'field_priority',
  }));
}

function detectTierByValue_(rawValue, resolverConfig) {
  const norm = normalizeCompetitionValue_(rawValue);
  if (!norm) return 'UNKNOWN';

  if (containsAlias_(norm, resolverConfig.denyAliases) || containsAliasMap_(norm, resolverConfig.denyAliasMap)) {
    return detectDeniedTier_(norm, resolverConfig.denyAliasMap);
  }

  if (containsAliasMap_(norm, resolverConfig.grandSlamAliasMap) || containsAlias_(norm, resolverConfig.grandSlamAliases)) return 'GRAND_SLAM';
  if (containsAliasMap_(norm, resolverConfig.wta1000AliasMap) || containsAlias_(norm, resolverConfig.wta1000Aliases)) return 'WTA_1000';
  if (containsAliasMap_(norm, resolverConfig.wta500AliasMap) || containsAlias_(norm, resolverConfig.wta500Aliases)) return 'WTA_500';
  if (/wta\s*125/.test(norm)) return 'WTA_125';
  if (isWta250Competition_(norm)) return 'WTA_250';
  if (/\bitf\b/.test(norm)) return 'ITF';
  if (/wta/.test(norm)) return 'OTHER';
  return 'UNKNOWN';
}

function containsAlias_(normalizedSource, aliases) {
  if (!aliases || !aliases.length) return false;
  for (let i = 0; i < aliases.length; i += 1) {
    if (normalizedSource.indexOf(aliases[i]) !== -1) return true;
  }
  return false;
}

function containsAliasMap_(normalizedSource, aliasMap) {
  if (!aliasMap) return false;
  const tiers = Object.keys(aliasMap);
  for (let i = 0; i < tiers.length; i += 1) {
    const aliases = aliasMap[tiers[i]] || [];
    if (containsAlias_(normalizedSource, aliases)) return true;
  }
  return false;
}

function detectDeniedTier_(normalizedSource, denyAliasMap) {
  const tiers = Object.keys(denyAliasMap || {});
  for (let i = 0; i < tiers.length; i += 1) {
    const tier = tiers[i];
    if (containsAlias_(normalizedSource, denyAliasMap[tier] || [])) return tier;
  }
  if (/wta\s*125/.test(normalizedSource)) return 'WTA_125';
  if (isWta250Competition_(normalizedSource)) return 'WTA_250';
  if (/\bitf\b/.test(normalizedSource)) return 'ITF';
  return 'OTHER';
}

function isWta250Competition_(normalizedSource) {
  if (!normalizedSource) return false;
  if (!/\bwta\b/.test(normalizedSource)) return false;
  return /\b250\b/.test(normalizedSource) || /\bwta\s+international\b/.test(normalizedSource);
}

function parseCompetitionSourceFieldsJson_(jsonText, fallbackJsonText) {
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

  if (!Array.isArray(parsed)) return ['competition', 'tournament', 'event_name', 'sport_title', 'home_team', 'away_team'];

  const out = [];
  const seen = {};
  parsed.forEach((field) => {
    const normalized = String(field || '').trim();
    if (!normalized || seen[normalized]) return;
    seen[normalized] = true;
    out.push(normalized);
  });

  return out.length ? out : ['competition', 'tournament', 'event_name', 'sport_title', 'home_team', 'away_team'];
}

function parseAliasMapJson_(jsonText, fallbackJsonText) {
  let parsed = {};
  try {
    parsed = JSON.parse(jsonText || fallbackJsonText || '{}');
  } catch (e) {
    try {
      parsed = JSON.parse(fallbackJsonText || '{}');
    } catch (ignored) {
      parsed = {};
    }
  }

  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return {};

  const aliasMap = {};
  Object.keys(parsed).forEach((tier) => {
    const list = Array.isArray(parsed[tier]) ? parsed[tier] : [];
    const deduped = {};
    list.forEach((value) => {
      const normalized = normalizeCompetitionValue_(value);
      if (normalized) deduped[normalized] = true;
    });
    aliasMap[String(tier)] = Object.keys(deduped);
  });

  return aliasMap;
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
  if (canonical === 'WTA_250') {
    return config.ALLOW_WTA_250
      ? { allowed: true, reason_code: 'allowed_wta250' }
      : { allowed: false, reason_code: 'rejected_wta250' };
  }
  if (canonical === 'ITF') return { allowed: false, reason_code: 'rejected_itf' };
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
  normalized = normalized
    .replace(/[’'`]/g, '')
    .replace(/[._,;:()\[\]{}!/?]/g, ' ')
    .replace(/[\-–—]/g, ' ')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  normalized = normalizePlayerNameAliasRules_(normalized);
  return aliasMap[normalized] || normalized;
}

function normalizePlayerNameAliasRules_(normalized) {
  const value = String(normalized || '').trim();
  if (!value) return '';

  const aliasRules = {
    'i swiatek': 'iga swiatek',
    'swiatek iga': 'iga swiatek',
    'iga swiatek': 'iga swiatek',
    'e rybakina': 'elena rybakina',
    'rybakina elena': 'elena rybakina',
    'elena rybakina': 'elena rybakina',
    'm kostyuk': 'marta kostyuk',
    'kostyuk marta': 'marta kostyuk',
    'marta kostyuk': 'marta kostyuk',
    's kartal': 'sonay kartal',
    'kartal sonay': 'sonay kartal',
    'sonay kartal': 'sonay kartal',
  };
  if (aliasRules[value]) return aliasRules[value];

  const surnameParticles = ['de', 'del', 'della', 'da', 'di', 'van', 'von', 'la', 'le', 'st', 'saint'];
  const tokens = value.split(' ');
  if (tokens.length >= 3) {
    for (let i = 1; i < tokens.length - 1; i += 1) {
      if (surnameParticles.indexOf(tokens[i]) >= 0) {
        const merged = tokens.slice(0, i).concat([tokens.slice(i).join(' ')]).join(' ').trim();
        if (merged) return merged;
      }
    }
  }

  if (/^[a-z]\s+[a-z]+(?:\s+[a-z]+)*$/.test(value)) {
    const initial = value.split(' ')[0];
    const rest = value.split(' ').slice(1).join(' ');
    if (aliasRules[initial + ' ' + rest]) return aliasRules[initial + ' ' + rest];
  }

  return value;
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
