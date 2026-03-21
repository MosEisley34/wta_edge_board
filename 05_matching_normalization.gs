// NOTE: Keep PLAYER_STATS_*_CACHE_* constants declared only in 05_player_stats_adapter.gs.
// If this matcher needs those values, reference the shared global constants instead of redeclaring.
function stageMatchEvents(runId, config, oddsEvents, scheduleEvents) {
  const start = Date.now();
  const toleranceMin = config.MATCH_TIME_TOLERANCE_MIN;
  const fallbackMin = config.MATCH_FALLBACK_EXPANSION_MIN;
  const fallbackHardMaxDeltaMin = Math.max(0, Number(config.MATCH_FALLBACK_HARD_MAX_DELTA_MIN || 1440));
  const identityMissingRateThreshold = Math.max(0, Math.min(1, Number(config.MATCHER_PLAYER_IDENTITY_MISSING_RATE_BLOCK_THRESHOLD || 0.6)));
  const identityMissingMinRows = Math.max(1, Number(config.MATCHER_PLAYER_IDENTITY_MISSING_MIN_ROWS || 3));
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

  const scheduleIdentityHealth = assessSchedulePlayerIdentityHealth_(scheduleEvents);
  const shouldBlockMatcherForIdentityCoverage = !!(
    oddsEvents && oddsEvents.length
    && scheduleIdentityHealth.total_schedule_rows > 0
    && scheduleIdentityHealth.missing_identity_rows >= identityMissingMinRows
    && scheduleIdentityHealth.missing_identity_rate >= identityMissingRateThreshold
  );

  if (shouldBlockMatcherForIdentityCoverage) {
    (oddsEvents || []).forEach((odds) => {
      const eventId = String((odds && odds.event_id) || '');
      rows.push({
        key: ['schedule_missing_player_identity', eventId].join('|'),
        odds_event_id: eventId,
        schedule_event_id: '',
        match_type: '',
        rejection_code: 'schedule_missing_player_identity',
        time_diff_min: '',
        competition_tier: '',
        odds_players_raw: JSON.stringify([String((odds && odds.player_1) || ''), String((odds && odds.player_2) || '')]),
        odds_players_normalized: JSON.stringify([]),
        candidate_players_raw: JSON.stringify([]),
        candidate_players_normalized: JSON.stringify([]),
        similarity_scores: JSON.stringify({}),
        primary_time_delta_min: '',
        fallback_time_delta_min: '',
        rejection_discriminator: 'schedule_player_identity_precheck_blocked',
        updated_at: new Date().toISOString(),
      });
      unmatched.push({
        odds_event_id: eventId,
        competition: String((odds && odds.competition) || ''),
        player_1: String((odds && odds.player_1) || ''),
        player_2: String((odds && odds.player_2) || ''),
        commence_time: odds && odds.commence_time && typeof odds.commence_time.toISOString === 'function' ? odds.commence_time.toISOString() : '',
        rejection_code: 'schedule_missing_player_identity',
        normalized_odds_players: [],
        normalized_schedule_players: [],
        nearest_schedule_candidate: null,
        primary_time_delta_min: '',
        fallback_time_delta_min: '',
      });
    });

    reasonCounts.schedule_missing_player_identity = Number((oddsEvents || []).length || 0);
    rejectedCount = Number((oddsEvents || []).length || 0);
    diagnosticRecordsWritten = Number((oddsEvents || []).length || 0);

    const blockedSummary = buildStageSummary_(runId, 'stageMatchEvents', start, {
      input_count: Number((oddsEvents || []).length || 0),
      output_count: 0,
      provider: 'internal_matcher',
      api_credit_usage: 0,
      reason_codes: Object.assign({}, reasonCounts, {
        matched_count: 0,
        rejected_count: rejectedCount,
        diagnostic_records_written: diagnosticRecordsWritten,
      }),
      reason_metadata: {
        matcher_precheck_blocked: true,
        matcher_precheck_reason: 'schedule_missing_player_identity',
        total_schedule_rows: scheduleIdentityHealth.total_schedule_rows,
        missing_identity_rows: scheduleIdentityHealth.missing_identity_rows,
        missing_identity_rate: scheduleIdentityHealth.missing_identity_rate,
        missing_identity_rate_threshold: identityMissingRateThreshold,
        missing_identity_min_rows_threshold: identityMissingMinRows,
        sampled_missing_schedule_event_ids: (scheduleIdentityHealth.sampled_missing_schedule_event_ids || []).slice(0, 5),
      },
    });

    return {
      rows,
      summary: blockedSummary,
      matchedCount: 0,
      rejectedCount,
      diagnosticRecordsWritten,
      unmatchedCount: rejectedCount,
      unmatched,
      canonicalizationExamples,
    };
  }

  const primary = oddsEvents.map((odds) => matchSingleOddsEvent_(
    odds,
    scheduleEvents,
    toleranceMin,
    aliasMap,
    canonicalizationExamples,
    { is_fallback_attempt: false, fallback_hard_max_delta_min: fallbackHardMaxDeltaMin }
  ));
  const unmatchedPrimary = primary.filter((res) => !res.matched);

  if (unmatchedPrimary.length === 0) reasonCounts.fallback_short_circuit = (reasonCounts.fallback_short_circuit || 0) + 1;

  const finalResults = unmatchedPrimary.length === 0
    ? primary
    : primary.map((res) => {
      if (res.matched) return res;
      const fallback = matchSingleOddsEvent_(
        res.odds,
        scheduleEvents,
        toleranceMin + fallbackMin,
        aliasMap,
        canonicalizationExamples,
        { is_fallback_attempt: true, fallback_hard_max_delta_min: fallbackHardMaxDeltaMin }
      );
      if (fallback.matched) {
        fallback.match_type = 'fallback_match';
        return fallback;
      }
      fallback.primary_time_delta_min = res.best_time_delta_min;
      fallback.fallback_time_delta_min = fallback.best_time_delta_min;
      fallback.nearest_schedule_candidate = fallback.nearest_schedule_candidate || res.nearest_schedule_candidate || null;
      fallback.rejection_code = fallback.rejection_code === 'outside_time_tolerance' ? 'fallback_exhausted' : fallback.rejection_code;
      return fallback;
    });

  finalResults.forEach((result) => {
    const rejectionDiagnostics = buildMatchRejectionDiagnostics_(result);
    if (result.matched) {
      rows.push({
        key: result.odds.event_id,
        odds_event_id: result.odds.event_id,
        schedule_event_id: result.schedule_event_id,
        match_type: result.match_type,
        rejection_code: '',
        time_diff_min: result.time_diff_min,
        competition_tier: result.competition_tier,
        odds_players_raw: '',
        odds_players_normalized: '',
        candidate_players_raw: '',
        candidate_players_normalized: '',
        similarity_scores: '',
        primary_time_delta_min: '',
        fallback_time_delta_min: '',
        rejection_discriminator: '',
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
      odds_players_raw: JSON.stringify(rejectionDiagnostics.odds_players_raw),
      odds_players_normalized: JSON.stringify(rejectionDiagnostics.odds_players_normalized),
      candidate_players_raw: JSON.stringify(rejectionDiagnostics.candidate_players_raw),
      candidate_players_normalized: JSON.stringify(rejectionDiagnostics.candidate_players_normalized),
      similarity_scores: JSON.stringify(rejectionDiagnostics.similarity_scores),
      primary_time_delta_min: rejectionDiagnostics.primary_time_delta_min,
      fallback_time_delta_min: rejectionDiagnostics.fallback_time_delta_min,
      rejection_discriminator: rejectionDiagnostics.rejection_discriminator,
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
      normalized_odds_players: result.normalized_odds_players || [],
      normalized_schedule_players: result.nearest_schedule_candidate ? (result.nearest_schedule_candidate.normalized_players || []) : [],
      nearest_schedule_candidate: result.nearest_schedule_candidate || null,
      primary_time_delta_min: result.primary_time_delta_min,
      fallback_time_delta_min: result.fallback_time_delta_min,
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

function assessSchedulePlayerIdentityHealth_(scheduleEvents) {
  const events = Array.isArray(scheduleEvents) ? scheduleEvents : [];
  let missingIdentityRows = 0;
  const sampledMissingScheduleEventIds = [];

  events.forEach((event, idx) => {
    const hasPlayerOneIdentity = hasSchedulePlayerIdentityValue_(event, 1);
    const hasPlayerTwoIdentity = hasSchedulePlayerIdentityValue_(event, 2);
    if (hasPlayerOneIdentity && hasPlayerTwoIdentity) return;
    missingIdentityRows += 1;
    if (sampledMissingScheduleEventIds.length < 5) {
      sampledMissingScheduleEventIds.push(String((event && event.event_id) || ('schedule_row_' + idx)));
    }
  });

  const totalRows = events.length;
  return {
    total_schedule_rows: totalRows,
    missing_identity_rows: missingIdentityRows,
    missing_identity_rate: totalRows > 0 ? Number((missingIdentityRows / totalRows).toFixed(4)) : 0,
    sampled_missing_schedule_event_ids: sampledMissingScheduleEventIds,
  };
}

function hasSchedulePlayerIdentityValue_(event, playerIndex) {
  if (!event) return false;
  const suffix = String(playerIndex || '');
  const valueCandidates = [
    event['matcher_player_' + suffix + '_canonical'],
    event['matcher_player_' + suffix + '_raw'],
    event['player_' + suffix],
  ];
  for (let i = 0; i < valueCandidates.length; i += 1) {
    if (String(valueCandidates[i] || '').trim()) return true;
  }
  return false;
}

function buildMatchRejectionDiagnostics_(result) {
  const nearest = result.nearest_schedule_candidate || null;
  const oddsPlayer1 = String((result.odds || {}).player_1 || '');
  const oddsPlayer2 = String((result.odds || {}).player_2 || '');
  const normalizedOddsPlayers = Array.isArray(result.normalized_odds_players) ? result.normalized_odds_players.slice(0, 2) : [];
  const oddsKey = normalizedOddsPlayers.join('|');
  const candidateNormalizedPlayers = nearest && Array.isArray(nearest.normalized_players) ? nearest.normalized_players.slice(0, 2) : [];
  const candidateKey = candidateNormalizedPlayers.join('|');
  const playerDistance = Number.isFinite(Number((nearest || {}).player_distance)) ? Number(nearest.player_distance) : '';
  const maxKeyLen = Math.max(oddsKey.length, candidateKey.length);
  const normalizedSimilarityScore = maxKeyLen > 0 && playerDistance !== ''
    ? Number((1 - (playerDistance / maxKeyLen)).toFixed(6))
    : '';
  return {
    odds_players_raw: [oddsPlayer1, oddsPlayer2],
    odds_players_normalized: normalizedOddsPlayers,
    candidate_players_raw: nearest ? [String(nearest.player_1 || ''), String(nearest.player_2 || '')] : [],
    candidate_players_normalized: candidateNormalizedPlayers,
    similarity_scores: {
      player_distance: playerDistance,
      normalized_similarity: normalizedSimilarityScore,
      initial_key_match: nearest ? !!nearest.initial_key_match : false,
    },
    primary_time_delta_min: result.primary_time_delta_min === '' ? '' : Number(result.primary_time_delta_min || 0),
    fallback_time_delta_min: result.fallback_time_delta_min === '' ? '' : Number(result.fallback_time_delta_min || 0),
    rejection_discriminator: String(result.rejection_code || ''),
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

function matchSingleOddsEvent_(odds, scheduleEvents, maxToleranceMin, aliasMap, canonicalizationExamples, options) {
  const opts = options || {};
  const isFallbackAttempt = !!opts.is_fallback_attempt;
  const fallbackHardMaxDeltaMin = Number(opts.fallback_hard_max_delta_min);
  const enforceFallbackHardCeiling = isFallbackAttempt && Number.isFinite(fallbackHardMaxDeltaMin) && fallbackHardMaxDeltaMin >= 0;
  const oddsPlayersPair = normalizePlayerPair_(odds.player_1, odds.player_2, aliasMap);
  const oddsPlayers = oddsPlayersPair.key;
  const oddsInitialKey = oddsPlayersPair.initial_key;
  canonicalizationExamples.push({
    sample_type: 'odds',
    raw_players: [odds.player_1, odds.player_2],
    canonical_players: oddsPlayers,
  });

  const samePlayers = [];
  const samePlayersByInitial = [];
  const nearestScheduleCandidate = buildNearestScheduleCandidate_(odds, scheduleEvents, oddsPlayersPair, aliasMap);
  scheduleEvents.forEach((sched) => {
    const schedPlayersPair = normalizePlayerPair_(sched.player_1, sched.player_2, aliasMap);
    const schedPlayers = schedPlayersPair.key;
    if (canonicalizationExamples.length < 25) {
      canonicalizationExamples.push({
        sample_type: 'schedule',
        raw_players: [sched.player_1, sched.player_2],
        canonical_players: schedPlayers,
      });
    }
    if (oddsPlayers === schedPlayers) samePlayers.push(sched);
    if (oddsInitialKey && schedPlayersPair.initial_key && oddsInitialKey === schedPlayersPair.initial_key) samePlayersByInitial.push(sched);
  });

  const candidates = samePlayers.length ? samePlayers : samePlayersByInitial;
  if (!candidates.length) {
    const exceededDayWindow = enforceFallbackHardCeiling
      && nearestScheduleCandidate
      && Number(nearestScheduleCandidate.time_delta_min || 0) > fallbackHardMaxDeltaMin;
    return {
      odds,
      matched: false,
      rejection_code: exceededDayWindow ? 'candidate_out_of_day_window' : 'no_player_match',
      normalized_odds_players: oddsPlayersPair.players,
      nearest_schedule_candidate: nearestScheduleCandidate,
      best_time_delta_min: nearestScheduleCandidate ? nearestScheduleCandidate.time_delta_min : '',
      primary_time_delta_min: '',
      fallback_time_delta_min: '',
    };
  }

  const inTolerance = candidates
    .map((sched) => ({
      sched,
      diffMin: Math.abs(odds.commence_time.getTime() - sched.start_time.getTime()) / 60000,
    }))
    .filter((candidate) => candidate.diffMin <= maxToleranceMin)
    .sort((a, b) => a.diffMin - b.diffMin);

  const bestTimeDeltaMin = candidates.length
    ? candidates.map((sched) => Math.abs(odds.commence_time.getTime() - sched.start_time.getTime()) / 60000).sort((a, b) => a - b)[0]
    : '';
  if (enforceFallbackHardCeiling && Number.isFinite(bestTimeDeltaMin) && bestTimeDeltaMin > fallbackHardMaxDeltaMin) {
    return {
      odds,
      matched: false,
      rejection_code: 'candidate_out_of_day_window',
      normalized_odds_players: oddsPlayersPair.players,
      nearest_schedule_candidate: nearestScheduleCandidate,
      best_time_delta_min: Number.isFinite(bestTimeDeltaMin) ? Math.round(bestTimeDeltaMin) : '',
      primary_time_delta_min: '',
      fallback_time_delta_min: '',
    };
  }
  if (!inTolerance.length) {
    return {
      odds,
      matched: false,
      rejection_code: 'outside_time_tolerance',
      normalized_odds_players: oddsPlayersPair.players,
      nearest_schedule_candidate: nearestScheduleCandidate,
      best_time_delta_min: Number.isFinite(bestTimeDeltaMin) ? Math.round(bestTimeDeltaMin) : '',
      primary_time_delta_min: '',
      fallback_time_delta_min: '',
    };
  }
  if (inTolerance.length > 1 && inTolerance[0].diffMin === inTolerance[1].diffMin) {
    return {
      odds,
      matched: false,
      rejection_code: 'ambiguous_candidate',
      normalized_odds_players: oddsPlayersPair.players,
      nearest_schedule_candidate: nearestScheduleCandidate,
      best_time_delta_min: Number.isFinite(bestTimeDeltaMin) ? Math.round(bestTimeDeltaMin) : '',
      primary_time_delta_min: '',
      fallback_time_delta_min: '',
    };
  }

  const winner = inTolerance[0];
  return {
    odds,
    matched: true,
    match_type: 'primary_match',
    schedule_event_id: winner.sched.event_id,
    competition_tier: winner.sched.canonical_tier,
    time_diff_min: Math.round(winner.diffMin),
    normalized_odds_players: oddsPlayersPair.players,
    nearest_schedule_candidate: nearestScheduleCandidate,
    best_time_delta_min: Number.isFinite(bestTimeDeltaMin) ? Math.round(bestTimeDeltaMin) : '',
    primary_time_delta_min: '',
    fallback_time_delta_min: '',
  };
}

function normalizePlayers_(a, b, aliasMap) {
  return normalizePlayerPair_(a, b, aliasMap).key;
}

function normalizePlayerPair_(a, b, aliasMap) {
  const players = [canonicalizePlayerName_(a, aliasMap), canonicalizePlayerName_(b, aliasMap)].sort();
  return {
    players: players,
    key: players.join('|'),
    initial_key: players.map(buildInitialSurnameKey_).sort().join('|'),
  };
}

function buildInitialSurnameKey_(canonicalName) {
  const tokens = String(canonicalName || '').trim().split(' ').filter(function (token) { return token; });
  if (!tokens.length) return '';
  const surname = tokens[tokens.length - 1];
  const first = tokens[0] || '';
  const initial = first ? first.charAt(0) : '';
  const primary = (initial + ' ' + surname).trim();
  if (tokens.length !== 2) return primary;
  const reverseInitial = surname ? surname.charAt(0) : '';
  const reverseSurname = first;
  const reverse = (reverseInitial + ' ' + reverseSurname).trim();
  if (!reverse || reverse === primary) return primary;
  return [primary, reverse].sort().join('||');
}

function buildNearestScheduleCandidate_(odds, scheduleEvents, oddsPlayersPair, aliasMap) {
  if (!scheduleEvents || !scheduleEvents.length) return null;
  const scored = scheduleEvents.map(function (sched) {
    const schedPlayersPair = normalizePlayerPair_(sched.player_1, sched.player_2, aliasMap);
    const playerDistance = computePairKeyDistance_(oddsPlayersPair.key, schedPlayersPair.key);
    const timeDeltaMin = Math.abs(odds.commence_time.getTime() - sched.start_time.getTime()) / 60000;
    const sameInitial = oddsPlayersPair.initial_key && oddsPlayersPair.initial_key === schedPlayersPair.initial_key;
    return {
      sched: sched,
      normalizedPlayers: schedPlayersPair.players,
      playerDistance: playerDistance,
      timeDeltaMin: timeDeltaMin,
      sameInitial: sameInitial,
    };
  });
  scored.sort(function (a, b) {
    if (a.playerDistance !== b.playerDistance) return a.playerDistance - b.playerDistance;
    if (a.sameInitial !== b.sameInitial) return a.sameInitial ? -1 : 1;
    if (a.timeDeltaMin !== b.timeDeltaMin) return a.timeDeltaMin - b.timeDeltaMin;
    return String((a.sched || {}).event_id || '').localeCompare(String((b.sched || {}).event_id || ''));
  });
  const winner = scored[0];
  return {
    event_id: winner.sched.event_id || '',
    player_1: winner.sched.player_1 || '',
    player_2: winner.sched.player_2 || '',
    normalized_players: winner.normalizedPlayers,
    start_time: winner.sched.start_time && winner.sched.start_time.toISOString ? winner.sched.start_time.toISOString() : '',
    player_distance: winner.playerDistance,
    time_delta_min: Math.round(winner.timeDeltaMin),
    initial_key_match: winner.sameInitial,
  };
}

function computePairKeyDistance_(a, b) {
  const source = String(a || '');
  const target = String(b || '');
  if (source === target) return 0;
  if (!source || !target) return Math.max(source.length, target.length);
  const rows = source.length + 1;
  const cols = target.length + 1;
  const dp = [];
  for (let i = 0; i < rows; i += 1) {
    dp[i] = [];
    dp[i][0] = i;
  }
  for (let j = 0; j < cols; j += 1) dp[0][j] = j;
  for (let i = 1; i < rows; i += 1) {
    for (let j = 1; j < cols; j += 1) {
      const cost = source.charAt(i - 1) === target.charAt(j - 1) ? 0 : 1;
      dp[i][j] = Math.min(
        dp[i - 1][j] + 1,
        dp[i][j - 1] + 1,
        dp[i - 1][j - 1] + cost
      );
    }
  }
  return dp[source.length][target.length];
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

  const knownGivenNames = {
    alexandra: true, alina: true, alycia: true, amanda: true, anastasia: true, anna: true, anhelina: true, aryna: true,
    barbora: true, beatriz: true, belinda: true, caroline: true, clara: true, coco: true, danka: true, daria: true,
    dayana: true, diana: true, ekaterina: true, elena: true, elina: true, elisabetta: true, elise: true, emma: true,
    eva: true, iga: true, irina: true, jasmine: true, jelena: true, jessica: true, julia: true, karolina: true,
    katarina: true, katerina: true, katie: true, klara: true, leylah: true, linda: true, liudmila: true, lucia: true,
    ludmilla: true, lourdes: true, magda: true, magdalena: true, maria: true, marie: true, marta: true, mayar: true, mirra: true,
    naomi: true, olga: true, ons: true, paula: true, petra: true, qinwen: true, sara: true, simona: true,
    sloane: true, sofia: true, sonay: true, sorana: true, svetlana: true, tatjana: true, veronika: true, victoria: true,
    yulia: true, zheng: true,
  };

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
    'j paolini': 'jasmine paolini',
    'paolini j': 'jasmine paolini',
    'm keys': 'madison keys',
    'keys m': 'madison keys',
    'b haddad maia': 'beatriz haddad maia',
    'haddad maia b': 'beatriz haddad maia',
    'e alexandrova': 'ekaterina alexandrova',
    'alexandrova e': 'ekaterina alexandrova',
    'k pliskova': 'karolina pliskova',
    'pliskova k': 'karolina pliskova',
    'v kudermetova': 'veronika kudermetova',
    'k rakhimova': 'kamilla rakhimova',
    'd yastremska': 'dayana yastremska',
    'q zheng': 'qinwen zheng',
    'kalininskaya anna': 'anna kalinskaya',
    'anna kalininskaya': 'anna kalinskaya',
  };
  if (aliasRules[value]) return aliasRules[value];

  const tokens = value.split(' ').filter(function (token) { return token; });
  if (tokens.length === 2) {
    const first = tokens[0];
    const second = tokens[1];
    const firstLooksGiven = knownGivenNames[first] === true || first.length === 1;
    const secondLooksGiven = knownGivenNames[second] === true || second.length === 1;
    if (!firstLooksGiven && secondLooksGiven) return second + ' ' + first;
  }

  const surnameParticles = ['de', 'del', 'della', 'da', 'di', 'van', 'von', 'la', 'le', 'st', 'saint'];
  if (tokens.length >= 3) {
    for (let i = 1; i < tokens.length - 1; i += 1) {
      if (surnameParticles.indexOf(tokens[i]) >= 0) {
        const merged = tokens.slice(0, i).concat([tokens.slice(i).join(' ')]).join(' ').trim();
        if (merged) return merged;
      }
    }
    const firstToken = tokens[0];
    const lastToken = tokens[tokens.length - 1];
    const firstLooksGiven = knownGivenNames[firstToken] === true || firstToken.length === 1;
    const lastLooksGiven = knownGivenNames[lastToken] === true || lastToken.length === 1;
    if (!firstLooksGiven && lastLooksGiven) {
      return [lastToken].concat(tokens.slice(0, tokens.length - 1)).join(' ');
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
