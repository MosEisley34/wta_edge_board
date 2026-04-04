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
  const scheduleDateAlignment = assessScheduleDateAlignmentWithOdds_(oddsEvents, scheduleEvents, config);

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

  if (scheduleDateAlignment.blocked) {
    (oddsEvents || []).forEach((odds) => {
      const eventId = String((odds && odds.event_id) || '');
      rows.push({
        key: ['schedule_date_misaligned_with_odds', eventId].join('|'),
        odds_event_id: eventId,
        schedule_event_id: '',
        match_type: '',
        rejection_code: 'schedule_date_misaligned_with_odds',
        time_diff_min: '',
        competition_tier: '',
        odds_players_raw: JSON.stringify([String((odds && odds.player_1) || ''), String((odds && odds.player_2) || '')]),
        odds_players_normalized: JSON.stringify([]),
        candidate_players_raw: JSON.stringify([]),
        candidate_players_normalized: JSON.stringify([]),
        similarity_scores: JSON.stringify({}),
        primary_time_delta_min: '',
        fallback_time_delta_min: '',
        rejection_discriminator: 'schedule_date_window_precheck_blocked',
        updated_at: new Date().toISOString(),
      });
      unmatched.push({
        odds_event_id: eventId,
        competition: String((odds && odds.competition) || ''),
        player_1: String((odds && odds.player_1) || ''),
        player_2: String((odds && odds.player_2) || ''),
        commence_time: odds && odds.commence_time && typeof odds.commence_time.toISOString === 'function' ? odds.commence_time.toISOString() : '',
        rejection_code: 'schedule_date_misaligned_with_odds',
        normalized_odds_players: [],
        normalized_schedule_players: [],
        nearest_schedule_candidate: null,
        primary_time_delta_min: '',
        fallback_time_delta_min: '',
      });
    });

    reasonCounts.schedule_date_misaligned_with_odds = Number((oddsEvents || []).length || 0);
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
        matcher_precheck_reason: 'schedule_date_misaligned_with_odds',
        schedule_date_alignment: scheduleDateAlignment.metadata,
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
    {
      is_fallback_attempt: false,
      fallback_hard_max_delta_min: fallbackHardMaxDeltaMin,
      nearest_candidate_min_similarity: Number.isFinite(Number(config.MATCH_NEAREST_CANDIDATE_MIN_SIMILARITY))
        ? Number(config.MATCH_NEAREST_CANDIDATE_MIN_SIMILARITY)
        : Number(DEFAULT_CONFIG.MATCH_NEAREST_CANDIDATE_MIN_SIMILARITY || 0.55),
    }
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
        {
          is_fallback_attempt: true,
          fallback_hard_max_delta_min: fallbackHardMaxDeltaMin,
          nearest_candidate_min_similarity: Number.isFinite(Number(config.MATCH_NEAREST_CANDIDATE_MIN_SIMILARITY))
            ? Number(config.MATCH_NEAREST_CANDIDATE_MIN_SIMILARITY)
            : Number(DEFAULT_CONFIG.MATCH_NEAREST_CANDIDATE_MIN_SIMILARITY || 0.55),
        }
      );
      if (fallback.matched) {
        fallback.match_type = 'fallback_match';
        return fallback;
      }
      fallback.primary_time_delta_min = res.best_time_delta_min;
      fallback.fallback_time_delta_min = fallback.best_time_delta_min;
      fallback.nearest_schedule_candidate = fallback.nearest_schedule_candidate || res.nearest_schedule_candidate || null;
      fallback.nearest_schedule_candidate_diagnostics = fallback.nearest_schedule_candidate_diagnostics
        || res.nearest_schedule_candidate_diagnostics
        || null;
      fallback.rejection_code = fallback.rejection_code === 'outside_time_tolerance' ? 'fallback_exhausted' : fallback.rejection_code;
      return fallback;
    });
  const softMatchEnabled = String(config.MATCH_SOFT_MATCH_ENABLED || 'true').toLowerCase() !== 'false';
  const softMatchedResults = softMatchEnabled
    ? finalResults.map(function (result) {
      if (result.matched) return result;
      const softMatch = trySoftMatchForResult_(result, scheduleEvents, aliasMap, config);
      if (!softMatch) return result;
      return softMatch;
    })
    : finalResults;
  if (softMatchEnabled) {
    softMatchedResults.forEach(function (result, idx) {
      if (!result || !result.matched || result.match_type !== 'soft_match') return;
      if (finalResults[idx] && !finalResults[idx].matched) {
        reasonCounts.soft_match_recovered = (reasonCounts.soft_match_recovered || 0) + 1;
      }
    });
  }

  const nearestPromotionEnabled = String(config.MATCH_NEAREST_PROMOTION_ENABLED || 'true').toLowerCase() !== 'false';
  const nearestPromotedResults = nearestPromotionEnabled
    ? softMatchedResults.map(function (result) {
      if (!result || result.matched) return result;
      const promoted = tryPromoteNearestScheduleCandidate_(result, scheduleEvents, aliasMap, config);
      return promoted || result;
    })
    : softMatchedResults;

  nearestPromotedResults.forEach((result) => {
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
      nearest_schedule_candidate_diagnostics: result.nearest_schedule_candidate_diagnostics || null,
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
    reason_metadata: buildUnresolvedDriftDiagnostics_(unmatched),
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

function assessScheduleDateAlignmentWithOdds_(oddsEvents, scheduleEvents, config) {
  const odds = Array.isArray(oddsEvents) ? oddsEvents : [];
  const schedule = Array.isArray(scheduleEvents) ? scheduleEvents : [];
  if (!odds.length || !schedule.length) return { blocked: false, metadata: { skipped: true, reason: 'missing_inputs' } };

  const alignmentBufferMin = Math.max(0, Number((config && config.MATCH_SCHEDULE_DATE_ALIGNMENT_BUFFER_MIN) || 720));
  const alignmentBufferMs = alignmentBufferMin * 60000;

  const oddsTimes = odds
    .map(function (event) { return event && event.commence_time; })
    .filter(function (value) { return value instanceof Date && !Number.isNaN(value.getTime()); })
    .map(function (value) { return value.getTime(); });
  const scheduleTimes = schedule
    .map(function (event) { return event && event.start_time; })
    .filter(function (value) { return value instanceof Date && !Number.isNaN(value.getTime()); })
    .map(function (value) { return value.getTime(); });

  if (!oddsTimes.length || !scheduleTimes.length) {
    return {
      blocked: false,
      metadata: {
        skipped: true,
        reason: 'missing_valid_dates',
        valid_odds_timestamps: oddsTimes.length,
        valid_schedule_timestamps: scheduleTimes.length,
      },
    };
  }

  const oddsMinMs = Math.min.apply(null, oddsTimes);
  const oddsMaxMs = Math.max.apply(null, oddsTimes);
  const scheduleMinMs = Math.min.apply(null, scheduleTimes);
  const scheduleMaxMs = Math.max.apply(null, scheduleTimes);
  const blocked = (scheduleMaxMs + alignmentBufferMs) < oddsMinMs || (oddsMaxMs + alignmentBufferMs) < scheduleMinMs;

  return {
    blocked: blocked,
    metadata: {
      alignment_buffer_min: alignmentBufferMin,
      odds_range_start: new Date(oddsMinMs).toISOString(),
      odds_range_end: new Date(oddsMaxMs).toISOString(),
      schedule_range_start: new Date(scheduleMinMs).toISOString(),
      schedule_range_end: new Date(scheduleMaxMs).toISOString(),
      ranges_overlap_with_buffer: !blocked,
    },
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
  const nearestDiagnostics = result.nearest_schedule_candidate_diagnostics || null;
  const oddsPlayer1 = String((result.odds || {}).player_1 || '');
  const oddsPlayer2 = String((result.odds || {}).player_2 || '');
  const normalizedOddsPlayers = Array.isArray(result.normalized_odds_players) ? result.normalized_odds_players.slice(0, 2) : [];
  const oddsKey = normalizedOddsPlayers.join('|');
  const candidateNormalizedPlayers = nearest && Array.isArray(nearest.normalized_players)
    ? nearest.normalized_players.slice(0, 2)
    : (nearestDiagnostics && Array.isArray(nearestDiagnostics.normalized_schedule_players)
      ? nearestDiagnostics.normalized_schedule_players.slice(0, 2)
      : []);
  const candidateKey = candidateNormalizedPlayers.join('|');
  const playerDistance = Number.isFinite(Number((nearest || {}).player_distance)) ? Number(nearest.player_distance) : '';
  const normalizedSimilarityScore = Number.isFinite(Number((nearest || {}).similarity_score))
    ? Number(nearest.similarity_score)
    : (Number.isFinite(Number((nearestDiagnostics || {}).similarity_score))
      ? Number(nearestDiagnostics.similarity_score)
      : (function () {
        const maxKeyLen = Math.max(oddsKey.length, candidateKey.length);
        return maxKeyLen > 0 && playerDistance !== ''
          ? Number((1 - (playerDistance / maxKeyLen)).toFixed(6))
          : '';
      })());
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
    nearest_candidate_diagnostics: nearestDiagnostics || {},
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
  const nearestCandidateMinSimilarity = Number.isFinite(Number(opts.nearest_candidate_min_similarity))
    ? Math.max(0, Math.min(1, Number(opts.nearest_candidate_min_similarity)))
    : 0;
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
  const nearestScheduleAssessment = buildNearestScheduleCandidateAssessment_(
    odds,
    scheduleEvents,
    oddsPlayersPair,
    aliasMap,
    nearestCandidateMinSimilarity
  );
  const nearestScheduleCandidate = nearestScheduleAssessment.candidate;
  const nearestScheduleDiagnostics = nearestScheduleAssessment.diagnostics;
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
      nearest_schedule_candidate_diagnostics: nearestScheduleDiagnostics,
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
      nearest_schedule_candidate_diagnostics: nearestScheduleDiagnostics,
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
      nearest_schedule_candidate_diagnostics: nearestScheduleDiagnostics,
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
      nearest_schedule_candidate_diagnostics: nearestScheduleDiagnostics,
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
    nearest_schedule_candidate_diagnostics: nearestScheduleDiagnostics,
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

function buildNearestScheduleCandidateAssessment_(odds, scheduleEvents, oddsPlayersPair, aliasMap, minSimilarityThreshold) {
  if (!scheduleEvents || !scheduleEvents.length) return { candidate: null, diagnostics: null };
  const scored = scheduleEvents.map(function (sched) {
    const schedPlayersPair = normalizePlayerPair_(sched.player_1, sched.player_2, aliasMap);
    const playerDistance = computePairKeyDistance_(oddsPlayersPair.key, schedPlayersPair.key);
    const timeDeltaMin = Math.abs(odds.commence_time.getTime() - sched.start_time.getTime()) / 60000;
    const sameInitial = oddsPlayersPair.initial_key && oddsPlayersPair.initial_key === schedPlayersPair.initial_key;
    const maxKeyLen = Math.max(String(oddsPlayersPair.key || '').length, String(schedPlayersPair.key || '').length);
    const normalizedSimilarity = maxKeyLen > 0 ? Number((1 - (playerDistance / maxKeyLen)).toFixed(6)) : 0;
    return {
      sched: sched,
      normalizedPlayers: schedPlayersPair.players,
      playerDistance: playerDistance,
      timeDeltaMin: timeDeltaMin,
      sameInitial: sameInitial,
      normalizedSimilarity: normalizedSimilarity,
    };
  });
  scored.sort(function (a, b) {
    if (a.playerDistance !== b.playerDistance) return a.playerDistance - b.playerDistance;
    if (a.sameInitial !== b.sameInitial) return a.sameInitial ? -1 : 1;
    if (a.timeDeltaMin !== b.timeDeltaMin) return a.timeDeltaMin - b.timeDeltaMin;
    return String((a.sched || {}).event_id || '').localeCompare(String((b.sched || {}).event_id || ''));
  });
  const winner = scored[0];
  const candidate = {
    event_id: winner.sched.event_id || '',
    player_1: winner.sched.player_1 || '',
    player_2: winner.sched.player_2 || '',
    normalized_players: winner.normalizedPlayers,
    start_time: winner.sched.start_time && winner.sched.start_time.toISOString ? winner.sched.start_time.toISOString() : '',
    player_distance: winner.playerDistance,
    similarity_score: winner.normalizedSimilarity,
    time_delta_min: Math.round(winner.timeDeltaMin),
    initial_key_match: winner.sameInitial,
  };
  const similarityThreshold = Number.isFinite(Number(minSimilarityThreshold)) ? Number(minSimilarityThreshold) : 0;
  const isViable = winner.normalizedSimilarity >= similarityThreshold;
  const diagnostics = {
    viability: isViable ? 'accepted' : 'rejected_similarity_threshold',
    normalized_odds_players: Array.isArray(oddsPlayersPair.players) ? oddsPlayersPair.players.slice(0, 2) : [],
    normalized_schedule_players: winner.normalizedPlayers.slice(0, 2),
    similarity_score: winner.normalizedSimilarity,
    similarity_threshold: similarityThreshold,
    time_delta_min: Math.round(winner.timeDeltaMin),
    candidate_event_id: String((winner.sched || {}).event_id || ''),
    candidate_competition_key: normalizeCompetitionForMatchJoin_(extractCompetitionForMatchJoin_(winner.sched)),
    best_candidate: candidate,
  };
  return { candidate: isViable ? candidate : null, diagnostics: diagnostics };
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
  const originalValue = String(normalized || '').trim();
  if (!originalValue) return '';

  const givenNameVariantMap = {
    yuliia: 'yulia',
    iuliia: 'yulia',
    juliia: 'yulia',
  };
  const canonicalizedGivenNameValue = applyGivenNameVariantAliases_(originalValue, givenNameVariantMap);
  const value = canonicalizedGivenNameValue || originalValue;

  const knownGivenNames = {
    alexandra: true, alina: true, alycia: true, amanda: true, anastasia: true, anna: true, anhelina: true, aryna: true,
    barbora: true, beatriz: true, belinda: true, caroline: true, clara: true, coco: true, danka: true, daria: true,
    dayana: true, diana: true, ekaterina: true, elena: true, elina: true, elisabetta: true, elise: true, emma: true,
    eva: true, iga: true, irina: true, jasmine: true, jelena: true, jessica: true, julia: true, karolina: true,
    katarina: true, katerina: true, katie: true, klara: true, leylah: true, linda: true, liudmila: true, lucia: true,
    ludmilla: true, lourdes: true, magda: true, magdalena: true, maria: true, marie: true, marta: true, mayar: true, mirra: true,
    naomi: true, olga: true, ons: true, paula: true, petra: true, qinwen: true, renata: true, sara: true, simona: true,
    sloane: true, sofia: true, sonay: true, sorana: true, svetlana: true, tatjana: true, veronika: true, victoria: true,
    yulia: true, zheng: true, hailey: true, akasha: true,
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
    'jelena ostapenko': 'jelena ostapenko',
    'ostapenko jelena': 'jelena ostapenko',
    'daria kasatkina': 'daria kasatkina',
    'kasatkina daria': 'daria kasatkina',
    'ons jabeur': 'ons jabeur',
    'jabeur ons': 'ons jabeur',
    'madison keys': 'madison keys',
    'keys madison': 'madison keys',
    'beatriz haddad maia': 'beatriz haddad maia',
    'haddad maia beatriz': 'beatriz haddad maia',
    'qinwen zheng': 'qinwen zheng',
    'zheng qinwen': 'qinwen zheng',
    's bejlek': 'sara bejlek',
    'bejlek s': 'sara bejlek',
    'sara bejlek': 'sara bejlek',
    'a urhobo': 'akasha urhobo',
    'urhobo a': 'akasha urhobo',
    'akasha urhobo': 'akasha urhobo',
    'h baptiste': 'hailey baptiste',
    'baptiste h': 'hailey baptiste',
    'hailey baptiste': 'hailey baptiste',
    'r zarazua': 'renata zarazua',
    'zarazua r': 'renata zarazua',
    'renata zarazua': 'renata zarazua',
  };
  if (aliasRules[value]) return aliasRules[value];

  const tokens = value.split(' ').filter(function (token) { return token; });
  if (tokens.length === 2) {
    const first = tokens[0];
    const second = tokens[1];
    const firstLooksGiven = knownGivenNames[first] === true || first.length === 1;
    const secondLooksGiven = knownGivenNames[second] === true || second.length === 1;
    if (!firstLooksGiven && secondLooksGiven) {
      const reordered = (second + ' ' + first).trim();
      if (aliasRules[reordered]) return aliasRules[reordered];
      return reordered;
    }
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
      const reordered = [lastToken].concat(tokens.slice(0, tokens.length - 1)).join(' ');
      if (aliasRules[reordered]) return aliasRules[reordered];
      return reordered;
    }
  }

  if (/^[a-z]\s+[a-z]+(?:\s+[a-z]+)*$/.test(value)) {
    const initial = value.split(' ')[0];
    const rest = value.split(' ').slice(1).join(' ');
    if (aliasRules[initial + ' ' + rest]) return aliasRules[initial + ' ' + rest];
    const expanded = expandPlayerInitialAlias_(initial, rest);
    if (expanded) return expanded;
  }

  if (/^[a-z]+(?:\s+[a-z]+)*\s+[a-z]$/.test(value)) {
    const pieces = value.split(' ');
    const trailingInitial = pieces[pieces.length - 1];
    const leading = pieces.slice(0, pieces.length - 1).join(' ');
    if (aliasRules[leading + ' ' + trailingInitial]) return aliasRules[leading + ' ' + trailingInitial];
    const expanded = expandPlayerInitialAlias_(trailingInitial, leading);
    if (expanded) return expanded;
  }

  return value;
}

function applyGivenNameVariantAliases_(value, variantMap) {
  const map = variantMap || {};
  const tokens = String(value || '').trim().split(' ').filter(function (token) { return token; });
  if (!tokens.length) return '';
  let changed = false;
  const rewritten = tokens.map(function (token) {
    const replacement = map[token];
    if (!replacement) return token;
    changed = true;
    return replacement;
  });
  if (!changed) return value;
  return rewritten.join(' ').trim();
}

function expandPlayerInitialAlias_(initial, surnameExpression) {
  const key = (String(initial || '').trim().charAt(0) + ' ' + String(surnameExpression || '').trim()).trim();
  const surnameInitialMap = {
    'i swiatek': 'iga swiatek',
    'e rybakina': 'elena rybakina',
    'm kostyuk': 'marta kostyuk',
    's kartal': 'sonay kartal',
    'j paolini': 'jasmine paolini',
    'm keys': 'madison keys',
    'b haddad maia': 'beatriz haddad maia',
    'e alexandrova': 'ekaterina alexandrova',
    'k pliskova': 'karolina pliskova',
    'v kudermetova': 'veronika kudermetova',
    'k rakhimova': 'kamilla rakhimova',
    'd yastremska': 'dayana yastremska',
    'q zheng': 'qinwen zheng',
    'o jabeur': 'ons jabeur',
    'd kasatkina': 'daria kasatkina',
    's bejlek': 'sara bejlek',
    'a urhobo': 'akasha urhobo',
    'h baptiste': 'hailey baptiste',
    'r zarazua': 'renata zarazua',
  };
  return surnameInitialMap[key] || '';
}

function trySoftMatchForResult_(result, scheduleEvents, aliasMap, config) {
  if (!result || !result.odds || !Array.isArray(scheduleEvents) || !scheduleEvents.length) return null;
  const odds = result.odds;
  const oddsPlayersPair = normalizePlayerPair_(odds.player_1, odds.player_2, aliasMap);
  const oddsCompetitionKey = normalizeCompetitionForMatchJoin_(extractCompetitionForMatchJoin_(odds));
  const softMaxDeltaMin = Math.max(0, Number(config.MATCH_SOFT_MATCH_MAX_DELTA_MIN || 90));
  const softSimilarityThreshold = Math.max(0, Math.min(1, Number(config.MATCH_SOFT_MATCH_MIN_SIMILARITY || 0.9)));
  const softAmbiguousWindowMin = Math.max(1, Number(config.MATCH_SOFT_MATCH_AMBIGUOUS_WINDOW_MIN || 15));
  const softSimilarityMargin = Math.max(0.001, Number(config.MATCH_SOFT_MATCH_SIMILARITY_MARGIN || 0.015));

  const candidates = scheduleEvents.map(function (sched) {
    const scheduleCompetitionKey = normalizeCompetitionForMatchJoin_(extractCompetitionForMatchJoin_(sched));
    if (!oddsCompetitionKey || !scheduleCompetitionKey || oddsCompetitionKey !== scheduleCompetitionKey) return null;
    if (!(odds.commence_time instanceof Date) || !(sched.start_time instanceof Date)) return null;
    const timeDeltaMin = Math.abs(odds.commence_time.getTime() - sched.start_time.getTime()) / 60000;
    if (timeDeltaMin > softMaxDeltaMin) return null;
    const schedPlayersPair = normalizePlayerPair_(sched.player_1, sched.player_2, aliasMap);
    const playerDistance = computePairKeyDistance_(oddsPlayersPair.key, schedPlayersPair.key);
    const maxKeyLen = Math.max(String(oddsPlayersPair.key || '').length, String(schedPlayersPair.key || '').length);
    const similarity = maxKeyLen > 0 ? Number((1 - (playerDistance / maxKeyLen)).toFixed(6)) : 0;
    if (similarity < softSimilarityThreshold) return null;
    return {
      sched: sched,
      similarity: similarity,
      timeDeltaMin: timeDeltaMin,
      playerDistance: playerDistance,
      normalizedPlayers: schedPlayersPair.players,
    };
  }).filter(function (entry) { return !!entry; });

  if (!candidates.length) return null;
  candidates.sort(function (a, b) {
    if (a.similarity !== b.similarity) return b.similarity - a.similarity;
    if (a.timeDeltaMin !== b.timeDeltaMin) return a.timeDeltaMin - b.timeDeltaMin;
    return String((a.sched || {}).event_id || '').localeCompare(String((b.sched || {}).event_id || ''));
  });
  const best = candidates[0];
  const second = candidates[1];
  if (second) {
    const similarBand = (best.similarity - second.similarity) <= softSimilarityMargin;
    const closeTimeBand = Math.abs(best.timeDeltaMin - second.timeDeltaMin) <= softAmbiguousWindowMin;
    if (similarBand && closeTimeBand) return null;
  }

  return {
    odds: odds,
    matched: true,
    match_type: 'soft_match',
    schedule_event_id: best.sched.event_id,
    competition_tier: best.sched.canonical_tier || '',
    time_diff_min: Math.round(best.timeDeltaMin),
    normalized_odds_players: oddsPlayersPair.players,
    nearest_schedule_candidate: {
      event_id: best.sched.event_id || '',
      player_1: best.sched.player_1 || '',
      player_2: best.sched.player_2 || '',
      normalized_players: best.normalizedPlayers,
      start_time: best.sched.start_time && best.sched.start_time.toISOString ? best.sched.start_time.toISOString() : '',
      player_distance: best.playerDistance,
      similarity_score: best.similarity,
      time_delta_min: Math.round(best.timeDeltaMin),
      initial_key_match: oddsPlayersPair.initial_key && oddsPlayersPair.initial_key === normalizePlayerPair_(best.sched.player_1, best.sched.player_2, aliasMap).initial_key,
    },
    nearest_schedule_candidate_diagnostics: {
      viability: 'accepted_soft_match',
      normalized_odds_players: oddsPlayersPair.players.slice(0, 2),
      normalized_schedule_players: best.normalizedPlayers.slice(0, 2),
      similarity_score: best.similarity,
      similarity_threshold: softSimilarityThreshold,
      time_delta_min: Math.round(best.timeDeltaMin),
      same_competition_window: true,
    },
    best_time_delta_min: Math.round(best.timeDeltaMin),
    primary_time_delta_min: result.best_time_delta_min,
    fallback_time_delta_min: result.best_time_delta_min,
  };
}

function tryPromoteNearestScheduleCandidate_(result, scheduleEvents, aliasMap, config) {
  if (!result || !result.odds || !Array.isArray(scheduleEvents) || !scheduleEvents.length) return null;
  const odds = result.odds;
  const oddsCompetitionKey = normalizeCompetitionForMatchJoin_(extractCompetitionForMatchJoin_(odds));
  const minSimilarity = Math.max(0, Math.min(1, Number(config.MATCH_NEAREST_PROMOTION_MIN_SIMILARITY || 0.94)));
  const maxDeltaMin = Math.max(0, Number(config.MATCH_NEAREST_PROMOTION_MAX_DELTA_MIN || 75));
  const allowDifferentCompetition = String(config.MATCH_NEAREST_PROMOTION_ALLOW_CROSS_COMPETITION || 'false').toLowerCase() === 'true';
  const diagnostics = result.nearest_schedule_candidate_diagnostics || {};
  const nearest = result.nearest_schedule_candidate || (diagnostics && diagnostics.best_candidate) || null;
  if (!nearest) return null;

  const candidateId = String(nearest.event_id || diagnostics.candidate_event_id || '');
  if (!candidateId) return null;
  const scheduleEvent = scheduleEvents.find(function (entry) {
    return String((entry && entry.event_id) || '') === candidateId;
  });
  if (!scheduleEvent) return null;

  const scheduleCompetitionKey = normalizeCompetitionForMatchJoin_(extractCompetitionForMatchJoin_(scheduleEvent));
  if (!allowDifferentCompetition && oddsCompetitionKey && scheduleCompetitionKey && oddsCompetitionKey !== scheduleCompetitionKey) return null;

  const similarityScore = Number.isFinite(Number(nearest.similarity_score))
    ? Number(nearest.similarity_score)
    : Number(diagnostics.similarity_score || 0);
  const timeDeltaMin = Number.isFinite(Number(nearest.time_delta_min))
    ? Number(nearest.time_delta_min)
    : Number(diagnostics.time_delta_min || 0);
  const initialKeyMatch = !!nearest.initial_key_match;
  if (similarityScore < minSimilarity) return null;
  if (timeDeltaMin > maxDeltaMin) return null;
  if (!initialKeyMatch && similarityScore < Math.max(0.965, minSimilarity + 0.015)) return null;

  const oddsPlayersPair = normalizePlayerPair_(odds.player_1, odds.player_2, aliasMap);
  const schedPlayersPair = normalizePlayerPair_(scheduleEvent.player_1, scheduleEvent.player_2, aliasMap);
  return {
    odds: odds,
    matched: true,
    match_type: 'nearest_candidate_promoted',
    schedule_event_id: scheduleEvent.event_id,
    competition_tier: scheduleEvent.canonical_tier || '',
    time_diff_min: Math.round(timeDeltaMin),
    normalized_odds_players: oddsPlayersPair.players,
    nearest_schedule_candidate: {
      event_id: scheduleEvent.event_id || '',
      player_1: scheduleEvent.player_1 || '',
      player_2: scheduleEvent.player_2 || '',
      normalized_players: schedPlayersPair.players,
      start_time: scheduleEvent.start_time && scheduleEvent.start_time.toISOString ? scheduleEvent.start_time.toISOString() : '',
      player_distance: Number.isFinite(Number(nearest.player_distance)) ? Number(nearest.player_distance) : '',
      similarity_score: similarityScore,
      time_delta_min: Math.round(timeDeltaMin),
      initial_key_match: oddsPlayersPair.initial_key && schedPlayersPair.initial_key && oddsPlayersPair.initial_key === schedPlayersPair.initial_key,
    },
    nearest_schedule_candidate_diagnostics: Object.assign({}, diagnostics, {
      viability: 'accepted_nearest_candidate_promotion',
      promotion_reason: 'high_confidence_nearest_candidate',
      same_competition_window: !oddsCompetitionKey || !scheduleCompetitionKey || oddsCompetitionKey === scheduleCompetitionKey,
      promotion_similarity_threshold: minSimilarity,
      promotion_max_delta_min: maxDeltaMin,
    }),
    best_time_delta_min: Math.round(timeDeltaMin),
    primary_time_delta_min: result.best_time_delta_min,
    fallback_time_delta_min: result.best_time_delta_min,
  };
}

function extractCompetitionForMatchJoin_(event) {
  if (!event) return '';
  const fields = [
    event.competition,
    event.tournament,
    event.tournament_name,
    event.event_name,
    event.league_name,
    event.canonical_tier,
  ];
  for (let i = 0; i < fields.length; i += 1) {
    const value = String(fields[i] || '').trim();
    if (value) return value;
  }
  return '';
}

function normalizeCompetitionForMatchJoin_(value) {
  const normalized = normalizeCompetitionValue_(value)
    .replace(/\bwta(?:\s+tour)?\b/g, 'wta')
    .replace(/\bqatar total ?energies?\b/g, 'doha')
    .replace(/\btotal ?energies?\b/g, 'doha')
    .replace(/\bintl\b/g, 'international')
    .replace(/\bchamps?\b/g, 'championships')
    .replace(/\bopen\b/g, ' ')
    .replace(/\bchampionships?\b/g, ' ')
    .replace(/\bmasters?\b/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  if (!normalized) return '';
  if (normalized === 'wta_500') return 'wta 500';
  const cityAlias = {
    doha: 'doha',
    qatar: 'doha',
    dubai: 'dubai',
    abu: 'abu',
    charleston: 'charleston',
    san: 'san',
    stuttgart: 'stuttgart',
  };
  let cityToken = '';
  Object.keys(cityAlias).some(function (token) {
    if (normalized.indexOf(token) < 0) return false;
    cityToken = cityAlias[token];
    return true;
  });
  if (normalized.indexOf('wta 500') >= 0 || normalized.indexOf('wta500') >= 0 || normalized.indexOf('wta_500') >= 0) {
    return cityToken ? ('wta500 ' + cityToken) : 'wta500';
  }
  return normalized;
}

function buildUnresolvedDriftDiagnostics_(unmatched) {
  const rows = Array.isArray(unmatched) ? unmatched : [];
  const competitionCounts = {};
  const playerPairCounts = {};
  const competitionFailureCounts = {};
  const normalizationFailureTypeCounts = {};
  rows.forEach(function (row) {
    const competitionKey = normalizeCompetitionForMatchJoin_(extractCompetitionForMatchJoin_(row));
    const rejectionCode = String((row && row.rejection_code) || 'unclassified');
    if (competitionKey) {
      if (!competitionCounts[competitionKey]) {
        competitionCounts[competitionKey] = {
          normalized_competition: competitionKey,
          count: 0,
          sample_competition: String((row && row.competition) || ''),
        };
      }
      competitionCounts[competitionKey].count += 1;

      const failureKey = competitionKey + '|' + rejectionCode;
      if (!competitionFailureCounts[failureKey]) {
        competitionFailureCounts[failureKey] = {
          normalized_competition: competitionKey,
          rejection_code: rejectionCode,
          count: 0,
        };
      }
      competitionFailureCounts[failureKey].count += 1;
    }
    const oddsPlayers = Array.isArray(row.normalized_odds_players) ? row.normalized_odds_players : [];
    const key = oddsPlayers.join('|');
    if (key) {
      if (!playerPairCounts[key]) {
        playerPairCounts[key] = {
          normalized_players: oddsPlayers.slice(0, 2),
          count: 0,
          sample_players: [String((row && row.player_1) || ''), String((row && row.player_2) || '')],
        };
      }
      playerPairCounts[key].count += 1;
    }
    classifyNormalizationFailureTypes_(row).forEach(function (type) {
      normalizationFailureTypeCounts[type] = Number(normalizationFailureTypeCounts[type] || 0) + 1;
    });
  });
  const topUnresolvedCompetitions = Object.keys(competitionCounts)
    .map(function (key) { return competitionCounts[key]; })
    .sort(function (a, b) { return b.count - a.count; })
    .slice(0, 5);
  const topUnresolvedPlayers = Object.keys(playerPairCounts)
    .map(function (key) { return playerPairCounts[key]; })
    .sort(function (a, b) { return b.count - a.count; })
    .slice(0, 5);
  const topCompetitionFailureTypes = Object.keys(competitionFailureCounts)
    .map(function (key) { return competitionFailureCounts[key]; })
    .sort(function (a, b) { return b.count - a.count; })
    .slice(0, 10);
  return {
    unresolved_drift_diagnostics_version: 2,
    unresolved_total: rows.length,
    top_unresolved_competition_examples: topUnresolvedCompetitions,
    top_unresolved_player_examples: topUnresolvedPlayers,
    top_unresolved_competition_failure_examples: topCompetitionFailureTypes,
    normalization_failure_type_counts: normalizationFailureTypeCounts,
  };
}

function classifyNormalizationFailureTypes_(row) {
  const entry = row || {};
  const types = {};
  const oddsPlayers = Array.isArray(entry.normalized_odds_players) ? entry.normalized_odds_players : [];
  const nearest = entry.nearest_schedule_candidate || {};
  const nearestDiagnostics = entry.nearest_schedule_candidate_diagnostics || {};
  const similarityScore = Number.isFinite(Number(nearest.similarity_score))
    ? Number(nearest.similarity_score)
    : Number(nearestDiagnostics.similarity_score || 0);
  const similarityThreshold = Number(nearestDiagnostics.similarity_threshold || 0);
  const competitionKey = normalizeCompetitionForMatchJoin_(extractCompetitionForMatchJoin_(entry));
  const candidateCompetitionKey = normalizeCompetitionForMatchJoin_(nearestDiagnostics.candidate_competition_key || '');

  if (!oddsPlayers.length) types.empty_normalized_odds_players = true;
  if (!entry.nearest_schedule_candidate) types.missing_nearest_schedule_candidate = true;
  if (candidateCompetitionKey && competitionKey && candidateCompetitionKey !== competitionKey) {
    types.competition_alias_drift = true;
  }
  if (similarityThreshold > 0 && similarityScore > 0 && similarityScore < similarityThreshold) {
    types.player_alias_similarity_below_threshold = true;
  }
  if ((entry.rejection_code === 'outside_time_tolerance' || entry.rejection_code === 'fallback_exhausted') && similarityScore >= 0.8) {
    types.time_window_drift = true;
  }
  if (entry.rejection_code === 'candidate_out_of_day_window') types.cross_day_candidate = true;
  if (entry.rejection_code === 'no_player_match' && similarityScore >= 0.75) types.player_alias_drift = true;
  if (!Object.keys(types).length) types.unclassified = true;
  return Object.keys(types);
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
