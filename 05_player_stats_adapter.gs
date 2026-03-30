// Canonical Matchstat row schema index map for the Apps Script project.
// Keep PLAYER_STATS_MATCHMX_ROW_IDX declared only in this module (project-wide canonical declaration).
const PLAYER_STATS_MATCHMX_ROW_IDX = {
  DATE: 0,
  EVENT: 1,
  SURFACE: 2,
  PLAYER_NAME: 3,
  OPPONENT: 4,
  SCORE: 5,
  RANKING: 6,
  RECENT_FORM: 7,
  SURFACE_WIN_RATE: 8,
  HOLD_PCT: 9,
  BREAK_PCT: 10,
  BP_SAVED_PCT: 11,
  BP_CONV_PCT: 12,
  FIRST_SERVE_IN_PCT: 13,
  FIRST_SERVE_POINTS_WON_PCT: 14,
  SECOND_SERVE_POINTS_WON_PCT: 15,
  RETURN_POINTS_WON_PCT: 16,
  DOMINANCE_RATIO: 17,
  TOTAL_POINTS_WON_PCT: 18,
};
const PLAYER_STATS_MATCHMX_MIN_FIELD_COUNT = PLAYER_STATS_MATCHMX_ROW_IDX.TOTAL_POINTS_WON_PCT + 1;

const PLAYER_STATS_H2H_CACHE_KEY = 'PLAYER_STATS_H2H_DATASET';
const PLAYER_STATS_LEADERS_CACHE_KEY = 'PLAYER_STATS_TA_LEADERS_PAYLOAD';
const PLAYER_STATS_CACHE_MAX_BYTES = 95000;
const PLAYER_STATS_CACHE_CHUNK_BYTES = 90000;
const PLAYER_STATS_LEADERS_CHUNK_KEY_PREFIX = PLAYER_STATS_LEADERS_CACHE_KEY + '::chunk::';
const PLAYER_STATS_COMPLETENESS_KEYS = [
  'ranking',
  'recent_form',
  'surface_win_rate',
  'hold_pct',
  'break_pct',
];
const PLAYER_STATS_CANONICAL_FEATURES = [
  'ranking', 'recent_form', 'recent_form_last_10', 'surface_win_rate', 'hold_pct', 'break_pct',
  'surface_hold_pct', 'surface_break_pct', 'surface_recent_form', 'bp_saved_pct', 'bp_conv_pct',
  'first_serve_in_pct', 'first_serve_points_won_pct', 'second_serve_points_won_pct', 'return_points_won_pct',
  'dr', 'tpw_pct',
];

function fetchPlayerStatsBatch_(config, canonicalPlayers, asOfTime) {
  const players = dedupePlayerNames_(canonicalPlayers || []);
  const asOfDate = asOfTime instanceof Date ? asOfTime : new Date(asOfTime || Date.now());
  const nowMs = Date.now();
  const ttlMs = Math.max(1, Number(config.PLAYER_STATS_CACHE_TTL_MIN || 10)) * 60000;
  const refreshMinMs = Math.max(1, Number(config.PLAYER_STATS_REFRESH_MIN || 5)) * 60000;
  const forceRefresh = !!config.PLAYER_STATS_FORCE_REFRESH;
  const cacheKey = buildPlayerStatsCacheKey_(players, asOfDate);
  const cachedPayload = getCachedPlayerStatsPayload_(cacheKey);

  if (!forceRefresh && cachedPayload && Number.isFinite(cachedPayload.cached_at_ms) && (nowMs - cachedPayload.cached_at_ms <= ttlMs)) {
    persistPlayerStatsMeta_(cachedPayload, 'cached_fresh', nowMs, players.length, asOfDate, {
      provider_available: true,
      last_success_at: new Date(cachedPayload.cached_at_ms || nowMs).toISOString(),
    });
    return {
      stats_by_player: cachedPayload.stats_by_player || {},
      reason_code: 'stats_cache_hit',
      source: 'cached_fresh',
      provider_available: true,
      api_credit_usage: 0,
      api_call_count: 0,
      scrape_call_count: 0,
    };
  }

  if (!forceRefresh && cachedPayload && Number.isFinite(cachedPayload.cached_at_ms) && (nowMs - cachedPayload.cached_at_ms < refreshMinMs)) {
    persistPlayerStatsMeta_(cachedPayload, 'cached_stale_refresh_throttled', nowMs, players.length, asOfDate, {
      provider_available: true,
      last_success_at: new Date(cachedPayload.cached_at_ms || nowMs).toISOString(),
    });
    return {
      stats_by_player: cachedPayload.stats_by_player || {},
      reason_code: 'stats_cache_stale_refresh_throttled',
      source: 'cached_stale_refresh_throttled',
      provider_available: true,
      api_credit_usage: 0,
      api_call_count: 0,
      scrape_call_count: 0,
    };
  }

  const live = fetchPlayerStatsFromProvider_(config, players, asOfDate);
  if (live.ok) {
    const liveStatsByPlayer = live.stats_by_player || {};
    const completeness = summarizePlayerStatsCompleteness_(liveStatsByPlayer);
    const dataAvailable = completeness.has_stats;
    const aggregateReasonCode = resolvePlayerStatsAggregateReasonCode_(live.reason_code, completeness);
    const cachePayload = {
      cache_key: cacheKey,
      cached_at_ms: nowMs,
      as_of_time: asOfDate.toISOString(),
      player_count: players.length,
      stats_by_player: liveStatsByPlayer,
    };
    setCachedPlayerStatsPayload_(cacheKey, cachePayload);
    persistPlayerStatsMeta_(cachePayload, 'fresh_api', nowMs, players.length, asOfDate, {
      api_call_count: Number(live.api_call_count || 0),
      scrape_call_count: Number(live.scrape_call_count || 0),
      provider_available: true,
      data_available: dataAvailable,
      players_with_non_null_stats: completeness.players_with_non_null_stats,
      players_with_null_only_stats: completeness.players_with_null_only_stats,
      aggregate_reason_code: aggregateReasonCode,
      selection_metadata: live.selection_metadata || null,
      last_success_at: new Date(nowMs).toISOString(),
      last_failure_reason: dataAvailable ? '' : String(aggregateReasonCode || 'player_stats_data_unavailable'),
    });

    return {
      stats_by_player: liveStatsByPlayer,
      reason_code: aggregateReasonCode || live.reason_code || 'player_stats_api_success',
      source: 'fresh_api',
      provider_available: true,
      api_credit_usage: Number(live.api_credit_usage || 0),
      api_call_count: Number(live.api_call_count || 0),
      selection_metadata: live.selection_metadata || null,
    };
  }

  const stalePayload = getStateJson_('PLAYER_STATS_STALE_PAYLOAD');
  if (stalePayload && stalePayload.stats_by_player) {
    persistPlayerStatsMeta_(stalePayload, 'cached_stale_fallback', nowMs, players.length, asOfDate, {
      api_call_count: Number(live.api_call_count || 0),
      scrape_call_count: Number(live.scrape_call_count || 0),
      provider_available: true,
      selection_metadata: live.selection_metadata || null,
      last_success_at: String(stalePayload.last_success_at || ''),
      last_failure_reason: String(live.reason_code || ''),
    });
    return {
      stats_by_player: stalePayload.stats_by_player || {},
      reason_code: 'stats_stale_fallback',
      source: 'cached_stale_fallback',
      provider_available: true,
      api_credit_usage: Number(live.api_credit_usage || 0),
      api_call_count: Number(live.api_call_count || 0),
      selection_metadata: live.selection_metadata || null,
    };
  }

  persistPlayerStatsMeta_({
    cache_key: cacheKey,
    cached_at_ms: nowMs,
    as_of_time: asOfDate.toISOString(),
    player_count: players.length,
    stats_by_player: {},
  }, 'provider_unavailable', nowMs, players.length, asOfDate, {
    api_call_count: Number(live.api_call_count || 0),
    scrape_call_count: Number(live.scrape_call_count || 0),
    provider_available: false,
    selection_metadata: live.selection_metadata || null,
    last_failure_reason: String(live.reason_code || 'player_stats_provider_unavailable'),
  });

  return {
    stats_by_player: {},
    reason_code: live.reason_code || 'player_stats_provider_unavailable',
    source: 'provider_unavailable',
    provider_available: false,
    api_credit_usage: Number(live.api_credit_usage || 0),
    api_call_count: Number(live.api_call_count || 0),
    selection_metadata: live.selection_metadata || null,
  };
}

function fetchPlayerStatsFromProvider_(config, canonicalPlayers, asOfTime) {
  const players = dedupePlayerNames_(canonicalPlayers || []);
  if (!players.length) {
    const noDemandPartitions = buildPlayerStatsReasonCodePartitioning_(['player_stats_no_players']);
    return {
      ok: true,
      reason_code: 'player_stats_no_players',
      stats_by_player: {},
      api_credit_usage: 0,
      api_call_count: 0,
      scrape_call_count: 0,
      selection_metadata: {
        reason_code_partitioning: noDemandPartitions,
      },
    };
  }

  const sourceConfigs = parsePlayerStatsSourceConfigs_(config);

  const attempts = [];
  let totalApiCalls = 0;
  const sourcePayloads = [];
  const sourceAttemptDiagnostics = [];
  const reasonCodePartitioning = initPlayerStatsReasonCodePartitioning_();

  sourceConfigs.forEach(function (sourceConfig) {
    const result = fetchPlayerStatsFromSingleSource_(sourceConfig, config, players, asOfTime);
    attempts.push(result.reason_code || 'player_stats_unknown_source_result');
    addPlayerStatsReasonCodeToPartitioning_(reasonCodePartitioning, result.reason_code);
    totalApiCalls += Number(result.api_call_count || 0);
    sourceAttemptDiagnostics.push({
      source_name: sourceConfig.source_name,
      reason_code: result.reason_code || '',
      attempted_endpoints: result.attempted_endpoints || [],
      missing_fields: result.missing_fields || [],
      contract_check_passed: result.contract_check_passed !== false,
    });
    if (result.ok && result.stats_by_player) {
      sourcePayloads.push({
        source_name: sourceConfig.source_name,
        stats_by_player: result.stats_by_player,
        endpoint_feature_sources_by_player: result.endpoint_feature_sources_by_player || {},
      });
    }
  });

  if (sourcePayloads.length) {
    const merged = mergePlayerStatsMaps_(sourcePayloads, players, config);
    const mergeDiagnostics = buildPlayerStatsMergeDiagnostics_(sourcePayloads, merged, players, sourceAttemptDiagnostics);
    const mergedHasData = Number((mergeDiagnostics.final || {}).players_with_non_null_stats || 0) > 0;
    const finalSelection = mergeDiagnostics.final || {};
    return {
      ok: true,
      reason_code: mergedHasData
        ? (sourcePayloads.length > 1 ? 'player_stats_multi_source_success' : 'player_stats_api_success')
        : 'stats_zero_coverage',
      detail: attempts.join(','),
      stats_by_player: merged,
      api_credit_usage: 0,
      api_call_count: totalApiCalls,
      scrape_call_count: 0,
      selection_metadata: {
        source_count: sourcePayloads.length,
        placeholder_feature_count: Number(finalSelection.placeholder_feature_count || 0),
        trusted_feature_count: Number(finalSelection.trusted_feature_count || 0),
        merge_diagnostics: mergeDiagnostics,
        source_attempts: sourceAttemptDiagnostics,
        reason_code_partitioning: reasonCodePartitioning,
      },
    };
  }

  const leadersSource = fetchPlayerStatsFromLeadersSource_(players, config, asOfTime);
  addPlayerStatsReasonCodeToPartitioning_(reasonCodePartitioning, leadersSource.reason_code);
  if (leadersSource.ok) {
    const taStatsByPlayer = leadersSource.stats_by_player || {};
    const unresolvedPlayers = players.filter(function (playerName) {
      return !playerStatsHasNonNullFeatures_(taStatsByPlayer[playerName]);
    });
    const cohortPolicy = buildPlayerStatsCohortPolicy_(config);
    const taSelectionMetadata = leadersSource.selection_metadata && typeof leadersSource.selection_metadata === 'object'
      ? leadersSource.selection_metadata
      : {};
    const taCoverageRatio = Number(taSelectionMetadata.coverage_ratio || 0);
    const taCoverageThreshold = Number(
      taSelectionMetadata.min_coverage_ratio_threshold !== undefined
        ? taSelectionMetadata.min_coverage_ratio_threshold
        : taSelectionMetadata.min_acceptable_coverage_ratio
    );
    const hasCoverageGate = Number.isFinite(taCoverageRatio) && Number.isFinite(taCoverageThreshold);
    const coverageBelowThreshold = hasCoverageGate
      ? taCoverageRatio < taCoverageThreshold
      : true;
    const finalStatsByPlayer = Object.assign({}, taStatsByPlayer);
    const fallbackDiagnostics = {
      unresolved_player_count: unresolvedPlayers.length,
      unresolved_players_sample: unresolvedPlayers.slice(0, 20),
      fallback_reasons_by_player: {},
      fallback_source_by_player: {},
      fallback_attempts: [],
      fallback_attempted_players_by_reason: {},
      fallback_calls: { sofascore: 0, scrape: 0 },
      fallback_resolved_counts: { sofascore: 0, scrape: 0 },
      ta_coverage_ratio: hasCoverageGate ? roundNumber_(taCoverageRatio, 4) : null,
      ta_min_coverage_ratio_threshold: hasCoverageGate ? roundNumber_(taCoverageThreshold, 4) : null,
      ta_coverage_below_threshold: hasCoverageGate ? coverageBelowThreshold : null,
      mode_gate_skipped_fallback: false,
    };
    const fallbackCandidates = unresolvedPlayers.filter(function (playerName) {
      const taRow = taStatsByPlayer[playerName] || {};
      const cohortDecision = classifyPlayerStatsCohort_(taRow, cohortPolicy);
      const cohortClass = String(cohortDecision.classification || '');
      const isUnknownRank = cohortClass === 'unknown_rank';
      const isOutOfCohort = cohortClass === 'out_of_cohort';
      const reason = isUnknownRank ? 'rank_unknown' : (isOutOfCohort ? 'out_of_cohort' : 'ta_null_only');
      fallbackDiagnostics.fallback_reasons_by_player[playerName] = reason;
      if (cohortPolicy.mode === 'leadersource' && !coverageBelowThreshold) return false;
      if (cohortPolicy.mode === 'top100' && isOutOfCohort) return false;
      return true;
    });
    const skippedByModeGate = unresolvedPlayers.filter(function (playerName) {
      return fallbackCandidates.indexOf(playerName) === -1;
    });
    fallbackDiagnostics.mode_gate_skipped_fallback = skippedByModeGate.length > 0;
    fallbackDiagnostics.skipped_by_mode_gate_count = skippedByModeGate.length;
    fallbackDiagnostics.skipped_by_mode_gate_sample = skippedByModeGate.slice(0, 20);
    fallbackCandidates.forEach(function (playerName) {
      const reason = String(fallbackDiagnostics.fallback_reasons_by_player[playerName] || 'ta_null_only');
      fallbackDiagnostics.fallback_attempted_players_by_reason[reason] = Number(fallbackDiagnostics.fallback_attempted_players_by_reason[reason] || 0) + 1;
    });

    let unresolvedAfterSofascore = fallbackCandidates.slice();
    const disableSofascore = !!config.DISABLE_SOFASCORE;
    if (!disableSofascore && unresolvedAfterSofascore.length) {
      const sofascoreFallback = fetchPlayerStatsFromSofascore_(config, unresolvedAfterSofascore, asOfTime);
      totalApiCalls += Number(sofascoreFallback.api_call_count || 0);
      fallbackDiagnostics.fallback_calls.sofascore += 1;
      fallbackDiagnostics.fallback_attempts.push({
        source_name: 'sofascore',
        reason_code: sofascoreFallback.reason_code || '',
        requested_player_count: unresolvedAfterSofascore.length,
        requested_players_by_reason: Object.assign({}, fallbackDiagnostics.fallback_attempted_players_by_reason),
        attempted_endpoints: sofascoreFallback.attempted_endpoints || [],
        missing_fields: sofascoreFallback.missing_fields || [],
      });
      const sofascoreStatsByPlayer = sofascoreFallback.stats_by_player || {};
      unresolvedAfterSofascore = unresolvedAfterSofascore.filter(function (playerName) {
        const fallbackStats = sofascoreStatsByPlayer[playerName];
        if (playerStatsHasNonNullFeatures_(fallbackStats)) {
          finalStatsByPlayer[playerName] = fallbackStats;
          fallbackDiagnostics.fallback_source_by_player[playerName] = 'sofascore';
          fallbackDiagnostics.fallback_resolved_counts.sofascore += 1;
          return false;
        }
        return true;
      });
    } else if (disableSofascore) {
      fallbackDiagnostics.fallback_attempts.push({
        source_name: 'sofascore',
        reason_code: 'player_stats_sofascore_disabled',
        requested_player_count: unresolvedAfterSofascore.length,
        requested_players_by_reason: Object.assign({}, fallbackDiagnostics.fallback_attempted_players_by_reason),
      });
    }

    let unresolvedAfterScrape = unresolvedAfterSofascore.slice();
    if (unresolvedAfterScrape.length) {
      const scraped = fetchPlayerStatsFromScrapeSources_(config, unresolvedAfterScrape);
      totalApiCalls += Number(scraped.api_call_count || 0);
      fallbackDiagnostics.fallback_calls.scrape += 1;
      fallbackDiagnostics.fallback_attempts.push({
        source_name: 'scrape',
        reason_code: scraped.reason_code || '',
        requested_player_count: unresolvedAfterScrape.length,
        requested_players_by_reason: Object.assign({}, fallbackDiagnostics.fallback_attempted_players_by_reason),
      });
      const scrapedStatsByPlayer = scraped.stats_by_player || {};
      unresolvedAfterScrape = unresolvedAfterScrape.filter(function (playerName) {
        const fallbackStats = scrapedStatsByPlayer[playerName];
        if (playerStatsHasNonNullFeatures_(fallbackStats)) {
          finalStatsByPlayer[playerName] = fallbackStats;
          fallbackDiagnostics.fallback_source_by_player[playerName] = 'scrape';
          fallbackDiagnostics.fallback_resolved_counts.scrape += 1;
          return false;
        }
        return true;
      });
    }

    const unresolvedAfterAllFallback = unresolvedAfterScrape.concat(skippedByModeGate);
    fallbackDiagnostics.unresolved_after_fallback_count = unresolvedAfterAllFallback.length;
    fallbackDiagnostics.unresolved_after_fallback_sample = unresolvedAfterAllFallback.slice(0, 20);
    const baseSelectionMetadata = leadersSource.selection_metadata && typeof leadersSource.selection_metadata === 'object'
      ? leadersSource.selection_metadata
      : {};
    const playerSourceByPlayer = Object.assign({}, baseSelectionMetadata.player_source_by_player || {});
    players.forEach(function (playerName) {
      if (fallbackDiagnostics.fallback_source_by_player[playerName]) {
        playerSourceByPlayer[playerName] = fallbackDiagnostics.fallback_source_by_player[playerName];
      } else if (!playerSourceByPlayer[playerName]) {
        playerSourceByPlayer[playerName] = 'tennis_abstract';
      }
    });

    return {
      ok: true,
      reason_code: leadersSource.reason_code || 'ta_matchmx_ok',
      detail: attempts.join(','),
      stats_by_player: finalStatsByPlayer,
      api_credit_usage: 0,
      api_call_count: totalApiCalls + Number(leadersSource.api_call_count || 0),
      scrape_call_count: Number(fallbackDiagnostics.fallback_calls.scrape || 0),
      selection_metadata: Object.assign({}, baseSelectionMetadata, {
        fallback_diagnostics: fallbackDiagnostics,
        player_source_by_player: playerSourceByPlayer,
        reason_code_partitioning: reasonCodePartitioning,
      }),
    };
  }

  const scraped = fetchPlayerStatsFromScrapeSources_(config, players);
  addPlayerStatsReasonCodeToPartitioning_(reasonCodePartitioning, scraped.reason_code);
  if (scraped.ok) {
    return {
      ok: true,
      reason_code: 'player_stats_scrape_success',
      detail: attempts.join(','),
      stats_by_player: scraped.stats_by_player || {},
      api_credit_usage: 0,
      api_call_count: totalApiCalls + Number(scraped.api_call_count || 0),
      scrape_call_count: Number(scraped.api_call_count || 0),
      selection_metadata: {
        reason_code_partitioning: reasonCodePartitioning,
      },
    };
  }

  return {
    ok: false,
    reason_code: leadersSource.reason_code || (attempts.length ? attempts[attempts.length - 1] : 'player_stats_provider_not_configured'),
    detail: attempts.join(','),
    api_credit_usage: 0,
    api_call_count: totalApiCalls + Number(leadersSource.api_call_count || 0) + Number(scraped.api_call_count || 0),
    scrape_call_count: Number(scraped.api_call_count || 0),
    selection_metadata: Object.assign({}, leadersSource.selection_metadata || {}, {
      reason_code_partitioning: reasonCodePartitioning,
    }),
  };
}

function initPlayerStatsReasonCodePartitioning_() {
  return {
    upstream_payload_empty_or_changed_shape: [],
    parser_contract_mismatch: [],
    no_demand_cases: [],
  };
}

function buildPlayerStatsReasonCodePartitioning_(reasonCodes) {
  const partitioning = initPlayerStatsReasonCodePartitioning_();
  (reasonCodes || []).forEach(function (code) {
    addPlayerStatsReasonCodeToPartitioning_(partitioning, code);
  });
  return partitioning;
}

function addPlayerStatsReasonCodeToPartitioning_(partitioning, reasonCode) {
  const code = String(reasonCode || '').trim();
  if (!code || !partitioning || typeof partitioning !== 'object') return;
  if (isPlayerStatsNoDemandReasonCode_(code)) {
    if (partitioning.no_demand_cases.indexOf(code) === -1) partitioning.no_demand_cases.push(code);
    return;
  }
  if (isPlayerStatsParserContractMismatchReasonCode_(code)) {
    if (partitioning.parser_contract_mismatch.indexOf(code) === -1) partitioning.parser_contract_mismatch.push(code);
    return;
  }
  if (isPlayerStatsUpstreamPayloadShapeReasonCode_(code)) {
    if (partitioning.upstream_payload_empty_or_changed_shape.indexOf(code) === -1) partitioning.upstream_payload_empty_or_changed_shape.push(code);
  }
}

function isPlayerStatsNoDemandReasonCode_(reasonCode) {
  return String(reasonCode || '') === 'player_stats_no_players';
}

function isPlayerStatsParserContractMismatchReasonCode_(reasonCode) {
  const code = String(reasonCode || '');
  if (!code) return false;
  if (code.indexOf('contract') >= 0) return true;
  if (code.indexOf('parse') >= 0) return true;
  if (code.indexOf('shape_invalid') >= 0) return true;
  if (code.indexOf('missing_keys') >= 0) return true;
  if (code.indexOf('non_json') >= 0) return true;
  return false;
}

function isPlayerStatsUpstreamPayloadShapeReasonCode_(reasonCode) {
  const code = String(reasonCode || '');
  if (!code) return false;
  if (code.indexOf('provider_returned_empty') >= 0) return true;
  if (code.indexOf('no_usable_stats_payload') >= 0) return true;
  if (code.indexOf('unusable_payload') >= 0) return true;
  if (code.indexOf('js_url_missing') >= 0) return true;
  if (code.indexOf('zero_coverage') >= 0) return true;
  return false;
}

function fetchPlayerStatsFromLeadersSource_(canonicalPlayers, config, asOfTime) {
  const ttlMs = Math.max(1, Number(config.PLAYER_STATS_CACHE_TTL_MIN || 10)) * 60000;
  const forceRefresh = !!config.PLAYER_STATS_FORCE_REFRESH;
  const nowMs = Date.now();
  const staleLeadersPayload = getStateJson_('PLAYER_STATS_TA_LEADERS_STALE_PAYLOAD');
  const staleLeadersCompleteness = summarizeTaLeadersPayloadCompleteness_(staleLeadersPayload, canonicalPlayers, asOfTime, config);

  if (!forceRefresh) {
    const cached = getCachedTaLeadersPayload_();
    if (cached && Array.isArray(cached.rows) && Number.isFinite(cached.cached_at_ms) && nowMs - cached.cached_at_ms <= ttlMs) {
      const cachedStats = normalizePlayerStatsResponse_(cached.rows, canonicalPlayers, {
        as_of_time: asOfTime,
        match_window_weeks: Number(config.PLAYER_STATS_MATCH_WINDOW_WEEKS || 52),
        recent_match_count: Number(config.PLAYER_STATS_RECENT_MATCH_COUNT || 0),
      });
      const cachedDiagnostics = summarizeTaLeadersParseDiagnostics_(cached.rows, cachedStats, null, canonicalPlayers);
      const cachedCompleteness = summarizePlayerStatsCompleteness_(cachedStats);
      const cachedQualityGate = evaluateTaLeadersQualityGate_(cachedDiagnostics, cachedCompleteness, config);
      if (!cachedQualityGate.meets_thresholds) {
        logTaLeadersCacheDiagnostic_('ta_quality_gate_failed', {
          reason_code: cachedQualityGate.reason_code || 'ta_matchmx_quality_gate_failed',
          quality_gate: cachedQualityGate,
          parsed_row_count: Number(cachedDiagnostics.parsed_row_count || 0),
          source: String(cached.source || 'ta_cached_payload'),
          fetched_at: new Date().toISOString(),
        });
      }
      return {
        ok: true,
        reason_code: cachedQualityGate.meets_thresholds ? 'ta_matchmx_cache_hit' : (cachedQualityGate.reason_code || 'ta_matchmx_quality_gate_failed'),
        stats_by_player: cachedStats,
        api_call_count: 0,
        selection_metadata: {
          source_selected: 'cache_payload',
          quality_gate: cachedQualityGate,
        },
      };
    }
  }

  const leadersUrl = String(config.PLAYER_STATS_TA_LEADERS_URL || 'https://www.tennisabstract.com/cgi-bin/leaders_wta.cgi?players=top').trim();
  const headers = {
    Accept: 'text/html',
    'User-Agent': String(config.PLAYER_STATS_FETCH_USER_AGENT || 'Mozilla/5.0 (compatible; WTA-Edge-Board/1.0)'),
  };

  const pageFetch = playerStatsFetchWithRetry_(leadersUrl, {
    method: 'get',
    muteHttpExceptions: true,
    headers: headers,
    followRedirects: true,
    validateHttpsCertificates: true,
  }, config);

  if (!pageFetch.ok) {
    if (staleLeadersCompleteness.has_rows && !staleLeadersCompleteness.is_null_only) {
      return {
        ok: true,
        reason_code: 'ta_matchmx_stale_fallback',
        stats_by_player: staleLeadersCompleteness.stats_by_player,
        api_call_count: Number(pageFetch.api_call_count || 0),
        selection_metadata: {
          source_selected: 'stale_payload',
          selection_reason: 'fresh_fetch_failed',
          stale_rows: staleLeadersCompleteness.row_count,
          stale_non_null_feature_total: staleLeadersCompleteness.non_null_feature_total,
          stale_null_only: staleLeadersCompleteness.is_null_only,
        },
      };
    }
    return {
      ok: false,
      reason_code: pageFetch.reason_code || 'ta_leaders_page_fetch_failed',
      stats_by_player: {},
      api_call_count: Number(pageFetch.api_call_count || 0),
      selection_metadata: {
        source_selected: 'none',
        selection_reason: 'fresh_fetch_failed_no_usable_stale',
        stale_rows: staleLeadersCompleteness.row_count,
        stale_non_null_feature_total: staleLeadersCompleteness.non_null_feature_total,
        stale_null_only: staleLeadersCompleteness.is_null_only,
      },
    };
  }

  const pageText = String(pageFetch.response.getContentText() || '');
  const jsUrl = extractLeadersJsUrl_(pageText, leadersUrl);
  if (!jsUrl) {
    return { ok: false, reason_code: 'ta_leaders_js_url_missing', stats_by_player: {}, api_call_count: Number(pageFetch.api_call_count || 0) };
  }

  sleepTennisAbstractRequestGap_(config);

  const jsFetch = playerStatsFetchWithRetry_(jsUrl, {
    method: 'get',
    muteHttpExceptions: true,
    headers: {
      Accept: 'application/javascript,text/plain',
      'User-Agent': String(config.PLAYER_STATS_FETCH_USER_AGENT || 'Mozilla/5.0 (compatible; WTA-Edge-Board/1.0)'),
    },
    followRedirects: true,
    validateHttpsCertificates: true,
  }, config);

  const totalCalls = Number(pageFetch.api_call_count || 0) + Number(jsFetch.api_call_count || 0);
  if (!jsFetch.ok) {
    if (staleLeadersCompleteness.has_rows && !staleLeadersCompleteness.is_null_only) {
      return {
        ok: true,
        reason_code: 'ta_matchmx_stale_fallback',
        stats_by_player: staleLeadersCompleteness.stats_by_player,
        api_call_count: totalCalls,
        selection_metadata: {
          source_selected: 'stale_payload',
          selection_reason: 'fresh_js_fetch_failed',
          stale_rows: staleLeadersCompleteness.row_count,
          stale_non_null_feature_total: staleLeadersCompleteness.non_null_feature_total,
          stale_null_only: staleLeadersCompleteness.is_null_only,
        },
      };
    }
    return {
      ok: false,
      reason_code: jsFetch.reason_code || 'ta_leaders_js_fetch_failed',
      stats_by_player: {},
      api_call_count: totalCalls,
      selection_metadata: {
        source_selected: 'none',
        selection_reason: 'fresh_js_fetch_failed_no_usable_stale',
        stale_rows: staleLeadersCompleteness.row_count,
        stale_non_null_feature_total: staleLeadersCompleteness.non_null_feature_total,
        stale_null_only: staleLeadersCompleteness.is_null_only,
      },
    };
  }

  const jsPayload = String(jsFetch.response.getContentText() || '');
  const extractedRows = extractMatchMxRows_(jsPayload);
  const structuredRows = extractedRows.rows || [];
  if (!structuredRows.length) {
    if (staleLeadersCompleteness.has_rows && !staleLeadersCompleteness.is_null_only) {
      return {
        ok: true,
        reason_code: 'ta_matchmx_stale_fallback',
        stats_by_player: staleLeadersCompleteness.stats_by_player,
        api_call_count: totalCalls,
        selection_metadata: {
          source_selected: 'stale_payload',
          selection_reason: 'fresh_parse_failed',
          stale_rows: staleLeadersCompleteness.row_count,
          stale_non_null_feature_total: staleLeadersCompleteness.non_null_feature_total,
          stale_null_only: staleLeadersCompleteness.is_null_only,
        },
      };
    }
    if (staleLeadersCompleteness.has_rows && staleLeadersCompleteness.is_null_only) {
      emitNoUsableStatsPayloadAlert_(config, {
        fresh_reason_code: 'ta_matchmx_parse_failed',
        stale_rows: staleLeadersCompleteness.row_count,
        stale_non_null_feature_total: staleLeadersCompleteness.non_null_feature_total,
      });
      return {
        ok: false,
        reason_code: 'no_usable_stats_payload',
        stats_by_player: {},
        api_call_count: totalCalls,
        selection_metadata: {
          source_selected: 'none',
          selection_reason: 'fresh_parse_failed_stale_null_only',
          stale_rows: staleLeadersCompleteness.row_count,
          stale_non_null_feature_total: staleLeadersCompleteness.non_null_feature_total,
          stale_null_only: true,
        },
      };
    }
    return { ok: false, reason_code: 'ta_matchmx_parse_failed', stats_by_player: {}, api_call_count: totalCalls };
  }

  const normalizedStatsByPlayer = normalizePlayerStatsResponse_(structuredRows, canonicalPlayers, {
    as_of_time: asOfTime,
    match_window_weeks: Number(config.PLAYER_STATS_MATCH_WINDOW_WEEKS || 52),
    recent_match_count: Number(config.PLAYER_STATS_RECENT_MATCH_COUNT || 0),
  });
  const parseDiagnostics = summarizeTaLeadersParseDiagnostics_(structuredRows, normalizedStatsByPlayer, extractedRows.diagnostics, canonicalPlayers);
  const statsCompleteness = summarizePlayerStatsCompleteness_(normalizedStatsByPlayer);
  const canonicalPlayerCount = Math.max(0, Number((canonicalPlayers || []).length || 0));
  const playersWithNonNullStats = Number(statsCompleteness.players_with_non_null_stats || 0);
  const coverageRatio = canonicalPlayerCount > 0 ? playersWithNonNullStats / canonicalPlayerCount : 1;
  const minPlayersWithStatsThreshold = Math.max(1, Number((config && config.PLAYER_STATS_TA_MIN_PLAYERS_WITH_STATS) || 2));
  const configuredCoverageRatioThreshold = Number(
    (config && config.PLAYER_STATS_MIN_ACCEPTABLE_COVERAGE_RATIO)
    || (config && config.PLAYER_STATS_TA_MIN_COVERAGE_RATIO)
    || 0
  );
  const minCoverageRatioThreshold = configuredCoverageRatioThreshold > 0
    ? Math.min(1, Math.max(0, configuredCoverageRatioThreshold))
    : (canonicalPlayerCount > 0 ? Math.min(1, minPlayersWithStatsThreshold / canonicalPlayerCount) : 0);
  const coverageRatioBelowThreshold = canonicalPlayerCount > 0
    && (coverageRatio < minCoverageRatioThreshold || playersWithNonNullStats < minPlayersWithStatsThreshold);
  const coverageRatioReasonCode = coverageRatioBelowThreshold ? 'ta_matchmx_coverage_ratio_low' : '';
  const degradedCoverageSignal = coverageRatioBelowThreshold ? {
    reason_code: coverageRatioReasonCode,
    coverage_ratio: roundNumber_(coverageRatio, 4),
    min_coverage_ratio_threshold: roundNumber_(minCoverageRatioThreshold, 4),
    min_acceptable_coverage_ratio: roundNumber_(minCoverageRatioThreshold, 4),
    players_with_non_null_stats: playersWithNonNullStats,
    canonical_player_count: canonicalPlayerCount,
    min_players_with_non_null_stats: minPlayersWithStatsThreshold,
  } : null;
  const coverage = evaluateTaLeadersParseCoverage_(parseDiagnostics, config);
  const hasCoverageMismatch = isTaLeadersCoverageMismatch_(parseDiagnostics);
  const coverageReasonCode = hasCoverageMismatch ? 'ta_parse_coverage_mismatch' : '';
  const qualityGate = evaluateTaLeadersQualityGate_(parseDiagnostics, statsCompleteness, config);
  const taHealthy = qualityGate.meets_thresholds && !hasCoverageMismatch;
  if (!taHealthy) {
    logTaLeadersCacheDiagnostic_('ta_quality_gate_failed', {
      reason_code: coverageReasonCode || qualityGate.reason_code || 'ta_matchmx_quality_gate_failed',
      quality_gate: qualityGate,
      parsed_row_count: Number(parseDiagnostics.parsed_row_count || 0),
      source: jsUrl,
      fetched_at: new Date().toISOString(),
    });
  }
  persistTaLeadersParseDiagnostics_(Object.assign({}, parseDiagnostics, {
    reason_code: taHealthy ? 'ta_matchmx_ok' : (coverageReasonCode || qualityGate.reason_code),
    players_with_non_null_stats: Number(statsCompleteness.players_with_non_null_stats || 0),
    coverage: coverage,
    quality_gate: qualityGate,
    source: jsUrl,
    fetched_at: new Date().toISOString(),
  }));

  const unusableRowsThreshold = Math.max(1, Number(config.PLAYER_STATS_TA_UNUSABLE_MIN_ROWS || 500));
  if (Number(parseDiagnostics.parsed_row_count || 0) >= unusableRowsThreshold && Number(statsCompleteness.players_with_non_null_stats || 0) === 0) {
    emitNoUsableStatsPayloadAlert_(config, {
      fresh_reason_code: 'ta_matchmx_unusable_payload',
      fresh_rows: Number(parseDiagnostics.parsed_row_count || 0),
      parsed_player_key_count: Number(parseDiagnostics.parsed_player_key_count || 0),
      fresh_non_null_feature_total: Number(coverage.non_null_feature_total || 0),
      players_with_non_null_stats: playersWithNonNullStats,
      canonical_player_count: canonicalPlayerCount,
      coverage_ratio: roundNumber_(coverageRatio, 4),
      min_coverage_ratio_threshold: roundNumber_(minCoverageRatioThreshold, 4),
      min_acceptable_coverage_ratio: roundNumber_(minCoverageRatioThreshold, 4),
      stale_rows: staleLeadersCompleteness.row_count,
      stale_non_null_feature_total: staleLeadersCompleteness.non_null_feature_total,
    });
    return {
      ok: false,
      reason_code: 'ta_matchmx_unusable_payload',
      stats_by_player: {},
      api_call_count: totalCalls,
      selection_metadata: {
        source_selected: 'none',
        selection_reason: 'fresh_unusable_non_null_zero',
        fresh_rows: Number(parseDiagnostics.parsed_row_count || 0),
        parsed_player_key_count: Number(parseDiagnostics.parsed_player_key_count || 0),
        fresh_non_null_feature_total: Number(coverage.non_null_feature_total || 0),
        players_with_non_null_stats: playersWithNonNullStats,
        canonical_player_count: canonicalPlayerCount,
        coverage_ratio: roundNumber_(coverageRatio, 4),
        min_coverage_ratio_threshold: roundNumber_(minCoverageRatioThreshold, 4),
        min_acceptable_coverage_ratio: roundNumber_(minCoverageRatioThreshold, 4),
        stale_rows: staleLeadersCompleteness.row_count,
        stale_non_null_feature_total: staleLeadersCompleteness.non_null_feature_total,
        stale_null_only: staleLeadersCompleteness.is_null_only,
        degraded_signal: degradedCoverageSignal,
      },
    };
  }

  if (!coverage.exceeds_threshold && hasCoverageMismatch) {
    const cachedHealthy = getCachedTaLeadersPayload_();
    if (cachedHealthy && Array.isArray(cachedHealthy.rows) && cachedHealthy.rows.length) {
      const cachedHealthyStats = normalizePlayerStatsResponse_(cachedHealthy.rows, canonicalPlayers, {
        as_of_time: asOfTime,
        match_window_weeks: Number(config.PLAYER_STATS_MATCH_WINDOW_WEEKS || 52),
        recent_match_count: Number(config.PLAYER_STATS_RECENT_MATCH_COUNT || 0),
      });
      const cachedCompleteness = summarizePlayerStatsCompleteness_(cachedHealthyStats);
      if (cachedCompleteness.has_stats) {
        return {
          ok: true,
          reason_code: coverageReasonCode || qualityGate.reason_code,
          stats_by_player: cachedHealthyStats,
          api_call_count: totalCalls,
        };
      }
    }
  }

  const cachePayload = {
    source: jsUrl,
    fetched_at: new Date().toISOString(),
    cached_at_ms: Date.now(),
    rows: structuredRows,
  };
  const cacheWriteResult = setCachedTaLeadersPayload_(cachePayload);
  if (!cacheWriteResult.ok) {
    logTaLeadersCacheDiagnostic_('cache_write_failed_non_fatal', {
      reason_code: cacheWriteResult.reason_code || 'cache_write_failed',
      storage_path: cacheWriteResult.storage_path || 'none',
      bytes: Number(cacheWriteResult.bytes || 0),
      chunk_count: Number(cacheWriteResult.chunk_count || 0),
    });
  }
  try {
    setStateValue_('PLAYER_STATS_TA_LEADERS_STALE_PAYLOAD', JSON.stringify(cachePayload), {
      source_meta: {
        source: cachePayload.source || '',
        cached_at_ms: Number(cachePayload.cached_at_ms || Date.now()),
        row_count: (cachePayload.rows || []).length,
        storage_path: cacheWriteResult.storage_path || '',
        reference_key: PLAYER_STATS_LEADERS_CACHE_KEY,
      },
    });
  } catch (e) {
    logTaLeadersCacheDiagnostic_('state_stale_payload_write_failed_non_fatal', {
      reason_code: 'state_write_failed',
      error: String((e && e.message) || e || ''),
    });
  }

  const statsByPlayer = normalizedStatsByPlayer;
  const forceReplaceNullOnlyStale = coverage.exceeds_threshold && staleLeadersCompleteness.has_rows && staleLeadersCompleteness.is_null_only;
  const parsedRowCount = Number(parseDiagnostics.parsed_row_count || 0);
  const hasParsedRows = parsedRowCount > 0;
  const hasResolvedScheduledPlayers = Object.keys(statsByPlayer).length > 0;
  const selectedReasonCode = hasResolvedScheduledPlayers
    ? (coverageRatioReasonCode || (taHealthy ? 'ta_matchmx_ok' : qualityGate.reason_code))
    : (hasParsedRows
      ? (coverageReasonCode || coverageRatioReasonCode || qualityGate.reason_code || 'ta_matchmx_coverage_miss')
      : 'ta_matchmx_parse_failed');
  return {
    ok: hasResolvedScheduledPlayers,
    reason_code: selectedReasonCode,
    stats_by_player: statsByPlayer,
    api_call_count: totalCalls,
    selection_metadata: {
      source_selected: 'fresh_payload',
      selection_reason: forceReplaceNullOnlyStale
        ? 'fresh_healthy_overrode_null_only_stale'
        : (coverageRatioBelowThreshold
          ? 'fresh_degraded_coverage_ratio_low'
          : (coverage.exceeds_threshold ? 'fresh_healthy_coverage' : 'fresh_default')),
      selection_reason_code: selectedReasonCode,
      fresh_rows: Number(parseDiagnostics.parsed_row_count || 0),
      fresh_non_null_feature_total: Number(coverage.non_null_feature_total || 0),
      fresh_coverage_threshold_rows: Number(coverage.row_threshold || 0),
      fresh_coverage_threshold_non_null_features: Number(coverage.non_null_threshold || 0),
      fresh_coverage_exceeds_threshold: coverage.exceeds_threshold,
      canonical_player_count: canonicalPlayerCount,
      players_with_non_null_stats: playersWithNonNullStats,
      coverage_ratio: roundNumber_(coverageRatio, 4),
      min_coverage_ratio_threshold: roundNumber_(minCoverageRatioThreshold, 4),
      min_acceptable_coverage_ratio: roundNumber_(minCoverageRatioThreshold, 4),
      stale_rows: staleLeadersCompleteness.row_count,
      stale_non_null_feature_total: staleLeadersCompleteness.non_null_feature_total,
      stale_null_only: staleLeadersCompleteness.is_null_only,
      cache_replacement_forced: forceReplaceNullOnlyStale,
      quality_gate: qualityGate,
      degraded_signal: degradedCoverageSignal,
    },
  };
}

function summarizeTaLeadersPayloadCompleteness_(payload, canonicalPlayers, asOfTime, config) {
  const rows = payload && Array.isArray(payload.rows) ? payload.rows : [];
  if (!rows.length) return { has_rows: false, row_count: 0, non_null_feature_total: 0, is_null_only: false, stats_by_player: {} };
  const statsByPlayer = normalizePlayerStatsResponse_(rows, canonicalPlayers, {
    as_of_time: asOfTime,
    match_window_weeks: Number(config.PLAYER_STATS_MATCH_WINDOW_WEEKS || 52),
    recent_match_count: Number(config.PLAYER_STATS_RECENT_MATCH_COUNT || 0),
  });
  const diagnostics = summarizeTaLeadersParseDiagnostics_(rows, statsByPlayer, null, canonicalPlayers);
  const nonNullCounts = diagnostics.normalized_non_null_counts || {};
  const nonNullFeatureTotal = Number(nonNullCounts.ranking || 0) + Number(nonNullCounts.hold_pct || 0) + Number(nonNullCounts.break_pct || 0);
  return {
    has_rows: true,
    row_count: rows.length,
    non_null_feature_total: nonNullFeatureTotal,
    is_null_only: nonNullFeatureTotal <= 0,
    stats_by_player: statsByPlayer,
  };
}

function evaluateTaLeadersParseCoverage_(diagnostics, config) {
  const info = diagnostics || {};
  const nonNull = info.normalized_non_null_counts || {};
  const nonNullFeatureTotal = Number(nonNull.ranking || 0) + Number(nonNull.hold_pct || 0) + Number(nonNull.break_pct || 0);
  const rowThreshold = Math.max(1, Number((config && config.PLAYER_STATS_TA_PARSE_COVERAGE_MIN_ROWS) || 500));
  const nonNullThreshold = Math.max(1, Number((config && config.PLAYER_STATS_TA_PARSE_COVERAGE_MIN_NON_NULL_FEATURES) || 4));
  return {
    parsed_row_count: Number(info.parsed_row_count || 0),
    non_null_feature_total: nonNullFeatureTotal,
    row_threshold: rowThreshold,
    non_null_threshold: nonNullThreshold,
    exceeds_threshold: Number(info.parsed_row_count || 0) >= rowThreshold && nonNullFeatureTotal >= nonNullThreshold,
  };
}


function evaluateTaLeadersQualityGate_(diagnostics, statsCompleteness, config) {
  const info = diagnostics || {};
  const completeness = statsCompleteness || {};
  const nonNullByFeature = info.non_null_by_feature || info.normalized_non_null_counts || {};
  const nameSanity = info.canonical_name_sanity || {};
  const overlap = info.canonical_overlap || {};
  const validNameRatio = Number(nameSanity.valid_ratio || 0);
  const invalidNameRatio = Math.max(0, 1 - validNameRatio);
  const minValidNameRatio = Math.max(0.5, Number((config && config.PLAYER_STATS_TA_MIN_VALID_NAME_RATIO) || 0.7));
  const overlapRatio = Number(overlap.overlap_with_scheduled_ratio || overlap.overlap_ratio || 0);
  const minOverlapRatio = Math.max(0.05, Number((config && config.PLAYER_STATS_TA_MIN_CANONICAL_OVERLAP_RATIO) || 0.1));
  const minPlayersWithStats = Math.max(1, Number((config && config.PLAYER_STATS_TA_MIN_PLAYERS_WITH_STATS) || 2));
  const minDistinctPlayers = Math.max(2, Number((config && config.PLAYER_STATS_TA_MIN_DISTINCT_PLAYERS) || 6));
  const nonNullFeatureTotal = Number(nonNullByFeature.ranking || 0) + Number(nonNullByFeature.hold_pct || 0) + Number(nonNullByFeature.break_pct || 0);
  const minNonNullFeatureTotal = Math.max(1, Number((config && config.PLAYER_STATS_TA_MIN_NON_NULL_FEATURE_TOTAL) || 6));
  const minParsedRows = Math.max(1, Number((config && config.PLAYER_STATS_TA_MIN_PARSED_ROWS) || 25));
  const parsedRowCount = Number(info.parsed_row_count || 0);
  const hasEnoughParsedRows = parsedRowCount >= minParsedRows;
  const nonZeroNonNullFeatureTotal = Number(info.non_zero_non_null_feature_total || 0);
  const minNonZeroNonNullFeatureTotal = Math.max(1, Number((config && config.PLAYER_STATS_TA_MIN_NON_ZERO_NON_NULL_FEATURE_TOTAL) || 2));
  const distinctPlayers = Number(info.unique_players_parsed || 0);
  const hasSingleCharCanonicalNames = Number(nameSanity.invalid_single_letter_token || 0) > 0;
  const hasLowQualityNames = validNameRatio < minValidNameRatio || invalidNameRatio > 0.45;
  const hasLowOverlap = overlapRatio < minOverlapRatio;
  const hasEnoughFeatures = nonNullFeatureTotal >= minNonNullFeatureTotal;
  const hasEnoughNonZeroFeatures = nonZeroNonNullFeatureTotal >= minNonZeroNonNullFeatureTotal;
  const hasEnoughPlayers = Number(completeness.players_with_non_null_stats || 0) >= minPlayersWithStats;
  const hasEnoughDistinctPlayers = distinctPlayers >= minDistinctPlayers;
  const hasCoreMetricCoverage = Number(nonNullByFeature.ranking || 0) > 0
    && Number(nonNullByFeature.hold_pct || 0) > 0
    && Number(nonNullByFeature.break_pct || 0) > 0;
  const meetsThresholds = !hasLowQualityNames
    && !hasLowOverlap
    && hasEnoughParsedRows
    && hasEnoughFeatures
    && hasEnoughNonZeroFeatures
    && hasEnoughPlayers
    && hasEnoughDistinctPlayers
    && hasCoreMetricCoverage
    && !hasSingleCharCanonicalNames;
  const failureReason = hasSingleCharCanonicalNames
    ? 'ta_matchmx_name_quality_low'
    : (!hasEnoughParsedRows
      ? 'ta_matchmx_rows_low'
      : (!hasEnoughDistinctPlayers
        ? 'ta_matchmx_distinct_players_low'
        : (!hasCoreMetricCoverage
          ? 'ta_matchmx_feature_coverage_low'
          : (!hasEnoughNonZeroFeatures
            ? 'ta_matchmx_feature_coverage_low'
            : (hasLowQualityNames
              ? 'ta_matchmx_name_quality_low'
              : (hasLowOverlap ? 'ta_matchmx_overlap_low' : 'ta_matchmx_feature_coverage_low'))))));

  return {
    meets_thresholds: meetsThresholds,
    valid_name_ratio: roundNumber_(validNameRatio, 3),
    invalid_name_ratio: roundNumber_(invalidNameRatio, 3),
    min_valid_name_ratio: minValidNameRatio,
    overlap_ratio: roundNumber_(overlapRatio, 3),
    min_overlap_ratio: minOverlapRatio,
    parsed_row_count: parsedRowCount,
    min_parsed_row_count: minParsedRows,
    non_null_feature_total: nonNullFeatureTotal,
    min_non_null_feature_total: minNonNullFeatureTotal,
    has_core_metric_coverage: hasCoreMetricCoverage,
    non_zero_non_null_feature_total: nonZeroNonNullFeatureTotal,
    min_non_zero_non_null_feature_total: minNonZeroNonNullFeatureTotal,
    players_with_non_null_stats: Number(completeness.players_with_non_null_stats || 0),
    min_players_with_non_null_stats: minPlayersWithStats,
    unique_players_parsed: distinctPlayers,
    min_distinct_players: minDistinctPlayers,
    has_single_char_canonical_names: hasSingleCharCanonicalNames,
    non_null_by_feature: nonNullByFeature,
    reason_code: meetsThresholds ? 'ta_matchmx_ok' : failureReason,
  };
}

function emitNoUsableStatsPayloadAlert_(config, payload) {
  const alertPayload = Object.assign({
    reason_code: 'no_usable_stats_payload',
    source: 'ta_leaders',
    detected_at: new Date().toISOString(),
  }, payload || {});
  logTaLeadersCacheDiagnostic_('no_usable_stats_payload', alertPayload);
  if (typeof logDiagnosticEvent_ === 'function') {
    try {
      logDiagnosticEvent_(config || {}, 'no_usable_stats_payload', alertPayload, 1);
    } catch (e) {
      // Non-fatal alert path.
    }
  }
}

function extractLeadersJsUrl_(html, baseUrl) {
  const text = String(html || '');
  const match = text.match(/(?:https?:)?\/\/[^"'\s]*jsmatches\/[^"'\s]*leadersource[^"'\s]*wta\.js|\/?jsmatches\/[^"'\s]*leadersource[^"'\s]*wta\.js/i);
  if (!match || !match[0]) return '';

  const value = String(match[0]);
  if (/^https?:\/\//i.test(value)) return value;
  if (/^\/\//.test(value)) return 'https:' + value;

  const base = String(baseUrl || 'https://www.tennisabstract.com/').replace(/\/[^/]*$/, '/');
  const originMatch = String(baseUrl || '').match(/^https?:\/\/[^/]+/i);
  if (/^\//.test(value) && originMatch && originMatch[0]) return originMatch[0] + value;
  return base + value.replace(/^\/+/, '');
}

function extractMatchMxRows_(payload) {
  const text = String(payload || '');
  const currentFormat = extractMatchMxRowsFromStructuredAssignment_(text);
  if (currentFormat.rows.length) return currentFormat;

  const arrayLiteralFormat = extractMatchMxRowsFromArrayLiteral_(text);
  if (arrayLiteralFormat.rows.length) return arrayLiteralFormat;

  const legacyFormat = extractMatchMxRowsFromLegacyAssignments_(text);
  return legacyFormat;
}



function stripQuotedJsStrings_(text) {
  const source = String(text || '');
  let quote = '';
  let escaped = false;
  let out = '';
  for (let i = 0; i < source.length; i += 1) {
    const ch = source[i];
    if (quote) {
      if (escaped) {
        escaped = false;
      } else if (ch === '\\') {
        escaped = true;
      } else if (ch === quote) {
        quote = '';
      }
      out += ' ';
      continue;
    }

    if (ch === '"' || ch === "'") {
      quote = ch;
      out += ' ';
      continue;
    }
    out += ch;
  }
  return out;
}

function findExpressionEndIndex_(text, startIndex) {
  const source = String(text || '');
  const start = Number(startIndex || 0);
  let quote = '';
  let escaped = false;
  let depthSquare = 0;
  let depthCurly = 0;
  for (let i = start; i < source.length; i += 1) {
    const ch = source[i];
    if (quote) {
      if (escaped) {
        escaped = false;
      } else if (ch === '\\') {
        escaped = true;
      } else if (ch === quote) {
        quote = '';
      }
      continue;
    }
    if (ch === '"' || ch === "'") {
      quote = ch;
      continue;
    }
    if (ch === '[') depthSquare += 1;
    else if (ch === ']') depthSquare = Math.max(0, depthSquare - 1);
    else if (ch === '{') depthCurly += 1;
    else if (ch === '}') depthCurly = Math.max(0, depthCurly - 1);
    else if (ch === ';' && depthSquare === 0 && depthCurly === 0) return i;
  }
  return -1;
}

function safeEvaluateJsLiteral_(literalText) {
  const expression = String(literalText || '').trim();
  if (!expression) return null;
  const stripped = stripQuotedJsStrings_(expression);
  if (/[();=]/.test(stripped)) return null;
  if (/=>|\b(?:function|return|while|for|if|new|this|constructor|prototype)\b/.test(stripped)) return null;
  try {
    return Function('"use strict"; return (' + expression + ');')();
  } catch (e) {
    return null;
  }
}

function findRowsContainerInMatchMxValue_(value, path) {
  const containerPath = path || 'matchmx';
  if (Array.isArray(value)) {
    if (!value.length) return { rows: [], container_path: containerPath };
    const first = value[0];
    if (Array.isArray(first)) return { rows: value, container_path: containerPath };
    if (first && typeof first === 'object' && Array.isArray(first.rows)) {
      return { rows: first.rows, container_path: containerPath + '[0].rows' };
    }
  }
  if (value && typeof value === 'object') {
    const preferred = ['rows', 'data', 'matchmx', 'leaders'];
    for (let i = 0; i < preferred.length; i += 1) {
      const key = preferred[i];
      if (Array.isArray(value[key])) {
        return { rows: value[key], container_path: containerPath + '.' + key };
      }
    }
  }
  return { rows: [], container_path: containerPath };
}

function extractMatchMxRowsFromStructuredAssignment_(payloadText) {
  const text = String(payloadText || '');
  const assignmentMatch = /\b(?:var|let|const)\s+matchmx\s*=/.exec(text);
  if (!assignmentMatch) return { rows: [], diagnostics: { parser_format: 'structured_assignment', reason: 'assignment_missing' } };
  const expressionStart = assignmentMatch.index + assignmentMatch[0].length;
  const expressionEnd = findExpressionEndIndex_(text, expressionStart);
  if (expressionEnd < 0) return { rows: [], diagnostics: { parser_format: 'structured_assignment', reason: 'assignment_end_missing' } };

  const expression = text.slice(expressionStart, expressionEnd).trim();
  const parsed = safeEvaluateJsLiteral_(expression);
  if (!parsed) return { rows: [], diagnostics: { parser_format: 'structured_assignment', reason: 'assignment_eval_failed' } };

  const container = findRowsContainerInMatchMxValue_(parsed, 'matchmx');
  const rawRows = Array.isArray(container.rows) ? container.rows : [];
  const tokenRows = rawRows
    .filter(function (row) { return Array.isArray(row) && row.length >= PLAYER_STATS_MATCHMX_MIN_FIELD_COUNT; })
    .map(function (row) { return row.map(function (cell) { return cell === null || cell === undefined ? '' : String(cell); }); });
  const schema = detectMatchMxSchema_(tokenRows);
  const rows = [];
  for (let i = 0; i < tokenRows.length; i += 1) {
    const structured = buildStructuredMatchMxRow_(tokenRows[i], schema.mapping);
    if (isUsableStructuredMatchMxRow_(structured)) rows.push(structured);
  }
  const diagnostics = Object.assign({}, schema.diagnostics, {
    parser_format: 'structured_assignment',
    row_container_path: container.container_path,
    token_row_count: tokenRows.length,
    structured_row_count: rows.length,
    valid_name_ratio: computeValidNameRatio_(rows),
    non_null_by_feature: summarizeMatchMxNonNullByFeature_(rows),
  });
  return { rows: rows, diagnostics: diagnostics };
}

function extractMatchMxRowsFromArrayLiteral_(payloadText) {
  const text = String(payloadText || '');
  const assignmentMatch = /\bvar\s+matchmx\s*=/.exec(text);
  if (!assignmentMatch) return { rows: [], diagnostics: { parser_format: 'array_literal', reason: 'assignment_missing' } };

  const assignmentStart = assignmentMatch.index + assignmentMatch[0].length;
  const arrayStart = text.indexOf('[', assignmentStart);
  if (arrayStart < 0) return { rows: [], diagnostics: { parser_format: 'array_literal', reason: 'array_start_missing' } };

  let arrayEnd = findMatchingBracketIndex_(text, arrayStart);
  if (arrayEnd < 0) {
    const statementEnd = text.indexOf('];', arrayStart);
    if (statementEnd < 0) return { rows: [], diagnostics: { parser_format: 'array_literal', reason: 'array_end_missing' } };
    arrayEnd = statementEnd;
  }

  const literalText = text.slice(arrayStart, arrayEnd + 1);
  return parseMatchMxRowsFromArrayLiteralText_(literalText);
}

function extractMatchMxRowsFromLegacyAssignments_(payloadText) {
  const rows = [];
  const invalidRowSamples = [];
  const text = String(payloadText || '');
  const rowRegex = /matchmx\s*(?:\[\s*\d+\s*\])?\s*=\s*\[/g;
  let match;

  while ((match = rowRegex.exec(text)) !== null) {
    const openingIndex = match.index + match[0].lastIndexOf('[');
    const extracted = extractTopLevelArrayLiteralBody_(text, openingIndex);
    if (!extracted.ok) {
      if (invalidRowSamples.length < 5) invalidRowSamples.push({ reason: 'row_array_unterminated', at: openingIndex });
      continue;
    }
    rowRegex.lastIndex = Math.max(rowRegex.lastIndex, extracted.next_index);

    const tokens = parseJsArrayTokens_(extracted.body);
    if (!hasMinimumMatchMxSchemaColumns_(tokens)) {
      if (invalidRowSamples.length < 5) invalidRowSamples.push({ reason: 'row_shape_invalid_for_matchmx_schema', token_count: tokens.length, token_sample: tokens.slice(0, 8) });
      continue;
    }
    const structured = buildStructuredMatchMxRow_(tokens);
    if (!isUsableStructuredMatchMxRow_(structured)) {
      if (invalidRowSamples.length < 5) invalidRowSamples.push({ reason: 'row_unusable_after_normalization', player_name: structured.player_name || '', score: structured.score || '' });
      continue;
    }
    rows.push(structured);
  }

  return {
    rows: rows,
    diagnostics: {
      parser_format: 'legacy_assignment',
      structured_row_count: rows.length,
      first_invalid_rows: invalidRowSamples,
      valid_name_ratio: computeValidNameRatio_(rows),
      non_null_by_feature: summarizeMatchMxNonNullByFeature_(rows),
    },
  };
}

function parseMatchMxRowsFromArrayLiteralText_(arrayLiteralText) {
  const literal = String(arrayLiteralText || '').trim();
  if (!literal || literal[0] !== '[') return { rows: [], diagnostics: { parser_format: 'array_literal', reason: 'invalid_literal' } };

  const tokenRows = [];
  let depth = 0;
  let quote = '';
  let escaped = false;
  let rowStart = -1;

  for (let i = 1; i < literal.length; i += 1) {
    const ch = literal[i];
    if (quote) {
      if (escaped) {
        escaped = false;
      } else if (ch === '\\') {
        escaped = true;
      } else if (ch === quote) {
        quote = '';
      }
      continue;
    }

    if (ch === '"' || ch === "'") {
      quote = ch;
      continue;
    }

    if (ch === '[') {
      if (depth === 0) rowStart = i;
      depth += 1;
      continue;
    }

    if (ch === ']') {
      if (depth <= 0) break;
      depth -= 1;
      if (depth === 0 && rowStart >= 0) {
        const rowBody = literal.slice(rowStart + 1, i);
        const tokens = parseJsArrayTokens_(rowBody);
        if (hasMinimumMatchMxSchemaColumns_(tokens)) tokenRows.push(tokens);
        rowStart = -1;
      }
    }
  }

  const schema = detectMatchMxSchema_(tokenRows);
  const rows = [];
  for (let i = 0; i < tokenRows.length; i += 1) {
    const structured = buildStructuredMatchMxRow_(tokenRows[i], schema.mapping);
    if (isUsableStructuredMatchMxRow_(structured)) rows.push(structured);
  }

  const diagnostics = Object.assign({}, schema.diagnostics, {
    parser_format: 'array_literal',
    token_row_count: tokenRows.length,
    structured_row_count: rows.length,
    valid_name_ratio: computeValidNameRatio_(rows),
    non_null_by_feature: summarizeMatchMxNonNullByFeature_(rows),
  });
  return { rows: rows, diagnostics: diagnostics };
}

function detectMatchMxSchema_(tokenRows) {
  const rows = Array.isArray(tokenRows) ? tokenRows : [];
  const fallback = buildDefaultMatchMxSchema_();
  if (!rows.length) return { mapping: fallback, diagnostics: { schema_source: 'default_empty' } };

  const maxColumns = rows.reduce(function (maxSoFar, row) { return Math.max(maxSoFar, Array.isArray(row) ? row.length : 0); }, 0);
  let bestNameCol = -1;
  let bestNameRatio = 0;
  for (let col = 0; col < maxColumns; col += 1) {
    const ratio = measurePlayerNameColumnQuality_(rows, col);
    if (ratio > bestNameRatio) {
      bestNameRatio = ratio;
      bestNameCol = col;
    }
  }

  const mapping = buildDefaultMatchMxSchema_();
  const diagnostics = {
    schema_source: 'detected',
    candidate_player_name_col: bestNameCol,
    candidate_player_name_ratio: roundNumber_(bestNameRatio, 3),
    selected_player_name_col: bestNameRatio >= 0.45 ? bestNameCol : PLAYER_STATS_MATCHMX_ROW_IDX.PLAYER_NAME,
  };
  mapping.player_name = diagnostics.selected_player_name_col;
  mapping.opponent = findBestNameLikeColumn_(rows, maxColumns, mapping.player_name, 0.25, PLAYER_STATS_MATCHMX_ROW_IDX.OPPONENT);
  mapping.score = findBestScoreColumn_(rows, maxColumns, PLAYER_STATS_MATCHMX_ROW_IDX.SCORE);
  mapping.ranking = findBestNumericColumn_(rows, maxColumns, function (n) { return n >= 1 && n <= 5000; }, PLAYER_STATS_MATCHMX_ROW_IDX.RANKING, [mapping.player_name, mapping.opponent, mapping.score]);

  const used = [mapping.player_name, mapping.opponent, mapping.score, mapping.ranking];
  const pctMatcher = function (n) { return n >= 0 && n <= 100.5; };
  mapping.recent_form = findBestNumericColumn_(rows, maxColumns, pctMatcher, PLAYER_STATS_MATCHMX_ROW_IDX.RECENT_FORM, used);
  used.push(mapping.recent_form);
  mapping.surface_win_rate = findBestNumericColumn_(rows, maxColumns, pctMatcher, PLAYER_STATS_MATCHMX_ROW_IDX.SURFACE_WIN_RATE, used);
  used.push(mapping.surface_win_rate);
  mapping.hold_pct = findBestNumericColumn_(rows, maxColumns, pctMatcher, PLAYER_STATS_MATCHMX_ROW_IDX.HOLD_PCT, used);
  used.push(mapping.hold_pct);
  mapping.break_pct = findBestNumericColumn_(rows, maxColumns, pctMatcher, PLAYER_STATS_MATCHMX_ROW_IDX.BREAK_PCT, used);
  used.push(mapping.break_pct);

  mapping.bp_saved_pct = findBestNumericColumn_(rows, maxColumns, pctMatcher, PLAYER_STATS_MATCHMX_ROW_IDX.BP_SAVED_PCT, used);
  used.push(mapping.bp_saved_pct);
  mapping.bp_conv_pct = findBestNumericColumn_(rows, maxColumns, pctMatcher, PLAYER_STATS_MATCHMX_ROW_IDX.BP_CONV_PCT, used);
  used.push(mapping.bp_conv_pct);
  mapping.first_serve_in_pct = findBestNumericColumn_(rows, maxColumns, pctMatcher, PLAYER_STATS_MATCHMX_ROW_IDX.FIRST_SERVE_IN_PCT, used);
  used.push(mapping.first_serve_in_pct);
  mapping.first_serve_points_won_pct = findBestNumericColumn_(rows, maxColumns, pctMatcher, PLAYER_STATS_MATCHMX_ROW_IDX.FIRST_SERVE_POINTS_WON_PCT, used);
  used.push(mapping.first_serve_points_won_pct);
  mapping.second_serve_points_won_pct = findBestNumericColumn_(rows, maxColumns, pctMatcher, PLAYER_STATS_MATCHMX_ROW_IDX.SECOND_SERVE_POINTS_WON_PCT, used);
  used.push(mapping.second_serve_points_won_pct);
  mapping.return_points_won_pct = findBestNumericColumn_(rows, maxColumns, pctMatcher, PLAYER_STATS_MATCHMX_ROW_IDX.RETURN_POINTS_WON_PCT, used);
  used.push(mapping.return_points_won_pct);
  mapping.dr = findBestNumericColumn_(rows, maxColumns, function (n) { return n >= 0 && n <= 5; }, PLAYER_STATS_MATCHMX_ROW_IDX.DOMINANCE_RATIO, used);
  used.push(mapping.dr);
  mapping.tpw_pct = findBestNumericColumn_(rows, maxColumns, pctMatcher, PLAYER_STATS_MATCHMX_ROW_IDX.TOTAL_POINTS_WON_PCT, used);

  diagnostics.mapping = mapping;
  return { mapping: mapping, diagnostics: diagnostics };
}

function buildDefaultMatchMxSchema_() {
  return {
    date: PLAYER_STATS_MATCHMX_ROW_IDX.DATE,
    event: PLAYER_STATS_MATCHMX_ROW_IDX.EVENT,
    surface: PLAYER_STATS_MATCHMX_ROW_IDX.SURFACE,
    player_name: PLAYER_STATS_MATCHMX_ROW_IDX.PLAYER_NAME,
    opponent: PLAYER_STATS_MATCHMX_ROW_IDX.OPPONENT,
    score: PLAYER_STATS_MATCHMX_ROW_IDX.SCORE,
    ranking: PLAYER_STATS_MATCHMX_ROW_IDX.RANKING,
    recent_form: PLAYER_STATS_MATCHMX_ROW_IDX.RECENT_FORM,
    surface_win_rate: PLAYER_STATS_MATCHMX_ROW_IDX.SURFACE_WIN_RATE,
    hold_pct: PLAYER_STATS_MATCHMX_ROW_IDX.HOLD_PCT,
    break_pct: PLAYER_STATS_MATCHMX_ROW_IDX.BREAK_PCT,
    bp_saved_pct: PLAYER_STATS_MATCHMX_ROW_IDX.BP_SAVED_PCT,
    bp_conv_pct: PLAYER_STATS_MATCHMX_ROW_IDX.BP_CONV_PCT,
    first_serve_in_pct: PLAYER_STATS_MATCHMX_ROW_IDX.FIRST_SERVE_IN_PCT,
    first_serve_points_won_pct: PLAYER_STATS_MATCHMX_ROW_IDX.FIRST_SERVE_POINTS_WON_PCT,
    second_serve_points_won_pct: PLAYER_STATS_MATCHMX_ROW_IDX.SECOND_SERVE_POINTS_WON_PCT,
    return_points_won_pct: PLAYER_STATS_MATCHMX_ROW_IDX.RETURN_POINTS_WON_PCT,
    dr: PLAYER_STATS_MATCHMX_ROW_IDX.DOMINANCE_RATIO,
    tpw_pct: PLAYER_STATS_MATCHMX_ROW_IDX.TOTAL_POINTS_WON_PCT,
  };
}

function measurePlayerNameColumnQuality_(rows, columnIndex) {
  let total = 0;
  let valid = 0;
  rows.forEach(function (row) {
    const value = String((row && row[columnIndex]) || '').trim();
    if (!value) return;
    total += 1;
    if (isLikelyFullPlayerName_(value)) valid += 1;
  });
  return total > 0 ? valid / total : 0;
}

function isLikelyFullPlayerName_(value) {
  const text = String(value || '').trim();
  if (!text || text.length < 4) return false;
  if (!/^[A-Za-z .'-]+$/.test(text)) return false;
  const lowered = text.toLowerCase();
  if (/^(?:i|pm|am|qf|sf|f|w|l|ret|wo)$/.test(lowered)) return false;
  const parts = text.split(/\s+/).filter(Boolean);
  if (parts.length < 2) return false;
  return parts.every(function (part) { return part.length >= 2; });
}

function findBestNameLikeColumn_(rows, maxColumns, excludeColumn, minRatio, fallback) {
  let bestIndex = fallback;
  let bestRatio = 0;
  for (let col = 0; col < maxColumns; col += 1) {
    if (col === excludeColumn) continue;
    const ratio = measurePlayerNameColumnQuality_(rows, col);
    if (ratio > bestRatio) {
      bestRatio = ratio;
      bestIndex = col;
    }
  }
  return bestRatio >= minRatio ? bestIndex : fallback;
}

function findBestScoreColumn_(rows, maxColumns, fallback) {
  const scoreRegex = /\d\s*[-:]\s*\d|\bret\b|\bwo\b/i;
  let bestIndex = fallback;
  let bestRatio = 0;
  for (let col = 0; col < maxColumns; col += 1) {
    let total = 0;
    let valid = 0;
    rows.forEach(function (row) {
      const value = String((row && row[col]) || '').trim();
      if (!value) return;
      total += 1;
      if (scoreRegex.test(value)) valid += 1;
    });
    const ratio = total > 0 ? valid / total : 0;
    if (ratio > bestRatio) {
      bestRatio = ratio;
      bestIndex = col;
    }
  }
  return bestRatio >= 0.2 ? bestIndex : fallback;
}

function findBestNumericColumn_(rows, maxColumns, predicate, fallback, excludedColumns) {
  const excluded = {};
  (excludedColumns || []).forEach(function (index) { excluded[index] = true; });
  let bestIndex = fallback;
  let bestScore = 0;
  for (let col = 0; col < maxColumns; col += 1) {
    if (excluded[col]) continue;
    let total = 0;
    let valid = 0;
    rows.forEach(function (row) {
      const raw = String((row && row[col]) || '').trim();
      if (!raw) return;
      const value = Number(raw);
      if (!Number.isFinite(value)) return;
      total += 1;
      if (predicate(value) || predicate(value * 100)) valid += 1;
    });
    const score = total > 0 ? valid / total : 0;
    if (score > bestScore) {
      bestScore = score;
      bestIndex = col;
    }
  }
  return bestScore >= 0.35 ? bestIndex : fallback;
}

function computeValidNameRatio_(rows) {
  const allRows = Array.isArray(rows) ? rows : [];
  if (!allRows.length) return 0;
  let valid = 0;
  allRows.forEach(function (row) {
    const canonical = canonicalizePlayerName_(String((row && row.player_name) || '').trim(), {});
    if (canonical && canonical.length >= 4 && canonical.indexOf(' ') > 0) valid += 1;
  });
  return valid / allRows.length;
}

function summarizeMatchMxNonNullByFeature_(rows) {
  const allRows = Array.isArray(rows) ? rows : [];
  const features = ['ranking', 'recent_form', 'surface_win_rate', 'hold_pct', 'break_pct'];
  const out = {};
  features.forEach(function (feature) { out[feature] = 0; });
  allRows.forEach(function (row) {
    features.forEach(function (feature) {
      if (row && row[feature] !== null && row[feature] !== undefined) out[feature] += 1;
    });
  });
  return out;
}

function findMatchingBracketIndex_(text, openingIndex) {
  const source = String(text || '');
  const start = Number(openingIndex || 0);
  if (start < 0 || source[start] !== '[') return -1;

  let depth = 0;
  let quote = '';
  let escaped = false;
  for (let i = start; i < source.length; i += 1) {
    const ch = source[i];
    if (quote) {
      if (escaped) {
        escaped = false;
      } else if (ch === '\\') {
        escaped = true;
      } else if (ch === quote) {
        quote = '';
      }
      continue;
    }

    if (ch === '"' || ch === "'") {
      quote = ch;
      continue;
    }

    if (ch === '[') {
      depth += 1;
      continue;
    }

    if (ch === ']') {
      depth -= 1;
      if (depth === 0) return i;
    }
  }

  return -1;
}

function summarizeTaLeadersParseDiagnostics_(rows, statsByPlayer, extractionDiagnostics, canonicalPlayers) {
  const parsedRows = Array.isArray(rows) ? rows : [];
  const normalizedMap = statsByPlayer && typeof statsByPlayer === 'object' ? statsByPlayer : {};
  const uniquePlayers = {};
  const parsedPlayerKeys = {};
  const sampleBefore = [];
  const sampleAfter = [];
  const sampleLimit = 8;
  const uniqueSanity = {
    total: 0,
    valid: 0,
    invalid_single_letter_token: 0,
    invalid_min_length: 0,
    invalid_alpha_ratio: 0,
  };

  parsedRows.forEach(function (row) {
    const rawPlayerName = String((row && row.player_name) || '').trim();
    if (rawPlayerName) {
      parsedPlayerKeys[rawPlayerName.toLowerCase()] = true;
      if (sampleBefore.length < sampleLimit) sampleBefore.push(rawPlayerName);
    }
    const canonical = canonicalizePlayerName_(rawPlayerName, {});
    if (canonical) {
      if (!uniquePlayers[canonical]) {
        uniquePlayers[canonical] = true;
        uniqueSanity.total += 1;
        const sanity = evaluateCanonicalPlayerNameSanity_(canonical);
        if (sanity.is_valid) uniqueSanity.valid += 1;
        if (sanity.invalid_single_letter_token) uniqueSanity.invalid_single_letter_token += 1;
        if (sanity.invalid_min_length) uniqueSanity.invalid_min_length += 1;
        if (sanity.invalid_alpha_ratio) uniqueSanity.invalid_alpha_ratio += 1;
      }
      if (sampleAfter.length < sampleLimit) sampleAfter.push(canonical);
    }
  });

  const requestedSamplesBefore = Object.keys(normalizedMap).slice(0, sampleLimit);
  const requestedSamplesAfter = requestedSamplesBefore.map(function (name) {
    return canonicalizePlayerName_(name, {});
  });
  const parsedCanonicalSet = uniquePlayers;
  const requestedCanonicalSet = {};
  const scheduledCanonicalPlayers = dedupePlayerNames_(canonicalPlayers || []);
  requestedSamplesAfter.forEach(function (name) {
    if (name) requestedCanonicalSet[name] = true;
  });
  scheduledCanonicalPlayers.forEach(function (name) {
    const canonical = canonicalizePlayerName_(name, {});
    if (canonical) requestedCanonicalSet[canonical] = true;
  });

  const overlapCanonicalSamples = [];
  Object.keys(parsedCanonicalSet).forEach(function (name) {
    if (requestedCanonicalSet[name] && overlapCanonicalSamples.length < sampleLimit) {
      overlapCanonicalSamples.push(name);
    }
  });
  const parsedUniqueCount = Object.keys(parsedCanonicalSet).length;
  const overlapCanonicalTotal = Object.keys(parsedCanonicalSet).reduce(function (count, name) {
    return requestedCanonicalSet[name] ? count + 1 : count;
  }, 0);
  const overlapWithScheduled = scheduledCanonicalPlayers.reduce(function (count, name) {
    const canonical = canonicalizePlayerName_(name, {});
    return canonical && parsedCanonicalSet[canonical] ? count + 1 : count;
  }, 0);

  let rankingNonNull = 0;
  let holdPctNonNull = 0;
  let breakPctNonNull = 0;
  let rankingNonZero = 0;
  let holdPctNonZero = 0;
  let breakPctNonZero = 0;
  Object.keys(normalizedMap).forEach(function (player) {
    const stats = normalizedMap[player];
    if (!stats || typeof stats !== 'object') return;
    if (stats.ranking !== null && stats.ranking !== undefined) {
      rankingNonNull += 1;
      if (Number(stats.ranking) !== 0) rankingNonZero += 1;
    }
    if (stats.hold_pct !== null && stats.hold_pct !== undefined) {
      holdPctNonNull += 1;
      if (Number(stats.hold_pct) !== 0) holdPctNonZero += 1;
    }
    if (stats.break_pct !== null && stats.break_pct !== undefined) {
      breakPctNonNull += 1;
      if (Number(stats.break_pct) !== 0) breakPctNonZero += 1;
    }
  });

  return Object.assign({}, extractionDiagnostics || {}, {
    parsed_row_count: parsedRows.length,
    parsed_player_key_count: Object.keys(parsedPlayerKeys).length,
    unique_players_parsed: parsedUniqueCount,
    parsed_player_key_samples_before_normalization: sampleBefore,
    parsed_player_key_samples_after_normalization: sampleAfter,
    requested_player_key_samples_before_normalization: requestedSamplesBefore,
    requested_player_key_samples_after_normalization: requestedSamplesAfter,
    canonical_player_key_overlap_samples: overlapCanonicalSamples,
    canonical_name_sanity: {
      total_unique: uniqueSanity.total,
      valid_unique: uniqueSanity.valid,
      valid_ratio: uniqueSanity.total > 0 ? uniqueSanity.valid / uniqueSanity.total : 0,
      invalid_single_letter_token: uniqueSanity.invalid_single_letter_token,
      invalid_min_length: uniqueSanity.invalid_min_length,
      invalid_alpha_ratio: uniqueSanity.invalid_alpha_ratio,
    },
    canonical_overlap: {
      parsed_unique: parsedUniqueCount,
      overlap_total: overlapCanonicalTotal,
      overlap_ratio: parsedUniqueCount > 0 ? overlapCanonicalTotal / parsedUniqueCount : 0,
      scheduled_total: scheduledCanonicalPlayers.length,
      overlap_with_scheduled: overlapWithScheduled,
      overlap_with_scheduled_ratio: scheduledCanonicalPlayers.length > 0 ? overlapWithScheduled / scheduledCanonicalPlayers.length : 0,
    },
    non_null_feature_count_by_field: {
      ranking: rankingNonNull,
      hold_pct: holdPctNonNull,
      break_pct: breakPctNonNull,
    },
    normalized_non_null_counts: {
      ranking: rankingNonNull,
      hold_pct: holdPctNonNull,
      break_pct: breakPctNonNull,
    },
    normalized_non_zero_non_null_counts: {
      ranking: rankingNonZero,
      hold_pct: holdPctNonZero,
      break_pct: breakPctNonZero,
    },
    non_zero_non_null_feature_total: rankingNonZero + holdPctNonZero + breakPctNonZero,
  });
}

function evaluateCanonicalPlayerNameSanity_(canonicalName) {
  const text = String(canonicalName || '').trim().replace(/\s+/g, ' ');
  const tokens = text ? text.split(' ') : [];
  const letters = text.replace(/[^A-Za-z]/g, '').length;
  const alphaRatio = text.length > 0 ? letters / text.length : 0;
  const hasSingleLetterToken = tokens.some(function (token) {
    return token.replace(/[^A-Za-z]/g, '').length === 1;
  });
  const invalidMinLength = text.length < 5;
  const invalidAlphaRatio = alphaRatio < 0.7;
  return {
    is_valid: !hasSingleLetterToken && !invalidMinLength && !invalidAlphaRatio,
    invalid_single_letter_token: hasSingleLetterToken,
    invalid_min_length: invalidMinLength,
    invalid_alpha_ratio: invalidAlphaRatio,
    alpha_ratio: alphaRatio,
  };
}

function isTaLeadersCoverageMismatch_(diagnostics) {
  const info = diagnostics || {};
  const parsedRowCount = Number(info.parsed_row_count || 0);
  const nonNull = info.normalized_non_null_counts || {};
  const normalizedTotal = Number(nonNull.ranking || 0) + Number(nonNull.hold_pct || 0) + Number(nonNull.break_pct || 0);
  return parsedRowCount > 500 && normalizedTotal <= 3;
}

function persistTaLeadersParseDiagnostics_(payload) {
  const diagnostic = Object.assign({}, payload || {});
  logTaLeadersCacheDiagnostic_('ta_parse_diagnostics', diagnostic);
  try {
    const existing = getStateJson_('PLAYER_STATS_LAST_FETCH_META') || {};
    setStateValue_('PLAYER_STATS_LAST_FETCH_META', JSON.stringify(Object.assign({}, existing, {
      ta_parse_diagnostic: diagnostic,
    })));
  } catch (e) {
    // Non-fatal best-effort diagnostics only.
  }
}

function parseJsArrayTokens_(arrayLiteralBody) {
  const tokens = splitTopLevelJsArrayTokens_(arrayLiteralBody);
  return tokens.map(function (token) {
    const stripped = String(token || '').trim();
    const lowered = stripped.toLowerCase();
    if (lowered === '' || lowered === 'null' || lowered === 'undefined' || lowered === 'nan') return '';
    if ((stripped[0] === '"' && stripped[stripped.length - 1] === '"') || (stripped[0] === "'" && stripped[stripped.length - 1] === "'")) {
      return unescapeJsStringToken_(stripped.slice(1, -1));
    }
    return stripped;
  });
}

function splitTopLevelJsArrayTokens_(arrayLiteralBody) {
  const body = String(arrayLiteralBody || '');
  const tokens = [];
  let current = '';
  let quote = '';
  let escaped = false;
  let bracketDepth = 0;
  let braceDepth = 0;
  let parenDepth = 0;

  for (let i = 0; i < body.length; i += 1) {
    const ch = body[i];
    if (escaped) {
      current += ch;
      escaped = false;
      continue;
    }
    if (quote) {
      if (ch === '\\') {
        current += ch;
        escaped = true;
        continue;
      }
      current += ch;
      if (ch === quote) quote = '';
      continue;
    }
    if (ch === '"' || ch === "'") {
      quote = ch;
      current += ch;
      continue;
    }
    if (ch === '[') {
      bracketDepth += 1;
      current += ch;
      continue;
    }
    if (ch === ']') {
      bracketDepth = Math.max(0, bracketDepth - 1);
      current += ch;
      continue;
    }
    if (ch === '{') {
      braceDepth += 1;
      current += ch;
      continue;
    }
    if (ch === '}') {
      braceDepth = Math.max(0, braceDepth - 1);
      current += ch;
      continue;
    }
    if (ch === '(') {
      parenDepth += 1;
      current += ch;
      continue;
    }
    if (ch === ')') {
      parenDepth = Math.max(0, parenDepth - 1);
      current += ch;
      continue;
    }
    if (ch === ',' && bracketDepth === 0 && braceDepth === 0 && parenDepth === 0) {
      tokens.push(current.trim());
      current = '';
      continue;
    }
    current += ch;
  }
  if (current || /,\s*$/.test(body)) {
    tokens.push(current.trim());
  }
  return tokens;
}

function unescapeJsStringToken_(value) {
  const text = String(value || '');
  return text
    .replace(/\\n/g, '\n')
    .replace(/\\r/g, '\r')
    .replace(/\\t/g, '\t')
    .replace(/\\"/g, '"')
    .replace(/\\'/g, "'")
    .replace(/\\\\/g, '\\');
}

function extractTopLevelArrayLiteralBody_(text, openingIndex) {
  const source = String(text || '');
  const start = Number(openingIndex || 0);
  if (start < 0 || source[start] !== '[') return { ok: false, body: '', next_index: start };
  const end = findMatchingBracketIndex_(source, start);
  if (end < 0) return { ok: false, body: '', next_index: source.length };
  return { ok: true, body: source.slice(start + 1, end), next_index: end + 1 };
}

function hasMinimumMatchMxSchemaColumns_(tokens) {
  return Array.isArray(tokens) && tokens.length >= PLAYER_STATS_MATCHMX_MIN_FIELD_COUNT;
}

function buildStructuredMatchMxRow_(tokens, schemaMapping) {
  const mapping = schemaMapping || buildDefaultMatchMxSchema_();

  function pick(index) {
    return Number.isInteger(index) && index >= 0 && index < tokens.length ? tokens[index] : '';
  }

  const score = String(pick(mapping.score) || '').trim();
  const hasWalkoverOrRet = /\b(?:ret|wo)\b/i.test(score);
  const numericStats = [];
  for (let i = 0; i < tokens.length; i += 1) {
    const value = Number(tokens[i]);
    numericStats.push(Number.isFinite(value) ? value : null);
  }

  function take(index) {
    const raw = pick(index);
    const value = Number(raw);
    return Number.isFinite(value) ? value : null;
  }

  return {
    date: String(pick(mapping.date) || ''),
    event: String(pick(mapping.event) || ''),
    surface: String(pick(mapping.surface) || ''),
    player_name: String(pick(mapping.player_name) || ''),
    opponent: String(pick(mapping.opponent) || ''),
    score: score,
    ranking: take(mapping.ranking),
    recent_form: hasWalkoverOrRet ? null : take(mapping.recent_form),
    surface_win_rate: hasWalkoverOrRet ? null : take(mapping.surface_win_rate),
    hold_pct: hasWalkoverOrRet ? null : take(mapping.hold_pct),
    break_pct: hasWalkoverOrRet ? null : take(mapping.break_pct),
    bp_saved_pct: hasWalkoverOrRet ? null : take(mapping.bp_saved_pct),
    bp_conv_pct: hasWalkoverOrRet ? null : take(mapping.bp_conv_pct),
    first_serve_in_pct: hasWalkoverOrRet ? null : take(mapping.first_serve_in_pct),
    first_serve_points_won_pct: hasWalkoverOrRet ? null : take(mapping.first_serve_points_won_pct),
    second_serve_points_won_pct: hasWalkoverOrRet ? null : take(mapping.second_serve_points_won_pct),
    return_points_won_pct: hasWalkoverOrRet ? null : take(mapping.return_points_won_pct),
    dr: hasWalkoverOrRet ? null : take(mapping.dr),
    tpw_pct: hasWalkoverOrRet ? null : take(mapping.tpw_pct),
    numeric_stats: numericStats,
  };
}


function hasNonNullCoreMatchMxStats_(row) {
  if (!row || typeof row !== 'object') return false;
  return row.ranking !== null && row.ranking !== undefined
    && row.hold_pct !== null && row.hold_pct !== undefined
    && row.break_pct !== null && row.break_pct !== undefined;
}

function isUsableStructuredMatchMxRow_(row) {
  if (!row || typeof row !== 'object') return false;
  if (!row.player_name || !isLikelyFullPlayerName_(row.player_name)) return false;
  if (!row.score) return false;
  return hasNonNullCoreMatchMxStats_(row);
}


function playerStatsFetchWithRetry_(url, requestOptions, config) {
  const maxRetries = Math.max(0, Number(config.PLAYER_STATS_FETCH_MAX_RETRIES || 3));
  const baseMs = Math.max(0, Number(config.PLAYER_STATS_FETCH_BACKOFF_BASE_MS || 300));
  const maxMs = Math.max(baseMs, Number(config.PLAYER_STATS_FETCH_BACKOFF_MAX_MS || 4000));
  let attempts = 0;
  let lastStatus = 0;

  for (let retry = 0; retry <= maxRetries; retry += 1) {
    attempts += 1;
    let response;
    try {
      response = UrlFetchApp.fetch(url, Object.assign({}, requestOptions || {}, { muteHttpExceptions: true }));
    } catch (e) {
      if (retry >= maxRetries) {
        return { ok: false, reason_code: 'ta_fetch_transport_error', api_call_count: attempts, error: e && e.message };
      }
      sleepBackoffWithJitter_(baseMs, maxMs, retry);
      continue;
    }

    const status = Number(response.getResponseCode() || 0);
    lastStatus = status;
    if (status >= 200 && status < 300) {
      return { ok: true, response: response, api_call_count: attempts, status_code: status };
    }

    if (!isTransientFetchStatus_(status) || retry >= maxRetries) {
      return { ok: false, reason_code: 'ta_http_' + status, api_call_count: attempts, status_code: status };
    }

    sleepBackoffWithJitter_(baseMs, maxMs, retry);
  }

  return { ok: false, reason_code: lastStatus ? ('ta_http_' + lastStatus) : 'ta_fetch_failed', api_call_count: attempts };
}

function isTransientFetchStatus_(statusCode) {
  const code = Number(statusCode || 0);
  return code === 408 || code === 425 || code === 429 || code === 500 || code === 502 || code === 503 || code === 504;
}

function sleepBackoffWithJitter_(baseMs, maxMs, retry) {
  const cap = Math.max(0, Number(maxMs || 0));
  const base = Math.max(0, Number(baseMs || 0));
  const exp = Math.min(cap || base, base * Math.pow(2, Math.max(0, Number(retry || 0))));
  const jitter = exp > 0 ? Math.floor(Math.random() * (Math.floor(exp * 0.3) + 1)) : 0;
  const delay = Math.min(cap || exp, exp + jitter);
  if (delay > 0) Utilities.sleep(delay);
}

function sleepTennisAbstractRequestGap_(config) {
  const baseDelay = Math.max(0, Number(config.PLAYER_STATS_TA_REQUEST_DELAY_MS || 0));
  const jitterMax = Math.max(0, Number(config.PLAYER_STATS_TA_REQUEST_JITTER_MS || 0));
  const jitter = jitterMax > 0 ? Math.floor(Math.random() * (jitterMax + 1)) : 0;
  const delay = baseDelay + jitter;
  if (delay > 0) Utilities.sleep(delay);
}

function parsePlayerStatsSourceConfigs_(config) {
  const baseValue = String(config.PLAYER_STATS_API_BASE_URL || '').trim();
  if (!baseValue) return [];
  const disableSofascore = !!(config && config.DISABLE_SOFASCORE);
  return baseValue.split(',')
    .map(function (value) {
      const token = String(value || '').trim();
      if (!token) return null;
      const pair = token.split('=');
      let sourceName = '';
      let baseUrl = token;
      if (pair.length >= 2) {
        sourceName = String(pair[0] || '').trim();
        baseUrl = pair.slice(1).join('=').trim();
      }
      const canonicalSourceName = canonicalizeStatsProviderName_(sourceName || inferStatsProviderNameFromUrl_(baseUrl));
      if (!String(baseUrl || '').trim() && canonicalSourceName === 'itf') {
        baseUrl = String(config.PLAYER_STATS_ITF_ENDPOINT || '').trim();
      }
      baseUrl = String(baseUrl || '').replace(/\/+$/, '');
      if (!baseUrl) return null;
      if (disableSofascore && canonicalSourceName === 'sofascore') return null;
      return {
        source_name: canonicalSourceName,
        base_url: baseUrl,
      };
    })
    .filter(function (value) { return !!value && !!value.base_url; });
}

function fetchPlayerStatsFromSingleSource_(sourceConfig, config, players, asOfTime) {
  const sourceName = canonicalizeStatsProviderName_(sourceConfig && sourceConfig.source_name);
  const baseUrl = sourceConfig && sourceConfig.base_url ? sourceConfig.base_url : '';
  if (sourceName === 'sofascore') {
    if (config && config.DISABLE_SOFASCORE) {
      return {
        ok: false,
        reason_code: 'player_stats_sofascore_disabled',
        stats_by_player: {},
        api_call_count: 0,
        source_name: 'sofascore',
        attempted_endpoints: [],
        missing_fields: [],
        contract_check_passed: true,
      };
    }
    return fetchPlayerStatsFromSofascore_(config, players, asOfTime);
  }
  if (sourceName === 'itf') {
    return fetchPlayerStatsFromItfRankings_(sourceConfig, config, players);
  }
  if (sourceName === 'tennis_abstract') {
    const taResult = fetchPlayerStatsFromLeadersSource_(players, config, asOfTime);
    const parserReady = taLeadersParserReadyForMerge_(taResult);
    return {
      ok: taResult.ok && parserReady,
      reason_code: taResult.reason_code || (parserReady ? 'ta_matchmx_ok' : 'ta_matchmx_parse_failed'),
      stats_by_player: taResult.stats_by_player || {},
      api_call_count: Number(taResult.api_call_count || 0),
      source_name: 'tennis_abstract',
      attempted_endpoints: [String(config.PLAYER_STATS_TA_LEADERS_URL || 'https://www.tennisabstract.com/cgi-bin/leaders_wta.cgi?players=top')],
      missing_fields: parserReady ? [] : ['ranking', 'hold_pct', 'break_pct'],
      contract_check_passed: parserReady,
      selection_metadata: taResult.selection_metadata || null,
    };
  }

  const endpoint = String(baseUrl || '').replace(/\/$/, '') + '/players/stats/batch';
  const body = JSON.stringify({
    players: players,
    as_of_time: asOfTime.toISOString(),
  });

  const headers = {
    Accept: 'application/json',
    'Content-Type': 'application/json',
  };
  if (String(config.PLAYER_STATS_API_KEY || '')) {
    headers['X-API-Key'] = String(config.PLAYER_STATS_API_KEY);
  }

  let response;
  try {
    response = UrlFetchApp.fetch(endpoint, {
      method: 'post',
      payload: body,
      headers: headers,
      muteHttpExceptions: true,
    });
  } catch (e) {
    return {
      ok: false,
      reason_code: 'player_stats_transport_error',
      api_call_count: 1,
      attempted_endpoints: [endpoint],
      missing_fields: ['ranking', 'recent_form', 'hold_pct', 'break_pct'],
      contract_check_passed: false,
    };
  }

  const status = Number(response.getResponseCode() || 0);
  if (status < 200 || status >= 300) {
    return {
      ok: false,
      reason_code: 'player_stats_http_' + status,
      api_call_count: 1,
      attempted_endpoints: [endpoint],
      missing_fields: ['ranking', 'recent_form', 'hold_pct', 'break_pct'],
      contract_check_passed: false,
    };
  }

  let parsed;
  try {
    parsed = JSON.parse(response.getContentText() || '{}');
  } catch (e) {
    return {
      ok: false,
      reason_code: 'player_stats_parse_error',
      api_call_count: 1,
      attempted_endpoints: [endpoint],
      missing_fields: ['ranking', 'recent_form', 'hold_pct', 'break_pct'],
      contract_check_passed: false,
    };
  }

  const adapted = adaptGenericProviderStats_(normalizePlayerStatsResponse_(parsed, players), players, sourceName || 'unknown');
  const missingCore = summarizeMissingCoreFields_(adapted, players);

  return {
    ok: true,
    reason_code: 'player_stats_api_success',
    stats_by_player: adapted,
    api_call_count: 1,
    source_name: sourceConfig && sourceConfig.source_name ? sourceConfig.source_name : 'unknown',
    attempted_endpoints: [endpoint],
    missing_fields: missingCore,
    contract_check_passed: missingCore.length < PLAYER_STATS_COMPLETENESS_KEYS.length,
  };
}

function taLeadersParserReadyForMerge_(result) {
  if (!result || !result.ok) return false;
  const qualityGate = result.selection_metadata && result.selection_metadata.quality_gate;
  return !!(qualityGate && qualityGate.meets_thresholds === true);
}

function fetchPlayerStatsFromItfRankings_(sourceConfig, config, players) {
  const endpoint = String((sourceConfig && sourceConfig.base_url) || config.PLAYER_STATS_ITF_ENDPOINT || '').trim();
  if (!endpoint) return { ok: false, reason_code: 'itf_endpoint_invalid', api_call_count: 0, contract_check_passed: false, missing_keys: ['data.rankings'] };

  let response;
  try {
    response = UrlFetchApp.fetch(endpoint, {
      method: 'get',
      headers: { Accept: 'application/json' },
      muteHttpExceptions: true,
    });
  } catch (e) {
    return { ok: false, reason_code: 'itf_endpoint_invalid', api_call_count: 1, contract_check_passed: false, missing_keys: ['transport_error'] };
  }

  const status = Number(response.getResponseCode() || 0);
  const responseHeaders = response.getHeaders ? response.getHeaders() : {};
  const contentType = String(responseHeaders['Content-Type'] || responseHeaders['content-type'] || '');
  const body = String(response.getContentText() || '');
  const isJsonContentType = contentType.toLowerCase().indexOf('application/json') >= 0;

  if (status === 404) {
    return {
      ok: false,
      reason_code: 'itf_contract_http_404',
      api_call_count: 1,
      contract_check_passed: false,
      missing_keys: ['http_2xx', 'data.rankings'],
    };
  }

  if (!isJsonContentType) {
    return {
      ok: false,
      reason_code: 'itf_contract_non_json',
      api_call_count: 1,
      contract_check_passed: false,
      missing_keys: ['content-type:application/json', 'data.rankings'],
    };
  }

  let payload = null;
  try {
    payload = JSON.parse(body || '{}');
  } catch (e) {
    return {
      ok: false,
      reason_code: 'itf_contract_non_json',
      api_call_count: 1,
      contract_check_passed: false,
      missing_keys: ['json_parse_error', 'data.rankings'],
    };
  }

  const missingKeys = [];
  if (!(status >= 200 && status < 300)) missingKeys.push('http_2xx');
  if (!jsonPathExists_(payload, ['data', 'rankings'])) missingKeys.push('data.rankings');

  if (missingKeys.length) {
    return {
      ok: false,
      reason_code: 'itf_endpoint_invalid',
      api_call_count: 1,
      contract_check_passed: false,
      missing_keys: missingKeys,
    };
  }

  const rankingRows = extractItfRankingRows_(payload);
  const rankByPlayer = {};
  rankingRows.forEach(function (row) {
    if (!row || typeof row !== 'object') return;
    const name = String(row.player_name || row.playerName || row.name || ((row.player && row.player.name) || '')).trim();
    const canonical = canonicalizePlayerName_(name);
    if (!canonical) return;
    const ranking = toNullableNumber_(row.ranking);
    const rank = ranking !== null ? ranking : toNullableNumber_(row.rank);
    const position = rank !== null ? rank : toNullableNumber_(row.position);
    rankByPlayer[canonical] = {
      ranking: position,
      recent_form: normalizeRateMetric_(row.recent_form, row.form, row.win_rate, row.winPercentage),
      hold_pct: normalizeRateMetric_(row.hold_pct, row.holdPercentage, row.service_games_won_pct),
      break_pct: normalizeRateMetric_(row.break_pct, row.breakPercentage, row.return_games_won_pct),
    };
  });

  const statsByPlayer = {};
  dedupePlayerNames_(players || []).forEach(function (playerName) {
    const canonical = canonicalizePlayerName_(playerName);
    const sourceRow = Object.prototype.hasOwnProperty.call(rankByPlayer, canonical) ? rankByPlayer[canonical] : null;
    const ranking = sourceRow ? sourceRow.ranking : null;
    const recentForm = sourceRow ? sourceRow.recent_form : null;
    const holdPct = sourceRow ? sourceRow.hold_pct : null;
    const breakPct = sourceRow ? sourceRow.break_pct : null;
    const rankingOnly = ranking !== null && holdPct === null && breakPct === null;
    statsByPlayer[canonical] = {
      ranking: ranking,
      recent_form: recentForm,
      recent_form_last_10: null,
      surface_win_rate: null,
      hold_pct: holdPct,
      break_pct: breakPct,
      surface_recent_form: null,
      stats_confidence: rankingOnly ? 0.25 : (ranking !== null ? 0.45 : 0),
      stats_confidence_band: rankingOnly ? 'low_ranking_only' : (ranking !== null ? 'medium_partial' : 'none'),
      source_used: 'itf_rankings',
      fallback_mode: rankingOnly ? 'ranking_only' : 'partial_core',
      has_stats: ranking !== null,
    };
  });

  return {
    ok: true,
    reason_code: 'player_stats_api_success',
    stats_by_player: statsByPlayer,
    api_call_count: 1,
    source_name: 'itf',
    contract_check_passed: true,
    missing_keys: [],
  };
}

function jsonPathExists_(obj, path) {
  if (!obj || !path || !path.length) return false;
  let node = obj;
  for (let i = 0; i < path.length; i += 1) {
    const key = path[i];
    if (!node || typeof node !== 'object' || !Object.prototype.hasOwnProperty.call(node, key)) return false;
    node = node[key];
  }
  return true;
}

function adaptGenericProviderStats_(statsByPlayer, players, sourceName) {
  const map = statsByPlayer || {};
  const canonicalPlayers = dedupePlayerNames_(players || []);
  const out = {};
  canonicalPlayers.forEach(function (playerName) {
    const row = map[playerName] || {};
    const ranking = row.ranking !== null && row.ranking !== undefined ? row.ranking : parseIntegerMetric_(row.rank, row.world_ranking);
    const recentForm = row.recent_form !== null && row.recent_form !== undefined ? row.recent_form : normalizeRateMetric_(row.recent_form_last_10, row.surface_recent_form);
    const holdPct = row.hold_pct !== null && row.hold_pct !== undefined ? row.hold_pct : normalizeRateMetric_(row.surface_hold_pct);
    const breakPct = row.break_pct !== null && row.break_pct !== undefined ? row.break_pct : normalizeRateMetric_(row.surface_break_pct);
    const nonNullCore = [ranking, recentForm, holdPct, breakPct].filter(function (v) { return v !== null && v !== undefined; }).length;
    out[playerName] = Object.assign({}, row, {
      ranking: ranking,
      recent_form: recentForm,
      hold_pct: holdPct,
      break_pct: breakPct,
      stats_confidence: row.stats_confidence !== null && row.stats_confidence !== undefined
        ? row.stats_confidence
        : (nonNullCore >= 4 ? 0.75 : (nonNullCore >= 2 ? 0.5 : (nonNullCore === 1 ? 0.3 : 0))),
      source_used: String(row.source_used || sourceName || ''),
      fallback_mode: String(row.fallback_mode || (nonNullCore < 4 ? 'limited_features' : 'full')),
    });
  });
  return out;
}

function summarizeMissingCoreFields_(statsByPlayer, players) {
  const missing = {};
  PLAYER_STATS_COMPLETENESS_KEYS.forEach(function (k) { missing[k] = 0; });
  const canonicalPlayers = dedupePlayerNames_(players || []);
  canonicalPlayers.forEach(function (playerName) {
    const row = statsByPlayer && statsByPlayer[playerName] ? statsByPlayer[playerName] : {};
    PLAYER_STATS_COMPLETENESS_KEYS.forEach(function (k) {
      if (row[k] === null || row[k] === undefined) missing[k] += 1;
    });
  });
  return Object.keys(missing).filter(function (k) { return Number(missing[k] || 0) > 0; });
}

function extractItfRankingRows_(payload) {
  const rankingsNode = payload && payload.data && payload.data.rankings;
  if (Array.isArray(rankingsNode)) return rankingsNode;
  if (rankingsNode && typeof rankingsNode === 'object') {
    if (Array.isArray(rankingsNode.rows)) return rankingsNode.rows;
    if (Array.isArray(rankingsNode.items)) return rankingsNode.items;
    if (Array.isArray(rankingsNode.players)) return rankingsNode.players;
  }
  return [];
}

function fetchPlayerStatsFromSofascore_(config, players, asOfTime) {
  const baseUrl = 'https://api.sofascore.com/api/v1';
  const asOfDate = asOfTime instanceof Date ? asOfTime : new Date(asOfTime || Date.now());
  const dateToken = Utilities.formatDate(asOfDate, 'UTC', 'yyyy-MM-dd');
  const eventEndpoints = [
    { name: 'events_live', url: baseUrl + '/sport/tennis/events/live', contract: { required_top_level_keys: ['events'], expected_payload_shape: { events: 'array' } } },
    {
      name: 'scheduled_events_by_date',
      url: baseUrl + '/sport/tennis/scheduled-events/' + dateToken,
      contract: { required_top_level_keys: ['events'], expected_payload_shape: { events: 'array' } },
    },
  ];

  const participantIndex = {};
  const canonicalPlayers = dedupePlayerNames_(players || []);
  let apiCallCount = 0;
  let sourceUsed = 'sofascore_live';
  const attemptedEndpoints = [];

  for (let i = 0; i < eventEndpoints.length; i += 1) {
    const endpoint = eventEndpoints[i];
    const parsed = fetchSofascoreJson_(endpoint.url, config, endpoint.contract);
    attemptedEndpoints.push(endpoint.url);
    apiCallCount += Number(parsed.api_call_count || 0);
    if (!parsed.ok) continue;

    indexSofascoreParticipants_(parsed.payload, participantIndex);
    if (i === 0 && Object.keys(participantIndex).length > 0) sourceUsed = 'sofascore_live';
    if (i > 0 && Object.keys(participantIndex).length > 0) sourceUsed = 'sofascore_live+scheduled';

    const matchedPlayers = canonicalPlayers.filter(function (name) { return !!participantIndex[name]; });
    if (matchedPlayers.length >= canonicalPlayers.length || matchedPlayers.length >= 4) break;
  }

  const statsByPlayer = {};
  const endpointFeatureSourcesByPlayer = {};
  let sawDomainMismatch = false;
  canonicalPlayers.forEach(function (playerName) {
    const participant = participantIndex[playerName];
    if (!participant || !participant.id) {
      statsByPlayer[playerName] = {
        ranking: null,
        recent_form: null,
        surface_win_rate: null,
        hold_pct: null,
        break_pct: null,
        stats_confidence: 0,
        source_used: sourceUsed,
        fallback_mode: 'participant_unmatched',
      };
      return;
    }

    const enrichment = fetchSofascorePlayerEnrichment_(participant.id, config);
    apiCallCount += Number(enrichment.api_call_count || 0);
    attemptedEndpoints.push.apply(attemptedEndpoints, enrichment.attempted_endpoints || []);

    if (String(enrichment.reason_code || '') === 'source_entity_domain_mismatch') {
      sawDomainMismatch = true;
      const domainMismatchReason = String(enrichment.domain_mismatch_reason || 'source_entity_domain_mismatch');
      statsByPlayer[playerName] = {
        ranking: null,
        recent_form: null,
        surface_win_rate: null,
        hold_pct: null,
        break_pct: null,
        stats_confidence: 0,
        source_used: sourceUsed,
        fallback_mode: 'source_entity_domain_mismatch',
        has_stats: false,
        failure_reason: domainMismatchReason,
      };
      endpointFeatureSourcesByPlayer[playerName] = {
        ranking: null,
        recent_form: null,
        hold_pct: null,
        break_pct: null,
      };
      return;
    }

    const ranking = extractSofascoreRanking_(enrichment.detail_payload);
    const recentForm = extractSofascoreFormProxy_(enrichment.recent_payload, participant.id);
    const holdMetric = extractSofascoreMetricWithEndpoint_(enrichment.payloads_with_endpoints, ['holdPct', 'holdPercentage', 'serviceGamesWonPct', 'service_game_win_pct']);
    const breakMetric = extractSofascoreMetricWithEndpoint_(enrichment.payloads_with_endpoints, ['breakPct', 'breakPercentage', 'returnGamesWonPct', 'break_points_converted_pct']);
    const holdPct = holdMetric.value;
    const breakPct = breakMetric.value;
    const rankingOnly = ranking !== null && holdPct === null && breakPct === null;
    const nonNullCore = [ranking, recentForm, holdPct, breakPct].filter(function (v) { return v !== null && v !== undefined; }).length;

    statsByPlayer[playerName] = {
      ranking: ranking,
      recent_form: recentForm,
      surface_win_rate: null,
      hold_pct: holdPct,
      break_pct: breakPct,
      stats_confidence: nonNullCore >= 4 ? 0.72 : (nonNullCore >= 2 ? 0.52 : (rankingOnly ? 0.26 : (nonNullCore === 1 ? 0.3 : 0))),
      stats_confidence_band: nonNullCore >= 4 ? 'medium' : (rankingOnly ? 'low_ranking_only' : (nonNullCore >= 2 ? 'medium_partial' : 'low')),
      source_used: sourceUsed,
      fallback_mode: rankingOnly ? 'ranking_only' : (nonNullCore > 0 ? 'limited_features' : 'detail_unavailable'),
      has_stats: nonNullCore > 0 || rankingOnly,
    };

    endpointFeatureSourcesByPlayer[playerName] = {
      ranking: ranking !== null && ranking !== undefined ? enrichment.detail_endpoint : null,
      recent_form: recentForm !== null && recentForm !== undefined ? enrichment.recent_endpoint : null,
      hold_pct: holdPct !== null && holdPct !== undefined ? holdMetric.endpoint : null,
      break_pct: breakPct !== null && breakPct !== undefined ? breakMetric.endpoint : null,
    };
  });

  const missingCore = summarizeMissingCoreFields_(statsByPlayer, canonicalPlayers);
  const hasAnyStats = canonicalPlayers.some(function (playerName) {
    const row = statsByPlayer[playerName] || {};
    return row.ranking !== null || row.recent_form !== null || row.hold_pct !== null || row.break_pct !== null;
  });

  return {
    ok: true,
    reason_code: sawDomainMismatch && !hasAnyStats ? 'source_entity_domain_mismatch' : 'player_stats_sofascore_success',
    stats_by_player: statsByPlayer,
    endpoint_feature_sources_by_player: endpointFeatureSourcesByPlayer,
    api_call_count: apiCallCount,
    source_name: 'sofascore',
    attempted_endpoints: dedupeStringList_(attemptedEndpoints),
    missing_fields: missingCore,
    contract_check_passed: missingCore.length < PLAYER_STATS_COMPLETENESS_KEYS.length,
  };
}

function fetchSofascorePlayerEnrichment_(playerId, config) {
  const pid = encodeURIComponent(String(playerId || ''));
  const detailEndpoint = { name: 'player_detail', url: 'https://api.sofascore.com/api/v1/player/' + pid, contract: { required_top_level_keys: ['player'], expected_payload_shape: { player: 'object' } } };
  const recentEndpoint = { name: 'player_recent_events', url: 'https://api.sofascore.com/api/v1/player/' + pid + '/events/last/0', contract: { required_top_level_keys: ['events'], expected_payload_shape: { events: 'array' } } };
  const statsOverallEndpoint = { name: 'player_stats_overall', url: 'https://api.sofascore.com/api/v1/player/' + pid + '/statistics/overall', contract: { required_top_level_keys: ['statistics'], expected_payload_shape: { statistics: 'object_or_array' } } };
  const statsLast52Endpoint = { name: 'player_stats_last_52', url: 'https://api.sofascore.com/api/v1/player/' + pid + '/statistics/last/52', contract: { required_top_level_keys: ['statistics'], expected_payload_shape: { statistics: 'object_or_array' } } };

  const detail = fetchSofascoreJson_(detailEndpoint.url, config, detailEndpoint.contract);
  if (!detail.ok) {
    return {
      detail_payload: detail.payload || null,
      recent_payload: null,
      stats_payloads: [],
      payloads_with_endpoints: [
        { endpoint: detailEndpoint.url, payload: detail.payload || null },
      ],
      detail_endpoint: detailEndpoint.url,
      recent_endpoint: null,
      api_call_count: Number(detail.api_call_count || 0),
      attempted_endpoints: [detailEndpoint.url],
      reason_code: String(detail.reason_code || 'sofascore_player_detail_unavailable'),
    };
  }

  const detailSportSlug = getSofascorePlayerSportSlug_(detail.payload);
  if (detail.ok && detailSportSlug !== 'tennis') {
    const mismatchReason = detailSportSlug
      ? ('source_entity_domain_mismatch_non_tennis_sport_slug_' + detailSportSlug)
      : 'source_entity_domain_mismatch_non_tennis_missing_sport_slug';
    return {
      detail_payload: detail.payload,
      recent_payload: null,
      stats_payloads: [],
      payloads_with_endpoints: [
        { endpoint: detailEndpoint.url, payload: detail.payload },
      ],
      detail_endpoint: detailEndpoint.url,
      recent_endpoint: null,
      api_call_count: Number(detail.api_call_count || 0),
      attempted_endpoints: [detailEndpoint.url],
      reason_code: 'source_entity_domain_mismatch',
      domain_mismatch_reason: mismatchReason,
    };
  }

  const recent = fetchSofascoreJson_(recentEndpoint.url, config, recentEndpoint.contract);
  const statsOverall = fetchSofascoreJson_(statsOverallEndpoint.url, config, statsOverallEndpoint.contract);
  const statsLast52 = fetchSofascoreJson_(statsLast52Endpoint.url, config, statsLast52Endpoint.contract);

  return {
    detail_payload: detail.payload,
    recent_payload: recent.payload,
    stats_payloads: [statsOverall.payload, statsLast52.payload],
    payloads_with_endpoints: [
      { endpoint: detailEndpoint.url, payload: detail.payload },
      { endpoint: statsOverallEndpoint.url, payload: statsOverall.payload },
      { endpoint: statsLast52Endpoint.url, payload: statsLast52.payload },
    ],
    detail_endpoint: detailEndpoint.url,
    recent_endpoint: recentEndpoint.url,
    api_call_count: Number(detail.api_call_count || 0) + Number(recent.api_call_count || 0) + Number(statsOverall.api_call_count || 0) + Number(statsLast52.api_call_count || 0),
    attempted_endpoints: [detailEndpoint.url, recentEndpoint.url, statsOverallEndpoint.url, statsLast52Endpoint.url],
    reason_code: 'sofascore_ok',
  };
}

function isSofascoreTennisPlayer_(payload) {
  return getSofascorePlayerSportSlug_(payload) === 'tennis';
}

function getSofascorePlayerSportSlug_(payload) {
  const sport = payload && payload.player && payload.player.sport;
  if (!sport || typeof sport !== 'object') return '';
  const slug = String(sport.slug || '').toLowerCase();
  return slug || '';
}

function fetchSofascorePlayerDetail_(playerId, config) {
  return fetchSofascoreJson_('https://api.sofascore.com/api/v1/player/' + encodeURIComponent(String(playerId || '')), config);
}

function fetchSofascoreRecentForm_(playerId, config) {
  return fetchSofascoreJson_('https://api.sofascore.com/api/v1/player/' + encodeURIComponent(String(playerId || '')) + '/events/last/0', config);
}

function extractSofascoreHoldPct_(enrichment) {
  return extractSofascoreMetricWithEndpoint_((enrichment && enrichment.payloads_with_endpoints) || [], ['holdPct', 'holdPercentage', 'serviceGamesWonPct', 'service_game_win_pct']).value;
}

function extractSofascoreBreakPct_(enrichment) {
  return extractSofascoreMetricWithEndpoint_((enrichment && enrichment.payloads_with_endpoints) || [], ['breakPct', 'breakPercentage', 'returnGamesWonPct', 'break_points_converted_pct']).value;
}

function extractSofascoreMetricWithEndpoint_(payloadsWithEndpoints, keys) {
  for (let i = 0; i < (payloadsWithEndpoints || []).length; i += 1) {
    const entry = payloadsWithEndpoints[i] || {};
    const value = deepFindMetricValue_(entry.payload, keys || []);
    const normalized = normalizeRateMetric_(value);
    if (normalized !== null && normalized !== undefined) return { value: normalized, endpoint: entry.endpoint || null };
  }
  return { value: null, endpoint: null };
}

function fetchSofascoreJson_(url, config, contract) {
  const headers = {
    Accept: 'application/json',
    'User-Agent': String(config.PLAYER_STATS_FETCH_USER_AGENT || 'Mozilla/5.0 (compatible; WTA-Edge-Board/1.0)'),
  };
  let response;
  try {
    response = UrlFetchApp.fetch(url, {
      method: 'get',
      muteHttpExceptions: true,
      headers: headers,
      followRedirects: true,
      validateHttpsCertificates: true,
    });
  } catch (e) {
    return { ok: false, reason_code: 'sofascore_transport_error', payload: null, api_call_count: 1 };
  }

  const status = Number(response.getResponseCode() || 0);
  if (status < 200 || status >= 300) {
    return { ok: false, reason_code: 'sofascore_http_' + status, payload: null, api_call_count: 1 };
  }

  let payload = null;
  try {
    payload = JSON.parse(response.getContentText() || '{}');
  } catch (e) {
    return { ok: false, reason_code: 'sofascore_parse_error', payload: null, api_call_count: 1 };
  }

  if (isSofascore404ErrorPayload_(payload)) {
    return { ok: false, reason_code: 'sofascore_contract_404_error_payload', payload: null, api_call_count: 1 };
  }

  const contractResult = validateSofascoreEndpointContract_(payload, contract);
  if (!contractResult.ok) {
    return { ok: false, reason_code: contractResult.reason_code, payload: null, api_call_count: 1, missing_keys: contractResult.missing_keys || [] };
  }

  return {
    ok: true,
    reason_code: 'sofascore_ok',
    payload: payload,
    api_call_count: 1,
  };
}

function isSofascore404ErrorPayload_(payload) {
  if (!payload || typeof payload !== 'object') return false;
  const stack = [payload];
  while (stack.length) {
    const node = stack.pop();
    if (!node || typeof node !== 'object') continue;
    const code = Number(node.code || node.status || node.statusCode || node.httpStatus || 0);
    const message = String(node.message || node.error || node.description || '').toLowerCase();
    if (code === 404 && (message.indexOf('not found') >= 0 || message.length > 0)) return true;
    const keys = Object.keys(node);
    for (let i = 0; i < keys.length; i += 1) stack.push(node[keys[i]]);
  }
  return false;
}

function validateSofascoreEndpointContract_(payload, contract) {
  const currentContract = contract || {};
  const requiredKeys = currentContract.required_top_level_keys || [];
  const missingKeys = [];
  for (let i = 0; i < requiredKeys.length; i += 1) {
    const key = requiredKeys[i];
    if (!payload || typeof payload !== 'object' || !Object.prototype.hasOwnProperty.call(payload, key)) missingKeys.push(key);
  }
  if (missingKeys.length) {
    return { ok: false, reason_code: 'sofascore_contract_missing_keys', missing_keys: missingKeys };
  }

  const expectedShape = currentContract.expected_payload_shape || {};
  const shapeKeys = Object.keys(expectedShape);
  for (let i = 0; i < shapeKeys.length; i += 1) {
    const key = shapeKeys[i];
    const typeExpectation = String(expectedShape[key] || '');
    const value = payload ? payload[key] : undefined;
    const isArray = Array.isArray(value);
    const isObject = !!value && typeof value === 'object' && !isArray;
    if (typeExpectation === 'array' && !isArray) return { ok: false, reason_code: 'sofascore_contract_shape_invalid', missing_keys: [] };
    if (typeExpectation === 'object' && !isObject) return { ok: false, reason_code: 'sofascore_contract_shape_invalid', missing_keys: [] };
    if (typeExpectation === 'object_or_array' && !isObject && !isArray) return { ok: false, reason_code: 'sofascore_contract_shape_invalid', missing_keys: [] };
  }

  return { ok: true, reason_code: 'sofascore_contract_ok', missing_keys: [] };
}


function deepFindMetricValue_(node, keys) {
  const targetKeys = (keys || []).map(function (k) { return String(k || '').toLowerCase(); });
  const stack = [node];
  while (stack.length) {
    const cur = stack.pop();
    if (!cur || typeof cur !== 'object') continue;
    if (Array.isArray(cur)) {
      for (let i = 0; i < cur.length; i += 1) stack.push(cur[i]);
      continue;
    }
    const entries = Object.keys(cur);
    for (let i = 0; i < entries.length; i += 1) {
      const key = entries[i];
      const lower = key.toLowerCase();
      const value = cur[key];
      if (targetKeys.indexOf(lower) >= 0) return value;
      if (targetKeys.indexOf(lower.replace(/[^a-z0-9]/g, '')) >= 0) return value;
      stack.push(value);
    }
  }
  return null;
}

function dedupeStringList_(list) {
  const out = [];
  const seen = {};
  (list || []).forEach(function (item) {
    const token = String(item || '').trim();
    if (!token || seen[token]) return;
    seen[token] = true;
    out.push(token);
  });
  return out;
}

function indexSofascoreParticipants_(payload, index) {
  const target = index || {};
  const events = payload && Array.isArray(payload.events) ? payload.events : [];

  events.forEach(function (event) {
    const teams = [event && event.homeTeam, event && event.awayTeam].filter(function (team) { return !!team; });
    teams.forEach(function (team) {
      const teamId = Number(team.id || (team.player && team.player.id) || (team.team && team.team.id) || 0);
      if (!teamId) return;

      const nameCandidates = extractSofascoreTeamNameCandidates_(team);
      nameCandidates.forEach(function (candidate) {
        const variants = splitSofascoreParticipantNameVariants_(candidate);
        variants.forEach(function (variant) {
          const canonicalName = canonicalizePlayerName_(variant, {});
          if (!canonicalName) return;
          target[canonicalName] = {
            id: teamId,
            raw_name: String(candidate || variant || canonicalName),
          };
        });
      });
    });
  });

  return target;
}

function extractSofascoreTeamNameCandidates_(team) {
  const root = team || {};
  const out = [];
  const stack = [{ node: root, depth: 0 }];
  while (stack.length) {
    const cur = stack.pop();
    const node = cur.node;
    const depth = Number(cur.depth || 0);
    if (!node || typeof node !== 'object' || depth > 3) continue;

    if (Array.isArray(node)) {
      for (let i = 0; i < node.length; i += 1) stack.push({ node: node[i], depth: depth + 1 });
      continue;
    }

    const keys = Object.keys(node);
    for (let i = 0; i < keys.length; i += 1) {
      const key = keys[i];
      const value = node[key];
      if (typeof value === 'string') {
        const normalizedKey = key.toLowerCase();
        if (normalizedKey === 'name' || normalizedKey === 'shortname' || normalizedKey === 'slug') {
          out.push(normalizedKey === 'slug' ? value.replace(/[-_]+/g, ' ') : value);
        }
      } else if (value && typeof value === 'object') {
        stack.push({ node: value, depth: depth + 1 });
      }
    }
  }
  return dedupeStringList_(out);
}

function splitSofascoreParticipantNameVariants_(rawName) {
  const name = String(rawName || '').trim();
  if (!name) return [];
  const parts = name.split(/\s*(?:\/|&|\+|\band\b)\s*/i)
    .map(function (part) { return String(part || '').trim(); })
    .filter(function (part) { return !!part; });
  const variants = dedupeStringList_([name].concat(parts.length >= 2 ? parts : []));
  return variants.length ? variants : [name];
}

function extractSofascoreRanking_(payload) {
  const player = payload && payload.player ? payload.player : (payload || {});
  return parseIntegerMetric_(
    player.ranking,
    player.rank,
    player.worldRanking,
    player.currentRanking,
    player.wtaRanking,
    (player.statistics && player.statistics.ranking)
  );
}

function extractSofascoreFormProxy_(payload, playerId) {
  const events = payload && Array.isArray(payload.events) ? payload.events : [];
  if (!events.length) return null;

  const pid = Number(playerId || 0);
  let wins = 0;
  let samples = 0;
  const maxMatches = Math.min(10, events.length);

  for (let i = 0; i < maxMatches; i += 1) {
    const event = events[i] || {};
    const home = event.homeTeam || {};
    const away = event.awayTeam || {};
    const winnerCode = Number(event.winnerCode);
    const isHome = Number(home.id || 0) === pid;
    const isAway = Number(away.id || 0) === pid;
    if (!isHome && !isAway) continue;

    const won = (isHome && winnerCode === 1) || (isAway && winnerCode === 2);
    wins += won ? 1 : 0;
    samples += 1;
  }

  if (!samples) return null;
  return roundNumber_(wins / samples, 3);
}

function fetchPlayerStatsFromScrapeSources_(config, canonicalPlayers) {
  const templates = parseScrapeUrlTemplates_(config.PLAYER_STATS_SCRAPE_URLS);
  if (!templates.length || !canonicalPlayers.length) {
    return {
      ok: false,
      reason_code: 'player_stats_scrape_not_configured',
      stats_by_player: {},
      api_call_count: 0,
      scrape_call_count: 0,
    };
  }

  const statsByPlayer = {};
  let apiCallCount = 0;

  canonicalPlayers.forEach(function (playerName) {
    const slug = buildPlayerSlug_(playerName);
    let playerStats = null;

    templates.forEach(function (template) {
      if (playerStats) return;
      const url = String(template || '').replace(/\{player\}/g, encodeURIComponent(slug));
      if (!url) return;

      apiCallCount += 1;
      try {
        const response = UrlFetchApp.fetch(url, {
          method: 'get',
          muteHttpExceptions: true,
          headers: { Accept: 'text/html,application/json' },
        });

        if (Number(response.getResponseCode() || 0) < 200 || Number(response.getResponseCode() || 0) >= 300) return;
        playerStats = extractStatsFromScrapeContent_(response.getContentText() || '', playerName);
      } catch (e) {
        playerStats = null;
      }
    });

    if (playerStats) statsByPlayer[playerName] = playerStats;
  });

  return {
    ok: Object.keys(statsByPlayer).length > 0,
    reason_code: Object.keys(statsByPlayer).length > 0 ? 'player_stats_scrape_success' : 'player_stats_scrape_empty',
    stats_by_player: statsByPlayer,
    api_call_count: apiCallCount,
  };
}

function parseScrapeUrlTemplates_(value) {
  return String(value || '').split(',')
    .map(function (part) { return String(part || '').trim(); })
    .filter(function (part) { return !!part; });
}

function buildPlayerSlug_(canonicalPlayerName) {
  return String(canonicalPlayerName || '').toLowerCase().trim().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '');
}

function extractStatsFromScrapeContent_(content, playerName) {
  const text = String(content || '');
  if (!text) return null;

  const statLineRegex = /(ranking|rank|recent_form|recent win rate|surface_win_rate|hold_pct|break_pct)\s*[:=]\s*([0-9]{1,3}(?:\.[0-9]+)?)/ig;
  const values = {};
  let match;
  while ((match = statLineRegex.exec(text)) !== null) {
    values[String(match[1] || '').toLowerCase().replace(/\s+/g, '_')] = Number(match[2]);
  }

  if (!Object.keys(values).length) return null;
  const normalized = normalizePlayerStatsResponse_([Object.assign({ player_name: playerName }, values)], [playerName]);
  return normalized[canonicalizePlayerName_(playerName, {})] || null;
}

function mergePlayerStatsMaps_(sourcePayloads, canonicalPlayers, config) {
  const merged = {};
  const players = dedupePlayerNames_(canonicalPlayers || []);
  const cohortPolicy = buildPlayerStatsCohortPolicy_(config);
  const sourceMaps = (sourcePayloads || []).map(function (entry) {
    return {
      source_name: canonicalizeStatsProviderName_(entry && entry.source_name),
      stats_map: (entry && entry.stats_by_player) || {},
    };
  });

  players.forEach(function (player) {
    const featureSelection = {};
    PLAYER_STATS_CANONICAL_FEATURES.forEach(function (feature) {
      const preferredSources = resolvePreferredSourcesForFeature_(feature);
      featureSelection[feature] = selectMetricWithProvenanceBySourcePriority_(sourceMaps, player, feature, preferredSources);
    });

    const resolvedRanking = resolveCanonicalRanking_(sourceMaps, player, featureSelection.ranking.value);
    const mergedRow = {
      ranking: resolvedRanking.resolved_rank,
      resolved_rank: resolvedRanking.resolved_rank,
      rank_source: resolvedRanking.rank_source,
      rank_confidence: resolvedRanking.rank_confidence,
      is_top100: resolvedRanking.resolved_rank !== null && resolvedRanking.resolved_rank <= 100,
      recent_form: featureSelection.recent_form.value,
      recent_form_last_10: featureSelection.recent_form_last_10.value,
      surface_win_rate: featureSelection.surface_win_rate.value,
      hold_pct: featureSelection.hold_pct.value,
      break_pct: featureSelection.break_pct.value,
      surface_hold_pct: featureSelection.surface_hold_pct.value,
      surface_break_pct: featureSelection.surface_break_pct.value,
      surface_recent_form: featureSelection.surface_recent_form.value,
      bp_saved_pct: featureSelection.bp_saved_pct.value,
      bp_conv_pct: featureSelection.bp_conv_pct.value,
      first_serve_in_pct: featureSelection.first_serve_in_pct.value,
      first_serve_points_won_pct: featureSelection.first_serve_points_won_pct.value,
      second_serve_points_won_pct: featureSelection.second_serve_points_won_pct.value,
      return_points_won_pct: featureSelection.return_points_won_pct.value,
      dr: featureSelection.dr.value,
      tpw_pct: featureSelection.tpw_pct.value,
    };

    const valueOrigin = {};
    let placeholderFeatureCount = 0;
    let trustedFeatureCount = 0;
    PLAYER_STATS_CANONICAL_FEATURES.forEach(function (feature) {
      const selected = featureSelection[feature] || {};
      valueOrigin[feature] = selected.value_origin || 'defaulted';
      placeholderFeatureCount += selected.is_placeholder ? 1 : 0;
      trustedFeatureCount += selected.is_trusted ? 1 : 0;
    });

    mergedRow.value_origin = valueOrigin;
    mergedRow.placeholder_feature_count = placeholderFeatureCount;
    mergedRow.trusted_feature_count = trustedFeatureCount;

    const rankingOnlyFallback = mergedRow.ranking !== null && mergedRow.ranking !== undefined
      && (mergedRow.hold_pct === null || mergedRow.hold_pct === undefined)
      && (mergedRow.break_pct === null || mergedRow.break_pct === undefined);
    const nonNullCore = [mergedRow.ranking, mergedRow.recent_form, mergedRow.hold_pct, mergedRow.break_pct]
      .filter(function (value) { return value !== null && value !== undefined; }).length;
    const trustedCore = ['ranking', 'recent_form', 'hold_pct', 'break_pct']
      .filter(function (feature) { return !!(featureSelection[feature] && featureSelection[feature].is_trusted); }).length;
    const placeholderOnlyBundle = nonNullCore > 0 && trustedFeatureCount === 0;
    mergedRow.has_stats = nonNullCore > 0 || rankingOnlyFallback;
    if (rankingOnlyFallback) {
      mergedRow.stats_confidence = 0.2;
      mergedRow.stats_confidence_band = 'low_ranking_only';
    } else if (placeholderOnlyBundle) {
      mergedRow.stats_confidence = 0.08;
      mergedRow.stats_confidence_band = 'low_placeholder_only';
    } else if (nonNullCore >= 4 && trustedCore >= 3) {
      mergedRow.stats_confidence = 0.78;
      mergedRow.stats_confidence_band = 'high';
    } else if (nonNullCore >= 2 && trustedCore >= 1) {
      mergedRow.stats_confidence = 0.56;
      mergedRow.stats_confidence_band = 'medium';
    } else if (nonNullCore >= 1) {
      mergedRow.stats_confidence = 0.32;
      mergedRow.stats_confidence_band = 'low';
    } else {
      mergedRow.stats_confidence = 0;
      mergedRow.stats_confidence_band = 'none';
    }

    const cohortDecision = classifyPlayerStatsCohort_(mergedRow, cohortPolicy);
    mergedRow.reason_metadata = Object.assign({}, mergedRow.reason_metadata || {}, {
      cohort_decision: cohortDecision.classification,
      cohort_policy_mode: cohortPolicy.mode,
      cohort_top_rank_max: cohortPolicy.top_rank_max,
      cohort_rank_value: cohortDecision.rank_value,
      rank_source: mergedRow.rank_source,
      rank_confidence: mergedRow.rank_confidence,
      is_top100: mergedRow.is_top100,
      cohort_fallback_allowed: cohortPolicy.allow_out_of_cohort_fallback,
      cohort_reason_code: cohortDecision.reason_code,
    });
    mergedRow.cohort = cohortDecision.classification;
    mergedRow.cohort_reason_code = cohortDecision.reason_code;
    mergedRow.allow_out_of_cohort_fallback = cohortPolicy.allow_out_of_cohort_fallback;
    mergedRow.fallback_mode = rankingOnlyFallback ? 'ranking_only' : '';
    merged[player] = mergedRow;
  });

  return merged;
}

function buildPlayerStatsCohortPolicy_(config) {
  const rawMode = String((config && config.PLAYER_STATS_COHORT_MODE) || 'leadersource').toLowerCase().trim();
  const mode = rawMode === 'top100' || rawMode === 'all' || rawMode === 'leadersource'
    ? rawMode
    : 'leadersource';
  return {
    mode: mode,
    top_rank_max: Math.max(1, Number((config && config.PLAYER_STATS_TOP_RANK_MAX) || 100)),
    allow_out_of_cohort_fallback: config && config.PLAYER_STATS_ALLOW_OUT_OF_COHORT_FALLBACK !== undefined
      ? !!config.PLAYER_STATS_ALLOW_OUT_OF_COHORT_FALLBACK
      : true,
  };
}

function classifyPlayerStatsCohort_(mergedRow, cohortPolicy) {
  const policy = cohortPolicy || buildPlayerStatsCohortPolicy_({});
  const rawRank = mergedRow ? (mergedRow.resolved_rank !== undefined ? mergedRow.resolved_rank : mergedRow.ranking) : null;
  const numericRank = Number(rawRank);
  const hasNumericRank = Number.isFinite(numericRank) && numericRank > 0;
  const rankValue = hasNumericRank ? numericRank : null;

  if (policy.mode === 'all') {
    return { classification: 'in_cohort', reason_code: 'cohort_mode_all', rank_value: rankValue };
  }
  if (!hasNumericRank) {
    return { classification: 'unknown_rank', reason_code: 'cohort_rank_unknown', rank_value: null };
  }
  if (numericRank <= policy.top_rank_max) {
    return { classification: 'in_cohort', reason_code: 'cohort_rank_within_threshold', rank_value: numericRank };
  }
  return { classification: 'out_of_cohort', reason_code: 'cohort_rank_outside_threshold', rank_value: numericRank };
}


function resolveCanonicalRanking_(sourceMaps, player, mergedRankingCandidate) {
  const preferredSources = ['wta_stats_zone', 'itf', 'tennis_abstract', 'tennis_explorer', 'sofascore'];
  const sourceRankConfidence = {
    wta_stats_zone: 'high',
    itf: 'high',
    tennis_abstract: 'medium',
    tennis_explorer: 'low',
    sofascore: 'low',
    merged: 'low',
  };
  const preferredBySource = {};
  preferredSources.forEach(function (sourceName) {
    preferredBySource[sourceName] = null;
  });

  (sourceMaps || []).forEach(function (entry) {
    const sourceName = canonicalizeStatsProviderName_(entry && entry.source_name);
    if (!Object.prototype.hasOwnProperty.call(preferredBySource, sourceName)) return;
    const row = entry && entry.stats_map ? entry.stats_map[player] : null;
    if (!row || typeof row !== 'object') return;
    const normalized = normalizeCanonicalRankingValue_(row.ranking);
    if (normalized === null) return;
    preferredBySource[sourceName] = normalized;
  });

  for (let i = 0; i < preferredSources.length; i += 1) {
    const sourceName = preferredSources[i];
    if (preferredBySource[sourceName] === null) continue;
    return {
      resolved_rank: preferredBySource[sourceName],
      rank_source: sourceName,
      rank_confidence: sourceRankConfidence[sourceName] || 'medium',
    };
  }

  const mergedCandidate = normalizeCanonicalRankingValue_(mergedRankingCandidate);
  if (mergedCandidate !== null) {
    return {
      resolved_rank: mergedCandidate,
      rank_source: 'merged',
      rank_confidence: sourceRankConfidence.merged,
    };
  }

  return {
    resolved_rank: null,
    rank_source: '',
    rank_confidence: 'none',
  };
}

function normalizeCanonicalRankingValue_(value) {
  if (value === null || value === undefined || value === '') return null;
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return null;
  const rounded = Math.round(numeric);
  if (Math.abs(numeric - rounded) > 0.000001) return null;
  if (rounded < 1 || rounded > 5000) return null;
  return rounded;
}

function resolvePreferredSourcesForFeature_(feature) {
  switch (feature) {
    case 'ranking': return ['wta_stats_zone', 'itf', 'tennis_abstract', 'tennis_explorer', 'sofascore'];
    case 'recent_form':
    case 'recent_form_last_10': return ['sofascore', 'tennis_explorer', 'wta_stats_zone', 'tennis_abstract', 'itf'];
    case 'surface_win_rate':
    case 'hold_pct':
    case 'break_pct': return ['tennis_abstract', 'wta_stats_zone', 'tennis_explorer', 'sofascore', 'itf'];
    case 'surface_hold_pct':
    case 'surface_break_pct':
    case 'bp_saved_pct':
    case 'bp_conv_pct':
    case 'first_serve_in_pct':
    case 'first_serve_points_won_pct':
    case 'second_serve_points_won_pct':
    case 'return_points_won_pct':
    case 'dr':
    case 'tpw_pct': return ['tennis_abstract'];
    case 'surface_recent_form': return ['tennis_abstract', 'sofascore', 'tennis_explorer', 'wta_stats_zone', 'itf'];
    default: return [];
  }
}

function selectMetricWithProvenanceBySourcePriority_(sourceMaps, player, key, preferredSources) {
  const preference = {};
  (preferredSources || []).forEach(function (name, idx) {
    preference[canonicalizeStatsProviderName_(name)] = idx;
  });
  const ordered = (sourceMaps || []).slice().sort(function (a, b) {
    const rankA = Object.prototype.hasOwnProperty.call(preference, a.source_name) ? preference[a.source_name] : 1000;
    const rankB = Object.prototype.hasOwnProperty.call(preference, b.source_name) ? preference[b.source_name] : 1000;
    if (rankA !== rankB) return rankA - rankB;
    return 0;
  });

  const candidates = [];
  ordered.forEach(function (entry) {
    const row = entry && entry.stats_map ? entry.stats_map[player] : null;
    if (!row || typeof row !== 'object') return;
    const value = row[key];
    if (value === null || value === undefined) return;
    candidates.push(buildMetricCandidateWithProvenance_(row, key, value));
  });
  if (!candidates.length) {
    return { value: null, value_origin: 'defaulted', is_placeholder: false, is_trusted: false };
  }

  for (let i = 0; i < candidates.length; i += 1) if (candidates[i].is_trusted) return candidates[i];
  for (let j = 0; j < candidates.length; j += 1) if (!candidates[j].is_placeholder) return candidates[j];
  return candidates[0];
}

function buildMetricCandidateWithProvenance_(statsRow, feature, value) {
  const explicitOrigin = extractFeatureValueOrigin_(statsRow, feature);
  const isZeroValue = Number(value) === 0;
  const hasExplicitZeroConfirmation = explicitOrigin && explicitOrigin !== 'defaulted';
  const isPlaceholder = isZeroValue && !hasExplicitZeroConfirmation;
  const valueOrigin = explicitOrigin || (isPlaceholder ? 'defaulted' : 'scraped');
  const isTrusted = !isPlaceholder && (valueOrigin === 'scraped' || valueOrigin === 'inferred');
  return {
    value: value,
    value_origin: valueOrigin,
    is_placeholder: isPlaceholder,
    is_trusted: isTrusted,
  };
}

function extractFeatureValueOrigin_(statsRow, feature) {
  if (!statsRow || typeof statsRow !== 'object') return '';
  const map = statsRow.value_origin;
  if (map && typeof map === 'object' && typeof map[feature] === 'string') {
    return normalizeValueOrigin_(map[feature]);
  }
  const perFeature = statsRow[feature + '_value_origin'];
  if (typeof perFeature === 'string') return normalizeValueOrigin_(perFeature);
  return '';
}

function normalizeValueOrigin_(raw) {
  const value = String(raw || '').trim().toLowerCase();
  if (value === 'scraped' || value === 'inferred' || value === 'defaulted' || value === 'imputed') return value;
  return '';
}

function selectMetricBySourcePriority_(sourceMaps, player, key, preferredSources) {
  const preference = {};
  (preferredSources || []).forEach(function (name, idx) {
    preference[canonicalizeStatsProviderName_(name)] = idx;
  });
  const ordered = (sourceMaps || []).slice().sort(function (a, b) {
    const rankA = Object.prototype.hasOwnProperty.call(preference, a.source_name) ? preference[a.source_name] : 1000;
    const rankB = Object.prototype.hasOwnProperty.call(preference, b.source_name) ? preference[b.source_name] : 1000;
    if (rankA !== rankB) return rankA - rankB;
    return 0;
  });
  return firstDefinedMetric_(ordered.map(function (entry) { return entry.stats_map; }), player, key);
}

function canonicalizeStatsProviderName_(name) {
  const raw = String(name || '').toLowerCase().trim();
  if (!raw) return 'unknown';
  if (raw.indexOf('tennis abstract') >= 0 || raw === 'ta' || raw === 'tennis_abstract') return 'tennis_abstract';
  if (raw.indexOf('wta') >= 0) return 'wta_stats_zone';
  if (raw.indexOf('itf') >= 0) return 'itf';
  if (raw.indexOf('explorer') >= 0) return 'tennis_explorer';
  if (raw.indexOf('sofascore') >= 0 || raw.indexOf('sofa') >= 0) return 'sofascore';
  return raw.replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '') || 'unknown';
}

function inferStatsProviderNameFromUrl_(url) {
  const host = String(url || '').toLowerCase();
  if (host.indexOf('tennisabstract') >= 0) return 'tennis_abstract';
  if (host.indexOf('wtastatszone') >= 0 || host.indexOf('wtatour') >= 0) return 'wta_stats_zone';
  if (host.indexOf('itf') >= 0) return 'itf';
  if (host.indexOf('tennisexplorer') >= 0) return 'tennis_explorer';
  if (host.indexOf('sofascore') >= 0) return 'sofascore';
  return 'unknown';
}

function buildPlayerStatsMergeDiagnostics_(sourcePayloads, mergedStatsByPlayer, canonicalPlayers, sourceAttemptDiagnostics) {
  const players = dedupePlayerNames_(canonicalPlayers || []);
  const perSourcePlayersParsed = {};
  const perFeatureContributions = {};
  const perFeatureEndpointContributions = {};
  PLAYER_STATS_CANONICAL_FEATURES.forEach(function (feature) {
    perFeatureContributions[feature] = {};
    perFeatureEndpointContributions[feature] = {};
  });

  (sourcePayloads || []).forEach(function (entry) {
    const sourceName = canonicalizeStatsProviderName_(entry && entry.source_name);
    const statsMap = (entry && entry.stats_by_player) || {};
    let parsedCount = 0;
    players.forEach(function (player) {
      const stats = statsMap[player];
      if (stats && typeof stats === 'object') parsedCount += 1;
    });
    perSourcePlayersParsed[sourceName] = parsedCount;
  });

  players.forEach(function (player) {
    const finalStats = mergedStatsByPlayer && mergedStatsByPlayer[player];
    if (!finalStats) return;
    PLAYER_STATS_CANONICAL_FEATURES.forEach(function (feature) {
      const value = finalStats[feature];
      if (value === null || value === undefined) return;
      let contributor = 'unknown';
      for (let i = 0; i < (sourcePayloads || []).length; i += 1) {
        const entry = sourcePayloads[i] || {};
        const sourceValue = entry.stats_by_player && entry.stats_by_player[player] ? entry.stats_by_player[player][feature] : null;
        if (sourceValue !== null && sourceValue !== undefined) {
          contributor = canonicalizeStatsProviderName_(entry.source_name);
          break;
        }
      }
      const featureMap = perFeatureContributions[feature] || {};
      featureMap[contributor] = Number(featureMap[contributor] || 0) + 1;
      perFeatureContributions[feature] = featureMap;

      const endpointMapBySource = perFeatureEndpointContributions[feature] || {};
      const endpointSourceMap = endpointMapBySource[contributor] || {};
      let endpointContributor = 'unknown_endpoint';
      for (let j = 0; j < (sourcePayloads || []).length; j += 1) {
        const candidate = sourcePayloads[j] || {};
        if (canonicalizeStatsProviderName_(candidate.source_name) !== contributor) continue;
        const endpointByPlayer = candidate.endpoint_feature_sources_by_player || {};
        const endpointRow = endpointByPlayer[player] || {};
        if (endpointRow[feature]) {
          endpointContributor = String(endpointRow[feature]);
          break;
        }
      }
      endpointSourceMap[endpointContributor] = Number(endpointSourceMap[endpointContributor] || 0) + 1;
      endpointMapBySource[contributor] = endpointSourceMap;
      perFeatureEndpointContributions[feature] = endpointMapBySource;
    });
  });

  const finalSummary = summarizePlayerStatsCompleteness_(mergedStatsByPlayer || {});
  const cohortSummary = summarizePlayerStatsCohort_(mergedStatsByPlayer || {});
  finalSummary.cohort_summary = cohortSummary;
  finalSummary.cohort_summary_reason_codes = cohortSummary.reason_codes;
  const diagnostics = {
    per_source_players_parsed: perSourcePlayersParsed,
    per_feature_non_null_contributions: perFeatureContributions,
    per_feature_endpoint_contributions: perFeatureEndpointContributions,
    final: finalSummary,
  };

  if (Number(finalSummary.players_with_non_null_stats || 0) === 0) {
    diagnostics.zero_coverage_guardrail = {
      attempted_endpoints_by_source: buildAttemptedEndpointsBySource_(sourceAttemptDiagnostics || []),
      missing_fields_by_source: buildMissingFieldsBySource_(sourceAttemptDiagnostics || []),
    };
  }

  return diagnostics;
}

function buildAttemptedEndpointsBySource_(sourceAttemptDiagnostics) {
  const out = {};
  (sourceAttemptDiagnostics || []).forEach(function (entry) {
    const sourceName = canonicalizeStatsProviderName_(entry && entry.source_name);
    out[sourceName] = dedupeStringList_(entry && entry.attempted_endpoints ? entry.attempted_endpoints : []);
  });
  return out;
}

function buildMissingFieldsBySource_(sourceAttemptDiagnostics) {
  const out = {};
  (sourceAttemptDiagnostics || []).forEach(function (entry) {
    const sourceName = canonicalizeStatsProviderName_(entry && entry.source_name);
    out[sourceName] = dedupeStringList_(entry && entry.missing_fields ? entry.missing_fields : []);
  });
  return out;
}

function normalizePlayerStatsResponse_(providerPayload, canonicalPlayers, options) {
  const rows = extractPlayerStatsRows_(providerPayload);
  if (isMatchMxRows_(rows)) {
    return aggregateMatchMxRowsToStatsByPlayer_(rows, canonicalPlayers, options || {});
  }
  const normalized = {};

  rows.forEach(function (row) {
    const canonicalName = canonicalizePlayerName_(row.player_name || row.player || row.name || row.canonical_name || '', {});
    if (!canonicalName) return;
    normalized[canonicalName] = {
      ranking: parseIntegerMetric_(row.ranking, row.rank, row.world_ranking),
      recent_form: normalizeRateMetric_(row.recent_form, row.form, row.recent_win_rate),
      recent_form_last_10: normalizeRateMetric_(row.recent_form_last_10, row.form_last_10, row.recent_10_form),
      surface_win_rate: normalizeRateMetric_(row.surface_win_rate, row.surfaceWinRate, row.surface_rate),
      hold_pct: normalizeRateMetric_(row.hold_pct, row.hold_percentage, row.serve_hold_pct),
      break_pct: normalizeRateMetric_(row.break_pct, row.break_percentage, row.return_break_pct),
      surface_hold_pct: normalizeRateMetric_(row.surface_hold_pct),
      surface_break_pct: normalizeRateMetric_(row.surface_break_pct),
      surface_recent_form: normalizeRateMetric_(row.surface_recent_form),
      bp_saved_pct: normalizeRateMetric_(row.bp_saved_pct, row.break_points_saved_pct),
      bp_conv_pct: normalizeRateMetric_(row.bp_conv_pct, row.break_points_converted_pct),
      first_serve_in_pct: normalizeRateMetric_(row.first_serve_in_pct, row.first_serve_pct),
      first_serve_points_won_pct: normalizeRateMetric_(row.first_serve_points_won_pct, row.first_serve_won_pct),
      second_serve_points_won_pct: normalizeRateMetric_(row.second_serve_points_won_pct, row.second_serve_won_pct),
      return_points_won_pct: normalizeRateMetric_(row.return_points_won_pct, row.return_won_pct),
      dr: normalizeFloatMetric_(row.dr, row.dominance_ratio),
      tpw_pct: normalizeRateMetric_(row.tpw_pct, row.total_points_won_pct),
      stats_confidence: normalizeFloatMetric_(row.stats_confidence),
      source_used: String(row.source_used || ''),
      fallback_mode: String(row.fallback_mode || ''),
      value_origin: row.value_origin && typeof row.value_origin === 'object' ? row.value_origin : null,
      placeholder_feature_count: Number(row.placeholder_feature_count || 0),
      trusted_feature_count: Number(row.trusted_feature_count || 0),
    };
  });

  const map = {};
  (canonicalPlayers || []).forEach(function (name) {
    const canonical = canonicalizePlayerName_(name, {});
    if (!canonical) return;
    map[canonical] = normalized[canonical] || null;
  });

  return map;
}

function isMatchMxRows_(rows) {
  return !!(rows && rows.length && Object.prototype.hasOwnProperty.call(rows[0], 'numeric_stats'));
}

function aggregateMatchMxRowsToStatsByPlayer_(rows, canonicalPlayers, options) {
  const grouped = {};
  const asOfDate = options.as_of_time instanceof Date ? options.as_of_time : new Date(options.as_of_time || Date.now());
  const weeks = Math.max(0, Number(options.match_window_weeks || 52));
  const recentMatchCount = Math.max(0, Number(options.recent_match_count || 0));
  const surfaceMinSample = Math.max(1, Number(options.surface_match_min_sample || 3));
  const windowStartMs = asOfDate.getTime() - (weeks * 7 * 24 * 60 * 60 * 1000);

  rows.forEach(function (row) {
    const canonicalName = canonicalizePlayerName_(row.player_name || row.player || row.name || '', {});
    if (!canonicalName) return;
    const matchDate = parseDateMs_(row.date);
    if (Number.isFinite(matchDate) && matchDate > asOfDate.getTime()) return;
    if (weeks > 0 && Number.isFinite(matchDate) && matchDate < windowStartMs) return;

    if (!grouped[canonicalName]) grouped[canonicalName] = [];
    grouped[canonicalName].push(row);
  });

  const stats = {};
  Object.keys(grouped).forEach(function (player) {
    const playerRows = grouped[player]
      .slice()
      .sort(function (a, b) { return parseDateMs_(b.date) - parseDateMs_(a.date); });
    const scopedRows = recentMatchCount > 0 ? playerRows.slice(0, recentMatchCount) : playerRows;
    const recentFormLast10 = normalizeRateMetric_(averageMetric_(playerRows.slice(0, 10), 'recent_form'));
    const targetSurface = String((options && options.surface) || (scopedRows[0] && scopedRows[0].surface) || '').trim().toLowerCase();
    const surfaceRows = targetSurface
      ? scopedRows.filter(function (row) { return String((row && row.surface) || '').trim().toLowerCase() === targetSurface; })
      : [];
    const useSurfaceRows = surfaceRows.length >= surfaceMinSample;

    const holdPct = normalizeRateMetric_(averageMetric_(scopedRows, 'hold_pct'));
    const breakPct = normalizeRateMetric_(averageMetric_(scopedRows, 'break_pct'));
    const recentForm = normalizeRateMetric_(averageMetric_(scopedRows, 'recent_form'));
    const surfaceHoldPct = useSurfaceRows ? normalizeRateMetric_(averageMetric_(surfaceRows, 'hold_pct')) : holdPct;
    const surfaceBreakPct = useSurfaceRows ? normalizeRateMetric_(averageMetric_(surfaceRows, 'break_pct')) : breakPct;
    const surfaceRecentForm = useSurfaceRows ? normalizeRateMetric_(averageMetric_(surfaceRows, 'recent_form')) : recentForm;

    stats[player] = {
      ranking: parseIntegerMetric_(scopedRows[0] && scopedRows[0].ranking),
      recent_form: recentForm,
      recent_form_last_10: recentFormLast10,
      surface_win_rate: normalizeRateMetric_(averageMetric_(scopedRows, 'surface_win_rate')),
      hold_pct: holdPct,
      break_pct: breakPct,
      surface_hold_pct: surfaceHoldPct,
      surface_break_pct: surfaceBreakPct,
      surface_recent_form: surfaceRecentForm,
      bp_saved_pct: normalizeRateMetric_(averageMetric_(scopedRows, 'bp_saved_pct')),
      bp_conv_pct: normalizeRateMetric_(averageMetric_(scopedRows, 'bp_conv_pct')),
      first_serve_in_pct: normalizeRateMetric_(averageMetric_(scopedRows, 'first_serve_in_pct')),
      first_serve_points_won_pct: normalizeRateMetric_(averageMetric_(scopedRows, 'first_serve_points_won_pct')),
      second_serve_points_won_pct: normalizeRateMetric_(averageMetric_(scopedRows, 'second_serve_points_won_pct')),
      return_points_won_pct: normalizeRateMetric_(averageMetric_(scopedRows, 'return_points_won_pct')),
      dr: normalizeFloatMetric_(averageMetric_(scopedRows, 'dr')),
      tpw_pct: normalizeRateMetric_(averageMetric_(scopedRows, 'tpw_pct')),
    };
  });

  const map = {};
  (canonicalPlayers || []).forEach(function (name) {
    const canonical = canonicalizePlayerName_(name, {});
    if (!canonical) return;
    map[canonical] = stats[canonical] || null;
  });
  return map;
}

function firstDefinedMetric_(maps, player, key) {
  for (let i = 0; i < (maps || []).length; i += 1) {
    const stats = maps[i] && maps[i][player];
    if (!stats) continue;
    if (stats[key] !== null && stats[key] !== undefined) return stats[key];
  }
  return null;
}

function parseDateMs_(value) {
  const ms = Date.parse(String(value || ''));
  return Number.isFinite(ms) ? ms : NaN;
}

function averageMetric_(rows, key) {
  let sum = 0;
  let count = 0;
  (rows || []).forEach(function (row) {
    const value = Number(row && row[key]);
    if (!Number.isFinite(value)) return;
    sum += value;
    count += 1;
  });
  return count ? (sum / count) : null;
}

function normalizeFloatMetric_() {
  for (let i = 0; i < arguments.length; i += 1) {
    const value = Number(arguments[i]);
    if (Number.isFinite(value)) return roundNumber_(value, 3);
  }
  return null;
}

function extractPlayerStatsRows_(providerPayload) {
  if (Array.isArray(providerPayload)) return providerPayload;
  if (providerPayload && Array.isArray(providerPayload.data)) return providerPayload.data;
  if (providerPayload && Array.isArray(providerPayload.players)) return providerPayload.players;

  if (providerPayload && providerPayload.players && typeof providerPayload.players === 'object') {
    return Object.keys(providerPayload.players).map(function (name) {
      const row = providerPayload.players[name] || {};
      row.player_name = row.player_name || name;
      return row;
    });
  }

  return [];
}

function parseIntegerMetric_() {
  for (let i = 0; i < arguments.length; i += 1) {
    const value = Number(arguments[i]);
    if (Number.isFinite(value)) return Math.round(value);
  }
  return null;
}

function normalizeRateMetric_() {
  for (let i = 0; i < arguments.length; i += 1) {
    const value = Number(arguments[i]);
    if (!Number.isFinite(value)) continue;
    const normalized = value > 1 ? value / 100 : value;
    return roundNumber_(normalized, 3);
  }
  return null;
}

function dedupePlayerNames_(players) {
  const deduped = {};
  (players || []).forEach(function (name) {
    const canonical = canonicalizePlayerName_(name, {});
    if (!canonical) return;
    deduped[canonical] = true;
  });
  return Object.keys(deduped);
}

function buildPlayerStatsCacheKey_(players, asOfTime) {
  const normalizedPlayers = dedupePlayerNames_(players || []).sort();
  const asOfBucketMin = Math.floor((asOfTime.getTime()) / 60000);
  return ['PLAYER_STATS_PAYLOAD', asOfBucketMin, stringHashCode_(normalizedPlayers.join('|'))].join('|');
}

function getCachedPlayerStatsPayload_(cacheKey) {
  try {
    const raw = CacheService.getScriptCache().get(cacheKey);
    if (!raw) return null;
    return JSON.parse(raw);
  } catch (e) {
    return null;
  }
}

function setCachedPlayerStatsPayload_(cacheKey, payload) {
  CacheService.getScriptCache().put(cacheKey, JSON.stringify(payload || {}), 21600);
}

function persistPlayerStatsMeta_(payload, source, nowMs, playerCount, asOfTime, telemetry) {
  const metrics = telemetry || {};
  const completeness = summarizePlayerStatsCompleteness_(payload.stats_by_player || {});
  const dataAvailable = metrics.data_available !== undefined
    ? metrics.data_available === true
    : completeness.has_stats;
  const aggregateReasonCode = String(metrics.aggregate_reason_code || resolvePlayerStatsAggregateReasonCode_('', completeness) || '');
  const serializable = {
    cache_key: payload.cache_key || '',
    cached_at_ms: Number(payload.cached_at_ms || nowMs),
    source: source,
    as_of_time: payload.as_of_time || asOfTime.toISOString(),
    has_stats: completeness.has_stats,
    player_count: Number(payload.player_count || playerCount || 0),
    players_with_non_null_stats: completeness.players_with_non_null_stats,
    players_with_null_only_stats: completeness.players_with_null_only_stats,
    stats_by_player: payload.stats_by_player || {},
  };

  setStateValue_('PLAYER_STATS_STALE_PAYLOAD', JSON.stringify(serializable), {
    source_meta: {
      source: source,
      cached_at_ms: serializable.cached_at_ms,
      player_count: serializable.player_count,
      stats_count: Object.keys(serializable.stats_by_player || {}).length,
      reference_key: serializable.cache_key || '',
      storage_path: 'cache_reference',
    },
  });
  setStateValue_('PLAYER_STATS_LAST_FETCH_META', JSON.stringify({
    cache_key: serializable.cache_key,
    cached_at_ms: serializable.cached_at_ms,
    source: source,
    as_of_time: serializable.as_of_time,
    has_stats: serializable.has_stats,
    player_count: serializable.player_count,
    players_with_non_null_stats: serializable.players_with_non_null_stats,
    players_with_null_only_stats: serializable.players_with_null_only_stats,
    api_call_count: Number(metrics.api_call_count || 0),
    scrape_call_count: Number(metrics.scrape_call_count || 0),
    provider_available: metrics.provider_available !== false,
    data_available: dataAvailable,
    aggregate_reason_code: aggregateReasonCode,
    selection_metadata: metrics.selection_metadata || null,
    last_success_at: String(metrics.last_success_at || ''),
    last_failure_reason: String(metrics.last_failure_reason || (dataAvailable ? '' : aggregateReasonCode)),
  }));
}

function summarizePlayerStatsCompleteness_(statsByPlayer) {
  const players = statsByPlayer && typeof statsByPlayer === 'object'
    ? Object.keys(statsByPlayer)
    : [];
  let playersWithNonNullStats = 0;
  let playersWithNullOnlyStats = 0;
  let placeholderFeatureCount = 0;
  let trustedFeatureCount = 0;

  players.forEach(function (player) {
    const stats = statsByPlayer[player];
    const hasNonNullStats = playerStatsHasNonNullFeatures_(stats);
    if (hasNonNullStats) playersWithNonNullStats += 1;
    else playersWithNullOnlyStats += 1;
    const quality = summarizeFeatureQualityCounts_(stats);
    placeholderFeatureCount += Number(quality.placeholder_feature_count || 0);
    trustedFeatureCount += Number(quality.trusted_feature_count || 0);
  });

  return {
    has_stats: playersWithNonNullStats > 0,
    players_with_non_null_stats: playersWithNonNullStats,
    players_with_null_only_stats: playersWithNullOnlyStats,
    placeholder_feature_count: placeholderFeatureCount,
    trusted_feature_count: trustedFeatureCount,
  };
}

function summarizeFeatureQualityCounts_(stats) {
  if (!stats || typeof stats !== 'object') return { placeholder_feature_count: 0, trusted_feature_count: 0 };
  if (Number.isFinite(Number(stats.placeholder_feature_count)) || Number.isFinite(Number(stats.trusted_feature_count))) {
    return {
      placeholder_feature_count: Number(stats.placeholder_feature_count || 0),
      trusted_feature_count: Number(stats.trusted_feature_count || 0),
    };
  }

  let placeholderFeatureCount = 0;
  let trustedFeatureCount = 0;
  PLAYER_STATS_CANONICAL_FEATURES.forEach(function (feature) {
    const value = stats[feature];
    if (value === null || value === undefined) return;
    const candidate = buildMetricCandidateWithProvenance_(stats, feature, value);
    placeholderFeatureCount += candidate.is_placeholder ? 1 : 0;
    trustedFeatureCount += candidate.is_trusted ? 1 : 0;
  });
  return {
    placeholder_feature_count: placeholderFeatureCount,
    trusted_feature_count: trustedFeatureCount,
  };
}

function playerStatsHasNonNullFeatures_(stats) {
  if (!stats || typeof stats !== 'object') return false;
  for (let i = 0; i < PLAYER_STATS_COMPLETENESS_KEYS.length; i += 1) {
    const value = stats[PLAYER_STATS_COMPLETENESS_KEYS[i]];
    if (value !== null && value !== undefined) return true;
  }
  return false;
}

function resolvePlayerStatsAggregateReasonCode_(baseReasonCode, completeness) {
  const reasonCode = String(baseReasonCode || '');
  const metrics = completeness || { has_stats: false, players_with_null_only_stats: 0 };
  const cohortSummary = metrics.cohort_summary || {};
  if (Number(cohortSummary.out_of_cohort || 0) > 0 && Number(cohortSummary.in_cohort || 0) === 0) {
    return 'player_stats_out_of_cohort_only';
  }
  if (Number(cohortSummary.unknown_rank || 0) > 0 && Number(cohortSummary.in_cohort || 0) === 0 && !metrics.has_stats) {
    return 'player_stats_unknown_rank_only';
  }
  if (metrics.has_stats) return reasonCode;
  if (Number(metrics.players_with_null_only_stats || 0) > 0) return 'provider_returned_null_features';
  return reasonCode || 'provider_returned_empty';
}

function summarizePlayerStatsCohort_(statsByPlayer) {
  const summary = {
    in_cohort: 0,
    out_of_cohort: 0,
    unknown_rank: 0,
    reason_codes: {},
  };
  const players = statsByPlayer && typeof statsByPlayer === 'object' ? Object.keys(statsByPlayer) : [];
  players.forEach(function (player) {
    const row = statsByPlayer[player] || {};
    const classification = String(row.cohort || 'unknown_rank');
    if (classification === 'in_cohort' || classification === 'out_of_cohort' || classification === 'unknown_rank') {
      summary[classification] += 1;
    } else {
      summary.unknown_rank += 1;
    }
    const reasonCode = String(row.cohort_reason_code || '');
    if (reasonCode) summary.reason_codes[reasonCode] = Number(summary.reason_codes[reasonCode] || 0) + 1;
  });
  return summary;
}

function getTaH2hRowForCanonicalPair_(config, playerA, playerB) {
  const coverage = getTaH2hCoverageForCanonicalPair_(config, playerA, playerB);
  return coverage && coverage.row ? coverage.row : null;
}

function getTaH2hCoverageForCanonicalPair_(config, playerA, playerB) {
  const canonicalA = canonicalizeTaH2hPlayerName_(playerA || '');
  const canonicalB = canonicalizeTaH2hPlayerName_(playerB || '');
  if (!canonicalA || !canonicalB || canonicalA === canonicalB) {
    return { row: null, reason_code: 'h2h_pair_invalid', reason_metadata: {} };
  }

  const dataset = getTaH2hDataset_(config || {});
  if (!dataset || !dataset.by_pair) {
    const h2hMeta = getStateJson_('PLAYER_STATS_H2H_LAST_FETCH_META') || {};
    const upstreamFailureReason = String(h2hMeta.last_failure_reason || '');
    if (upstreamFailureReason === 'ta_h2h_fetch_failed' || upstreamFailureReason === 'ta_h2h_parse_failed') {
      return {
        row: null,
        reason_code: upstreamFailureReason,
        reason_metadata: {
          category: 'pipeline_failure',
          expected_missing: false,
        },
      };
    }
    return { row: null, reason_code: 'h2h_dataset_unavailable', reason_metadata: {} };
  }

  const directKey = buildTaH2hPairKey_(canonicalA, canonicalB);
  const direct = dataset.by_pair[directKey];
  if (direct) {
    return {
      row: direct,
      reason_code: '',
      reason_metadata: {
        matched_pair_verified: true,
        matched_lookup_key: directKey,
        canonical_pair_key: buildTaH2hCanonicalPairKey_(canonicalA, canonicalB),
      },
    };
  }

  const reverseKey = buildTaH2hPairKey_(canonicalB, canonicalA);
  const reverse = dataset.by_pair[reverseKey];
  if (reverse) {
    return {
      row: {
        player_a: canonicalA,
        player_b: canonicalB,
        wins_a: Number(reverse.wins_b || 0),
        wins_b: Number(reverse.wins_a || 0),
        source_updated_date: reverse.source_updated_date || dataset.source_updated_date || '',
      },
      reason_code: '',
      reason_metadata: {
        matched_pair_verified: true,
        matched_lookup_key: reverseKey,
        canonical_pair_key: buildTaH2hCanonicalPairKey_(canonicalA, canonicalB),
        matched_by_reverse_lookup: true,
      },
    };
  }

  const playersInMatrix = buildTaH2hPlayersInMatrixSet_(dataset);
  const hasA = playersInMatrix[canonicalA] === true;
  const hasB = playersInMatrix[canonicalB] === true;
  const debugSample = buildTaH2hLookupDebugSample_(dataset, canonicalA, canonicalB);
  return {
    row: null,
    reason_code: hasA && hasB ? 'h2h_partial_coverage' : 'h2h_player_not_in_matrix',
    reason_metadata: hasA && hasB
      ? {
          category: 'source_coverage',
          coverage_scope: 'top_15_matrix',
          expected_missing: true,
          debug_sample: debugSample,
        }
      : {
          category: 'source_coverage',
          coverage_scope: 'top_15_matrix',
          expected_missing: true,
          limitation: 'player_not_in_top_15_matrix',
          debug_sample: debugSample,
        },
  };
}

function buildTaH2hPlayersInMatrixSet_(dataset) {
  const rows = dataset && Array.isArray(dataset.rows) ? dataset.rows : [];
  const seen = {};
  rows.forEach(function (row) {
    const playerA = canonicalizeTaH2hPlayerName_(row && row.player_a);
    const playerB = canonicalizeTaH2hPlayerName_(row && row.player_b);
    if (playerA) seen[playerA] = true;
    if (playerB) seen[playerB] = true;
  });
  return seen;
}

function getTaH2hDataset_(config) {
  const runtimeConfig = config || {};
  const forceRefresh = toBoolean_(runtimeConfig.PLAYER_STATS_FORCE_REFRESH, false);
  const ttlMs = Math.max(1, Number(runtimeConfig.PLAYER_STATS_CACHE_TTL_MIN || 10)) * 60000;
  const nowMs = Date.now();

  if (!forceRefresh) {
    const cached = getCachedTaH2hDataset_();
    if (cached && Number.isFinite(cached.cached_at_ms) && nowMs - cached.cached_at_ms <= ttlMs) {
      return cached;
    }
  }

  const fresh = fetchTaH2hDatasetFromSource_(runtimeConfig);
  if (fresh.ok) {
    const payload = fresh.payload || { rows: [], by_pair: {} };
    payload.h2h_mode_reason_code = String(fresh.reason_code || 'ta_h2h_ok');
    payload.cached_at_ms = nowMs;
    setCachedTaH2hDataset_(payload);
    const sourceType = payload.h2h_mode_reason_code === 'h2h_source_empty_table' ? 'fresh_h2h_empty_table' : 'fresh_h2h_page';
    persistTaH2hDatasetState_(payload, sourceType, {
      provider_available: true,
      last_success_at: new Date(nowMs).toISOString(),
      api_call_count: Number(fresh.api_call_count || 0),
      scrape_call_count: Number(fresh.api_call_count || 0),
    });
    return payload;
  }

  const stale = getStateJson_('PLAYER_STATS_H2H_STALE_PAYLOAD');
  if (stale && stale.by_pair) {
    persistTaH2hDatasetState_(stale, 'cached_stale_fallback', {
      provider_available: false,
      last_success_at: String(stale.fetched_at || ''),
      last_failure_reason: String(fresh.reason_code || 'ta_h2h_fetch_failed'),
      api_call_count: Number(fresh.api_call_count || 0),
      scrape_call_count: Number(fresh.api_call_count || 0),
    });
    return stale;
  }

  persistTaH2hDatasetState_({ rows: [], by_pair: {}, source: runtimeConfig.PLAYER_STATS_TA_H2H_URL || '', source_updated_date: '', fetched_at: new Date(nowMs).toISOString(), cached_at_ms: nowMs }, 'provider_unavailable', {
    provider_available: false,
    last_failure_reason: String(fresh.reason_code || 'ta_h2h_fetch_failed'),
    api_call_count: Number(fresh.api_call_count || 0),
    scrape_call_count: Number(fresh.api_call_count || 0),
  });

  return null;
}

function fetchTaH2hDatasetFromSource_(config) {
  const url = String(config.PLAYER_STATS_TA_H2H_URL || 'https://tennisabstract.com/reports/h2hMatrixWta.html').trim();
  if (!url) return { ok: false, reason_code: 'ta_h2h_url_missing' };

  const headers = {
    Accept: 'text/html',
    'User-Agent': String(config.PLAYER_STATS_FETCH_USER_AGENT || 'Mozilla/5.0 (compatible; WTA-Edge-Board/1.0)'),
  };
  sleepTennisAbstractRequestGap_(config);
  const fetchResult = playerStatsFetchWithRetry_(url, {
    method: 'get',
    headers: headers,
    muteHttpExceptions: true,
    followRedirects: true,
    validateHttpsCertificates: true,
  }, config);

  if (!fetchResult.ok) return { ok: false, reason_code: fetchResult.reason_code || 'ta_h2h_fetch_failed', api_call_count: Number(fetchResult.api_call_count || 0) };

  const status = Number(fetchResult.status_code || 0);
  const html = String(fetchResult.response.getContentText() || '');
  const parsed = parseTaH2hPageHtml_(html);
  if (!parsed.ok) {
    logTaH2hParseDiagnostic_('ta_h2h_parse_failed', parsed.diagnostics || {});
    return { ok: false, reason_code: 'ta_h2h_parse_failed', api_call_count: Number(fetchResult.api_call_count || 0) };
  }

  if (parsed.empty_table) {
    logTaH2hParseDiagnostic_('ta_h2h_empty_table', parsed.diagnostics || {});
  }

  const rows = normalizeTaH2hRowsForDataset_(parsed.rows || []);
  const sourceUpdatedDate = parsed.source_updated_date || '';
  const byPair = {};
  rows.forEach(function (row) {
    row.source_updated_date = sourceUpdatedDate;
    byPair[buildTaH2hPairKey_(row.player_a, row.player_b)] = row;
  });

  return {
    ok: true,
    reason_code: parsed.empty_table ? 'h2h_source_empty_table' : 'ta_h2h_ok',
    payload: {
      source: url,
      source_updated_date: sourceUpdatedDate,
      fetched_at: new Date().toISOString(),
      schema_version: parsed.schema_version || '',
      rows: rows,
      by_pair: byPair,
    },
    api_call_count: Number(fetchResult.api_call_count || 0),
  };
}

function parseTaH2hPageHtml_(html) {
  const text = String(html || '');
  const diagnostics = buildTaH2hHtmlDiagnostics_(text);
  diagnostics.parse_step_failed = '';

  if (!text.trim()) {
    diagnostics.parse_step_failed = 'html_empty';
    return { ok: false, rows: [], source_updated_date: '', schema_version: 'unknown', empty_table: false, diagnostics: diagnostics };
  }

  const sourceUpdatedDate = extractTaH2hSourceUpdatedDate_(text);
  const matrixHtml = extractTaH2hMatrixTableHtml_(text);
  let schemaVersion = 'anchor_fallback_v1';
  let rows = [];

  if (matrixHtml) {
    schemaVersion = 'ta_h2h_matrix_table_v1';
    rows = extractTaH2hMatrixRows_(matrixHtml);
    if (!rows.length) rows = extractTaH2hStructuredMatrixRows_(matrixHtml);
    diagnostics.table_detected = true;
  } else {
    rows = extractTaH2hMatrixRows_(text);
    if (!rows.length) rows = extractTaH2hStructuredMatrixRows_(text);
    diagnostics.table_detected = false;
  }

  if (rows.length) {
    return {
      ok: true,
      rows: rows,
      source_updated_date: sourceUpdatedDate,
      schema_version: schemaVersion,
      empty_table: false,
      diagnostics: diagnostics,
    };
  }

  const hasAnyAnchorOrTable = /<a\b|<table\b/i.test(matrixHtml || text);
  if (hasAnyAnchorOrTable) {
    return {
      ok: true,
      rows: [],
      source_updated_date: sourceUpdatedDate,
      schema_version: schemaVersion,
      empty_table: true,
      diagnostics: diagnostics,
    };
  }

  diagnostics.parse_step_failed = matrixHtml ? 'row_extraction_no_scores' : 'schema_detection_no_table_or_links';
  return {
    ok: false,
    rows: [],
    source_updated_date: sourceUpdatedDate,
    schema_version: schemaVersion,
    empty_table: false,
    diagnostics: diagnostics,
  };
}

function extractTaH2hMatrixTableHtml_(html) {
  const text = String(html || '');
  const tableRegex = /<table\b[^>]*>[\s\S]*?<\/table>/gi;
  let match;
  while ((match = tableRegex.exec(text)) !== null) {
    const tableHtml = String(match[0] || '');
    const hasPlayerParams = /player1=|player2=|playera=|playerb=/i.test(tableHtml);
    const hasScorePattern = /\d+\s*[-–:]\s*\d+/i.test(stripHtmlTags_(tableHtml));
    if (hasPlayerParams || hasScorePattern) return tableHtml;
  }
  return '';
}

function extractTaH2hMatrixRows_(html) {
  const text = String(html || '');
  const rows = [];
  const seen = {};
  const anchorRegex = /<a\b[^>]*href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi;
  let match;

  while ((match = anchorRegex.exec(text)) !== null) {
    const href = String(match[1] || '');
    const bodyText = stripHtmlTags_(match[2] || '');
    const score = extractH2hWinsFromText_(bodyText) || extractH2hWinsFromText_(href);
    if (!score) continue;

    const names = extractTaH2hPlayersFromHref_(href);
    if (!names) continue;

    const playerA = canonicalizeTaH2hPlayerName_(names.player_a);
    const playerB = canonicalizeTaH2hPlayerName_(names.player_b);
    if (!playerA || !playerB || playerA === playerB) continue;

    const key = buildTaH2hPairKey_(playerA, playerB);
    if (seen[key]) continue;
    seen[key] = true;

    rows.push({
      player_a: playerA,
      player_b: playerB,
      wins_a: score.wins_a,
      wins_b: score.wins_b,
      source_updated_date: '',
    });
  }

  return rows;
}

function normalizeTaH2hRowsForDataset_(rows) {
  const sourceRows = Array.isArray(rows) ? rows : [];
  const normalizedRows = [];
  const seen = {};

  for (let i = 0; i < sourceRows.length; i += 1) {
    const row = sourceRows[i] || {};
    const playerA = canonicalizeTaH2hPlayerName_(row.player_a);
    const playerB = canonicalizeTaH2hPlayerName_(row.player_b);
    const winsA = Number(row.wins_a);
    const winsB = Number(row.wins_b);
    if (!playerA || !playerB || playerA === playerB) continue;
    if (!Number.isFinite(winsA) || !Number.isFinite(winsB) || winsA < 0 || winsB < 0) continue;

    const pairKey = buildTaH2hPairKey_(playerA, playerB);
    if (seen[pairKey]) continue;
    seen[pairKey] = true;

    normalizedRows.push({
      player_a: playerA,
      player_b: playerB,
      wins_a: winsA,
      wins_b: winsB,
      source_updated_date: '',
    });
  }

  return normalizedRows;
}

function extractTaH2hStructuredMatrixRows_(html) {
  const text = String(html || '');
  const trRegex = /<tr\b[^>]*>([\s\S]*?)<\/tr>/gi;
  const rows = [];
  const seen = {};
  const tableRows = [];
  let trMatch;

  while ((trMatch = trRegex.exec(text)) !== null) {
    tableRows.push(String(trMatch[1] || ''));
  }
  if (!tableRows.length) return rows;

  const headerCells = extractTaH2hCells_(tableRows[0]);
  const headerByIndex = {};
  for (let i = 1; i < headerCells.length; i += 1) {
    const parsedHeaderName = extractTaH2hNameFromCell_(headerCells[i]);
    const plainHeader = stripHtmlTags_(headerCells[i]).replace(/\s+/g, ' ').trim();
    if (!parsedHeaderName) continue;
    if (/^vs\s*1\s*-\s*(5|10|15)$/i.test(plainHeader)) continue;
    headerByIndex[i] = parsedHeaderName;
  }

  for (let rowIndex = 1; rowIndex < tableRows.length; rowIndex += 1) {
    const cells = extractTaH2hCells_(tableRows[rowIndex]);
    if (!cells.length) continue;

    const rowPlayer = extractTaH2hNameFromCell_(cells[0]);
    if (!rowPlayer) continue;

    for (let colIndex = 1; colIndex < cells.length; colIndex += 1) {
      const colPlayer = headerByIndex[colIndex] || '';
      if (!colPlayer) continue;
      if (rowPlayer === colPlayer) continue;

      const score = extractH2hWinsFromText_(stripHtmlTags_(cells[colIndex]));
      if (!score) continue;

      const ordered = [rowPlayer, colPlayer].sort();
      const dedupeKey = ordered[0] + '||' + ordered[1];
      if (seen[dedupeKey]) continue;
      seen[dedupeKey] = true;

      rows.push({
        player_a: rowPlayer,
        player_b: colPlayer,
        wins_a: score.wins_a,
        wins_b: score.wins_b,
        source_updated_date: '',
      });
    }
  }

  return rows;
}

function extractTaH2hCells_(rowHtml) {
  const cellRegex = /<(th|td)\b[^>]*>[\s\S]*?<\/\1>/gi;
  const cells = [];
  let match;
  while ((match = cellRegex.exec(String(rowHtml || ''))) !== null) {
    cells.push(String(match[0] || ''));
  }
  return cells;
}

function extractTaH2hNameFromCell_(cellHtml) {
  const text = String(cellHtml || '');
  const anchorMatch = text.match(/<a\b[^>]*>([\s\S]*?)<\/a>/i);
  const anchorTagMatch = text.match(/<a\b([^>]*)>/i);
  const attrs = anchorTagMatch ? String(anchorTagMatch[1] || '') : '';
  const titleMatch = attrs.match(/\btitle=["']([^"']+)["']/i);
  const rawName = (titleMatch && titleMatch[1]) || (anchorMatch && stripHtmlTags_(anchorMatch[1])) || stripHtmlTags_(text);
  return normalizeTaH2hMatrixName_(rawName);
}

function normalizeTaH2hMatrixName_(rawName) {
  const clean = stripHtmlTags_(String(rawName || '')).replace(/\s+/g, ' ').trim();
  if (!clean) return '';
  return canonicalizeTaH2hPlayerName_(clean);
}

function buildTaH2hHtmlDiagnostics_(html) {
  const text = String(html || '');
  return {
    html_signature: [
      '<table=' + (/<table\b/i.test(text) ? '1' : '0'),
      '<a=' + (/<a\b/i.test(text) ? '1' : '0'),
      'player_params=' + (/player1=|player2=|playera=|playerb=|p1=|p2=/i.test(text) ? '1' : '0'),
      'score_pattern=' + (/\d+\s*[-–:]\s*\d+/i.test(stripHtmlTags_(text)) ? '1' : '0'),
    ].join(';'),
    html_sha256: computeSha256Hex_(text),
    html_length: text.length,
    table_detected: false,
  };
}

function computeSha256Hex_(text) {
  const bytes = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, String(text || ''));
  const hex = [];
  for (let i = 0; i < bytes.length; i += 1) {
    const value = bytes[i] < 0 ? bytes[i] + 256 : bytes[i];
    const part = value.toString(16);
    hex.push(part.length === 1 ? '0' + part : part);
  }
  return hex.join('');
}

function extractTaH2hSourceUpdatedDate_(html) {
  const plain = stripHtmlTags_(html || '').replace(/\s+/g, ' ').trim();
  const match = plain.match(/last\s+update\s*:?\s*([^|\-]{4,40})/i);
  if (!match || !match[1]) return '';

  const value = String(match[1] || '').trim().replace(/[.;,]$/, '');
  const parsedMs = Date.parse(value);
  if (!Number.isFinite(parsedMs)) return value;
  return new Date(parsedMs).toISOString().slice(0, 10);
}

function extractTaH2hPlayersFromHref_(href) {
  const query = String(href || '').split('?')[1] || '';
  if (!query) return null;

  const params = {};
  query.split('&').forEach(function (entry) {
    if (!entry) return;
    const parts = entry.split('=');
    const key = decodeURIComponentSafe_(parts[0] || '').toLowerCase();
    const value = decodeURIComponentSafe_(parts.slice(1).join('=') || '');
    if (!key) return;
    params[key] = value;
  });

  const playerA = params.player1 || params.playera || params.p1 || params.a || params.winner || '';
  const playerB = params.player2 || params.playerb || params.p2 || params.b || params.loser || '';
  if (!playerA || !playerB) return null;

  return { player_a: playerA, player_b: playerB };
}

function extractH2hWinsFromText_(text) {
  const match = String(text || '').match(/(\d+)\s*[-–:]\s*(\d+)/);
  if (!match) return null;
  return {
    wins_a: Number(match[1]),
    wins_b: Number(match[2]),
  };
}

function stripHtmlTags_(html) {
  return String(html || '').replace(/<[^>]+>/g, ' ').replace(/&nbsp;/gi, ' ').replace(/&amp;/gi, '&');
}

function decodeURIComponentSafe_(value) {
  try {
    return decodeURIComponent(String(value || '').replace(/\+/g, ' '));
  } catch (e) {
    return String(value || '');
  }
}

function buildTaH2hPairKey_(playerA, playerB) {
  return [canonicalizeTaH2hPlayerName_(playerA || ''), canonicalizeTaH2hPlayerName_(playerB || '')].join('||');
}

function buildTaH2hCanonicalPairKey_(playerA, playerB) {
  return [canonicalizeTaH2hPlayerName_(playerA || ''), canonicalizeTaH2hPlayerName_(playerB || '')].sort().join('||');
}

function buildTaH2hLookupDebugSample_(dataset, canonicalA, canonicalB) {
  const scheduleKey = buildTaH2hPairKey_(canonicalA, canonicalB);
  const reverseScheduleKey = buildTaH2hPairKey_(canonicalB, canonicalA);
  const byPair = dataset && dataset.by_pair ? dataset.by_pair : {};
  const keys = Object.keys(byPair);
  const normalizedDatasetKeyMap = {};
  keys.forEach(function (key) {
    const normalized = normalizeTaH2hPairKey_(key);
    if (normalized) normalizedDatasetKeyMap[normalized] = true;
  });
  const normalizedDatasetKeys = Object.keys(normalizedDatasetKeyMap);
  const comparison = {
    schedule_pair_keys: [scheduleKey, reverseScheduleKey],
    schedule_matches_normalized_dataset: {
      direct: normalizedDatasetKeyMap[scheduleKey] === true,
      reverse: normalizedDatasetKeyMap[reverseScheduleKey] === true,
    },
    dataset_key_counts: {
      raw: keys.length,
      normalized_unique: normalizedDatasetKeys.length,
    },
    normalized_key_drift_samples: keys
      .filter(function (key) { return normalizeTaH2hPairKey_(key) !== key; })
      .slice(0, 5)
      .map(function (key) {
        return { raw: key, normalized: normalizeTaH2hPairKey_(key) };
      }),
  };
  if (!keys.length) {
    return {
      schedule_key: scheduleKey,
      reverse_schedule_key: reverseScheduleKey,
      requested_pair_keys: [scheduleKey, reverseScheduleKey],
      pair_key_comparison: comparison,
      nearest_candidate_keys: [],
      edit_distance_top_matches: [],
    };
  }

  const requestedPlayers = [canonicalA, canonicalB];
  const scored = keys.map(function (key) {
    const parts = String(key || '').split('||');
    const keyPlayers = [String(parts[0] || ''), String(parts[1] || '')];
    let overlap = 0;
    requestedPlayers.forEach(function (player) {
      if (keyPlayers.indexOf(player) >= 0) overlap += 1;
    });
    const score = overlap * 10 - Math.abs(key.length - scheduleKey.length) / 100;
    return { key: key, score: score };
  });

  scored.sort(function (a, b) {
    if (b.score !== a.score) return b.score - a.score;
    return String(a.key || '').localeCompare(String(b.key || ''));
  });

  const distanceMatches = keys.map(function (key) {
    return {
      key: key,
      distance: computeLevenshteinDistance_(scheduleKey, key),
    };
  });
  distanceMatches.sort(function (a, b) {
    if (a.distance !== b.distance) return a.distance - b.distance;
    return String(a.key || '').localeCompare(String(b.key || ''));
  });

  return {
    schedule_key: scheduleKey,
    reverse_schedule_key: reverseScheduleKey,
    requested_pair_keys: [scheduleKey, reverseScheduleKey],
    pair_key_comparison: comparison,
    nearest_candidate_keys: scored.slice(0, 5).map(function (entry) { return entry.key; }),
    edit_distance_top_matches: distanceMatches.slice(0, 5),
  };
}

function canonicalizeTaH2hPlayerName_(name) {
  const canonical = canonicalizePlayerName_(name || '', {});
  if (!canonical) return '';
  return canonical.replace(/^\d+(?:\s+|\s*\|\|\s*)+/, '').trim();
}

function normalizeTaH2hPairKey_(pairKey) {
  const parts = String(pairKey || '').split('||');
  if (parts.length < 2) return '';
  let leftRaw = '';
  let rightRaw = '';
  if (parts.length === 2) {
    leftRaw = parts[0];
    rightRaw = parts[1];
  } else if (parts.length % 2 === 0) {
    const half = parts.length / 2;
    leftRaw = parts.slice(0, half).join('||');
    rightRaw = parts.slice(half).join('||');
  } else {
    leftRaw = parts[0];
    rightRaw = parts.slice(1).join('||');
  }
  const left = canonicalizeTaH2hPlayerName_(leftRaw);
  const right = canonicalizeTaH2hPlayerName_(rightRaw);
  if (!left || !right) return '';
  return left + '||' + right;
}

function computeLevenshteinDistance_(source, target) {
  const a = String(source || '');
  const b = String(target || '');
  if (!a) return b.length;
  if (!b) return a.length;

  const prev = [];
  const curr = [];
  for (let j = 0; j <= b.length; j += 1) prev[j] = j;

  for (let i = 1; i <= a.length; i += 1) {
    curr[0] = i;
    for (let j = 1; j <= b.length; j += 1) {
      const cost = a.charAt(i - 1) === b.charAt(j - 1) ? 0 : 1;
      curr[j] = Math.min(
        curr[j - 1] + 1,
        prev[j] + 1,
        prev[j - 1] + cost
      );
    }
    for (let j = 0; j <= b.length; j += 1) prev[j] = curr[j];
  }
  return prev[b.length];
}

function getCachedTaLeadersPayload_() {
  try {
    const raw = CacheService.getScriptCache().get(PLAYER_STATS_LEADERS_CACHE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (parsed && parsed._cache_encoding === 'chunked_gzip_base64') {
      const chunkCount = Math.max(0, Number(parsed.chunk_count || 0));
      if (!chunkCount) return null;
      const chunkKeys = [];
      for (let i = 0; i < chunkCount; i += 1) {
        chunkKeys.push((parsed.chunk_key_prefix || PLAYER_STATS_LEADERS_CHUNK_KEY_PREFIX) + i);
      }
      const chunks = CacheService.getScriptCache().getAll(chunkKeys);
      const joined = chunkKeys.map(function (key) { return chunks[key] || ''; }).join('');
      if (!joined || joined.length < Number(parsed.compressed_base64_length || 0)) return null;
      const compressedBytes = Utilities.base64Decode(joined);
      const inflated = Utilities.ungzip(Utilities.newBlob(compressedBytes));
      return JSON.parse(inflated.getDataAsString());
    }
    return parsed;
  } catch (e) {
    logTaLeadersCacheDiagnostic_('cache_read_failed_non_fatal', {
      reason_code: 'cache_read_failed',
      error: String((e && e.message) || e || ''),
    });
    return null;
  }
}

function setCachedTaLeadersPayload_(payload) {
  const cache = CacheService.getScriptCache();
  const serial = JSON.stringify(payload || {});
  const serialBytes = utf8ByteLength_(serial);
  logTaLeadersCacheDiagnostic_('cache_payload_size_measured', {
    bytes: serialBytes,
    limit_bytes: PLAYER_STATS_CACHE_MAX_BYTES,
  });

  const writeDirect = function (text) {
    cache.put(PLAYER_STATS_LEADERS_CACHE_KEY, text, 21600);
    clearTaLeadersChunkKeys_(cache);
    return {
      ok: true,
      storage_path: 'direct',
      bytes: utf8ByteLength_(text),
      chunk_count: 0,
    };
  };

  try {
    if (serialBytes <= PLAYER_STATS_CACHE_MAX_BYTES) {
      const directResult = writeDirect(serial);
      logTaLeadersCacheDiagnostic_('cache_write_succeeded', directResult);
      return directResult;
    }

    const prunedPayload = pruneTaLeadersPayloadForCache_(payload);
    const prunedSerial = JSON.stringify(prunedPayload);
    const prunedBytes = utf8ByteLength_(prunedSerial);
    if (prunedBytes <= PLAYER_STATS_CACHE_MAX_BYTES) {
      const prunedResult = writeDirect(prunedSerial);
      prunedResult.storage_path = 'pruned';
      logTaLeadersCacheDiagnostic_('cache_write_succeeded', {
        storage_path: 'pruned',
        bytes: prunedBytes,
        original_bytes: serialBytes,
      });
      return prunedResult;
    }

    const chunkedResult = setChunkedCompressedTaLeadersPayload_(cache, payload, serialBytes);
    if (chunkedResult.ok) {
      logTaLeadersCacheDiagnostic_('cache_write_succeeded', chunkedResult);
      return chunkedResult;
    }

    const normalizedPayload = normalizeTaLeadersPayloadForCache_(payload);
    const normalizedSerial = JSON.stringify(normalizedPayload);
    const normalizedBytes = utf8ByteLength_(normalizedSerial);
    if (normalizedBytes <= PLAYER_STATS_CACHE_MAX_BYTES) {
      const normalizedResult = writeDirect(normalizedSerial);
      normalizedResult.storage_path = 'normalized_subset';
      logTaLeadersCacheDiagnostic_('cache_write_succeeded', {
        storage_path: 'normalized_subset',
        bytes: normalizedBytes,
        original_bytes: serialBytes,
      });
      return normalizedResult;
    }

    logTaLeadersCacheDiagnostic_('cache_write_skipped_over_limit', {
      storage_path: 'none',
      bytes: serialBytes,
      pruned_bytes: prunedBytes,
      normalized_bytes: normalizedBytes,
    });
    return {
      ok: false,
      reason_code: 'cache_payload_exceeds_limit',
      storage_path: 'none',
      bytes: serialBytes,
      chunk_count: 0,
    };
  } catch (e) {
    return {
      ok: false,
      reason_code: 'cache_write_exception',
      storage_path: 'none',
      bytes: serialBytes,
      chunk_count: 0,
      error: String((e && e.message) || e || ''),
    };
  }
}

function setChunkedCompressedTaLeadersPayload_(cache, payload, originalBytes) {
  const jsonText = JSON.stringify(payload || {});
  const gzBlob = Utilities.gzip(Utilities.newBlob(jsonText), 'ta_leaders_cache.gz');
  const compressedBase64 = Utilities.base64Encode(gzBlob.getBytes());
  const compressedBytes = utf8ByteLength_(compressedBase64);
  const chunks = [];
  for (let start = 0; start < compressedBase64.length; start += PLAYER_STATS_CACHE_CHUNK_BYTES) {
    chunks.push(compressedBase64.slice(start, start + PLAYER_STATS_CACHE_CHUNK_BYTES));
  }
  if (!chunks.length || chunks.length > 25) {
    return {
      ok: false,
      reason_code: 'chunk_count_out_of_bounds',
      storage_path: 'chunked_compressed',
      bytes: compressedBytes,
      chunk_count: chunks.length,
    };
  }

  const keyValues = {};
  chunks.forEach(function (chunk, idx) {
    keyValues[PLAYER_STATS_LEADERS_CHUNK_KEY_PREFIX + idx] = chunk;
  });
  cache.putAll(keyValues, 21600);
  const manifest = {
    _cache_encoding: 'chunked_gzip_base64',
    chunk_key_prefix: PLAYER_STATS_LEADERS_CHUNK_KEY_PREFIX,
    chunk_count: chunks.length,
    compressed_base64_length: compressedBase64.length,
    cached_at_ms: Date.now(),
  };
  cache.put(PLAYER_STATS_LEADERS_CACHE_KEY, JSON.stringify(manifest), 21600);

  return {
    ok: true,
    storage_path: 'chunked_compressed',
    bytes: compressedBytes,
    original_bytes: Number(originalBytes || 0),
    chunk_count: chunks.length,
  };
}

function clearTaLeadersChunkKeys_(cache) {
  const keys = [];
  for (let i = 0; i < 25; i += 1) {
    keys.push(PLAYER_STATS_LEADERS_CHUNK_KEY_PREFIX + i);
  }
  cache.removeAll(keys);
}

function normalizeTaLeadersPayloadForCache_(payload) {
  const base = payload || {};
  return {
    source: base.source || '',
    fetched_at: base.fetched_at || '',
    cached_at_ms: Number(base.cached_at_ms || Date.now()),
    rows: ((base.rows || []).map(function (row) {
      return {
        date: row.date,
        player_name: row.player_name,
        ranking: row.ranking,
        recent_form: row.recent_form,
        surface_win_rate: row.surface_win_rate,
        hold_pct: row.hold_pct,
        break_pct: row.break_pct,
        bp_saved_pct: row.bp_saved_pct,
        bp_conv_pct: row.bp_conv_pct,
        first_serve_in_pct: row.first_serve_in_pct,
        first_serve_points_won_pct: row.first_serve_points_won_pct,
        second_serve_points_won_pct: row.second_serve_points_won_pct,
        return_points_won_pct: row.return_points_won_pct,
        dr: row.dr,
        tpw_pct: row.tpw_pct,
        numeric_stats: row.numeric_stats,
      };
    })),
  };
}

function pruneTaLeadersPayloadForCache_(payload) {
  const base = payload || {};
  const pruned = {
    source: base.source || '',
    fetched_at: base.fetched_at || '',
    cached_at_ms: Number(base.cached_at_ms || Date.now()),
    rows: (base.rows || []).map(function (row) {
      const copy = Object.assign({}, row || {});
      delete copy.event;
      delete copy.surface;
      delete copy.opponent;
      delete copy.score;
      return copy;
    }),
  };
  return pruned;
}

function utf8ByteLength_(text) {
  return Utilities.newBlob(String(text || '')).getBytes().length;
}

function logTaLeadersCacheDiagnostic_(eventName, payload) {
  const entry = {
    event: eventName,
    payload: payload || {},
    logged_at: new Date().toISOString(),
  };
  Logger.log(JSON.stringify(entry));
  try {
    setStateValue_('PLAYER_STATS_TA_LEADERS_CACHE_DIAGNOSTIC', JSON.stringify(entry));
  } catch (e) {
    // Non-fatal best-effort diagnostics only.
  }
}

function logTaH2hParseDiagnostic_(eventName, payload) {
  const entry = {
    event: eventName,
    payload: payload || {},
    logged_at: new Date().toISOString(),
  };
  Logger.log(JSON.stringify(entry));
  try {
    setStateValue_('PLAYER_STATS_TA_H2H_PARSE_DIAGNOSTIC', JSON.stringify(entry));
  } catch (e) {
    // Non-fatal best-effort diagnostics only.
  }
}

function logTaH2hLookupDiagnostic_(eventName, payload) {
  const entry = {
    event: eventName,
    payload: payload || {},
    logged_at: new Date().toISOString(),
  };
  Logger.log(JSON.stringify(entry));
  try {
    setStateValue_('PLAYER_STATS_TA_H2H_LOOKUP_DIAGNOSTIC', JSON.stringify(entry));
  } catch (e) {
    // Non-fatal best-effort diagnostics only.
  }
}

function getCachedTaH2hDataset_() {
  try {
    const raw = CacheService.getScriptCache().get(PLAYER_STATS_H2H_CACHE_KEY);
    if (!raw) return null;
    return JSON.parse(raw);
  } catch (e) {
    return null;
  }
}

function setCachedTaH2hDataset_(payload) {
  CacheService.getScriptCache().put(PLAYER_STATS_H2H_CACHE_KEY, JSON.stringify(payload || {}), 21600);
}

function persistTaH2hDatasetState_(payload, source, telemetry) {
  const rows = (payload && payload.rows) || [];
  const byPair = (payload && payload.by_pair) || {};
  const sourceUpdatedDate = (payload && payload.source_updated_date) || '';
  const serializableRows = rows.map(function (row) {
    return {
      player_a: row.player_a,
      player_b: row.player_b,
      wins_a: Number(row.wins_a || 0),
      wins_b: Number(row.wins_b || 0),
      source_updated_date: sourceUpdatedDate,
    };
  });

  const serializable = {
    source: (payload && payload.source) || '',
    source_updated_date: sourceUpdatedDate,
    fetched_at: (payload && payload.fetched_at) || new Date().toISOString(),
    cached_at_ms: Number((payload && payload.cached_at_ms) || Date.now()),
    row_count: serializableRows.length,
    rows: serializableRows,
    by_pair: byPair,
  };

  setStateValue_('PLAYER_STATS_H2H_STALE_PAYLOAD', JSON.stringify(serializable), {
    source_meta: {
      source: serializable.source,
      source_type: source || '',
      cached_at_ms: serializable.cached_at_ms,
      row_count: serializable.row_count,
      reference_key: PLAYER_STATS_H2H_CACHE_KEY,
      storage_path: 'cache_reference',
    },
  });
  const metrics = telemetry || {};
  setStateValue_('PLAYER_STATS_H2H_LAST_FETCH_META', JSON.stringify({
    source: serializable.source,
    source_updated_date: serializable.source_updated_date,
    fetched_at: serializable.fetched_at,
    cached_at_ms: serializable.cached_at_ms,
    source_type: source || '',
    row_count: serializable.row_count,
    api_call_count: Number(metrics.api_call_count || 0),
    scrape_call_count: Number(metrics.scrape_call_count || 0),
    provider_available: metrics.provider_available !== false,
    last_success_at: String(metrics.last_success_at || ''),
    last_failure_reason: String(metrics.last_failure_reason || ''),
  }));
}
