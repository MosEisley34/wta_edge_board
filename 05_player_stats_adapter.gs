const MATCHMX_ROW_IDX = {
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
    return {
      ok: true,
      reason_code: 'player_stats_no_players',
      stats_by_player: {},
      api_credit_usage: 0,
      api_call_count: 0,
      scrape_call_count: 0,
    };
  }

  const sourceConfigs = parsePlayerStatsSourceConfigs_(config);

  const attempts = [];
  let totalApiCalls = 0;
  const sourcePayloads = [];

  sourceConfigs.forEach(function (sourceConfig) {
    const result = fetchPlayerStatsFromSingleSource_(sourceConfig, config, players, asOfTime);
    attempts.push(result.reason_code || 'player_stats_unknown_source_result');
    totalApiCalls += Number(result.api_call_count || 0);
    if (result.ok && result.stats_by_player) {
      sourcePayloads.push({
        source_name: sourceConfig.source_name,
        stats_by_player: result.stats_by_player,
      });
    }
  });

  if (sourcePayloads.length) {
    const merged = mergePlayerStatsMaps_(sourcePayloads, players);
    const mergeDiagnostics = buildPlayerStatsMergeDiagnostics_(sourcePayloads, merged, players);
    const mergedHasData = Number((mergeDiagnostics.final || {}).players_with_non_null_stats || 0) > 0;
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
        merge_diagnostics: mergeDiagnostics,
      },
    };
  }

  const sofascoreFallback = fetchPlayerStatsFromSofascore_(config, players, asOfTime);
  totalApiCalls += Number(sofascoreFallback.api_call_count || 0);
  if (sofascoreFallback.ok) {
    return {
      ok: true,
      reason_code: sofascoreFallback.reason_code || 'player_stats_sofascore_success',
      detail: attempts.join(','),
      stats_by_player: sofascoreFallback.stats_by_player || {},
      api_credit_usage: 0,
      api_call_count: totalApiCalls,
      scrape_call_count: 0,
      selection_metadata: {
        source_count: 1,
        merge_diagnostics: buildPlayerStatsMergeDiagnostics_([{ source_name: 'sofascore', stats_by_player: sofascoreFallback.stats_by_player || {} }], sofascoreFallback.stats_by_player || {}, players),
      },
    };
  }

  const leadersSource = fetchPlayerStatsFromLeadersSource_(players, config, asOfTime);
  if (leadersSource.ok) {
    return {
      ok: true,
      reason_code: leadersSource.reason_code || 'ta_matchmx_ok',
      detail: attempts.join(','),
      stats_by_player: leadersSource.stats_by_player || {},
      api_credit_usage: 0,
      api_call_count: totalApiCalls + Number(leadersSource.api_call_count || 0),
      scrape_call_count: 0,
      selection_metadata: leadersSource.selection_metadata || null,
    };
  }

  const scraped = fetchPlayerStatsFromScrapeSources_(config, players);
  if (scraped.ok) {
    return {
      ok: true,
      reason_code: 'player_stats_scrape_success',
      detail: attempts.join(','),
      stats_by_player: scraped.stats_by_player || {},
      api_credit_usage: 0,
      api_call_count: totalApiCalls + Number(scraped.api_call_count || 0),
      scrape_call_count: Number(scraped.api_call_count || 0),
    };
  }

  return {
    ok: false,
    reason_code: leadersSource.reason_code || (attempts.length ? attempts[attempts.length - 1] : 'player_stats_provider_not_configured'),
    detail: attempts.join(','),
    api_credit_usage: 0,
    api_call_count: totalApiCalls + Number(leadersSource.api_call_count || 0) + Number(scraped.api_call_count || 0),
    scrape_call_count: Number(scraped.api_call_count || 0),
    selection_metadata: leadersSource.selection_metadata || null,
  };
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
      return {
        ok: true,
        reason_code: 'ta_matchmx_cache_hit',
        stats_by_player: normalizePlayerStatsResponse_(cached.rows, canonicalPlayers, {
          as_of_time: asOfTime,
          match_window_weeks: Number(config.PLAYER_STATS_MATCH_WINDOW_WEEKS || 52),
          recent_match_count: Number(config.PLAYER_STATS_RECENT_MATCH_COUNT || 0),
        }),
        api_call_count: 0,
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
  const parseDiagnostics = summarizeTaLeadersParseDiagnostics_(structuredRows, normalizedStatsByPlayer, extractedRows.diagnostics);
  const statsCompleteness = summarizePlayerStatsCompleteness_(normalizedStatsByPlayer);
  const coverage = evaluateTaLeadersParseCoverage_(parseDiagnostics, config);
  const hasCoverageMismatch = isTaLeadersCoverageMismatch_(parseDiagnostics);
  const coverageReasonCode = hasCoverageMismatch ? 'ta_parse_coverage_mismatch' : '';
  const qualityGate = evaluateTaLeadersQualityGate_(parseDiagnostics, statsCompleteness, config);
  const taHealthy = qualityGate.meets_thresholds && !hasCoverageMismatch;
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
      players_with_non_null_stats: Number(statsCompleteness.players_with_non_null_stats || 0),
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
        players_with_non_null_stats: Number(statsCompleteness.players_with_non_null_stats || 0),
        stale_rows: staleLeadersCompleteness.row_count,
        stale_non_null_feature_total: staleLeadersCompleteness.non_null_feature_total,
        stale_null_only: staleLeadersCompleteness.is_null_only,
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
  return {
    ok: Object.keys(statsByPlayer).length > 0,
    reason_code: Object.keys(statsByPlayer).length > 0 ? (taHealthy ? 'ta_matchmx_ok' : qualityGate.reason_code) : 'ta_matchmx_parse_failed',
    stats_by_player: statsByPlayer,
    api_call_count: totalCalls,
    selection_metadata: {
      source_selected: 'fresh_payload',
      selection_reason: forceReplaceNullOnlyStale ? 'fresh_healthy_overrode_null_only_stale' : (coverage.exceeds_threshold ? 'fresh_healthy_coverage' : 'fresh_default'),
      fresh_rows: Number(parseDiagnostics.parsed_row_count || 0),
      fresh_non_null_feature_total: Number(coverage.non_null_feature_total || 0),
      fresh_coverage_threshold_rows: Number(coverage.row_threshold || 0),
      fresh_coverage_threshold_non_null_features: Number(coverage.non_null_threshold || 0),
      fresh_coverage_exceeds_threshold: coverage.exceeds_threshold,
      stale_rows: staleLeadersCompleteness.row_count,
      stale_non_null_feature_total: staleLeadersCompleteness.non_null_feature_total,
      stale_null_only: staleLeadersCompleteness.is_null_only,
      cache_replacement_forced: forceReplaceNullOnlyStale,
      quality_gate: qualityGate,
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
  const diagnostics = summarizeTaLeadersParseDiagnostics_(rows, statsByPlayer);
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
  const validNameRatio = Number(info.valid_name_ratio || 0);
  const invalidNameRatio = Math.max(0, 1 - validNameRatio);
  const minValidNameRatio = Math.max(0.2, Number((config && config.PLAYER_STATS_TA_MIN_VALID_NAME_RATIO) || 0.55));
  const minPlayersWithStats = Math.max(1, Number((config && config.PLAYER_STATS_TA_MIN_PLAYERS_WITH_STATS) || 2));
  const nonNullFeatureTotal = Number(nonNullByFeature.ranking || 0) + Number(nonNullByFeature.hold_pct || 0) + Number(nonNullByFeature.break_pct || 0);
  const minNonNullFeatureTotal = Math.max(1, Number((config && config.PLAYER_STATS_TA_MIN_NON_NULL_FEATURE_TOTAL) || 4));
  const hasLowQualityNames = validNameRatio < minValidNameRatio || invalidNameRatio > 0.45;
  const hasEnoughFeatures = nonNullFeatureTotal >= minNonNullFeatureTotal;
  const hasEnoughPlayers = Number(completeness.players_with_non_null_stats || 0) >= minPlayersWithStats;
  const meetsThresholds = !hasLowQualityNames && hasEnoughFeatures && hasEnoughPlayers;

  return {
    meets_thresholds: meetsThresholds,
    valid_name_ratio: roundNumber_(validNameRatio, 3),
    invalid_name_ratio: roundNumber_(invalidNameRatio, 3),
    min_valid_name_ratio: minValidNameRatio,
    non_null_feature_total: nonNullFeatureTotal,
    min_non_null_feature_total: minNonNullFeatureTotal,
    players_with_non_null_stats: Number(completeness.players_with_non_null_stats || 0),
    min_players_with_non_null_stats: minPlayersWithStats,
    non_null_by_feature: nonNullByFeature,
    reason_code: meetsThresholds ? 'ta_matchmx_ok' : (hasLowQualityNames ? 'ta_matchmx_name_quality_low' : 'ta_matchmx_feature_coverage_low'),
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
  const currentFormat = extractMatchMxRowsFromArrayLiteral_(text);
  if (currentFormat.rows.length) return currentFormat;

  const legacyRows = extractMatchMxRowsFromLegacyAssignments_(text);
  return {
    rows: legacyRows,
    diagnostics: {
      parser_format: 'legacy_assignment',
      selected_player_name_col: MATCHMX_ROW_IDX.PLAYER_NAME,
      valid_name_ratio: computeValidNameRatio_(legacyRows),
      non_null_by_feature: summarizeMatchMxNonNullByFeature_(legacyRows),
    },
  };
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
  const text = String(payloadText || '');
  const rowRegex = /matchmx\s*(?:\[\s*\d+\s*\])?\s*=\s*\[([\s\S]*?)\]\s*;/g;
  let match;

  while ((match = rowRegex.exec(text)) !== null) {
    const tokens = parseJsArrayTokens_(match[1]);
    if (tokens.length < 6) continue;
    const structured = buildStructuredMatchMxRow_(tokens);
    if (!structured.player_name || !structured.score) continue;
    rows.push(structured);
  }

  return rows;
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
        if (tokens.length >= 6) tokenRows.push(tokens);
        rowStart = -1;
      }
    }
  }

  const schema = detectMatchMxSchema_(tokenRows);
  const rows = [];
  for (let i = 0; i < tokenRows.length; i += 1) {
    const structured = buildStructuredMatchMxRow_(tokenRows[i], schema.mapping);
    if (structured.player_name && structured.score) rows.push(structured);
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
    selected_player_name_col: bestNameRatio >= 0.45 ? bestNameCol : MATCHMX_ROW_IDX.PLAYER_NAME,
  };
  mapping.player_name = diagnostics.selected_player_name_col;
  mapping.opponent = findBestNameLikeColumn_(rows, maxColumns, mapping.player_name, 0.25, MATCHMX_ROW_IDX.OPPONENT);
  mapping.score = findBestScoreColumn_(rows, maxColumns, MATCHMX_ROW_IDX.SCORE);
  mapping.ranking = findBestNumericColumn_(rows, maxColumns, function (n) { return n >= 1 && n <= 5000; }, MATCHMX_ROW_IDX.RANKING, [mapping.player_name, mapping.opponent, mapping.score]);

  const used = [mapping.player_name, mapping.opponent, mapping.score, mapping.ranking];
  const pctMatcher = function (n) { return n >= 0 && n <= 100.5; };
  mapping.recent_form = findBestNumericColumn_(rows, maxColumns, pctMatcher, MATCHMX_ROW_IDX.RECENT_FORM, used);
  used.push(mapping.recent_form);
  mapping.surface_win_rate = findBestNumericColumn_(rows, maxColumns, pctMatcher, MATCHMX_ROW_IDX.SURFACE_WIN_RATE, used);
  used.push(mapping.surface_win_rate);
  mapping.hold_pct = findBestNumericColumn_(rows, maxColumns, pctMatcher, MATCHMX_ROW_IDX.HOLD_PCT, used);
  used.push(mapping.hold_pct);
  mapping.break_pct = findBestNumericColumn_(rows, maxColumns, pctMatcher, MATCHMX_ROW_IDX.BREAK_PCT, used);
  used.push(mapping.break_pct);

  mapping.bp_saved_pct = findBestNumericColumn_(rows, maxColumns, pctMatcher, MATCHMX_ROW_IDX.BP_SAVED_PCT, used);
  used.push(mapping.bp_saved_pct);
  mapping.bp_conv_pct = findBestNumericColumn_(rows, maxColumns, pctMatcher, MATCHMX_ROW_IDX.BP_CONV_PCT, used);
  used.push(mapping.bp_conv_pct);
  mapping.first_serve_in_pct = findBestNumericColumn_(rows, maxColumns, pctMatcher, MATCHMX_ROW_IDX.FIRST_SERVE_IN_PCT, used);
  used.push(mapping.first_serve_in_pct);
  mapping.first_serve_points_won_pct = findBestNumericColumn_(rows, maxColumns, pctMatcher, MATCHMX_ROW_IDX.FIRST_SERVE_POINTS_WON_PCT, used);
  used.push(mapping.first_serve_points_won_pct);
  mapping.second_serve_points_won_pct = findBestNumericColumn_(rows, maxColumns, pctMatcher, MATCHMX_ROW_IDX.SECOND_SERVE_POINTS_WON_PCT, used);
  used.push(mapping.second_serve_points_won_pct);
  mapping.return_points_won_pct = findBestNumericColumn_(rows, maxColumns, pctMatcher, MATCHMX_ROW_IDX.RETURN_POINTS_WON_PCT, used);
  used.push(mapping.return_points_won_pct);
  mapping.dr = findBestNumericColumn_(rows, maxColumns, function (n) { return n >= 0 && n <= 5; }, MATCHMX_ROW_IDX.DOMINANCE_RATIO, used);
  used.push(mapping.dr);
  mapping.tpw_pct = findBestNumericColumn_(rows, maxColumns, pctMatcher, MATCHMX_ROW_IDX.TOTAL_POINTS_WON_PCT, used);

  diagnostics.mapping = mapping;
  return { mapping: mapping, diagnostics: diagnostics };
}

function buildDefaultMatchMxSchema_() {
  return {
    date: MATCHMX_ROW_IDX.DATE,
    event: MATCHMX_ROW_IDX.EVENT,
    surface: MATCHMX_ROW_IDX.SURFACE,
    player_name: MATCHMX_ROW_IDX.PLAYER_NAME,
    opponent: MATCHMX_ROW_IDX.OPPONENT,
    score: MATCHMX_ROW_IDX.SCORE,
    ranking: MATCHMX_ROW_IDX.RANKING,
    recent_form: MATCHMX_ROW_IDX.RECENT_FORM,
    surface_win_rate: MATCHMX_ROW_IDX.SURFACE_WIN_RATE,
    hold_pct: MATCHMX_ROW_IDX.HOLD_PCT,
    break_pct: MATCHMX_ROW_IDX.BREAK_PCT,
    bp_saved_pct: MATCHMX_ROW_IDX.BP_SAVED_PCT,
    bp_conv_pct: MATCHMX_ROW_IDX.BP_CONV_PCT,
    first_serve_in_pct: MATCHMX_ROW_IDX.FIRST_SERVE_IN_PCT,
    first_serve_points_won_pct: MATCHMX_ROW_IDX.FIRST_SERVE_POINTS_WON_PCT,
    second_serve_points_won_pct: MATCHMX_ROW_IDX.SECOND_SERVE_POINTS_WON_PCT,
    return_points_won_pct: MATCHMX_ROW_IDX.RETURN_POINTS_WON_PCT,
    dr: MATCHMX_ROW_IDX.DOMINANCE_RATIO,
    tpw_pct: MATCHMX_ROW_IDX.TOTAL_POINTS_WON_PCT,
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

function summarizeTaLeadersParseDiagnostics_(rows, statsByPlayer, extractionDiagnostics) {
  const parsedRows = Array.isArray(rows) ? rows : [];
  const normalizedMap = statsByPlayer && typeof statsByPlayer === 'object' ? statsByPlayer : {};
  const uniquePlayers = {};
  const parsedPlayerKeys = {};
  const sampleBefore = [];
  const sampleAfter = [];
  const sampleLimit = 8;

  parsedRows.forEach(function (row) {
    const rawPlayerName = String((row && row.player_name) || '').trim();
    if (rawPlayerName) {
      parsedPlayerKeys[rawPlayerName.toLowerCase()] = true;
      if (sampleBefore.length < sampleLimit) sampleBefore.push(rawPlayerName);
    }
    const canonical = canonicalizePlayerName_(rawPlayerName, {});
    if (canonical) uniquePlayers[canonical] = true;
    if (canonical && sampleAfter.length < sampleLimit) sampleAfter.push(canonical);
  });

  const requestedSamplesBefore = Object.keys(normalizedMap).slice(0, sampleLimit);
  const requestedSamplesAfter = requestedSamplesBefore.map(function (name) {
    return canonicalizePlayerName_(name, {});
  });
  const parsedCanonicalSet = uniquePlayers;
  const requestedCanonicalSet = {};
  requestedSamplesAfter.forEach(function (name) {
    if (name) requestedCanonicalSet[name] = true;
  });

  const overlapCanonicalSamples = [];
  Object.keys(parsedCanonicalSet).forEach(function (name) {
    if (requestedCanonicalSet[name] && overlapCanonicalSamples.length < sampleLimit) {
      overlapCanonicalSamples.push(name);
    }
  });

  let rankingNonNull = 0;
  let holdPctNonNull = 0;
  let breakPctNonNull = 0;
  Object.keys(normalizedMap).forEach(function (player) {
    const stats = normalizedMap[player];
    if (!stats || typeof stats !== 'object') return;
    if (stats.ranking !== null && stats.ranking !== undefined) rankingNonNull += 1;
    if (stats.hold_pct !== null && stats.hold_pct !== undefined) holdPctNonNull += 1;
    if (stats.break_pct !== null && stats.break_pct !== undefined) breakPctNonNull += 1;
  });

  return Object.assign({}, extractionDiagnostics || {}, {
    parsed_row_count: parsedRows.length,
    parsed_player_key_count: Object.keys(parsedPlayerKeys).length,
    unique_players_parsed: Object.keys(uniquePlayers).length,
    parsed_player_key_samples_before_normalization: sampleBefore,
    parsed_player_key_samples_after_normalization: sampleAfter,
    requested_player_key_samples_before_normalization: requestedSamplesBefore,
    requested_player_key_samples_after_normalization: requestedSamplesAfter,
    canonical_player_key_overlap_samples: overlapCanonicalSamples,
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
  });
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
  const body = String(arrayLiteralBody || '');
  const pattern = /"((?:\\.|[^"\\])*)"|'((?:\\.|[^'\\])*)'|([^,]+)/g;
  const tokens = [];
  let part;

  while ((part = pattern.exec(body)) !== null) {
    const raw = part[1] !== undefined ? part[1] : (part[2] !== undefined ? part[2] : part[3]);
    const normalized = String(raw || '').trim();
    if (normalized === 'null' || normalized === 'undefined') {
      tokens.push('');
      continue;
    }
    tokens.push(normalized.replace(/\\"/g, '"').replace(/\\'/g, "'"));
  }

  return tokens;
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
      return {
        source_name: canonicalSourceName,
        base_url: baseUrl,
      };
    })
    .filter(function (value) { return !!value && !!value.base_url; });
}

function fetchPlayerStatsFromSingleSource_(sourceConfig, config, players, asOfTime) {
  const sourceName = canonicalizeStatsProviderName_(sourceConfig && sourceConfig.source_name);
  if (sourceName === 'sofascore') {
    return fetchPlayerStatsFromSofascore_(config, players, asOfTime);
  }
  if (sourceName === 'itf') {
    return fetchPlayerStatsFromItfRankings_(sourceConfig, config, players);
  }

  const baseUrl = sourceConfig && sourceConfig.base_url ? sourceConfig.base_url : '';
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
    };
  }

  const status = Number(response.getResponseCode() || 0);
  if (status < 200 || status >= 300) {
    return {
      ok: false,
      reason_code: 'player_stats_http_' + status,
      api_call_count: 1,
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
    };
  }

  return {
    ok: true,
    reason_code: 'player_stats_api_success',
    stats_by_player: normalizePlayerStatsResponse_(parsed, players),
    api_call_count: 1,
    source_name: sourceConfig && sourceConfig.source_name ? sourceConfig.source_name : 'unknown',
  };
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
  let payload = null;
  try {
    payload = JSON.parse(body || '{}');
  } catch (e) {
    payload = null;
  }

  const missingKeys = [];
  if (!(status >= 200 && status < 300)) missingKeys.push('http_2xx');
  if (contentType.toLowerCase().indexOf('application/json') < 0) missingKeys.push('content-type:application/json');
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
    if (position !== null) rankByPlayer[canonical] = position;
  });

  const statsByPlayer = {};
  dedupePlayerNames_(players || []).forEach(function (playerName) {
    const canonical = canonicalizePlayerName_(playerName);
    statsByPlayer[canonical] = {
      ranking: Object.prototype.hasOwnProperty.call(rankByPlayer, canonical) ? rankByPlayer[canonical] : null,
      recent_form: null,
      recent_form_last_10: null,
      surface_win_rate: null,
      hold_pct: null,
      break_pct: null,
      surface_recent_form: null,
      stats_confidence: Object.prototype.hasOwnProperty.call(rankByPlayer, canonical) ? 0.4 : 0,
      source_used: 'itf_rankings',
      fallback_mode: 'ranking_only',
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
  const eventUrls = [
    baseUrl + '/sport/tennis/events/live',
    baseUrl + '/sport/tennis/events/scheduled',
  ];
  const asOfDate = asOfTime instanceof Date ? asOfTime : new Date(asOfTime || Date.now());
  const dateToken = Utilities.formatDate(asOfDate, 'UTC', 'yyyy-MM-dd');
  eventUrls.push(baseUrl + '/sport/tennis/scheduled-events/' + dateToken);

  const participantIndex = {};
  const canonicalPlayers = dedupePlayerNames_(players || []);
  let apiCallCount = 0;
  let sourceUsed = 'sofascore_live';

  for (let i = 0; i < eventUrls.length; i += 1) {
    const parsed = fetchSofascoreJson_(eventUrls[i], config);
    apiCallCount += Number(parsed.api_call_count || 0);
    if (!parsed.ok) continue;

    indexSofascoreParticipants_(parsed.payload, participantIndex);
    if (i === 0 && Object.keys(participantIndex).length > 0) sourceUsed = 'sofascore_live';
    if (i > 0 && Object.keys(participantIndex).length > 0) sourceUsed = 'sofascore_live+scheduled';

    const matchedPlayers = canonicalPlayers.filter(function (name) { return !!participantIndex[name]; });
    if (matchedPlayers.length >= canonicalPlayers.length || matchedPlayers.length >= 4) break;
  }

  const statsByPlayer = {};
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

    const detail = fetchSofascorePlayerDetail_(participant.id, config);
    const recent = fetchSofascoreRecentForm_(participant.id, config);
    apiCallCount += Number(detail.api_call_count || 0) + Number(recent.api_call_count || 0);

    const ranking = extractSofascoreRanking_(detail.payload);
    const recentForm = extractSofascoreFormProxy_(recent.payload, participant.id);
    const nonNullCore = [ranking, recentForm].filter(function (v) { return v !== null && v !== undefined; }).length;

    statsByPlayer[playerName] = {
      ranking: ranking,
      recent_form: recentForm,
      surface_win_rate: null,
      hold_pct: null,
      break_pct: null,
      stats_confidence: nonNullCore >= 2 ? 0.6 : (nonNullCore === 1 ? 0.35 : 0),
      source_used: sourceUsed,
      fallback_mode: nonNullCore > 0 ? 'limited_features' : 'detail_unavailable',
    };
  });

  return {
    ok: true,
    reason_code: 'player_stats_sofascore_success',
    stats_by_player: statsByPlayer,
    api_call_count: apiCallCount,
    source_name: 'sofascore',
  };
}

function fetchSofascorePlayerDetail_(playerId, config) {
  return fetchSofascoreJson_('https://api.sofascore.com/api/v1/player/' + encodeURIComponent(String(playerId || '')), config);
}

function fetchSofascoreRecentForm_(playerId, config) {
  return fetchSofascoreJson_('https://api.sofascore.com/api/v1/player/' + encodeURIComponent(String(playerId || '')) + '/events/last/0', config);
}

function fetchSofascoreJson_(url, config) {
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

  try {
    return {
      ok: true,
      reason_code: 'sofascore_ok',
      payload: JSON.parse(response.getContentText() || '{}'),
      api_call_count: 1,
    };
  } catch (e) {
    return { ok: false, reason_code: 'sofascore_parse_error', payload: null, api_call_count: 1 };
  }
}

function indexSofascoreParticipants_(payload, index) {
  const target = index || {};
  const events = payload && Array.isArray(payload.events) ? payload.events : [];

  events.forEach(function (event) {
    const teams = [event && event.homeTeam, event && event.awayTeam].filter(function (team) { return !!team; });
    teams.forEach(function (team) {
      const canonicalName = canonicalizePlayerName_(team.name || team.shortName || '', {});
      if (!canonicalName) return;
      target[canonicalName] = {
        id: team.id,
        raw_name: team.name || team.shortName || canonicalName,
      };
    });
  });

  return target;
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

function mergePlayerStatsMaps_(sourcePayloads, canonicalPlayers) {
  const merged = {};
  const players = dedupePlayerNames_(canonicalPlayers || []);
  const sourceMaps = (sourcePayloads || []).map(function (entry) {
    return {
      source_name: canonicalizeStatsProviderName_(entry && entry.source_name),
      stats_map: (entry && entry.stats_by_player) || {},
    };
  });

  players.forEach(function (player) {
    merged[player] = {
      ranking: selectMetricBySourcePriority_(sourceMaps, player, 'ranking', ['wta_stats_zone', 'itf', 'tennis_abstract', 'tennis_explorer', 'sofascore']),
      recent_form: selectMetricBySourcePriority_(sourceMaps, player, 'recent_form', ['sofascore', 'tennis_explorer', 'wta_stats_zone', 'tennis_abstract', 'itf']),
      recent_form_last_10: selectMetricBySourcePriority_(sourceMaps, player, 'recent_form_last_10', ['sofascore', 'tennis_explorer', 'wta_stats_zone', 'tennis_abstract', 'itf']),
      surface_win_rate: selectMetricBySourcePriority_(sourceMaps, player, 'surface_win_rate', ['tennis_abstract', 'wta_stats_zone', 'tennis_explorer', 'sofascore', 'itf']),
      hold_pct: selectMetricBySourcePriority_(sourceMaps, player, 'hold_pct', ['tennis_abstract', 'wta_stats_zone', 'tennis_explorer', 'sofascore', 'itf']),
      break_pct: selectMetricBySourcePriority_(sourceMaps, player, 'break_pct', ['tennis_abstract', 'wta_stats_zone', 'tennis_explorer', 'sofascore', 'itf']),
      surface_hold_pct: selectMetricBySourcePriority_(sourceMaps, player, 'surface_hold_pct', ['tennis_abstract']),
      surface_break_pct: selectMetricBySourcePriority_(sourceMaps, player, 'surface_break_pct', ['tennis_abstract']),
      surface_recent_form: selectMetricBySourcePriority_(sourceMaps, player, 'surface_recent_form', ['tennis_abstract', 'sofascore', 'tennis_explorer', 'wta_stats_zone', 'itf']),
      bp_saved_pct: selectMetricBySourcePriority_(sourceMaps, player, 'bp_saved_pct', ['tennis_abstract']),
      bp_conv_pct: selectMetricBySourcePriority_(sourceMaps, player, 'bp_conv_pct', ['tennis_abstract']),
      first_serve_in_pct: selectMetricBySourcePriority_(sourceMaps, player, 'first_serve_in_pct', ['tennis_abstract']),
      first_serve_points_won_pct: selectMetricBySourcePriority_(sourceMaps, player, 'first_serve_points_won_pct', ['tennis_abstract']),
      second_serve_points_won_pct: selectMetricBySourcePriority_(sourceMaps, player, 'second_serve_points_won_pct', ['tennis_abstract']),
      return_points_won_pct: selectMetricBySourcePriority_(sourceMaps, player, 'return_points_won_pct', ['tennis_abstract']),
      dr: selectMetricBySourcePriority_(sourceMaps, player, 'dr', ['tennis_abstract']),
      tpw_pct: selectMetricBySourcePriority_(sourceMaps, player, 'tpw_pct', ['tennis_abstract']),
    };
  });

  return merged;
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

function buildPlayerStatsMergeDiagnostics_(sourcePayloads, mergedStatsByPlayer, canonicalPlayers) {
  const players = dedupePlayerNames_(canonicalPlayers || []);
  const perSourcePlayersParsed = {};
  const perFeatureContributions = {};
  PLAYER_STATS_CANONICAL_FEATURES.forEach(function (feature) { perFeatureContributions[feature] = {}; });

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
    });
  });

  return {
    per_source_players_parsed: perSourcePlayersParsed,
    per_feature_non_null_contributions: perFeatureContributions,
    final: summarizePlayerStatsCompleteness_(mergedStatsByPlayer || {}),
  };
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

  players.forEach(function (player) {
    const stats = statsByPlayer[player];
    const hasNonNullStats = playerStatsHasNonNullFeatures_(stats);
    if (hasNonNullStats) playersWithNonNullStats += 1;
    else playersWithNullOnlyStats += 1;
  });

  return {
    has_stats: playersWithNonNullStats > 0,
    players_with_non_null_stats: playersWithNonNullStats,
    players_with_null_only_stats: playersWithNullOnlyStats,
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
  if (metrics.has_stats) return reasonCode;
  if (Number(metrics.players_with_null_only_stats || 0) > 0) return 'provider_returned_null_features';
  return reasonCode || 'provider_returned_empty';
}

function getTaH2hRowForCanonicalPair_(config, playerA, playerB) {
  const coverage = getTaH2hCoverageForCanonicalPair_(config, playerA, playerB);
  return coverage && coverage.row ? coverage.row : null;
}

function getTaH2hCoverageForCanonicalPair_(config, playerA, playerB) {
  const canonicalA = canonicalizePlayerName_(playerA || '', {});
  const canonicalB = canonicalizePlayerName_(playerB || '', {});
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
  if (direct) return { row: direct, reason_code: '', reason_metadata: {} };

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
      reason_metadata: {},
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
    const playerA = canonicalizePlayerName_(row && row.player_a, {});
    const playerB = canonicalizePlayerName_(row && row.player_b, {});
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

    const playerA = canonicalizePlayerName_(names.player_a, {});
    const playerB = canonicalizePlayerName_(names.player_b, {});
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
    const playerA = canonicalizePlayerName_(row.player_a, {});
    const playerB = canonicalizePlayerName_(row.player_b, {});
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
  return canonicalizePlayerName_(clean, {});
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
  return [String(playerA || ''), String(playerB || '')].join('||');
}

function buildTaH2hLookupDebugSample_(dataset, canonicalA, canonicalB) {
  const requestedDirectKey = buildTaH2hPairKey_(canonicalA, canonicalB);
  const requestedReverseKey = buildTaH2hPairKey_(canonicalB, canonicalA);
  const byPair = dataset && dataset.by_pair ? dataset.by_pair : {};
  const keys = Object.keys(byPair);
  if (!keys.length) {
    return {
      requested_pair_keys: [requestedDirectKey, requestedReverseKey],
      nearest_available_keys: [],
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
    const score = overlap * 10 - Math.abs(key.length - requestedDirectKey.length) / 100;
    return { key: key, score: score };
  });

  scored.sort(function (a, b) {
    if (b.score !== a.score) return b.score - a.score;
    return String(a.key || '').localeCompare(String(b.key || ''));
  });

  return {
    requested_pair_keys: [requestedDirectKey, requestedReverseKey],
    nearest_available_keys: scored.slice(0, 5).map(function (entry) { return entry.key; }),
  };
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
