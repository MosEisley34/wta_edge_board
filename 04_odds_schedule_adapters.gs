function stageFetchOdds(runId, config, fetchWindow) {
  const start = Date.now();
  const source = 'the_odds_api';
  const lookaheadMs = config.LOOKAHEAD_HOURS * 60 * 60 * 1000;
  const now = Date.now();
  const windowStartMs = fetchWindow && Number.isFinite(fetchWindow.startMs) ? fetchWindow.startMs : now;
  const windowEndMs = fetchWindow && Number.isFinite(fetchWindow.endMs) ? fetchWindow.endMs : now + lookaheadMs;
  const runtimeConfig = getCreditAwareRuntimeConfig_(config);
  const cacheTtlMs = Math.max(1, runtimeConfig.odds_window_cache_ttl_min) * 60000;
  const refreshMinMs = Math.max(1, runtimeConfig.odds_window_refresh_min) * 60000;
  const forceRefresh = !!config.ODDS_WINDOW_FORCE_REFRESH;
  const cacheResult = getCachedPayload_('ODDS_WINDOW_PAYLOAD');
  let adapter;

  if (!forceRefresh && cacheResult && Number.isFinite(cacheResult.cached_at_ms) && (now - cacheResult.cached_at_ms <= cacheTtlMs)) {
    adapter = {
      events: cacheResult.events,
      reason_code: 'odds_cache_hit',
      api_credit_usage: 0,
      api_call_count: 0,
      credit_headers: {},
      selected_source: 'cached_fresh',
      window_meta: buildWindowMeta_(cacheResult.events, cacheResult.cached_at_ms, 'cached_fresh', windowStartMs, windowEndMs),
    };
  } else if (!forceRefresh && cacheResult && Number.isFinite(cacheResult.cached_at_ms) && (now - cacheResult.cached_at_ms < refreshMinMs)) {
    adapter = {
      events: cacheResult.events,
      reason_code: 'odds_cache_stale_refresh_throttled',
      api_credit_usage: 0,
      api_call_count: 0,
      credit_headers: {},
      selected_source: 'cached_stale_fallback',
      window_meta: buildWindowMeta_(cacheResult.events, cacheResult.cached_at_ms, 'cached_stale_fallback', windowStartMs, windowEndMs),
    };
  } else {
    adapter = fetchOddsWindowFromOddsApi_(config, windowStartMs, windowEndMs);
    adapter.selected_source = 'fresh_api';

    if (adapter.reason_code === 'odds_api_success') {
      setCachedPayload_('ODDS_WINDOW_PAYLOAD', adapter.events, buildWindowMeta_(adapter.events, now, 'fresh_api', windowStartMs, windowEndMs));
    } else {
      const stale = getStateJson_('ODDS_WINDOW_STALE_PAYLOAD');
      if (stale && stale.events && stale.events.length) {
        adapter = {
          events: stale.events.map(deserializeOddsEvent_),
          reason_code: 'odds_stale_fallback',
          api_credit_usage: adapter.api_credit_usage,
          api_call_count: adapter.api_call_count,
          credit_headers: adapter.credit_headers,
          selected_source: 'cached_stale_fallback',
          window_meta: {
            cached_at_ms: Number(stale.cached_at_ms) || now,
            source: 'cached_stale_fallback',
            has_games: !!stale.has_games,
            event_count: Number(stale.event_count || 0),
            window_start_ms: Number(stale.window_start_ms || windowStartMs),
            window_end_ms: Number(stale.window_end_ms || windowEndMs),
          },
        };
      }
    }
  }

  const raw = adapter.events || [];
  const filtered = raw.filter((event) => event.commence_time.getTime() >= windowStartMs && event.commence_time.getTime() <= windowEndMs);

  const selectedSource = adapter.selected_source || 'fresh_api';
  const selectedMeta = adapter.window_meta || buildWindowMeta_(filtered, Date.now(), selectedSource, windowStartMs, windowEndMs);

  setStateValue_('ODDS_WINDOW_STALE_PAYLOAD', JSON.stringify({
    cached_at_ms: selectedMeta.cached_at_ms,
    source: selectedSource,
    has_games: filtered.length > 0,
    event_count: filtered.length,
    window_start_ms: windowStartMs,
    window_end_ms: windowEndMs,
    events: filtered.map(serializeOddsEvent_),
  }));

  setStateValue_('ODDS_WINDOW_LAST_FETCH_META', JSON.stringify({
    cached_at_ms: selectedMeta.cached_at_ms,
    source: selectedSource,
    has_games: filtered.length > 0,
    event_count: filtered.length,
    window_start_ms: windowStartMs,
    window_end_ms: windowEndMs,
  }));

  const rows = filtered.map((event) => ({
    key: [event.event_id, event.market, event.outcome].join('|'),
    event_id: event.event_id,
    bookmaker: event.bookmaker,
    bookmaker_keys_considered: (event.bookmaker_keys_considered || []).join(','),
    market: event.market,
    outcome: event.outcome,
    price: event.price,
    odds_timestamp: event.odds_updated_time.toISOString(),
    odds_updated_time: event.odds_updated_time.toISOString(),
    odds_updated_epoch_ms: event.odds_updated_time.getTime(),
    provider_odds_updated_time: event.provider_odds_updated_time ? event.provider_odds_updated_time.toISOString() : '',
    ingestion_timestamp: event.ingestion_timestamp.toISOString(),
    commence_time: event.commence_time.toISOString(),
    commence_epoch_ms: event.commence_time.getTime(),
    competition: event.competition,
    tournament: event.tournament || '',
    event_name: event.event_name || '',
    sport_title: event.sport_title || '',
    home_team: event.home_team || '',
    away_team: event.away_team || '',
    player_1: event.player_1,
    player_2: event.player_2,
    source,
    updated_at: new Date().toISOString(),
  }));

  const summary = buildStageSummary_(runId, 'stageFetchOdds', start, {
    input_count: raw.length,
    output_count: filtered.length,
    provider: source,
    api_credit_usage: adapter.api_credit_usage,
    reason_codes: {
      [adapter.reason_code]: 1,
      ['source_' + selectedSource]: 1,
      within_window: filtered.length,
      outside_window: raw.length - filtered.length,
      odds_rows_emitted: filtered.length,
      events_missing_h2h_outcomes: adapter.events_missing_h2h_outcomes || 0,
      bookmakers_without_h2h_market: adapter.bookmakers_without_h2h_market || 0,
      runtime_mode_soft_degraded: runtimeConfig.mode === 'soft' ? 1 : 0,
    },
  });

  if (runtimeConfig.reason_code) summary.reason_codes[runtimeConfig.reason_code] = (summary.reason_codes[runtimeConfig.reason_code] || 0) + 1;

  const creditSnapshot = updateCreditStateFromHeaders_(runId, adapter.credit_headers || {});
  if (!creditSnapshot.header_present) summary.reason_codes.credit_header_missing = (summary.reason_codes.credit_header_missing || 0) + 1;

  const oddsCreditsObservedAt = localAndUtcTimestamps_(new Date());
  setStateValue_('LAST_ODDS_API_CREDITS', JSON.stringify({
    run_id: runId,
    observed_at: oddsCreditsObservedAt.local,
    observed_at_utc: oddsCreditsObservedAt.utc,
    api_call_count: adapter.api_call_count || 0,
    credit_headers: adapter.credit_headers || {},
  }));

  return { events: filtered, rows, summary, selected_source: selectedSource };
}

function stageFetchSchedule(runId, config, oddsEvents) {
  const start = Date.now();
  const source = 'the_odds_api_schedule';
  const now = Date.now();
  const window = deriveScheduleWindowFromOdds_(oddsEvents, config);
  const runtimeConfig = getCreditAwareRuntimeConfig_(config);
  const cacheTtlMs = Math.max(1, runtimeConfig.odds_window_cache_ttl_min) * 60000;
  const refreshMinMs = Math.max(1, runtimeConfig.odds_window_refresh_min) * 60000;
  const forceRefresh = !!config.ODDS_WINDOW_FORCE_REFRESH;
  const cacheResult = getCachedSchedulePayload_('SCHEDULE_WINDOW_PAYLOAD');
  let scheduleResp;
  let scheduleWindowMetrics = {
    primary_window_matched: 0,
    expanded_window_fallback_used: 0,
    expanded_window_short_circuit: 0,
  };

  if (!window) {
    scheduleResp = {
      events: [],
      reason_code: 'schedule_window_empty',
      api_credit_usage: 0,
      api_call_count: 0,
      credit_headers: {},
      selected_source: 'cached_stale_fallback',
    };
  } else if (!forceRefresh && cacheResult && Number.isFinite(cacheResult.cached_at_ms) && (now - cacheResult.cached_at_ms <= cacheTtlMs)) {
    scheduleResp = {
      events: cacheResult.events,
      reason_code: 'schedule_cache_hit',
      api_credit_usage: 0,
      api_call_count: 0,
      credit_headers: {},
      selected_source: 'cached_fresh',
    };
  } else if (!forceRefresh && cacheResult && Number.isFinite(cacheResult.cached_at_ms) && (now - cacheResult.cached_at_ms < refreshMinMs)) {
    scheduleResp = {
      events: cacheResult.events,
      reason_code: 'schedule_cache_stale_refresh_throttled',
      api_credit_usage: 0,
      api_call_count: 0,
      credit_headers: {},
      selected_source: 'cached_stale_fallback',
    };
  } else {
    scheduleResp = fetchScheduleFromOddsApi_(config, window);
    scheduleResp.selected_source = 'fresh_api';

    if (scheduleResp.reason_code === 'schedule_api_success') {
      setCachedSchedulePayload_('SCHEDULE_WINDOW_PAYLOAD', scheduleResp.events, {
        cached_at_ms: now,
        source: 'fresh_api',
        has_games: scheduleResp.events.length > 0,
        event_count: scheduleResp.events.length,
        window_start_ms: new Date(window.startIso).getTime(),
        window_end_ms: new Date(window.endIso).getTime(),
      });
    } else {
      const stale = getStateJson_('SCHEDULE_WINDOW_STALE_PAYLOAD');
      if (stale && stale.events && stale.events.length) {
        scheduleResp = {
          events: stale.events.map(deserializeScheduleEvent_),
          reason_code: 'schedule_stale_fallback',
          api_credit_usage: scheduleResp.api_credit_usage,
          api_call_count: scheduleResp.api_call_count,
          credit_headers: scheduleResp.credit_headers,
          selected_source: 'cached_stale_fallback',
          selected_cached_at_ms: Number(stale.cached_at_ms) || now,
        };
      }
    }
  }

  if (window && oddsEvents && oddsEvents.length) {
    const primaryCoverage = assessScheduleCoverage_(oddsEvents, scheduleResp.events || [], config);
    const shouldExpand = hasMaterialScheduleCoverageGap_(primaryCoverage);

    if (primaryCoverage.all_matched) {
      scheduleWindowMetrics.primary_window_matched = 1;
      scheduleWindowMetrics.expanded_window_short_circuit = 1;
    }

    if (shouldExpand && runtimeConfig.schedule_refresh_non_critical_enabled) {
      const expandedWindow = expandScheduleWindow_(window, runtimeConfig.match_fallback_expansion_min);
      const expandedResp = fetchScheduleFromOddsApi_(config, expandedWindow);

      if (expandedResp.reason_code === 'schedule_api_success') {
        scheduleResp.events = mergeScheduleEventsById_(scheduleResp.events || [], expandedResp.events || []);
        scheduleWindowMetrics.expanded_window_fallback_used = 1;
      }

      scheduleResp.api_credit_usage = Number(scheduleResp.api_credit_usage || 0) + Number(expandedResp.api_credit_usage || 0);
      scheduleResp.api_call_count = Number(scheduleResp.api_call_count || 0) + Number(expandedResp.api_call_count || 0);
      scheduleResp.credit_headers = mergeCreditHeaders_(scheduleResp.credit_headers || {}, expandedResp.credit_headers || {});
    }
  }

  const inWindow = scheduleResp.events || [];

  const reasonCounts = {};
  const canonicalExamples = [];
  const unresolvedCompetitions = [];
  const unresolvedCompetitionCounts = {};
  const rows = [];
  const allowedEvents = [];
  const tierResolverConfig = buildCompetitionTierResolverConfig_(config);

  inWindow.forEach((event) => {
    const resolved = resolveCompetitionTier_(event, tierResolverConfig);
    const canonical = resolved.canonical_tier;
    const decision = isAllowedTournament(canonical, config);
    reasonCounts[decision.reason_code] = (reasonCounts[decision.reason_code] || 0) + 1;
    canonicalExamples.push({
      raw_name: event.competition,
      canonical_tier: canonical,
      reason_code: decision.reason_code,
      matched_by: resolved.matched_by,
      matched_field: resolved.matched_field,
      resolver_fields: resolved.raw_fields,
    });

    if (canonical === 'UNKNOWN') {
      unresolvedCompetitions.push({
        event_id: event.event_id,
        competition: event.competition,
        source_fields: resolved.raw_fields,
      });

      const unresolvedKey = getUnresolvedCompetitionKey_(resolved.raw_fields);
      unresolvedCompetitionCounts[unresolvedKey] = (unresolvedCompetitionCounts[unresolvedKey] || 0) + 1;
    }

    rows.push({
      key: event.event_id,
      event_id: event.event_id,
      match_id: event.match_id,
      start_time: event.start_time.toISOString(),
      start_epoch_ms: event.start_time.getTime(),
      competition: event.competition,
      player_1: event.player_1,
      player_2: event.player_2,
      canonical_tier: canonical,
      is_allowed: decision.allowed,
      reason_code: decision.reason_code,
      source,
      updated_at: new Date().toISOString(),
    });

    if (decision.allowed) {
      allowedEvents.push({
        event_id: event.event_id,
        match_id: event.match_id,
        start_time: event.start_time,
        competition: event.competition,
        canonical_tier: canonical,
        player_1: event.player_1,
        player_2: event.player_2,
      });
    }
  });

  const windowStartMs = window ? new Date(window.startIso).getTime() : null;
  const windowEndMs = window ? new Date(window.endIso).getTime() : null;
  const selectedSource = scheduleResp.selected_source || 'fresh_api';
  const selectedAtMs = Number(scheduleResp.selected_cached_at_ms)
    || ((cacheResult && Number.isFinite(cacheResult.cached_at_ms) && selectedSource !== 'fresh_api') ? cacheResult.cached_at_ms : now);

  setStateValue_('SCHEDULE_WINDOW_STALE_PAYLOAD', JSON.stringify({
    cached_at_ms: selectedAtMs,
    source: selectedSource,
    has_games: inWindow.length > 0,
    event_count: inWindow.length,
    window_start_ms: windowStartMs,
    window_end_ms: windowEndMs,
    events: inWindow.map(serializeScheduleEvent_),
  }));

  setStateValue_('SCHEDULE_WINDOW_LAST_FETCH_META', JSON.stringify({
    cached_at_ms: selectedAtMs,
    source: selectedSource,
    has_games: inWindow.length > 0,
    event_count: inWindow.length,
    window_start_ms: windowStartMs,
    window_end_ms: windowEndMs,
  }));

  const summary = buildStageSummary_(runId, 'stageFetchSchedule', start, {
    input_count: inWindow.length,
    output_count: inWindow.length,
    provider: source,
    api_credit_usage: scheduleResp.api_credit_usage,
    reason_codes: reasonCounts,
  });

  summary.reason_codes[scheduleResp.reason_code] = (summary.reason_codes[scheduleResp.reason_code] || 0) + 1;
  summary.reason_codes['source_' + selectedSource] = (summary.reason_codes['source_' + selectedSource] || 0) + 1;
  summary.reason_codes.primary_window_matched = (summary.reason_codes.primary_window_matched || 0) + scheduleWindowMetrics.primary_window_matched;
  summary.reason_codes.expanded_window_fallback_used = (summary.reason_codes.expanded_window_fallback_used || 0) + scheduleWindowMetrics.expanded_window_fallback_used;
  summary.reason_codes.expanded_window_short_circuit = (summary.reason_codes.expanded_window_short_circuit || 0) + scheduleWindowMetrics.expanded_window_short_circuit;
  summary.reason_codes.runtime_mode_soft_degraded = (summary.reason_codes.runtime_mode_soft_degraded || 0) + (runtimeConfig.mode === 'soft' ? 1 : 0);
  if (!runtimeConfig.schedule_refresh_non_critical_enabled) {
    summary.reason_codes.schedule_refresh_non_critical_skipped = (summary.reason_codes.schedule_refresh_non_critical_skipped || 0) + 1;
  }
  if (runtimeConfig.reason_code) summary.reason_codes[runtimeConfig.reason_code] = (summary.reason_codes[runtimeConfig.reason_code] || 0) + 1;

  const scheduleCreditSnapshot = updateCreditStateFromHeaders_(runId, scheduleResp.credit_headers || {});
  if (!scheduleCreditSnapshot.header_present) {
    summary.reason_codes.credit_header_missing = (summary.reason_codes.credit_header_missing || 0) + 1;
  }

  const scheduleCreditsObservedAt = localAndUtcTimestamps_(new Date());
  setStateValue_('LAST_SCHEDULE_API_CREDITS', JSON.stringify({
    run_id: runId,
    observed_at: scheduleCreditsObservedAt.local,
    observed_at_utc: scheduleCreditsObservedAt.utc,
    api_call_count: scheduleResp.api_call_count || 0,
    credit_headers: scheduleResp.credit_headers || {},
  }));

  return {
    events: allowedEvents,
    rows,
    summary,
    canonicalExamples,
    unresolvedCompetitions,
    unresolvedCompetitionCounts,
    topUnresolvedCompetitions: getTopCompetitionStrings_(unresolvedCompetitionCounts, 20),
    allowedCount: allowedEvents.length,
    selected_source: selectedSource,
  };
}

function getUnresolvedCompetitionKey_(sourceFields) {
  for (let i = 0; i < sourceFields.length; i += 1) {
    const candidate = normalizeCompetitionValue_(sourceFields[i].value);
    if (candidate) return candidate;
  }
  return 'unknown_competition_value';
}

function getTopCompetitionStrings_(countMap, topN) {
  return Object.keys(countMap || {})
    .map((key) => ({ competition: key, count: countMap[key] }))
    .sort((a, b) => b.count - a.count)
    .slice(0, topN || 10);
}



function getCreditRuntimeMode_(config) {
  const snapshot = getStateJson_('ODDS_API_CREDIT_SNAPSHOT') || {};
  const remaining = Number(snapshot.remaining);
  const headerPresent = !!snapshot.header_present;
  const remainingIsNumeric = snapshot.remaining_is_numeric === true || Number.isFinite(remaining);
  const softLimit = Math.max(0, Number(config.ODDS_MIN_CREDITS_SOFT_LIMIT || 0));
  const hardLimit = Math.max(0, Number(config.ODDS_MIN_CREDITS_HARD_LIMIT || 0));

  if (!headerPresent || !remainingIsNumeric) {
    snapshot.limit_enforced = false;
    setStateValue_('ODDS_API_CREDIT_SNAPSHOT', JSON.stringify(snapshot));
    return {
      mode: 'normal',
      snapshot,
      reason_code: 'credit_header_missing',
    };
  }

  if (remaining <= hardLimit) {
    snapshot.limit_enforced = true;
    setStateValue_('ODDS_API_CREDIT_SNAPSHOT', JSON.stringify(snapshot));
    return {
      mode: 'hard',
      snapshot,
      reason_code: 'credit_hard_limit_skip_odds',
    };
  }

  snapshot.limit_enforced = false;
  setStateValue_('ODDS_API_CREDIT_SNAPSHOT', JSON.stringify(snapshot));

  if (remaining <= softLimit) {
    return {
      mode: 'soft',
      snapshot,
      reason_code: 'credit_soft_limit_degraded_mode',
    };
  }

  return {
    mode: 'normal',
    snapshot,
    reason_code: '',
  };
}

function getCreditAwareRuntimeConfig_(config) {
  const modeInfo = getCreditRuntimeMode_(config);
  const runtime = {
    mode: modeInfo.mode,
    reason_code: modeInfo.reason_code,
    snapshot: modeInfo.snapshot,
    odds_window_cache_ttl_min: Number(config.ODDS_WINDOW_CACHE_TTL_MIN || 5),
    odds_window_refresh_min: Number(config.ODDS_WINDOW_REFRESH_MIN || 5),
    match_fallback_expansion_min: Number(config.MATCH_FALLBACK_EXPANSION_MIN || 120),
    schedule_refresh_non_critical_enabled: true,
  };

  if (modeInfo.mode === 'soft') {
    runtime.odds_window_cache_ttl_min = Math.max(runtime.odds_window_cache_ttl_min, runtime.odds_window_cache_ttl_min * 2);
    runtime.odds_window_refresh_min = Math.max(runtime.odds_window_refresh_min, runtime.odds_window_refresh_min * 2);
    runtime.match_fallback_expansion_min = 0;
    runtime.schedule_refresh_non_critical_enabled = false;
  }

  return runtime;
}

function updateCreditStateFromHeaders_(runId, headers) {
  const creditHeaders = headers || {};
  const used = Number(creditHeaders.requests_used);
  const remaining = Number(creditHeaders.requests_remaining);
  const last = Number(creditHeaders.requests_last);
  const hasUsed = creditHeaders.requests_used !== undefined && creditHeaders.requests_used !== null;
  const hasRemaining = creditHeaders.requests_remaining !== undefined && creditHeaders.requests_remaining !== null;
  const hasLast = creditHeaders.requests_last !== undefined && creditHeaders.requests_last !== null;
  const hasHeaderValues = hasUsed || hasRemaining || hasLast;
  const remainingIsNumeric = Number.isFinite(remaining);

  const snapshot = {
    run_id: runId,
    used: Number.isFinite(used) ? used : null,
    remaining: remainingIsNumeric ? remaining : null,
    last: Number.isFinite(last) ? last : null,
    timestamp: formatLocalIso_(new Date()),
    timestamp_utc: new Date().toISOString(),
    header_present: hasHeaderValues,
    remaining_is_numeric: remainingIsNumeric,
    limit_enforced: false,
  };

  setStateValue_('ODDS_API_CREDIT_SNAPSHOT', JSON.stringify(snapshot));
  return snapshot;
}

function buildSkippedOddsStage_(runId, reasonCode, message) {
  const start = Date.now();
  return {
    events: [],
    rows: [],
    summary: buildStageSummary_(runId, 'stageFetchOdds', start, {
      input_count: 0,
      output_count: 0,
      provider: 'the_odds_api',
      api_credit_usage: 0,
      reason_codes: {
        [reasonCode]: 1,
      },
      message: message || '',
    }),
  };
}

function resolveOddsWindowForPipeline_(config, nowMs) {
  const runtimeConfig = getCreditAwareRuntimeConfig_(config);
  const runtimeSnapshot = runtimeConfig.snapshot || {};
  const hardLimitEnforced = runtimeConfig.mode === 'hard'
    && runtimeSnapshot.limit_enforced === true
    && runtimeSnapshot.remaining_is_numeric === true;
  const lookaheadMs = config.LOOKAHEAD_HOURS * 60 * 60 * 1000;

  const sourceWindow = {
    startIso: new Date(nowMs).toISOString(),
    endIso: new Date(nowMs + lookaheadMs).toISOString(),
  };
  const scheduleResp = fetchScheduleFromOddsApi_(config, sourceWindow);
  updateCreditStateFromHeaders_('', scheduleResp.credit_headers || {});
  const targetTiers = {
    WTA_250: true,
    WTA_500: true,
    WTA_1000: true,
    GRAND_SLAM: true,
  };
  const tierResolverConfig = buildCompetitionTierResolverConfig_(config);

  const eligibleStarts = (scheduleResp.events || [])
    .filter((event) => targetTiers[resolveCompetitionTier_(event, tierResolverConfig).canonical_tier])
    .map((event) => event.start_time.getTime())
    .filter((value) => Number.isFinite(value));

  const cachedPayload = getCachedPayload_('ODDS_WINDOW_PAYLOAD') || {};
  const cacheHasEvents = !!((cachedPayload.events || []).length || Number(cachedPayload.event_count || 0) > 0 || cachedPayload.has_games);
  const stalePayload = getStateJson_('ODDS_WINDOW_STALE_PAYLOAD') || {};
  const staleHasEvents = !!((stalePayload.events || []).length || Number(stalePayload.event_count || 0) > 0 || stalePayload.has_games);
  const bootstrapCachedPayloadHasEvents = cacheHasEvents || staleHasEvents;
  const bootstrapWindowHours = Math.max(12, Number(config.ODDS_BOOTSTRAP_LOOKAHEAD_HOURS || 18));
  const bootstrapWindowMs = bootstrapWindowHours * 60 * 60 * 1000;
  const previousRefreshMeta = getStateJson_('ODDS_REFRESH_MODE_META') || {};
  const previousRefreshMode = String(previousRefreshMeta.current_refresh_mode || '');

  const decisionBase = {
    schedule_reason_code: scheduleResp.reason_code,
    schedule_api_credit_usage: scheduleResp.api_credit_usage || 0,
    schedule_api_call_count: scheduleResp.api_call_count || 0,
    schedule_credit_headers: scheduleResp.credit_headers || {},
    eligible_match_count: eligibleStarts.length,
    no_games_behavior: String(config.ODDS_NO_GAMES_BEHAVIOR || 'SKIP').toUpperCase(),
    runtime_credit_mode: runtimeConfig.mode,
    bootstrap_mode: false,
    bootstrap_window_hours: bootstrapWindowHours,
    bootstrap_cached_payload_has_events: bootstrapCachedPayloadHasEvents,
    bootstrap_cached_payload_source: cacheHasEvents ? (cachedPayload.source || 'script_cache') : (stalePayload.source || ''),
    previous_refresh_mode: previousRefreshMode,
    transitioned_from_bootstrap_to_active_window: false,
  };

  const bootstrapEligible = !eligibleStarts.length
    && decisionBase.no_games_behavior === 'SKIP'
    && !bootstrapCachedPayloadHasEvents;

  if (bootstrapEligible) {
    if (hardLimitEnforced) {
      return Object.assign({}, decisionBase, {
        should_fetch_odds: false,
        decision_reason_code: 'odds_refresh_bootstrap_blocked_by_credit_limit',
        decision_message: 'Bootstrap odds fetch blocked by enforced hard credit limit.',
        selected_source: '',
        bootstrap_mode: false,
        current_refresh_mode: 'bootstrap_blocked_by_credit_limit',
        odds_fetch_window: null,
        refresh_window_start_ms: null,
        refresh_window_end_ms: null,
      });
    }

    return Object.assign({}, decisionBase, {
      should_fetch_odds: true,
      decision_reason_code: 'odds_refresh_bootstrap_fetch',
      decision_message: 'No eligible schedule matches and cache/stale payload has no events; forcing bootstrap odds fetch window.',
      selected_source: 'bootstrap_static_window',
      bootstrap_mode: true,
      current_refresh_mode: 'bootstrap',
      odds_fetch_window: {
        startMs: nowMs,
        endMs: nowMs + bootstrapWindowMs,
      },
      refresh_window_start_ms: nowMs,
      refresh_window_end_ms: nowMs + bootstrapWindowMs,
    });
  }

  if (hardLimitEnforced) {
    return Object.assign({}, decisionBase, {
      should_fetch_odds: false,
      decision_reason_code: 'credit_hard_limit_skip_odds',
      decision_message: 'Remaining API credits are below hard limit; running observation-only mode.',
      selected_source: '',
      current_refresh_mode: 'credit_hard_limit_skip',
      odds_fetch_window: null,
      refresh_window_start_ms: null,
      refresh_window_end_ms: null,
    });
  }

  if (!eligibleStarts.length) {
    if (decisionBase.no_games_behavior === 'SKIP') {
      return Object.assign({}, decisionBase, {
        should_fetch_odds: false,
        decision_reason_code: 'odds_refresh_bootstrap_inactive',
        decision_message: 'Bootstrap odds fetch is inactive because cached payload already has events.',
        odds_fetch_window: null,
        refresh_window_start_ms: null,
        refresh_window_end_ms: null,
        current_refresh_mode: 'bootstrap_inactive_cached_events',
      });
    }

    if (decisionBase.no_games_behavior === 'FALLBACK_STATIC_WINDOW') {
      return Object.assign({}, decisionBase, {
        should_fetch_odds: true,
        decision_reason_code: 'odds_refresh_executed_in_window',
        decision_message: 'No eligible schedule matches; using fallback static odds window.',
        selected_source: 'fallback_static_window',
        odds_fetch_window: {
          startMs: nowMs,
          endMs: nowMs + lookaheadMs,
        },
        refresh_window_start_ms: nowMs,
        refresh_window_end_ms: nowMs + lookaheadMs,
        current_refresh_mode: 'fallback_static_window',
      });
    }

    return Object.assign({}, decisionBase, {
      should_fetch_odds: false,
      decision_reason_code: 'odds_refresh_skipped_no_games',
      decision_message: 'No eligible schedule matches in lookahead window.',
      odds_fetch_window: null,
      refresh_window_start_ms: null,
      refresh_window_end_ms: null,
      current_refresh_mode: 'no_games_skipped',
    });
  }

  const firstEligibleStartMs = Math.min.apply(null, eligibleStarts);
  const lastEligibleStartMs = Math.max.apply(null, eligibleStarts);
  const refreshWindowStartMs = firstEligibleStartMs - (config.ODDS_WINDOW_PRE_FIRST_MIN * 60000);
  const refreshWindowEndMs = lastEligibleStartMs + (config.ODDS_WINDOW_POST_LAST_MIN * 60000);
  const inRefreshWindow = nowMs >= refreshWindowStartMs && nowMs <= refreshWindowEndMs;

  return Object.assign({}, decisionBase, {
    first_eligible_start_ms: firstEligibleStartMs,
    last_eligible_start_ms: lastEligibleStartMs,
    refresh_window_start_ms: refreshWindowStartMs,
    refresh_window_end_ms: refreshWindowEndMs,
    selected_source: inRefreshWindow ? 'fresh_api' : '',
    should_fetch_odds: inRefreshWindow,
    current_refresh_mode: inRefreshWindow ? 'active_window' : 'outside_active_window',
    transitioned_from_bootstrap_to_active_window: inRefreshWindow && previousRefreshMode === 'bootstrap',
    decision_reason_code: inRefreshWindow ? 'odds_refresh_executed_in_window' : 'odds_refresh_skipped_outside_window',
    decision_message: inRefreshWindow
      ? 'Current time is inside schedule-derived refresh window.'
      : 'Current time is outside schedule-derived refresh window.',
    odds_fetch_window: inRefreshWindow ? {
      startMs: Math.max(nowMs, refreshWindowStartMs),
      endMs: refreshWindowEndMs,
    } : null,
  });
}

function deriveScheduleWindowFromOdds_(oddsEvents, config) {
  if (!oddsEvents || !oddsEvents.length) return null;
  const commenceTimes = oddsEvents.map((e) => e.commence_time.getTime());
  const minMs = Math.min.apply(null, commenceTimes) - config.SCHEDULE_BUFFER_BEFORE_MIN * 60000;
  const maxMs = Math.max.apply(null, commenceTimes) + config.SCHEDULE_BUFFER_AFTER_MIN * 60000;
  return {
    startIso: new Date(minMs).toISOString(),
    endIso: new Date(maxMs).toISOString(),
  };
}

function expandScheduleWindow_(window, expansionMin) {
  const expandMs = Math.max(0, Number(expansionMin || 0)) * 60000;
  const startMs = new Date(window.startIso).getTime();
  const endMs = new Date(window.endIso).getTime();
  return {
    startIso: new Date(startMs - expandMs).toISOString(),
    endIso: new Date(endMs + expandMs).toISOString(),
  };
}

function assessScheduleCoverage_(oddsEvents, scheduleEvents, config) {
  const canonicalizationExamples = [];
  const aliasMap = buildPlayerAliasMap_(config.PLAYER_ALIAS_MAP_JSON);
  const toleranceMin = Number(config.MATCH_TIME_TOLERANCE_MIN || 0);
  let matchedCount = 0;

  (oddsEvents || []).forEach((oddsEvent) => {
    const result = matchSingleOddsEvent_(oddsEvent, scheduleEvents || [], toleranceMin, aliasMap, canonicalizationExamples);
    if (result.matched) matchedCount += 1;
  });

  const totalOdds = (oddsEvents || []).length;
  return {
    matched_count: matchedCount,
    unmatched_count: Math.max(0, totalOdds - matchedCount),
    total_odds_events: totalOdds,
    all_matched: totalOdds > 0 && matchedCount === totalOdds,
  };
}

function hasMaterialScheduleCoverageGap_(coverage) {
  return !!(coverage && coverage.unmatched_count > 0);
}

function mergeScheduleEventsById_(primaryEvents, expandedEvents) {
  const mergedById = {};

  (primaryEvents || []).forEach((event) => {
    if (!event || !event.event_id) return;
    mergedById[event.event_id] = event;
  });

  (expandedEvents || []).forEach((event) => {
    if (!event || !event.event_id) return;
    if (!mergedById[event.event_id]) mergedById[event.event_id] = event;
  });

  return Object.keys(mergedById).map((eventId) => mergedById[eventId]);
}

function mergeCreditHeaders_(primaryHeaders, secondaryHeaders) {
  return {
    requests_used: Number(primaryHeaders.requests_used || 0) + Number(secondaryHeaders.requests_used || 0),
    requests_remaining: Number(secondaryHeaders.requests_remaining || primaryHeaders.requests_remaining || 0),
    requests_last: Number(secondaryHeaders.requests_last || primaryHeaders.requests_last || 0),
  };
}

function fetchOddsWindowFromOddsApi_(config, startMs, endMs) {
  if (!config.ODDS_API_KEY) {
    return { events: [], reason_code: 'missing_api_key', api_credit_usage: 0, api_call_count: 0, credit_headers: {} };
  }
  const url = config.ODDS_API_BASE_URL + '/sports/' + encodeURIComponent(config.ODDS_SPORT_KEY) + '/odds'
    + '?apiKey=' + encodeURIComponent(config.ODDS_API_KEY)
    + '&regions=' + encodeURIComponent(config.ODDS_REGIONS)
    + '&markets=' + encodeURIComponent(config.ODDS_MARKETS)
    + '&oddsFormat=' + encodeURIComponent(config.ODDS_ODDS_FORMAT)
    + '&commenceTimeFrom=' + encodeURIComponent(new Date(startMs).toISOString())
    + '&commenceTimeTo=' + encodeURIComponent(new Date(endMs).toISOString());
  const fetched = callOddsApi_(url);
  if (!fetched.ok) return fetched;

  const events = [];
  let eventsMissingH2hOutcomes = 0;
  let bookmakersWithoutH2hMarket = 0;

  (fetched.payload || []).forEach((event) => {
    const bestByOutcome = {};
    const allBookmakers = event.bookmakers || [];

    allBookmakers.forEach((bookmaker) => {
      const h2hMarket = (bookmaker.markets || []).find((market) => market.key === 'h2h');
      if (!h2hMarket) {
        bookmakersWithoutH2hMarket += 1;
        return;
      }

      (h2hMarket.outcomes || []).forEach((outcome) => {
        const price = Number(outcome.price);
        if (!outcome.name || !Number.isFinite(price)) return;

        const side = String(outcome.name);
        const providerOddsUpdatedTime = outcome.last_update || h2hMarket.last_update || bookmaker.last_update || '';
        const parsedProviderOddsUpdatedTime = providerOddsUpdatedTime ? new Date(providerOddsUpdatedTime) : null;
        const providerOddsTimestamp = parsedProviderOddsUpdatedTime && !Number.isNaN(parsedProviderOddsUpdatedTime.getTime())
          ? parsedProviderOddsUpdatedTime
          : null;

        const candidate = {
          bookmaker: bookmaker.key || '',
          price,
          provider_odds_updated_time: providerOddsTimestamp,
        };

        if (!bestByOutcome[side]) {
          bestByOutcome[side] = {
            best: candidate,
            bookmakers: {},
          };
        }

        if (!bestByOutcome[side].bookmakers[candidate.bookmaker]) {
          bestByOutcome[side].bookmakers[candidate.bookmaker] = true;
        }

        if (candidate.price > bestByOutcome[side].best.price) {
          bestByOutcome[side].best = candidate;
        }
      });
    });

    const sides = Object.keys(bestByOutcome);
    if (!sides.length) {
      eventsMissingH2hOutcomes += 1;
      return;
    }

    sides.forEach((side) => {
      const best = bestByOutcome[side].best;
      const ingestionTimestamp = new Date();
      events.push({
        event_id: event.id,
        bookmaker: best.bookmaker,
        bookmaker_keys_considered: Object.keys(bestByOutcome[side].bookmakers),
        market: 'h2h',
        outcome: side,
        price: best.price,
        provider_odds_updated_time: best.provider_odds_updated_time,
        ingestion_timestamp: ingestionTimestamp,
        odds_updated_time: best.provider_odds_updated_time || ingestionTimestamp,
        commence_time: new Date(event.commence_time),
        competition: event.tournament || event.sport_title || '',
        tournament: event.tournament || '',
        event_name: event.name || '',
        sport_title: event.sport_title || '',
        home_team: event.home_team || '',
        away_team: event.away_team || '',
        player_1: event.home_team || '',
        player_2: event.away_team || '',
      });
    });
  });

  return {
    events,
    reason_code: 'odds_api_success',
    api_credit_usage: fetched.api_credit_usage,
    api_call_count: 1,
    credit_headers: fetched.credit_headers,
    events_missing_h2h_outcomes: eventsMissingH2hOutcomes,
    bookmakers_without_h2h_market: bookmakersWithoutH2hMarket,
  };
}

function fetchScheduleFromOddsApi_(config, window) {
  if (!config.ODDS_API_KEY) {
    return { events: [], reason_code: 'missing_api_key', api_credit_usage: 0, api_call_count: 0, credit_headers: {} };
  }
  const url = config.ODDS_API_BASE_URL + '/sports/' + encodeURIComponent(config.ODDS_SPORT_KEY) + '/events'
    + '?apiKey=' + encodeURIComponent(config.ODDS_API_KEY)
    + '&commenceTimeFrom=' + encodeURIComponent(window.startIso)
    + '&commenceTimeTo=' + encodeURIComponent(window.endIso);
  const fetched = callOddsApi_(url);
  if (!fetched.ok) return fetched;

  return {
    events: (fetched.payload || []).map((event) => ({
      event_id: event.id,
      match_id: event.id,
      start_time: new Date(event.commence_time),
      competition: event.tournament || event.sport_title || '',
      tournament: event.tournament || '',
      event_name: event.name || '',
      sport_title: event.sport_title || '',
      home_team: event.home_team || '',
      away_team: event.away_team || '',
      player_1: event.home_team || '',
      player_2: event.away_team || '',
    })),
    reason_code: 'schedule_api_success',
    api_credit_usage: fetched.api_credit_usage,
    api_call_count: 1,
    credit_headers: fetched.credit_headers,
  };
}

function callOddsApi_(url) {
  const resp = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
  const status = resp.getResponseCode();
  const headers = resp.getAllHeaders();
  const rawUsed = headers['x-requests-used'] || headers['X-Requests-Used'];
  const rawRemaining = headers['x-requests-remaining'] || headers['X-Requests-Remaining'];
  const rawLast = headers['x-requests-last'] || headers['X-Requests-Last'];
  const hasCreditHeaders = rawUsed !== undefined || rawRemaining !== undefined || rawLast !== undefined;
  const creditHeaders = {
    requests_used: rawUsed === undefined || rawUsed === null || rawUsed === '' ? null : Number(rawUsed),
    requests_remaining: rawRemaining === undefined || rawRemaining === null || rawRemaining === '' ? null : Number(rawRemaining),
    requests_last: rawLast === undefined || rawLast === null || rawLast === '' ? null : Number(rawLast),
  };

  if (status < 200 || status >= 300) {
    return {
      ok: false,
      payload: [],
      reason_code: hasCreditHeaders ? 'api_http_' + status : 'credit_header_missing',
      api_credit_usage: creditHeaders.requests_last || 0,
      api_call_count: 1,
      credit_headers: creditHeaders,
    };
  }

  return {
    ok: true,
    payload: JSON.parse(resp.getContentText() || '[]'),
    api_credit_usage: creditHeaders.requests_last || 0,
    api_call_count: 1,
    credit_headers: creditHeaders,
    reason_code: hasCreditHeaders ? 'api_ok' : 'credit_header_missing',
  };
}

function getCachedPayload_(key) {
  try {
    const raw = CacheService.getScriptCache().get(key);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    return {
      cached_at_ms: parsed.cached_at_ms,
      source: parsed.source || 'fresh_api',
      has_games: !!parsed.has_games,
      event_count: Number(parsed.event_count || 0),
      window_start_ms: Number(parsed.window_start_ms || 0),
      window_end_ms: Number(parsed.window_end_ms || 0),
      events: (parsed.events || []).map(deserializeOddsEvent_),
    };
  } catch (e) {
    return null;
  }
}

function setCachedPayload_(key, events, meta) {
  const mergedMeta = Object.assign({
    cached_at_ms: Date.now(),
    source: 'fresh_api',
    has_games: !!(events && events.length),
    event_count: events ? events.length : 0,
    window_start_ms: null,
    window_end_ms: null,
  }, meta || {});

  CacheService.getScriptCache().put(key, JSON.stringify({
    cached_at_ms: mergedMeta.cached_at_ms,
    source: mergedMeta.source,
    has_games: mergedMeta.has_games,
    event_count: mergedMeta.event_count,
    window_start_ms: mergedMeta.window_start_ms,
    window_end_ms: mergedMeta.window_end_ms,
    events: (events || []).map(serializeOddsEvent_),
  }), 21600);
}

function buildWindowMeta_(events, cachedAtMs, source, windowStartMs, windowEndMs) {
  return {
    cached_at_ms: Number(cachedAtMs) || Date.now(),
    source: source || 'fresh_api',
    has_games: !!(events && events.length),
    event_count: events ? events.length : 0,
    window_start_ms: Number(windowStartMs) || null,
    window_end_ms: Number(windowEndMs) || null,
  };
}

function getCachedSchedulePayload_(key) {
  try {
    const raw = CacheService.getScriptCache().get(key);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    return {
      cached_at_ms: parsed.cached_at_ms,
      source: parsed.source || 'fresh_api',
      has_games: !!parsed.has_games,
      event_count: Number(parsed.event_count || 0),
      window_start_ms: Number(parsed.window_start_ms || 0),
      window_end_ms: Number(parsed.window_end_ms || 0),
      events: (parsed.events || []).map(deserializeScheduleEvent_),
    };
  } catch (e) {
    return null;
  }
}

function setCachedSchedulePayload_(key, events, meta) {
  const mergedMeta = Object.assign({
    cached_at_ms: Date.now(),
    source: 'fresh_api',
    has_games: !!(events && events.length),
    event_count: events ? events.length : 0,
    window_start_ms: null,
    window_end_ms: null,
  }, meta || {});

  CacheService.getScriptCache().put(key, JSON.stringify({
    cached_at_ms: mergedMeta.cached_at_ms,
    source: mergedMeta.source,
    has_games: mergedMeta.has_games,
    event_count: mergedMeta.event_count,
    window_start_ms: mergedMeta.window_start_ms,
    window_end_ms: mergedMeta.window_end_ms,
    events: (events || []).map(serializeScheduleEvent_),
  }), 21600);
}

function serializeScheduleEvent_(event) {
  return {
    event_id: event.event_id,
    match_id: event.match_id,
    start_time: event.start_time.toISOString(),
    competition: event.competition,
    tournament: event.tournament || '',
    event_name: event.event_name || '',
    sport_title: event.sport_title || '',
    home_team: event.home_team || '',
    away_team: event.away_team || '',
    player_1: event.player_1,
    player_2: event.player_2,
  };
}

function deserializeScheduleEvent_(event) {
  return {
    event_id: event.event_id,
    match_id: event.match_id || event.event_id,
    start_time: new Date(event.start_time),
    competition: event.competition,
    tournament: event.tournament || '',
    event_name: event.event_name || '',
    sport_title: event.sport_title || '',
    home_team: event.home_team || '',
    away_team: event.away_team || '',
    player_1: event.player_1,
    player_2: event.player_2,
  };
}

function serializeOddsEvent_(event) {
  return {
    event_id: event.event_id,
    bookmaker: event.bookmaker,
    bookmaker_keys_considered: event.bookmaker_keys_considered,
    market: event.market,
    outcome: event.outcome,
    price: event.price,
    odds_timestamp: event.odds_updated_time.toISOString(),
    odds_updated_time: event.odds_updated_time.toISOString(),
    provider_odds_updated_time: event.provider_odds_updated_time ? event.provider_odds_updated_time.toISOString() : '',
    ingestion_timestamp: event.ingestion_timestamp.toISOString(),
    commence_time: event.commence_time.toISOString(),
    competition: event.competition,
    tournament: event.tournament || '',
    event_name: event.event_name || '',
    sport_title: event.sport_title || '',
    home_team: event.home_team || '',
    away_team: event.away_team || '',
    player_1: event.player_1,
    player_2: event.player_2,
  };
}

function deserializeOddsEvent_(event) {
  return {
    event_id: event.event_id,
    bookmaker: event.bookmaker,
    bookmaker_keys_considered: event.bookmaker_keys_considered || [],
    market: event.market,
    outcome: event.outcome,
    price: event.price,
    provider_odds_updated_time: event.provider_odds_updated_time ? new Date(event.provider_odds_updated_time) : null,
    ingestion_timestamp: new Date(event.ingestion_timestamp || event.odds_updated_time || event.odds_timestamp || event.commence_time),
    odds_updated_time: new Date(event.odds_updated_time || event.odds_timestamp || event.ingestion_timestamp || event.commence_time),
    odds_timestamp: new Date(event.odds_updated_time || event.odds_timestamp || event.ingestion_timestamp || event.commence_time),
    commence_time: new Date(event.commence_time),
    competition: event.competition,
    tournament: event.tournament || '',
    event_name: event.event_name || '',
    sport_title: event.sport_title || '',
    home_team: event.home_team || '',
    away_team: event.away_team || '',
    player_1: event.player_1,
    player_2: event.player_2,
  };
}
