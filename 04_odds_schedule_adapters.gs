function stageFetchOdds(runId, config, fetchWindow, opts) {
  const start = Date.now();
  const source = 'the_odds_api';
  const lookaheadMs = config.LOOKAHEAD_HOURS * 60 * 60 * 1000;
  const now = Date.now();
  const windowStartMs = fetchWindow && Number.isFinite(fetchWindow.startMs) ? fetchWindow.startMs : now;
  const windowEndMs = fetchWindow && Number.isFinite(fetchWindow.endMs) ? fetchWindow.endMs : now + lookaheadMs;
  const runtimeConfig = getCreditAwareRuntimeConfig_(config);
  const cacheTtlMs = Math.max(1, runtimeConfig.odds_window_cache_ttl_min) * 60000;
  const refreshMinMs = Math.max(1, runtimeConfig.odds_window_refresh_min) * 60000;
  const options = opts || {};
  const forceRefresh = !!config.ODDS_WINDOW_FORCE_REFRESH;
  const bypassStaleFallback = !!options.bypass_stale_fallback;
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
      if (!bypassStaleFallback && stale && stale.events && stale.events.length) {
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
      } else if (bypassStaleFallback) {
        adapter.reason_code = 'odds_api_failure_no_stale_fallback';
        adapter.events = Array.isArray(adapter.events) ? adapter.events : [];
      }
    }
  }

  const raw = adapter.events || [];
  const filtered = raw.filter((event) => event.commence_time.getTime() >= windowStartMs && event.commence_time.getTime() <= windowEndMs);

  const selectedSource = adapter.selected_source || 'fresh_api';
  const classifiedEvents = filtered.map(function (event) {
    const classification = classifyOpeningTimestampPolicy_(selectedSource, event);
    return Object.assign({}, event, classification);
  });
  const selectedMeta = adapter.window_meta || buildWindowMeta_(classifiedEvents, Date.now(), selectedSource, windowStartMs, windowEndMs);
  const actualWindowStartMs = Number.isFinite(adapter.window_request_start_ms) ? adapter.window_request_start_ms : windowStartMs;
  const actualWindowEndMs = Number.isFinite(adapter.window_request_end_ms) ? adapter.window_request_end_ms : windowEndMs;

  setStateValue_('ODDS_WINDOW_STALE_PAYLOAD', JSON.stringify({
    cached_at_ms: selectedMeta.cached_at_ms,
    source: selectedSource,
    has_games: classifiedEvents.length > 0,
    event_count: classifiedEvents.length,
    window_start_ms: actualWindowStartMs,
    window_end_ms: actualWindowEndMs,
    events: classifiedEvents.map(serializeOddsEvent_),
  }));

  setStateValue_('ODDS_WINDOW_LAST_FETCH_META', JSON.stringify({
    cached_at_ms: selectedMeta.cached_at_ms,
    source: selectedSource,
    has_games: classifiedEvents.length > 0,
    event_count: classifiedEvents.length,
    window_start_ms: actualWindowStartMs,
    window_end_ms: actualWindowEndMs,
  }));

  const rows = classifiedEvents.map((event) => ({
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
    opening_price: Number.isFinite(Number(event.opening_price)) ? Number(event.opening_price) : '',
    evaluation_price: Number.isFinite(Number(event.evaluation_price)) ? Number(event.evaluation_price) : (Number.isFinite(Number(event.price)) ? Number(event.price) : ''),
    price_delta_bps: Number.isFinite(Number(event.price_delta_bps)) ? Number(event.price_delta_bps) : '',
    open_timestamp: event.open_timestamp ? event.open_timestamp.toISOString() : (event.provider_odds_updated_time ? event.provider_odds_updated_time.toISOString() : ''),
    open_timestamp_epoch_ms: event.open_timestamp ? event.open_timestamp.getTime() : (event.provider_odds_updated_time ? event.provider_odds_updated_time.getTime() : ''),
    open_timestamp_type: event.open_timestamp_type || '',
    open_timestamp_source: event.open_timestamp_source || '',
    opening_lag_policy_tier: event.opening_lag_policy_tier || '',
    opening_lag_minutes: Number.isFinite(Number(event.opening_lag_minutes)) ? Number(event.opening_lag_minutes) : '',
    opening_lag_evaluated_at: event.opening_lag_evaluated_at ? event.opening_lag_evaluated_at.toISOString() : '',
    decision_gate_status: event.decision_gate_status || '',
    is_actionable: event.is_actionable !== false,
    reason_code: event.reason_code || '',
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
    player_1_hold_pct: pickEventValue_(event, ['player_1_hold_pct']),
    player_2_hold_pct: pickEventValue_(event, ['player_2_hold_pct']),
    player_1_break_pct: pickEventValue_(event, ['player_1_break_pct']),
    player_2_break_pct: pickEventValue_(event, ['player_2_break_pct']),
    player_1_form_score: pickEventValue_(event, ['player_1_form_score', 'player_1_recent_form']),
    player_2_form_score: pickEventValue_(event, ['player_2_form_score', 'player_2_recent_form']),
    h2h_p1_wins: pickEventValue_(event, ['h2h_p1_wins']),
    h2h_p2_wins: pickEventValue_(event, ['h2h_p2_wins']),
    h2h_total_matches: pickEventValue_(event, ['h2h_total_matches']),
    surface: pickEventValue_(event, ['surface', 'court_surface']),
    stats_source: pickEventValue_(event, ['stats_source']),
    h2h_source: pickEventValue_(event, ['h2h_source']),
    stats_as_of: pickEventValue_(event, ['stats_as_of', 'as_of_time']),
    source,
    updated_at: new Date().toISOString(),
  }));

  const normalizedOddsCreditHeaders = normalizeCreditHeaders_(adapter.credit_headers || {});

  const summary = buildStageSummary_(runId, 'stageFetchOdds', start, {
    config: config,
    input_count: raw.length,
    output_count: classifiedEvents.length,
    provider: source,
    api_credit_usage: adapter.api_credit_usage,
    reason_codes: {
      [adapter.reason_code]: 1,
      ['source_' + selectedSource]: 1,
      within_window: classifiedEvents.length,
      outside_window: raw.length - classifiedEvents.length,
      odds_rows_emitted: classifiedEvents.length,
      events_missing_h2h_outcomes: adapter.events_missing_h2h_outcomes || 0,
      bookmakers_without_h2h_market: adapter.bookmakers_without_h2h_market || 0,
      runtime_mode_soft_degraded: runtimeConfig.mode === 'soft' ? 1 : 0,
      stale_fallback_bypassed: bypassStaleFallback ? 1 : 0,
    },
  });

  if (runtimeConfig.reason_code) summary.reason_codes[runtimeConfig.reason_code] = (summary.reason_codes[runtimeConfig.reason_code] || 0) + 1;

  const creditSnapshot = updateCreditStateFromHeaders_(runId, normalizedOddsCreditHeaders);
  if (!creditSnapshot.header_present) summary.reason_codes.credit_header_missing = (summary.reason_codes.credit_header_missing || 0) + 1;

  const oddsCreditsObservedAt = localAndUtcTimestamps_(new Date());
  setStateValue_('LAST_ODDS_API_CREDITS', JSON.stringify({
    run_id: runId,
    observed_at: oddsCreditsObservedAt.local,
    observed_at_utc: oddsCreditsObservedAt.utc,
    api_call_count: adapter.api_call_count || 0,
    credit_headers: normalizedOddsCreditHeaders,
  }));


  logDiagnosticEvent_(config, 'stageFetchOdds_window_diagnostics', {
    run_id: runId,
    selected_source: selectedSource,
    requested_window_start: formatLocalIso_(new Date(windowStartMs)),
    requested_window_end: formatLocalIso_(new Date(windowEndMs)),
    actual_window_start: formatLocalIso_(new Date(actualWindowStartMs)),
    actual_window_end: formatLocalIso_(new Date(actualWindowEndMs)),
    raw_event_count: raw.length,
    filtered_event_count: classifiedEvents.length,
    dropped_outside_window_count: raw.length - classifiedEvents.length,
    sample_event_ids: classifiedEvents.slice(0, 10).map((event) => event.event_id),
    credit_headers: normalizedOddsCreditHeaders,
  }, 2);

  return { events: classifiedEvents, rows, summary, selected_source: selectedSource };
}

function classifyOpeningTimestampPolicy_(selectedSource, event) {
  const source = String(selectedSource || 'fresh_api');
  const hasProviderTs = event && event.provider_odds_updated_time instanceof Date && !Number.isNaN(event.provider_odds_updated_time.getTime());
  const hasOpenTs = event && event.open_timestamp instanceof Date && !Number.isNaN(event.open_timestamp.getTime());

  if (source === 'fresh_api') {
    return {
      open_timestamp_type: hasProviderTs || hasOpenTs ? 'provider_live' : 'missing',
      open_timestamp_source: hasProviderTs ? 'provider_odds_updated_time' : (hasOpenTs ? 'open_timestamp' : 'missing'),
      opening_lag_policy_tier: 'strict_gate',
    };
  }

  if (source === 'cached_stale_fallback') {
    return {
      open_timestamp_type: hasProviderTs || hasOpenTs ? 'cached_stale' : 'missing_cached_stale',
      open_timestamp_source: hasProviderTs ? 'cached_provider_odds_updated_time' : (hasOpenTs ? 'cached_open_timestamp' : 'missing'),
      opening_lag_policy_tier: 'fallback_cached_stale',
    };
  }

  return {
    open_timestamp_type: hasProviderTs || hasOpenTs ? 'cached_fresh' : 'missing_cached_fresh',
    open_timestamp_source: hasProviderTs ? 'cached_provider_odds_updated_time' : (hasOpenTs ? 'cached_open_timestamp' : 'missing'),
    opening_lag_policy_tier: 'strict_gate',
  };
}

function stageFetchSchedule(runId, config, oddsEvents, opts) {
  const start = Date.now();
  const source = 'the_odds_api_schedule';
  const options = opts || {};
  const now = Date.now();
  const window = deriveScheduleWindowFromOdds_(oddsEvents, config);
  const runtimeConfig = getCreditAwareRuntimeConfig_(config);
  const cacheTtlMs = Math.max(1, runtimeConfig.odds_window_cache_ttl_min) * 60000;
  const refreshMinMs = Math.max(1, runtimeConfig.odds_window_refresh_min) * 60000;
  const forceRefresh = !!config.ODDS_WINDOW_FORCE_REFRESH;
  const cacheResult = getCachedSchedulePayload_('SCHEDULE_WINDOW_PAYLOAD');
  const stalePayload = getStateJson_('SCHEDULE_WINDOW_STALE_PAYLOAD') || {};
  const stalePayloadEventCount = Math.max(
    Number(stalePayload.event_count || 0),
    Array.isArray(stalePayload.events) ? stalePayload.events.length : 0
  );
  const stalePayloadIsEmpty = stalePayloadEventCount === 0;
  const lastFetchMeta = getStateJson_('SCHEDULE_WINDOW_LAST_FETCH_META') || {};
  const staleEmptyForcedLiveAlreadyAttemptedThisRun = String(lastFetchMeta.run_id || '') === String(runId || '')
    && lastFetchMeta.stale_fallback_empty_forced_live === true;
  const oddsRefreshSkippedOutsideWindow = !!options.odds_refresh_skipped_outside_window;
  const oddsWindowBootstrapMode = !!options.odds_window_bootstrap_mode;
  let scheduleResp;
  let liveFetchHappened = false;
  let staleFallbackUsedWithEvents = 0;
  let staleFallbackEmptyForcedLive = 0;
  let scheduleWindowMetrics = {
    primary_window_matched: 0,
    expanded_window_fallback_used: 0,
    expanded_window_short_circuit: 0,
    invalid_time_window_auto_expanded: 0,
    odds_schedule_window_mismatch_high_severity: 0,
  };

  if (oddsRefreshSkippedOutsideWindow && !oddsWindowBootstrapMode) {
    const hasFreshCache = !!(cacheResult && Number.isFinite(cacheResult.cached_at_ms) && (now - cacheResult.cached_at_ms <= cacheTtlMs));
    if (hasFreshCache) {
      scheduleResp = {
        events: cacheResult.events,
        reason_code: 'schedule_fetch_skipped_outside_window_credit_saver',
        api_credit_usage: 0,
        api_call_count: 0,
        credit_headers: {},
        selected_source: 'cached_fresh',
        selected_cached_at_ms: cacheResult.cached_at_ms,
      };
    } else {
      scheduleResp = {
        events: [],
        reason_code: 'schedule_fetch_skipped_outside_window_credit_saver_cache_expired',
        api_credit_usage: 0,
        api_call_count: 0,
        credit_headers: {},
        selected_source: 'credit_saver_skip',
      };
    }
  } else if (!window) {
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
    const stalePayloadHasEvents = !stalePayloadIsEmpty;
    scheduleResp = {
      events: cacheResult.events,
      reason_code: 'schedule_cache_stale_refresh_throttled',
      api_credit_usage: 0,
      api_call_count: 0,
      credit_headers: {},
      selected_source: stalePayloadHasEvents ? 'cached_stale_fallback' : 'fresh_api',
    };

    if (stalePayloadIsEmpty && !staleEmptyForcedLiveAlreadyAttemptedThisRun) {
      const forcedLiveResp = fetchScheduleFromOddsApi_(config, window);
      liveFetchHappened = true;
      staleFallbackEmptyForcedLive = 1;
      scheduleResp = Object.assign({}, forcedLiveResp, {
        selected_source: 'fresh_api',
      });

      if (forcedLiveResp.reason_code === 'schedule_api_success' || forcedLiveResp.reason_code === 'schedule_api_success_sport_key_fallback') {
        setCachedSchedulePayload_('SCHEDULE_WINDOW_PAYLOAD', scheduleResp.events, {
          cached_at_ms: now,
          source: 'fresh_api',
          has_games: scheduleResp.events.length > 0,
          event_count: scheduleResp.events.length,
          window_start_ms: new Date(window.startIso).getTime(),
          window_end_ms: new Date(window.endIso).getTime(),
        });
      }
    }
  } else {
    scheduleResp = fetchScheduleFromOddsApi_(config, window);
    liveFetchHappened = true;
    scheduleResp.selected_source = 'fresh_api';

    if (scheduleResp.reason_code === 'schedule_api_success' || scheduleResp.reason_code === 'schedule_api_success_sport_key_fallback') {
      setCachedSchedulePayload_('SCHEDULE_WINDOW_PAYLOAD', scheduleResp.events, {
        cached_at_ms: now,
        source: 'fresh_api',
        has_games: scheduleResp.events.length > 0,
        event_count: scheduleResp.events.length,
        window_start_ms: new Date(window.startIso).getTime(),
        window_end_ms: new Date(window.endIso).getTime(),
      });
    } else {
      const stale = stalePayload;
      if (stale && stale.events && stale.events.length) {
        staleFallbackUsedWithEvents = 1;
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

  scheduleResp.live_fetch_happened = liveFetchHappened;
  scheduleResp.stale_fallback_used_with_events = staleFallbackUsedWithEvents;
  scheduleResp.stale_fallback_empty_forced_live = staleFallbackEmptyForcedLive;

  const scheduleInvalidWindowMismatch = !!window
    && !!(oddsEvents && oddsEvents.length)
    && (!scheduleResp.events || !scheduleResp.events.length)
    && scheduleResp.reason_code === 'invalid_time_window';

  if (scheduleInvalidWindowMismatch) {
    scheduleWindowMetrics.odds_schedule_window_mismatch_high_severity = 1;
    logDiagnosticEvent_(config, 'stageFetchSchedule_invalid_window_mismatch', {
      severity: 'HIGH',
      run_id: runId,
      odds_event_count: oddsEvents.length,
      schedule_event_count: (scheduleResp.events || []).length,
      reason_code: scheduleResp.reason_code,
      window_start: window.startIso,
      window_end: window.endIso,
    }, 1);

    const autoExpandMinutes = Math.max(Number(runtimeConfig.match_fallback_expansion_min || 0), 180);
    const autoExpandedWindow = expandScheduleWindow_(window, autoExpandMinutes);
    const autoExpandedResp = fetchScheduleFromOddsApi_(config, autoExpandedWindow, {
      allow_invalid_window_relaxed_retry: false,
    });
    scheduleResp.api_credit_usage = Number(scheduleResp.api_credit_usage || 0) + Number(autoExpandedResp.api_credit_usage || 0);
    scheduleResp.api_call_count = Number(scheduleResp.api_call_count || 0) + Number(autoExpandedResp.api_call_count || 0);
    scheduleResp.credit_headers = mergeCreditHeaders_(scheduleResp.credit_headers || {}, autoExpandedResp.credit_headers || {});

    if (autoExpandedResp.events && autoExpandedResp.events.length) {
      scheduleResp.events = mergeScheduleEventsById_(scheduleResp.events || [], autoExpandedResp.events || []);
      scheduleResp.reason_code = 'invalid_time_window_recovered_relaxed_query';
      scheduleWindowMetrics.invalid_time_window_auto_expanded = 1;
      scheduleWindowMetrics.expanded_window_fallback_used = 1;
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

  const scheduleEnrichment = enrichScheduleEventsFromTennisAbstract_(config, scheduleResp.events || []);
  scheduleResp.events = scheduleEnrichment.events;

  const inWindow = scheduleResp.events || [];

  const reasonCounts = {};
  const canonicalExamples = [];
  const unresolvedCompetitions = [];
  const unresolvedCompetitionCounts = {};
  const rejectionDiagnostics = [];
  const rows = [];
  const allowedEvents = [];
  const tierResolverConfig = buildCompetitionTierResolverConfig_(config);

  inWindow.forEach((event) => {
    const resolved = resolveCompetitionTier_(event, tierResolverConfig);
    const canonical = resolved.canonical_tier;
    const decision = isAllowedTournament(canonical, config);
    const decisionTrace = describeCompetitionDecision_(resolved, decision);
    reasonCounts[decision.reason_code] = (reasonCounts[decision.reason_code] || 0) + 1;
    canonicalExamples.push({
      raw_name: event.competition,
      canonical_tier: canonical,
      reason_code: decision.reason_code,
      competition_decision_trace: decisionTrace,
      matched_by: resolved.matched_by,
      matched_field: resolved.matched_field,
      matched_value: resolved.matched_value || '',
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
      player_1_hold_pct: pickEventValue_(event, ['player_1_hold_pct']),
      player_2_hold_pct: pickEventValue_(event, ['player_2_hold_pct']),
      player_1_break_pct: pickEventValue_(event, ['player_1_break_pct']),
      player_2_break_pct: pickEventValue_(event, ['player_2_break_pct']),
      player_1_form_score: pickEventValue_(event, ['player_1_form_score', 'player_1_recent_form']),
      player_2_form_score: pickEventValue_(event, ['player_2_form_score', 'player_2_recent_form']),
      h2h_p1_wins: pickEventValue_(event, ['h2h_p1_wins']),
      h2h_p2_wins: pickEventValue_(event, ['h2h_p2_wins']),
      h2h_total_matches: pickEventValue_(event, ['h2h_total_matches']),
      surface: pickEventValue_(event, ['surface', 'court_surface']),
      stats_source: pickEventValue_(event, ['stats_source']),
      h2h_source: pickEventValue_(event, ['h2h_source']),
      stats_as_of: pickEventValue_(event, ['stats_as_of', 'as_of_time']),
      canonical_tier: canonical,
      resolved_source_field: resolved.matched_field || '',
      resolved_source_value: resolved.matched_value || '',
      is_allowed: decision.allowed,
      reason_code: decision.reason_code,
      source,
      updated_at: new Date().toISOString(),
    });

    if (!decision.allowed) {
      rejectionDiagnostics.push({
        event_id: event.event_id,
        reason_code: decision.reason_code,
        canonical_tier: canonical,
        source_field: decisionTrace.source_field,
        source_value: decisionTrace.raw_competition,
        canonical_competition: decisionTrace.canonical_competition,
        resolved_tier: decisionTrace.resolved_tier,
        allow_decision: decisionTrace.allow_decision,
        decision_reason: decisionTrace.decision_reason,
      });
    }

    if (decision.allowed) {
      allowedEvents.push({
        event_id: event.event_id,
        match_id: event.match_id,
        start_time: event.start_time,
        competition: event.competition,
        canonical_tier: canonical,
        player_1: event.player_1,
        player_2: event.player_2,
        surface: pickEventValue_(event, ['surface', 'court_surface']),
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
    run_id: runId,
    cached_at_ms: selectedAtMs,
    source: selectedSource,
    has_games: inWindow.length > 0,
    event_count: inWindow.length,
    live_fetch_happened: !!scheduleResp.live_fetch_happened,
    stale_fallback_empty_forced_live: !!scheduleResp.stale_fallback_empty_forced_live,
    window_start_ms: windowStartMs,
    window_end_ms: windowEndMs,
  }));

  const summary = buildStageSummary_(runId, 'stageFetchSchedule', start, {
    config: config,
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
  summary.reason_codes.invalid_time_window_auto_expanded = (summary.reason_codes.invalid_time_window_auto_expanded || 0) + scheduleWindowMetrics.invalid_time_window_auto_expanded;
  summary.reason_codes.odds_schedule_window_mismatch_high_severity = (summary.reason_codes.odds_schedule_window_mismatch_high_severity || 0) + scheduleWindowMetrics.odds_schedule_window_mismatch_high_severity;
  summary.reason_codes.stale_fallback_used_with_events = (summary.reason_codes.stale_fallback_used_with_events || 0) + Number(scheduleResp.stale_fallback_used_with_events || 0);
  summary.reason_codes.stale_fallback_empty_forced_live = (summary.reason_codes.stale_fallback_empty_forced_live || 0) + Number(scheduleResp.stale_fallback_empty_forced_live || 0);
  summary.reason_codes.live_fetch_happened = (summary.reason_codes.live_fetch_happened || 0) + (scheduleResp.live_fetch_happened ? 1 : 0);
  summary.reason_codes[scheduleEnrichment.reason_code] = (summary.reason_codes[scheduleEnrichment.reason_code] || 0) + 1;
  if (scheduleEnrichment.stats_reason_code) {
    const statsReason = 'schedule_enrichment_stats_' + scheduleEnrichment.stats_reason_code;
    summary.reason_codes[statsReason] = (summary.reason_codes[statsReason] || 0) + 1;
  }
  if (scheduleEnrichment.h2h_reason_code) {
    const h2hReason = 'schedule_enrichment_h2h_' + scheduleEnrichment.h2h_reason_code;
    summary.reason_codes[h2hReason] = (summary.reason_codes[h2hReason] || 0) + 1;
  }
  summary.reason_codes.schedule_enrichment_stats_rows_applied = (summary.reason_codes.schedule_enrichment_stats_rows_applied || 0) + Number(scheduleEnrichment.stats_rows_applied || 0);
  summary.reason_codes.schedule_enrichment_h2h_rows_applied = (summary.reason_codes.schedule_enrichment_h2h_rows_applied || 0) + Number(scheduleEnrichment.h2h_rows_applied || 0);
  summary.reason_codes.schedule_enrichment_h2h_missing = (summary.reason_codes.schedule_enrichment_h2h_missing || 0) + Number(scheduleEnrichment.h2h_missing || 0);
  summary.reason_codes.schedule_enrichment_h2h_pairs_requested = (summary.reason_codes.schedule_enrichment_h2h_pairs_requested || 0) + Number(scheduleEnrichment.h2h_pairs_requested || 0);
  summary.reason_codes.schedule_enrichment_h2h_pairs_found = (summary.reason_codes.schedule_enrichment_h2h_pairs_found || 0) + Number(scheduleEnrichment.h2h_pairs_found || 0);
  const h2hMissingReasonCodes = scheduleEnrichment.h2h_missing_reason_codes || {};
  Object.keys(h2hMissingReasonCodes).forEach(function (reasonCode) {
    if (!reasonCode) return;
    const reasonKey = 'schedule_enrichment_h2h_missing_' + reasonCode;
    summary.reason_codes[reasonKey] = (summary.reason_codes[reasonKey] || 0) + Number(h2hMissingReasonCodes[reasonCode] || 0);
  });
  const h2hMissingClassification = scheduleEnrichment.h2h_missing_classification || {};
  summary.reason_codes.schedule_enrichment_h2h_missing_source_partial_coverage = (summary.reason_codes.schedule_enrichment_h2h_missing_source_partial_coverage || 0) + Number(h2hMissingClassification.source_partial_coverage || 0);
  summary.reason_codes.schedule_enrichment_h2h_missing_player_not_found = (summary.reason_codes.schedule_enrichment_h2h_missing_player_not_found || 0) + Number(h2hMissingClassification.player_not_found || 0);
  summary.reason_codes.schedule_enrichment_h2h_missing_matrix_gap = (summary.reason_codes.schedule_enrichment_h2h_missing_matrix_gap || 0) + Number(h2hMissingClassification.matrix_gap || 0);
  summary.reason_codes.schedule_enrichment_h2h_missing_parse_issues = (summary.reason_codes.schedule_enrichment_h2h_missing_parse_issues || 0) + Number(h2hMissingClassification.parse_issues || 0);
  summary.reason_codes.schedule_enrichment_h2h_missing_source_dataset_unavailable = (summary.reason_codes.schedule_enrichment_h2h_missing_source_dataset_unavailable || 0) + Number(h2hMissingClassification.source_dataset_unavailable || 0);
  summary.reason_codes.schedule_enrichment_h2h_missing_invalid_h2h_pair = (summary.reason_codes.schedule_enrichment_h2h_missing_invalid_h2h_pair || 0) + Number(h2hMissingClassification.invalid_h2h_pair || 0);
  summary.reason_codes.schedule_enrichment_h2h_missing_unclassified = (summary.reason_codes.schedule_enrichment_h2h_missing_unclassified || 0) + Number(h2hMissingClassification.unclassified || 0);
  if (!oddsEvents || !oddsEvents.length) {
    summary.reason_codes.schedule_window_fallback_no_odds = (summary.reason_codes.schedule_window_fallback_no_odds || 0) + 1;
  }
  if (options.bootstrap_empty_cycle_mitigation_active) {
    summary.reason_codes.bootstrap_empty_cycle_detected = (summary.reason_codes.bootstrap_empty_cycle_detected || 0) + 1;
  }
  summary.reason_codes.runtime_mode_soft_degraded = (summary.reason_codes.runtime_mode_soft_degraded || 0) + (runtimeConfig.mode === 'soft' ? 1 : 0);
  if (!runtimeConfig.schedule_refresh_non_critical_enabled) {
    summary.reason_codes.schedule_refresh_non_critical_skipped = (summary.reason_codes.schedule_refresh_non_critical_skipped || 0) + 1;
  }
  if (runtimeConfig.reason_code) summary.reason_codes[runtimeConfig.reason_code] = (summary.reason_codes[runtimeConfig.reason_code] || 0) + 1;

  const normalizedScheduleCreditHeaders = normalizeCreditHeaders_(scheduleResp.credit_headers || {});
  const scheduleCreditSnapshot = updateCreditStateFromHeaders_(runId, normalizedScheduleCreditHeaders);
  if (!scheduleCreditSnapshot.header_present) {
    summary.reason_codes.credit_header_missing = (summary.reason_codes.credit_header_missing || 0) + 1;
  }

  const scheduleCreditsObservedAt = localAndUtcTimestamps_(new Date());
  const burnRateSummary = updateCreditBurnRateState_(
    runId,
    config,
    scheduleResp.api_call_count,
    scheduleCreditSnapshot,
    scheduleCreditsObservedAt.utc
  );
  if (burnRateSummary.warning_lt_7d) {
    summary.reason_codes.credit_burn_projected_exhaustion_lt_7d = (summary.reason_codes.credit_burn_projected_exhaustion_lt_7d || 0) + 1;
  }
  if (burnRateSummary.warning_lt_3d) {
    summary.reason_codes.credit_burn_projected_exhaustion_lt_3d = (summary.reason_codes.credit_burn_projected_exhaustion_lt_3d || 0) + 1;
  }

  setStateValue_('LAST_SCHEDULE_API_CREDITS', JSON.stringify({
    run_id: runId,
    observed_at: scheduleCreditsObservedAt.local,
    observed_at_utc: scheduleCreditsObservedAt.utc,
    api_call_count: scheduleResp.api_call_count || 0,
    credit_headers: normalizedScheduleCreditHeaders,
    credit_snapshot: scheduleCreditSnapshot,
    burn_rate: burnRateSummary,
  }));

  
  const topUnresolvedCompetitions = getTopCompetitionStrings_(unresolvedCompetitionCounts, 20);

  logDiagnosticEvent_(config, 'stageFetchSchedule_window_diagnostics', {
    run_id: runId,
    selected_source: selectedSource,
    in_window_count: inWindow.length,
    allowed_count: allowedEvents.length,
    blocked_count: inWindow.length - allowedEvents.length,
    unresolved_competition_count: unresolvedCompetitions.length,
    top_unresolved_competitions: topUnresolvedCompetitions.slice(0, 10),
    window_metrics: scheduleWindowMetrics,
    credit_headers: normalizedScheduleCreditHeaders,
    schedule_enrichment: {
      reason_code: scheduleEnrichment.reason_code,
      stats_reason_code: scheduleEnrichment.stats_reason_code,
      h2h_reason_code: scheduleEnrichment.h2h_reason_code || '',
      h2h_impact: scheduleEnrichment.h2h_impact || null,
      canonical_player_count: scheduleEnrichment.canonical_player_count,
      stats_rows_applied: scheduleEnrichment.stats_rows_applied,
      h2h_rows_applied: scheduleEnrichment.h2h_rows_applied,
      h2h_missing: scheduleEnrichment.h2h_missing,
      h2h_pairs_requested: scheduleEnrichment.h2h_pairs_requested,
      h2h_pairs_found: scheduleEnrichment.h2h_pairs_found,
      h2h_missing_reason_codes: scheduleEnrichment.h2h_missing_reason_codes || {},
      h2h_missing_classification: scheduleEnrichment.h2h_missing_classification || {},
      source_routing: scheduleEnrichment.source_routing || {
        model_mode: String((config && config.MODEL_MODE) || ''),
        disable_sofascore: !!(config && config.DISABLE_SOFASCORE),
      },
      failed: scheduleEnrichment.failed,
      error: scheduleEnrichment.error || '',
    },
  }, 2);

  if (rejectionDiagnostics.length) {
    logDiagnosticEvent_(config, 'stageFetchSchedule_competition_resolution_trace', {
      run_id: runId,
      diagnostics: rejectionDiagnostics,
    }, Math.min(50, rejectionDiagnostics.length));
  }

  return {
    events: allowedEvents,
    rows,
    summary,
    canonicalExamples,
    rejectionDiagnostics,
    unresolvedCompetitions,
    unresolvedCompetitionCounts,
    topUnresolvedCompetitions: topUnresolvedCompetitions,
    allowedCount: allowedEvents.length,
    selected_source: selectedSource,
  };
}

function enrichScheduleEventsFromTennisAbstract_(config, events) {
  const scheduleEvents = events || [];
  if (!scheduleEvents.length) {
    return {
      events: scheduleEvents,
      reason_code: 'schedule_enrichment_no_schedule_events',
      stats_reason_code: '',
      h2h_reason_code: '',
      h2h_impact: null,
      canonical_player_count: 0,
      stats_rows_applied: 0,
      h2h_rows_applied: 0,
      h2h_missing: 0,
      h2h_pairs_requested: 0,
      h2h_pairs_found: 0,
      h2h_missing_reason_codes: {},
      h2h_missing_classification: {
        source_partial_coverage: 0,
        player_not_found: 0,
        matrix_gap: 0,
        parse_issues: 0,
        source_dataset_unavailable: 0,
        invalid_h2h_pair: 0,
        unclassified: 0,
      },
      h2h_lookup_debug_samples: [],
      source_routing: {
        model_mode: String((config && config.MODEL_MODE) || ''),
        disable_sofascore: !!(config && config.DISABLE_SOFASCORE),
      },
      failed: false,
      error: '',
    };
  }

  try {
    const canonicalPlayers = buildCanonicalSchedulePlayers_(scheduleEvents, Date.now());
    if (!canonicalPlayers.length) {
      return {
        events: scheduleEvents,
        reason_code: 'schedule_enrichment_no_upcoming_players',
        stats_reason_code: '',
        h2h_reason_code: '',
        h2h_impact: null,
        canonical_player_count: 0,
        stats_rows_applied: 0,
        h2h_rows_applied: 0,
        h2h_missing: 0,
        h2h_pairs_requested: 0,
        h2h_pairs_found: 0,
        h2h_missing_reason_codes: {},
        h2h_missing_classification: {
          source_partial_coverage: 0,
          player_not_found: 0,
          matrix_gap: 0,
          parse_issues: 0,
          source_dataset_unavailable: 0,
          invalid_h2h_pair: 0,
          unclassified: 0,
        },
        h2h_lookup_debug_samples: [],
        failed: false,
        error: '',
      };
    }

    const asOfTime = scheduleEvents.reduce(function (latest, event) {
      if (!(event && event.start_time instanceof Date)) return latest;
      return event.start_time.getTime() > latest.getTime() ? event.start_time : latest;
    }, new Date(0));
    const statsBatch = fetchPlayerStatsBatch_(config, canonicalPlayers, asOfTime.getTime() > 0 ? asOfTime : new Date());
    const statsByPlayer = statsBatch.stats_by_player || {};
    const sourceRouting = {
      model_mode: String((config && config.MODEL_MODE) || ''),
      disable_sofascore: !!(config && config.DISABLE_SOFASCORE),
    };
    let statsRowsApplied = 0;
    let h2hRowsApplied = 0;
    let h2hMissing = 0;
    let h2hPairsRequested = 0;
    let h2hPairsFound = 0;
    const h2hMissingReasonCodes = {};
    const h2hMissingClassification = {
      source_partial_coverage: 0,
      player_not_found: 0,
      matrix_gap: 0,
      parse_issues: 0,
      source_dataset_unavailable: 0,
      invalid_h2h_pair: 0,
      unclassified: 0,
    };
    const h2hLookupDebugSamples = [];
    const h2hDatasetMeta = getStateJson_('PLAYER_STATS_H2H_LAST_FETCH_META') || {};
    const h2hSummaryReasonCode = resolveScheduleEnrichmentH2hReasonCode_(h2hDatasetMeta);
    const h2hImpact = buildScheduleEnrichmentH2hImpact_(h2hSummaryReasonCode, h2hDatasetMeta);
    let h2hDataset = { by_pair: {} };
    try {
      h2hDataset = getTaH2hDataset_(config || {}) || { by_pair: {} };
    } catch (datasetError) {
      h2hDataset = { by_pair: {} };
      logTaH2hLookupDiagnostic_('ta_h2h_schedule_dataset_snapshot_failed', {
        reason_code: 'dataset_snapshot_failed_non_fatal',
        error: String(datasetError && datasetError.message ? datasetError.message : datasetError),
      });
    }
    const datasetKeys = Object.keys((h2hDataset && h2hDataset.by_pair) || {});
    const scheduleNormalizedKeys = [];
    const scheduleSeen = {};
    scheduleEvents.forEach(function (event) {
      const playerA = canonicalizePlayerName_(event && event.player_1, {});
      const playerB = canonicalizePlayerName_(event && event.player_2, {});
      if (!playerA || !playerB || playerA === playerB) return;
      const scheduleKey = buildTaH2hPairKey_(playerA, playerB);
      if (scheduleSeen[scheduleKey]) return;
      scheduleSeen[scheduleKey] = true;
      scheduleNormalizedKeys.push(scheduleKey);
    });
    logTaH2hLookupDiagnostic_('ta_h2h_schedule_lookup_keys', {
      schedule_pair_keys: scheduleNormalizedKeys.slice(0, 25),
      schedule_pair_count: scheduleNormalizedKeys.length,
      dataset_pair_keys_sample: datasetKeys.slice(0, 25),
      dataset_pair_count: datasetKeys.length,
      h2h_reason_code: h2hSummaryReasonCode,
    });

    const enrichedEvents = scheduleEvents.map(function (event) {
      const playerA = canonicalizePlayerName_(event.player_1, {});
      const playerB = canonicalizePlayerName_(event.player_2, {});
      const statsA = playerA ? statsByPlayer[playerA] : null;
      const statsB = playerB ? statsByPlayer[playerB] : null;
      const hasH2hRequestablePair = !!(playerA && playerB && playerA !== playerB);
      const h2hCoverage = hasH2hRequestablePair ? getTaH2hCoverageForCanonicalPair_(config, playerA, playerB) : null;
      const h2hRow = h2hCoverage && h2hCoverage.row ? h2hCoverage.row : null;

      const merged = Object.assign({}, event);
      const didApplyA = mergeScheduleStatsIntoEvent_(merged, 'player_1', statsA);
      const didApplyB = mergeScheduleStatsIntoEvent_(merged, 'player_2', statsB);
      if (didApplyA || didApplyB) {
        merged.stats_source = merged.stats_source || (statsBatch.source || 'ta_enrichment');
        merged.stats_as_of = merged.stats_as_of || new Date().toISOString();
      }
      if (didApplyA) statsRowsApplied += 1;
      if (didApplyB) statsRowsApplied += 1;

      if (hasH2hRequestablePair) h2hPairsRequested += 1;

      const h2hPairVerified = !!(h2hCoverage && h2hCoverage.reason_metadata && h2hCoverage.reason_metadata.matched_pair_verified === true);
      if (h2hRow) {
        merged.h2h_p1_wins = Number(h2hRow.wins_a || 0);
        merged.h2h_p2_wins = Number(h2hRow.wins_b || 0);
        merged.h2h_total_matches = Number(h2hRow.wins_a || 0) + Number(h2hRow.wins_b || 0);
        merged.h2h_source = merged.h2h_source || 'ta_h2h_matrix';
        h2hRowsApplied += 1;
        if (h2hPairVerified) h2hPairsFound += 1;
      } else if (hasH2hRequestablePair) {
        h2hMissing += 1;
        const missingReasonCode = String((h2hCoverage && h2hCoverage.reason_code) || 'h2h_missing');
        h2hMissingReasonCodes[missingReasonCode] = (h2hMissingReasonCodes[missingReasonCode] || 0) + 1;
        const missingClassificationKey = classifyScheduleEnrichmentH2hMissingReason_(missingReasonCode);
        h2hMissingClassification[missingClassificationKey] = (h2hMissingClassification[missingClassificationKey] || 0) + 1;

        const debugSample = h2hCoverage && h2hCoverage.reason_metadata && h2hCoverage.reason_metadata.debug_sample;
        if (debugSample && h2hLookupDebugSamples.length < 3) {
          h2hLookupDebugSamples.push({
            event_id: merged.event_id || '',
            players: [playerA, playerB],
            reason_code: missingReasonCode,
            schedule_key: debugSample.schedule_key || '',
            requested_pair_keys: debugSample.requested_pair_keys || [],
            nearest_candidate_keys: debugSample.nearest_candidate_keys || [],
            edit_distance_top_matches: debugSample.edit_distance_top_matches || [],
          });
        }
      }

      return merged;
    });

    return {
      events: enrichedEvents,
      reason_code: 'schedule_enrichment_ta_completed',
      stats_reason_code: statsBatch.reason_code || '',
      h2h_reason_code: h2hSummaryReasonCode,
      h2h_impact: h2hImpact,
      canonical_player_count: canonicalPlayers.length,
      stats_rows_applied: statsRowsApplied,
      h2h_rows_applied: h2hRowsApplied,
      h2h_missing: h2hMissing,
      h2h_pairs_requested: h2hPairsRequested,
      h2h_pairs_found: h2hPairsFound,
      h2h_missing_reason_codes: h2hMissingReasonCodes,
      h2h_missing_classification: h2hMissingClassification,
      h2h_lookup_debug_samples: h2hLookupDebugSamples,
      source_routing: sourceRouting,
      failed: false,
      error: '',
    };
  } catch (error) {
    return {
      events: scheduleEvents,
      reason_code: 'schedule_enrichment_ta_failed_non_fatal',
      stats_reason_code: '',
      h2h_reason_code: '',
      h2h_impact: null,
      canonical_player_count: 0,
      stats_rows_applied: 0,
      h2h_rows_applied: 0,
      h2h_missing: 0,
      h2h_pairs_requested: 0,
      h2h_pairs_found: 0,
      h2h_missing_reason_codes: {},
      h2h_missing_classification: {
        source_partial_coverage: 0,
        player_not_found: 0,
        matrix_gap: 0,
        parse_issues: 0,
        source_dataset_unavailable: 0,
        invalid_h2h_pair: 0,
        unclassified: 0,
      },
      h2h_lookup_debug_samples: [],
      source_routing: {
        model_mode: String((config && config.MODEL_MODE) || ''),
        disable_sofascore: !!(config && config.DISABLE_SOFASCORE),
      },
      failed: true,
      error: String(error && error.message ? error.message : error),
    };
  }
}

function classifyScheduleEnrichmentH2hMissingReason_(reasonCode) {
  const code = String(reasonCode || '').trim();
  if (code === 'h2h_player_not_in_matrix') return 'player_not_found';
  if (code === 'ta_h2h_parse_failed') return 'parse_issues';
  if (code === 'h2h_missing') return 'matrix_gap';
  if (code === 'h2h_partial_coverage') return 'source_partial_coverage';
  if (code === 'h2h_dataset_unavailable' || code === 'ta_h2h_fetch_failed') return 'source_dataset_unavailable';
  if (code === 'h2h_pair_invalid') return 'invalid_h2h_pair';
  return 'unclassified';
}


function resolveScheduleEnrichmentH2hReasonCode_(meta) {
  const reason = String((meta && meta.last_failure_reason) || '').trim();
  if (reason === 'h2h_source_empty_table' || reason === 'ta_h2h_empty_table') return 'h2h_source_empty_table';
  return '';
}

function buildScheduleEnrichmentH2hImpact_(reasonCode, meta) {
  const reason = String(reasonCode || '').trim();
  if (!reason) return null;

  const sourceType = String((meta && meta.source_type) || '');
  const rowCount = Number((meta && meta.row_count) || 0);
  return {
    reason_code: reason,
    h2h_features_unavailable: true,
    model_fallback_scope: 'h2h_features_only',
    h2h_source_type: sourceType,
    h2h_row_count: Number.isFinite(rowCount) ? rowCount : 0,
  };
}

function buildCanonicalSchedulePlayers_(events, nowMs) {
  const now = Number(nowMs || Date.now());
  const canonicalPlayers = [];

  (events || []).forEach(function (event) {
    const startMs = event && event.start_time instanceof Date ? event.start_time.getTime() : NaN;
    if (!Number.isFinite(startMs) || startMs < now) return;

    const playerA = canonicalizePlayerName_(event.player_1, {});
    const playerB = canonicalizePlayerName_(event.player_2, {});
    if (playerA) canonicalPlayers.push(playerA);
    if (playerB) canonicalPlayers.push(playerB);
  });

  return dedupePlayerNames_(canonicalPlayers);
}

function mergeScheduleStatsIntoEvent_(event, playerKeyPrefix, statsPayload) {
  if (!statsPayload) return false;
  const holdPct = toNullableNumber_(statsPayload.hold_pct);
  const breakPct = toNullableNumber_(statsPayload.break_pct);
  const formScore = toNullableNumber_(statsPayload.recent_form);

  let updated = false;
  if (!hasNumericValue_(event[playerKeyPrefix + '_hold_pct']) && holdPct !== null) {
    event[playerKeyPrefix + '_hold_pct'] = holdPct;
    updated = true;
  }
  if (!hasNumericValue_(event[playerKeyPrefix + '_break_pct']) && breakPct !== null) {
    event[playerKeyPrefix + '_break_pct'] = breakPct;
    updated = true;
  }
  const currentForm = pickEventValue_(event, [playerKeyPrefix + '_form_score', playerKeyPrefix + '_recent_form']);
  if (!hasNumericValue_(currentForm) && formScore !== null) {
    event[playerKeyPrefix + '_form_score'] = formScore;
    updated = true;
  }

  return updated;
}

function hasNumericValue_(value) {
  return Number.isFinite(Number(value));
}

function toNullableNumber_(value) {
  if (value === null || value === undefined || value === '') return null;
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
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
    odds_refresh_tier_low_upper_min: Math.max(0, Number(config.ODDS_REFRESH_TIER_LOW_UPPER_MIN || 240)),
    odds_refresh_tier_med_upper_min: Math.max(0, Number(config.ODDS_REFRESH_TIER_MED_UPPER_MIN || 180)),
    odds_refresh_tier_high_upper_min: Math.max(0, Number(config.ODDS_REFRESH_TIER_HIGH_UPPER_MIN || 90)),
    odds_refresh_tier_low_interval_min: Math.max(1, Number(config.ODDS_REFRESH_TIER_LOW_INTERVAL_MIN || 20)),
    odds_refresh_tier_med_interval_min: Math.max(1, Number(config.ODDS_REFRESH_TIER_MED_INTERVAL_MIN || 10)),
    odds_refresh_tier_high_interval_min: Math.max(1, Number(config.ODDS_REFRESH_TIER_HIGH_INTERVAL_MIN || 5)),
    match_fallback_expansion_min: Number(config.MATCH_FALLBACK_EXPANSION_MIN || 120),
    schedule_refresh_non_critical_enabled: true,
  };

  if (modeInfo.mode === 'soft') {
    runtime.odds_window_cache_ttl_min = Math.max(runtime.odds_window_cache_ttl_min, runtime.odds_window_cache_ttl_min * 2);
    runtime.odds_window_refresh_min = Math.max(runtime.odds_window_refresh_min, runtime.odds_window_refresh_min * 2);
    runtime.odds_refresh_tier_low_interval_min = Math.max(runtime.odds_refresh_tier_low_interval_min, runtime.odds_refresh_tier_low_interval_min * 2);
    runtime.odds_refresh_tier_med_interval_min = Math.max(runtime.odds_refresh_tier_med_interval_min, runtime.odds_refresh_tier_med_interval_min * 2);
    runtime.odds_refresh_tier_high_interval_min = Math.max(runtime.odds_refresh_tier_high_interval_min, runtime.odds_refresh_tier_high_interval_min * 2);
    runtime.match_fallback_expansion_min = 0;
    runtime.schedule_refresh_non_critical_enabled = false;
  }

  return runtime;
}

function updateCreditStateFromHeaders_(runId, headers) {
  const creditHeaders = normalizeCreditHeaders_(headers || {});
  const used = creditHeaders.requests_used;
  const remaining = creditHeaders.requests_remaining;
  const last = creditHeaders.requests_last;
  const hasHeaderKeys = hasCreditHeaders_(creditHeaders);
  const hasUsableHeaderValues = hasUsableCreditHeaders_(creditHeaders);
  const remainingIsNumeric = Number.isFinite(remaining);
  const headerState = hasUsableHeaderValues
    ? 'numeric'
    : (hasHeaderKeys ? 'non_numeric' : 'absent');

  const snapshot = {
    run_id: runId,
    used: Number.isFinite(used) ? used : null,
    remaining: remainingIsNumeric ? remaining : null,
    last: Number.isFinite(last) ? last : null,
    timestamp: formatLocalIso_(new Date()),
    timestamp_utc: new Date().toISOString(),
    header_present: hasUsableHeaderValues,
    header_state: headerState,
    header_keys_present: hasHeaderKeys,
    remaining_is_numeric: remainingIsNumeric,
    limit_enforced: false,
  };

  setStateValue_('ODDS_API_CREDIT_SNAPSHOT', JSON.stringify(snapshot));
  return snapshot;
}

function updateCreditBurnRateState_(runId, config, scheduleApiCallCount, creditSnapshot, observedAtUtc) {
  const snapshot = creditSnapshot || {};
  const previousScheduleCredits = getStateJson_('LAST_SCHEDULE_API_CREDITS') || {};
  const previousBurnState = getStateJson_('ODDS_API_BURN_RATE_STATE') || {};
  const nowUtc = String(observedAtUtc || new Date().toISOString());
  const nowMs = Date.parse(nowUtc);

  const previousObservedAtUtc = String(previousScheduleCredits.observed_at_utc || '');
  const previousObservedMs = Date.parse(previousObservedAtUtc);
  const elapsedDays = Number.isFinite(nowMs) && Number.isFinite(previousObservedMs) && nowMs > previousObservedMs
    ? (nowMs - previousObservedMs) / 86400000
    : 0;

  const currentRemaining = toNullableNumber_(snapshot.remaining);
  const previousRemaining = toNullableNumber_((previousScheduleCredits.credit_snapshot || {}).remaining);
  let instantaneousCallsPerDay = null;

  if (elapsedDays > 0 && Number.isFinite(currentRemaining) && Number.isFinite(previousRemaining)) {
    const creditsSpent = Math.max(0, previousRemaining - currentRemaining);
    if (creditsSpent > 0) {
      instantaneousCallsPerDay = creditsSpent / elapsedDays;
    }
  }

  if (!Number.isFinite(instantaneousCallsPerDay)) {
    const callsThisRun = Number(scheduleApiCallCount || 0);
    const configuredMinutes = Math.max(1, Number((config && config.PIPELINE_TRIGGER_EVERY_MIN) || 15));
    instantaneousCallsPerDay = callsThisRun * (1440 / configuredMinutes);
  }

  const previousRolling = Number(previousBurnState.calls_per_day_rolling);
  const hasPreviousRolling = Number.isFinite(previousRolling) && previousRolling >= 0;
  const nextRolling = hasPreviousRolling
    ? ((previousRolling * 0.7) + (instantaneousCallsPerDay * 0.3))
    : instantaneousCallsPerDay;

  const projectedDaysRemaining = Number.isFinite(currentRemaining) && Number.isFinite(nextRolling) && nextRolling > 0
    ? (currentRemaining / nextRolling)
    : null;

  const burnState = {
    run_id: runId,
    observed_at_utc: nowUtc,
    observed_at: formatLocalIso_(new Date(Number.isFinite(nowMs) ? nowMs : Date.now())),
    calls_per_day_instantaneous: Number.isFinite(instantaneousCallsPerDay) ? instantaneousCallsPerDay : null,
    calls_per_day_rolling: Number.isFinite(nextRolling) ? nextRolling : null,
    projected_days_remaining: Number.isFinite(projectedDaysRemaining) ? projectedDaysRemaining : null,
    warning_lt_7d: Number.isFinite(projectedDaysRemaining) && projectedDaysRemaining < 7,
    warning_lt_3d: Number.isFinite(projectedDaysRemaining) && projectedDaysRemaining < 3,
    credits_remaining: Number.isFinite(currentRemaining) ? currentRemaining : null,
    previous_observed_at_utc: previousObservedAtUtc || '',
    previous_remaining: Number.isFinite(previousRemaining) ? previousRemaining : null,
    elapsed_days: elapsedDays > 0 ? elapsedDays : null,
  };

  setStateValue_('ODDS_API_BURN_RATE_STATE', JSON.stringify(burnState));
  return burnState;
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


function computeOddsTierCadenceDecision_(runtimeConfig, firstEligibleStartMs, nowMs, lastObservedMarketUpdateMs) {
  const firstStartMs = Number(firstEligibleStartMs);
  const now = Number(nowMs);
  const minutesUntilFirstMatch = Number.isFinite(firstStartMs) && Number.isFinite(now)
    ? Math.max(0, (firstStartMs - now) / 60000)
    : Number.POSITIVE_INFINITY;

  const lowUpper = Math.max(0, Number(runtimeConfig.odds_refresh_tier_low_upper_min || 240));
  const medUpper = Math.max(0, Number(runtimeConfig.odds_refresh_tier_med_upper_min || 180));
  const highUpper = Math.max(0, Number(runtimeConfig.odds_refresh_tier_high_upper_min || 90));
  const lowInterval = Math.max(1, Number(runtimeConfig.odds_refresh_tier_low_interval_min || 20));
  const medInterval = Math.max(1, Number(runtimeConfig.odds_refresh_tier_med_interval_min || 10));
  const highInterval = Math.max(1, Number(runtimeConfig.odds_refresh_tier_high_interval_min || 5));

  let tier = 'outside_tier_window';
  let cadenceMin = 0;

  if (minutesUntilFirstMatch <= highUpper) {
    tier = 'high';
    cadenceMin = highInterval;
  } else if (minutesUntilFirstMatch <= medUpper) {
    tier = 'medium';
    cadenceMin = medInterval;
  } else if (minutesUntilFirstMatch <= lowUpper) {
    tier = 'low';
    cadenceMin = lowInterval;
  }

  const cadenceMs = Math.max(1, cadenceMin) * 60000;
  const normalizedLastObservedMs = Number(lastObservedMarketUpdateMs || 0);
  const mostRecentObservedMs = Number.isFinite(normalizedLastObservedMs) ? normalizedLastObservedMs : 0;
  const lastAttemptMeta = getStateJson_('ODDS_WINDOW_LAST_FETCH_META') || {};
  const lastAttemptMs = Number(lastAttemptMeta.cached_at_ms || 0);
  const elapsedSinceAttemptMs = Number.isFinite(lastAttemptMs) && lastAttemptMs > 0 ? Math.max(0, now - lastAttemptMs) : Number.POSITIVE_INFINITY;
  const unchangedSinceAttempt = Number.isFinite(lastAttemptMs) && lastAttemptMs > 0 && mostRecentObservedMs > 0 && mostRecentObservedMs <= lastAttemptMs;
  const unchangedThrottleActive = cadenceMin > 0 && unchangedSinceAttempt && elapsedSinceAttemptMs < cadenceMs;

  return {
    tier,
    cadence_min: cadenceMin,
    cadence_ms: cadenceMs,
    minutes_until_first_match: minutesUntilFirstMatch,
    should_skip_for_cadence: cadenceMin > 0 && unchangedThrottleActive,
    cadence_reason_code: unchangedThrottleActive ? 'odds_refresh_skipped_tier_cadence_no_market_update' : '',
    unchanged_since_last_fetch: unchangedSinceAttempt,
    latest_market_update_ms: mostRecentObservedMs || null,
    last_fetch_cached_at_ms: Number.isFinite(lastAttemptMs) ? lastAttemptMs : null,
    elapsed_since_last_fetch_ms: Number.isFinite(elapsedSinceAttemptMs) ? elapsedSinceAttemptMs : null,
  };
}

function computeCanonicalTimeWindow_(opts) {
  const options = opts || {};
  const referenceMsRaw = Number(options.reference_ms);
  const referenceMs = Number.isFinite(referenceMsRaw) ? referenceMsRaw : Date.now();
  const lookaheadHours = Math.max(0, Number(options.lookahead_hours || 0));
  const lookaheadMs = lookaheadHours * 60 * 60 * 1000;
  const beforeMs = Math.max(0, Number(options.buffer_before_min || 0)) * 60000;
  const afterMs = Math.max(0, Number(options.buffer_after_min || 0)) * 60000;
  const eventTimes = (options.event_times_ms || []).filter(function (value) {
    return Number.isFinite(Number(value));
  }).map(function (value) {
    return Number(value);
  });

  const hasEventTimes = eventTimes.length > 0;
  const startCandidateMs = hasEventTimes
    ? (Math.min.apply(null, eventTimes) - beforeMs)
    : (referenceMs - beforeMs);
  const endCandidateMs = hasEventTimes
    ? (Math.max.apply(null, eventTimes) + afterMs)
    : (referenceMs + lookaheadMs + afterMs);

  const normalized = normalizeOddsWindowMs_(startCandidateMs, endCandidateMs);
  return {
    start_ms: normalized.start_ms,
    end_ms: normalized.end_ms,
    start_iso: Number.isFinite(normalized.start_ms) ? new Date(normalized.start_ms).toISOString() : '',
    end_iso: Number.isFinite(normalized.end_ms) ? new Date(normalized.end_ms).toISOString() : '',
    source: hasEventTimes ? 'event_times' : 'reference_clock',
  };
}

function resolveOddsWindowForPipeline_(config, nowMs) {
  const runtimeConfig = getCreditAwareRuntimeConfig_(config);
  const runtimeSnapshot = runtimeConfig.snapshot || {};
  const hardLimitEnforced = runtimeConfig.mode === 'hard'
    && runtimeSnapshot.limit_enforced === true
    && runtimeSnapshot.remaining_is_numeric === true;
  const lookaheadMs = config.LOOKAHEAD_HOURS * 60 * 60 * 1000;
  const emptyCycleState = getStateJson_('BOOTSTRAP_EMPTY_CYCLE_STATE') || {};
  const emptyCycleThreshold = Math.max(1, Number(config.BOOTSTRAP_EMPTY_CYCLE_THRESHOLD || 3));
  const emptyCycleCount = Number(emptyCycleState.consecutive_empty_cycles || 0);
  const emptyCycleMitigationReady = emptyCycleCount >= emptyCycleThreshold && !emptyCycleState.mitigation_applied_for_cycle;
  const sourceWindowCanonical = computeCanonicalTimeWindow_({
    reference_ms: nowMs,
    lookahead_hours: config.LOOKAHEAD_HOURS + (emptyCycleMitigationReady ? 6 : 0),
    buffer_before_min: emptyCycleMitigationReady ? 180 : 0,
    buffer_after_min: 0,
  });
  const sourceStartMs = sourceWindowCanonical.start_ms;
  const sourceEndMs = sourceWindowCanonical.end_ms;

  const sourceWindow = {
    startIso: sourceWindowCanonical.start_iso,
    endIso: sourceWindowCanonical.end_iso,
  };
  const scheduleResp = fetchScheduleFromOddsApi_(config, sourceWindow, {
    force_multi_key_discovery: emptyCycleMitigationReady,
  });
  updateCreditStateFromHeaders_('', scheduleResp.credit_headers || {});

  if (emptyCycleMitigationReady) {
    setStateValue_('BOOTSTRAP_EMPTY_CYCLE_STATE', JSON.stringify(Object.assign({}, emptyCycleState, {
      mitigation_applied_for_cycle: true,
      mitigation_applied_at: formatLocalIso_(new Date()),
      mitigation_applied_at_utc: new Date().toISOString(),
      mitigation_window_start_ms: sourceStartMs,
      mitigation_window_end_ms: sourceEndMs,
    })));
  }
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
    bootstrap_empty_cycle_mitigation_active: emptyCycleMitigationReady,
    bootstrap_empty_cycle_count: emptyCycleCount,
    bootstrap_empty_cycle_threshold: emptyCycleThreshold,
  };

  const bootstrapEligible = !eligibleStarts.length
    && decisionBase.no_games_behavior === 'SKIP'
    && !bootstrapCachedPayloadHasEvents;

  if (bootstrapEligible) {
    const bootstrapWindow = computeCanonicalTimeWindow_({
      reference_ms: nowMs,
      lookahead_hours: bootstrapWindowHours,
    });
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
      decision_message: emptyCycleMitigationReady
        ? 'No eligible schedule matches; bootstrap empty-cycle mitigation broadened schedule scan and forced multi-key discovery once.'
        : 'No eligible schedule matches and cache/stale payload has no events; forcing bootstrap odds fetch window.',
      selected_source: 'bootstrap_static_window',
      bootstrap_mode: true,
      current_refresh_mode: 'bootstrap',
      odds_fetch_window: {
        startMs: bootstrapWindow.start_ms,
        endMs: bootstrapWindow.end_ms,
      },
      refresh_window_start_ms: bootstrapWindow.start_ms,
      refresh_window_end_ms: bootstrapWindow.end_ms,
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
      const staticWindow = computeCanonicalTimeWindow_({
        reference_ms: nowMs,
        lookahead_hours: config.LOOKAHEAD_HOURS,
      });
      return Object.assign({}, decisionBase, {
        should_fetch_odds: true,
        decision_reason_code: 'odds_refresh_executed_in_window',
        decision_message: 'No eligible schedule matches; using fallback static odds window.',
        selected_source: 'fallback_static_window',
        odds_fetch_window: {
          startMs: staticWindow.start_ms,
          endMs: staticWindow.end_ms,
        },
        refresh_window_start_ms: staticWindow.start_ms,
        refresh_window_end_ms: staticWindow.end_ms,
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
  const refreshWindow = computeCanonicalTimeWindow_({
    reference_ms: nowMs,
    event_times_ms: [firstEligibleStartMs, lastEligibleStartMs],
    buffer_before_min: config.ODDS_WINDOW_PRE_FIRST_MIN,
    buffer_after_min: config.ODDS_WINDOW_POST_LAST_MIN,
  });
  const refreshWindowStartMs = refreshWindow.start_ms;
  const refreshWindowEndMs = refreshWindow.end_ms;
  const inRefreshWindow = nowMs >= refreshWindowStartMs && nowMs <= refreshWindowEndMs;

  const cachedOddsPayload = getCachedPayload_('ODDS_WINDOW_PAYLOAD') || {};
  const staleOddsPayload = getStateJson_('ODDS_WINDOW_STALE_PAYLOAD') || {};
  const lastObservedFromCached = (cachedOddsPayload.events || []).reduce(function (latest, event) {
    const observedMs = event && event.odds_updated_time instanceof Date
      ? event.odds_updated_time.getTime()
      : Number(event && event.odds_updated_epoch_ms);
    return Number.isFinite(observedMs) ? Math.max(latest, observedMs) : latest;
  }, 0);
  const lastObservedFromStale = (staleOddsPayload.events || []).reduce(function (latest, event) {
    const observedMs = Number(event && (event.odds_updated_epoch_ms || event.provider_odds_updated_epoch_ms));
    return Number.isFinite(observedMs) ? Math.max(latest, observedMs) : latest;
  }, 0);
  const latestObservedMarketUpdateMs = Math.max(lastObservedFromCached, lastObservedFromStale, 0);
  const tierCadence = computeOddsTierCadenceDecision_(runtimeConfig, firstEligibleStartMs, nowMs, latestObservedMarketUpdateMs);
  const shouldFetchOdds = inRefreshWindow && !tierCadence.should_skip_for_cadence;

  return Object.assign({}, decisionBase, {
    first_eligible_start_ms: firstEligibleStartMs,
    last_eligible_start_ms: lastEligibleStartMs,
    refresh_window_start_ms: refreshWindowStartMs,
    refresh_window_end_ms: refreshWindowEndMs,
    selected_source: shouldFetchOdds ? 'fresh_api' : '',
    should_fetch_odds: shouldFetchOdds,
    current_refresh_mode: inRefreshWindow ? 'active_window' : 'outside_active_window',
    refresh_tier: tierCadence.tier,
    refresh_cadence_min: tierCadence.cadence_min,
    minutes_until_first_match: tierCadence.minutes_until_first_match,
    latest_market_update_ms: tierCadence.latest_market_update_ms,
    last_fetch_cached_at_ms: tierCadence.last_fetch_cached_at_ms,
    elapsed_since_last_fetch_ms: tierCadence.elapsed_since_last_fetch_ms,
    unchanged_since_last_fetch: tierCadence.unchanged_since_last_fetch,
    transitioned_from_bootstrap_to_active_window: shouldFetchOdds && previousRefreshMode === 'bootstrap',
    decision_reason_code: !inRefreshWindow
      ? 'odds_refresh_skipped_outside_window'
      : (tierCadence.should_skip_for_cadence
        ? tierCadence.cadence_reason_code
        : 'odds_refresh_executed_in_window'),
    decision_message: !inRefreshWindow
      ? 'Current time is outside schedule-derived refresh window.'
      : (tierCadence.should_skip_for_cadence
        ? 'Inside refresh window but skipping immediate refetch because markets are unchanged and cadence interval has not elapsed.'
        : 'Current time is inside schedule-derived refresh window.'),
    odds_fetch_window: shouldFetchOdds ? {
      startMs: Math.max(nowMs, refreshWindowStartMs),
      endMs: refreshWindowEndMs,
    } : null,
  });
}

function deriveScheduleWindowFromOdds_(oddsEvents, config) {
  const commenceTimes = (oddsEvents || []).map(function (event) {
    return event && event.commence_time ? event.commence_time.getTime() : NaN;
  });
  const window = computeCanonicalTimeWindow_({
    reference_ms: Date.now(),
    lookahead_hours: Math.max(1, Number(config.LOOKAHEAD_HOURS || 36)),
    buffer_before_min: Number(config.SCHEDULE_BUFFER_BEFORE_MIN || 0),
    buffer_after_min: Number(config.SCHEDULE_BUFFER_AFTER_MIN || 0),
    event_times_ms: commenceTimes,
  });
  return {
    startIso: window.start_iso,
    endIso: window.end_iso,
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
    const dedupeKey = buildScheduleEventDedupeKey_(event);
    if (!dedupeKey) return;
    mergedById[dedupeKey] = event;
  });

  (expandedEvents || []).forEach((event) => {
    const dedupeKey = buildScheduleEventDedupeKey_(event);
    if (!dedupeKey) return;
    if (!mergedById[dedupeKey]) mergedById[dedupeKey] = event;
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
  const queryBuild = buildOddsApiOddsQuery_(config, startMs, endMs);
  if (!queryBuild.ok) {
    Logger.log(JSON.stringify(sanitizeForLog_({
      event: 'odds_api_request_validation_failed',
      reason_code: queryBuild.reason_code,
      detail: queryBuild.detail,
      detail_code: queryBuild.detail_code,
      query_params: queryBuild.query_params,
      request_param_diagnostics: queryBuild.diagnostics,
      validation_errors: queryBuild.errors,
    })));
    return {
      events: [],
      reason_code: queryBuild.reason_code,
      detail: queryBuild.detail,
      detail_code: queryBuild.detail_code,
      api_credit_usage: 0,
      api_call_count: 0,
      credit_headers: {},
    };
  }
  const query = queryBuild.query;
  const initialWindowMeta = {
    window_request_start_ms: queryBuild.window_start_ms,
    window_request_end_ms: queryBuild.window_end_ms,
  };
  const queryValidation = validateOddsApiOddsQuery_(query);
  if (!queryValidation.ok) {
    Logger.log(JSON.stringify(sanitizeForLog_({
      event: 'odds_api_request_validation_failed',
      reason_code: queryValidation.reason_code,
      detail: queryValidation.detail,
      detail_code: queryValidation.detail_code,
      query_params: buildOddsApiDiagnosticQueryParams_(query),
      request_param_diagnostics: queryValidation.diagnostics,
      validation_errors: queryValidation.errors,
    })));
    return {
      events: [],
      reason_code: queryValidation.reason_code,
      api_credit_usage: 0,
      api_call_count: 0,
      credit_headers: {},
    };
  }

  const fetched = callOddsApiWithSportKeyFallback_(config, {
    endpoint: 'odds',
    query: query,
  });
  if (!fetched.ok && fetched.reason_code === 'invalid_time_window') {
    const fallbackStart = Date.now();
    const bootstrapLookaheadHours = Number(config.ODDS_BOOTSTRAP_LOOKAHEAD_HOURS || 0);
    const fallbackLookaheadHours = Math.max(6, Number.isFinite(bootstrapLookaheadHours) ? bootstrapLookaheadHours : 0);
    const fallbackEnd = fallbackStart + (fallbackLookaheadHours * 60 * 60 * 1000);
    const fallbackQueryBuild = buildOddsApiOddsQuery_(config, fallbackStart, fallbackEnd);

    if (fallbackQueryBuild.ok) {
      const fallbackFetched = callOddsApiWithSportKeyFallback_(config, {
        endpoint: 'odds',
        query: fallbackQueryBuild.query,
      });

      if (fallbackFetched.ok) {
        fetched.ok = true;
        fetched.status_code = fallbackFetched.status_code;
        fetched.payload = fallbackFetched.payload;
        fetched.reason_code = 'invalid_time_window_recovered';
        fetched.api_credit_usage = Number(fetched.api_credit_usage || 0) + Number(fallbackFetched.api_credit_usage || 0);
        fetched.api_call_count = Number(fetched.api_call_count || 0) + Number(fallbackFetched.api_call_count || 0);
        fetched.credit_headers = fallbackFetched.credit_headers;
        fetched.selected_sport_keys = fallbackFetched.selected_sport_keys;
        fetched.selected_sport_key_count = fallbackFetched.selected_sport_key_count;
        fetched.selected_sport_key_source = fallbackFetched.selected_sport_key_source;
        fetched.selected_sport_key_fallback = fallbackFetched.selected_sport_key_fallback;
        fetched.window_request_start_ms = fallbackQueryBuild.window_start_ms;
        fetched.window_request_end_ms = fallbackQueryBuild.window_end_ms;
      } else {
        const relaxedFallback = fetchOddsWindowWithoutWindowParams_(config, fallbackQueryBuild.query, {
          start_ms: fallbackQueryBuild.window_start_ms,
          end_ms: fallbackQueryBuild.window_end_ms,
        });
        if (relaxedFallback.ok) {
          fetched.ok = true;
          fetched.status_code = relaxedFallback.status_code;
          fetched.payload = relaxedFallback.payload;
          fetched.reason_code = 'invalid_time_window_recovered_relaxed_query';
          fetched.api_credit_usage = Number(fetched.api_credit_usage || 0) + Number(fallbackFetched.api_credit_usage || 0) + Number(relaxedFallback.api_credit_usage || 0);
          fetched.api_call_count = Number(fetched.api_call_count || 0) + Number(fallbackFetched.api_call_count || 0) + Number(relaxedFallback.api_call_count || 0);
          fetched.credit_headers = relaxedFallback.credit_headers;
          fetched.selected_sport_keys = relaxedFallback.selected_sport_keys;
          fetched.selected_sport_key_count = relaxedFallback.selected_sport_key_count;
          fetched.selected_sport_key_source = relaxedFallback.selected_sport_key_source;
          fetched.selected_sport_key_fallback = relaxedFallback.selected_sport_key_fallback;
          fetched.window_request_start_ms = fallbackQueryBuild.window_start_ms;
          fetched.window_request_end_ms = fallbackQueryBuild.window_end_ms;
        } else {
        return {
          events: [],
          reason_code: 'invalid_time_window_retry_failed',
          detail: fallbackFetched.detail || fetched.detail,
          detail_code: fallbackFetched.detail_code || fetched.detail_code,
          api_credit_usage: Number(fetched.api_credit_usage || 0) + Number(fallbackFetched.api_credit_usage || 0) + Number(relaxedFallback.api_credit_usage || 0),
          api_call_count: Number(fetched.api_call_count || 0) + Number(fallbackFetched.api_call_count || 0) + Number(relaxedFallback.api_call_count || 0),
          credit_headers: relaxedFallback.credit_headers || fallbackFetched.credit_headers || fetched.credit_headers || {},
          window_request_start_ms: fallbackQueryBuild.window_start_ms,
          window_request_end_ms: fallbackQueryBuild.window_end_ms,
        };
        }
      }
    }
  }
  if (!fetched.ok) {
    fetched.window_request_start_ms = initialWindowMeta.window_request_start_ms;
    fetched.window_request_end_ms = initialWindowMeta.window_request_end_ms;
    return fetched;
  }

  const events = [];
  let eventsMissingH2hOutcomes = 0;
  let bookmakersWithoutH2hMarket = 0;

  (ensureArrayPayload_(fetched.payload) || []).forEach((event) => {
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
        const openingLagEvaluatedAt = new Date();
        const openingLagMinutes = providerOddsTimestamp
          ? Math.max(0, Math.round((openingLagEvaluatedAt.getTime() - providerOddsTimestamp.getTime()) / 60000))
          : null;

        const candidate = {
          bookmaker: bookmaker.key || '',
          price,
          opening_price: price,
          evaluation_price: price,
          price_delta_bps: 0,
          provider_odds_updated_time: providerOddsTimestamp,
          open_timestamp: providerOddsTimestamp,
          opening_lag_minutes: openingLagMinutes,
          opening_lag_evaluated_at: openingLagEvaluatedAt,
          decision_gate_status: 'odds_snapshot_recorded',
          is_actionable: true,
          reason_code: '',
          open_timestamp_type: 'provider_live',
          open_timestamp_source: providerOddsTimestamp ? 'provider_odds_updated_time' : 'missing',
          opening_lag_policy_tier: 'strict_gate',
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
        opening_price: Number.isFinite(Number(best.opening_price)) ? Number(best.opening_price) : Number(best.price),
        evaluation_price: Number.isFinite(Number(best.evaluation_price)) ? Number(best.evaluation_price) : Number(best.price),
        price_delta_bps: Number.isFinite(Number(best.price_delta_bps)) ? Number(best.price_delta_bps) : 0,
        provider_odds_updated_time: best.provider_odds_updated_time,
        open_timestamp: best.open_timestamp || null,
        opening_lag_minutes: Number.isFinite(Number(best.opening_lag_minutes)) ? Number(best.opening_lag_minutes) : null,
        opening_lag_evaluated_at: best.opening_lag_evaluated_at || null,
        decision_gate_status: best.decision_gate_status || 'odds_snapshot_recorded',
        is_actionable: best.is_actionable !== false,
        reason_code: best.reason_code || '',
        open_timestamp_type: best.open_timestamp_type || 'provider_live',
        open_timestamp_source: best.open_timestamp_source || (best.provider_odds_updated_time ? 'provider_odds_updated_time' : 'missing'),
        opening_lag_policy_tier: best.opening_lag_policy_tier || 'strict_gate',
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
        surface: pickEventValue_(event, ['surface', 'court_surface']),
      });
    });
  });

  return {
    events,
    reason_code: (fetched.reason_code === 'invalid_time_window_recovered' || fetched.reason_code === 'invalid_time_window_recovered_relaxed_query')
      ? fetched.reason_code
      : (fetched.selected_sport_key_fallback && fetched.selected_sport_key_fallback !== 'none' ? 'odds_api_success_sport_key_fallback' : 'odds_api_success'),
    api_credit_usage: fetched.api_credit_usage,
    api_call_count: fetched.api_call_count || 1,
    credit_headers: fetched.credit_headers,
    events_missing_h2h_outcomes: eventsMissingH2hOutcomes,
    bookmakers_without_h2h_market: bookmakersWithoutH2hMarket,
    window_request_start_ms: Number(fetched.window_request_start_ms || initialWindowMeta.window_request_start_ms),
    window_request_end_ms: Number(fetched.window_request_end_ms || initialWindowMeta.window_request_end_ms),
  };
}

function fetchOddsWindowWithoutWindowParams_(config, query, expectedWindow) {
  const relaxedQuery = Object.assign({}, query || {});
  delete relaxedQuery.commenceTimeFrom;
  delete relaxedQuery.commenceTimeTo;

  const fetched = callOddsApiWithSportKeyFallback_(config, {
    endpoint: 'odds',
    query: relaxedQuery,
  });
  if (!fetched.ok) return fetched;

  const parsedStartMs = Number(expectedWindow && expectedWindow.start_ms);
  const parsedEndMs = Number(expectedWindow && expectedWindow.end_ms);
  const hasWindow = Number.isFinite(parsedStartMs) && Number.isFinite(parsedEndMs) && parsedEndMs > parsedStartMs;
  if (!hasWindow) return fetched;

  const payload = ensureArrayPayload_(fetched.payload) || [];
  fetched.payload = payload.filter(function (event) {
    if (!event || !event.commence_time) return false;
    const commenceMs = new Date(event.commence_time).getTime();
    return Number.isFinite(commenceMs) && commenceMs >= parsedStartMs && commenceMs <= parsedEndMs;
  });
  return fetched;
}

function fetchScheduleWindowWithoutWindowParams_(config, sportKeys, expectedWindow) {
  const responses = [];

  (sportKeys || []).forEach(function (sportKey) {
    const url = buildOddsApiSportUrl_(config, sportKey, 'events', {});
    const resp = callOddsApi_(url, {
      debug: config.VERBOSE_LOGGING,
      resolved_sport_keys: sportKeys,
      max_retries: Number(config.ODDS_API_MAX_RETRIES || 3),
      backoff_base_ms: Number(config.ODDS_API_BACKOFF_BASE_MS || 250),
      backoff_max_ms: Number(config.ODDS_API_BACKOFF_MAX_MS || 3000),
    });
    resp.sport_key = sportKey;
    responses.push(resp);
  });

  const successful = responses.filter(function (resp) { return resp.ok; });
  const firstFailure = responses[0] || {
    ok: false,
    status_code: 404,
    payload: [],
    reason_code: 'api_http_404',
    api_credit_usage: 0,
    api_call_count: 0,
    credit_headers: {},
  };

  if (!successful.length) {
    return {
      ok: false,
      status_code: firstFailure.status_code,
      payload: [],
      reason_code: firstFailure.reason_code,
      api_credit_usage: responses.reduce(function (sum, resp) { return sum + Number(resp.api_credit_usage || 0); }, 0),
      api_call_count: responses.reduce(function (sum, resp) { return sum + Number(resp.api_call_count || 0); }, 0),
      credit_headers: responses.length ? responses[responses.length - 1].credit_headers : {},
    };
  }

  let normalizedEvents = normalizeAndDeduplicateScheduleEvents_(successful.map(function (resp) {
    return ensureArrayPayload_(resp.payload) || [];
  }));
  const parsedStartMs = Number(expectedWindow && expectedWindow.start_ms);
  const parsedEndMs = Number(expectedWindow && expectedWindow.end_ms);
  const hasWindow = Number.isFinite(parsedStartMs) && Number.isFinite(parsedEndMs) && parsedEndMs > parsedStartMs;
  if (hasWindow) {
    normalizedEvents = normalizedEvents.filter(function (event) {
      const startMs = event && event.start_time instanceof Date ? event.start_time.getTime() : NaN;
      return Number.isFinite(startMs) && startMs >= parsedStartMs && startMs <= parsedEndMs;
    });
  }

  return {
    ok: true,
    status_code: successful[0].status_code,
    payload: normalizedEvents,
    reason_code: 'invalid_time_window_recovered_relaxed_query',
    api_credit_usage: responses.reduce(function (sum, resp) { return sum + Number(resp.api_credit_usage || 0); }, 0),
    api_call_count: responses.reduce(function (sum, resp) { return sum + Number(resp.api_call_count || 0); }, 0),
    credit_headers: responses.length ? responses[responses.length - 1].credit_headers : {},
  };
}

function fetchScheduleFromOddsApi_(config, window, opts) {
  if (!config.ODDS_API_KEY) {
    return { events: [], reason_code: 'missing_api_key', api_credit_usage: 0, api_call_count: 0, credit_headers: {} };
  }
  const options = opts || {};
  const allowInvalidWindowRelaxedRetry = options.allow_invalid_window_relaxed_retry !== false;
  const resolvedSportKeys = resolveActiveWtaSportKeys_(config, null, {
    force_discovery: !!options.force_multi_key_discovery,
  });
  const sportKeys = resolvedSportKeys.sport_keys || [];
  if (!sportKeys.length) {
    return {
      events: [],
      reason_code: 'schedule_no_active_wta_keys',
      api_credit_usage: 0,
      api_call_count: 0,
      credit_headers: {},
      resolved_sport_keys: [],
    };
  }
  const responses = [];
  const expectedWindowMs = {
    start_ms: new Date(window.startIso).getTime(),
    end_ms: new Date(window.endIso).getTime(),
  };
  const windowPreflight = validateOddsFetchWindowMs_(expectedWindowMs.start_ms, expectedWindowMs.end_ms);

  if (!windowPreflight.ok && allowInvalidWindowRelaxedRetry) {
    const relaxedPreflight = fetchScheduleWindowWithoutWindowParams_(config, sportKeys, {
      start_ms: windowPreflight.start_ms,
      end_ms: windowPreflight.end_ms,
    });
    if (relaxedPreflight.ok) {
      return {
        events: relaxedPreflight.payload || [],
        reason_code: relaxedPreflight.reason_code,
        api_credit_usage: relaxedPreflight.api_credit_usage,
        api_call_count: relaxedPreflight.api_call_count,
        credit_headers: relaxedPreflight.credit_headers,
        resolved_sport_keys: sportKeys,
      };
    }
    return {
      events: [],
      reason_code: 'invalid_time_window',
      api_credit_usage: Number(relaxedPreflight.api_credit_usage || 0),
      api_call_count: Number(relaxedPreflight.api_call_count || 0),
      credit_headers: relaxedPreflight.credit_headers || {},
      resolved_sport_keys: sportKeys,
    };
  }

  sportKeys.forEach((sportKey) => {
    const url = buildOddsApiSportUrl_(config, sportKey, 'events', {
      commenceTimeFrom: window.startIso,
      commenceTimeTo: window.endIso,
    });
    const resp = callOddsApi_(url, {
      debug: config.VERBOSE_LOGGING,
      resolved_sport_keys: sportKeys,
      max_retries: Number(config.ODDS_API_MAX_RETRIES || 3),
      backoff_base_ms: Number(config.ODDS_API_BACKOFF_BASE_MS || 250),
      backoff_max_ms: Number(config.ODDS_API_BACKOFF_MAX_MS || 3000),
    });
    resp.sport_key = sportKey;
    responses.push(resp);
  });

  const successful = responses.filter((resp) => resp.ok);
  const firstSuccess = successful[0] || null;
  const firstFailure = responses[0] || {
    ok: false,
    status_code: 404,
    payload: [],
    reason_code: 'api_http_404',
    api_credit_usage: 0,
    api_call_count: 0,
    credit_headers: {},
  };

  if (!firstSuccess) {
    const hadInvalidTimeWindow = responses.some(function (resp) { return resp.reason_code === 'invalid_time_window'; });
    if (hadInvalidTimeWindow && allowInvalidWindowRelaxedRetry) {
      const relaxedFallback = fetchScheduleWindowWithoutWindowParams_(config, sportKeys, {
        start_ms: expectedWindowMs.start_ms,
        end_ms: expectedWindowMs.end_ms,
      });
      if (relaxedFallback.ok) {
        return {
          events: relaxedFallback.payload || [],
          reason_code: relaxedFallback.reason_code,
          api_credit_usage: responses.reduce((sum, resp) => sum + Number(resp.api_credit_usage || 0), 0) + Number(relaxedFallback.api_credit_usage || 0),
          api_call_count: responses.reduce((sum, resp) => sum + Number(resp.api_call_count || 0), 0) + Number(relaxedFallback.api_call_count || 0),
          credit_headers: relaxedFallback.credit_headers || (responses.length ? responses[responses.length - 1].credit_headers : {}),
          resolved_sport_keys: sportKeys,
        };
      }
      return {
        events: [],
        reason_code: 'invalid_time_window',
        api_credit_usage: responses.reduce((sum, resp) => sum + Number(resp.api_credit_usage || 0), 0) + Number(relaxedFallback.api_credit_usage || 0),
        api_call_count: responses.reduce((sum, resp) => sum + Number(resp.api_call_count || 0), 0) + Number(relaxedFallback.api_call_count || 0),
        credit_headers: relaxedFallback.credit_headers || (responses.length ? responses[responses.length - 1].credit_headers : {}),
        resolved_sport_keys: sportKeys,
      };
    }
    if (resolvedSportKeys.fallback === 'none_active_wta_keys') {
      return {
        events: [],
        reason_code: 'schedule_no_active_wta_keys',
        api_credit_usage: responses.reduce((sum, resp) => sum + Number(resp.api_credit_usage || 0), 0),
        api_call_count: responses.reduce((sum, resp) => sum + Number(resp.api_call_count || 0), 0),
        credit_headers: responses.length ? responses[responses.length - 1].credit_headers : {},
      };
    }
    return firstFailure;
  }

  const normalizedEvents = normalizeAndDeduplicateScheduleEvents_(successful.map((resp) => ensureArrayPayload_(resp.payload) || []));
  let reasonCode = resolvedSportKeys.fallback && resolvedSportKeys.fallback !== 'none'
    ? 'schedule_api_success_sport_key_fallback'
    : 'schedule_api_success';

  if (!normalizedEvents.length) {
    reasonCode = resolvedSportKeys.fallback === 'none_active_wta_keys'
      ? 'schedule_no_active_wta_keys'
      : 'schedule_no_games_in_window';
  }

  return {
    events: normalizedEvents,
    reason_code: reasonCode,
    api_credit_usage: responses.reduce((sum, resp) => sum + Number(resp.api_credit_usage || 0), 0),
    api_call_count: responses.reduce((sum, resp) => sum + Number(resp.api_call_count || 0), 0),
    credit_headers: responses.length ? responses[responses.length - 1].credit_headers : {},
    resolved_sport_keys: sportKeys,
  };
}

function normalizeAndDeduplicateScheduleEvents_(payloadLists) {
  const deduped = {};

  (payloadLists || []).forEach((list) => {
    (list || []).forEach((event) => {
      const eventId = String((event && event.id) || '').trim();
      const commenceIso = String((event && event.commence_time) || '').trim();
      if (!eventId || !commenceIso) return;

      const startTime = new Date(commenceIso);
      if (Number.isNaN(startTime.getTime())) return;

      const normalizedEvent = {
        event_id: eventId,
        match_id: eventId,
        start_time: startTime,
        competition: event.tournament || event.sport_title || '',
        tournament: event.tournament || '',
        event_name: event.name || '',
        sport_title: event.sport_title || '',
        home_team: event.home_team || '',
        away_team: event.away_team || '',
        player_1: event.home_team || '',
        player_2: event.away_team || '',
        surface: pickEventValue_(event, ['surface', 'court_surface']),
      };

      const dedupeKey = buildScheduleEventDedupeKey_(normalizedEvent);
      if (!dedupeKey) return;
      if (!deduped[dedupeKey]) deduped[dedupeKey] = normalizedEvent;
    });
  });

  return Object.keys(deduped).map((dedupeKey) => deduped[dedupeKey]);
}

function buildScheduleEventDedupeKey_(event) {
  if (!event || !event.event_id || !(event.start_time instanceof Date)) return '';
  const startMs = event.start_time.getTime();
  if (!Number.isFinite(startMs)) return '';
  return String(event.event_id) + '|' + String(startMs);
}

function callOddsApiWithSportKeyFallback_(config, opts) {
  const resolvedSportKeys = resolveActiveWtaSportKeys_(config);
  const sportKeys = resolvedSportKeys.sport_keys || [];
  if (!sportKeys.length) {
    return {
      ok: false,
      status_code: 0,
      payload: [],
      reason_code: 'odds_no_active_wta_keys',
      api_credit_usage: 0,
      api_call_count: 0,
      credit_headers: {},
      selected_sport_keys: [],
      selected_sport_key_count: 0,
      selected_sport_key_source: resolvedSportKeys.source,
      selected_sport_key_fallback: resolvedSportKeys.fallback,
    };
  }
  const responses = [];

  sportKeys.forEach((sportKey) => {
    const url = buildOddsApiSportUrl_(config, sportKey, opts.endpoint, opts.query);
    const resp = callOddsApi_(url, {
      debug: config.VERBOSE_LOGGING,
      resolved_sport_keys: sportKeys,
      max_retries: Number(config.ODDS_API_MAX_RETRIES || 3),
      backoff_base_ms: Number(config.ODDS_API_BACKOFF_BASE_MS || 250),
      backoff_max_ms: Number(config.ODDS_API_BACKOFF_MAX_MS || 3000),
    });
    resp.sport_key = sportKey;
    responses.push(resp);
  });

  const successful = responses.filter((resp) => resp.ok);
  const firstSuccess = successful[0] || null;
  const firstFailure = responses[0] || {
    ok: false,
    status_code: 404,
    payload: [],
    reason_code: 'api_http_404',
    api_credit_usage: 0,
    api_call_count: 0,
    credit_headers: {},
  };
  const selected = firstSuccess || firstFailure;

  logOddsSportKeyResolution_('request_selection', sportKeys, resolvedSportKeys.fallback, {
    source: resolvedSportKeys.source,
    selected_sport_key_count: sportKeys.length,
  });

  return {
    ok: !!firstSuccess,
    status_code: selected.status_code,
    payload: mergeOddsApiPayloads_(successful.map((resp) => ensureArrayPayload_(resp.payload) || [])),
    reason_code: selected.reason_code,
    api_credit_usage: responses.reduce((sum, resp) => sum + Number(resp.api_credit_usage || 0), 0),
    api_call_count: responses.reduce((sum, resp) => sum + Number(resp.api_call_count || 0), 0),
    credit_headers: responses.length ? responses[responses.length - 1].credit_headers : {},
    selected_sport_keys: sportKeys,
    selected_sport_key_count: sportKeys.length,
    selected_sport_key_source: resolvedSportKeys.source,
    selected_sport_key_fallback: resolvedSportKeys.fallback,
  };
}

function resolveActiveWtaSportKeys_(config, deps, opts) {
  const cacheKey = 'ODDS_ACTIVE_WTA_SPORT_KEYS';
  const cacheTtlSec = Math.max(60, Math.min(600, Number(config.ODDS_CACHE_TTL_SEC || 300)));
  const adapters = deps || {};
  const options = opts || {};
  const getCached = adapters.getCachedOddsSportKeys || getCachedOddsSportKeys_;
  const setCached = adapters.setCachedOddsSportKeys || setCachedOddsSportKeys_;
  const callOddsApi = adapters.callOddsApi || callOddsApi_;
  const logResolution = adapters.logOddsSportKeyResolution || logOddsSportKeyResolution_;

  const cached = getCached(cacheKey);
  const configuredSportKeys = parseConfiguredSportKeys_(config && config.ODDS_SPORT_KEY);
  let cachedFallbackCandidate = [];
  let unknownConfiguredFromCache = [];
  if (!options.force_discovery && cached && cached.length) {
    if (configuredSportKeys.length) {
      const configuredKnownCached = configuredSportKeys.filter(function (key) { return cached.indexOf(key) >= 0; });
      const unknownConfigured = configuredSportKeys.filter(function (key) { return cached.indexOf(key) < 0; });
      if (configuredKnownCached.length) {
        const selected = dedupeSportKeys_(configuredKnownCached.concat(cached));
        const fallback = unknownConfigured.length ? 'unknown_sport_fallback_resolved' : 'none';
        logResolution('selected_configured_from_cache', selected, fallback, {
          source: 'cache',
          configured_sport_keys: configuredSportKeys,
          unknown_configured_sport_keys: unknownConfigured,
        });
        return { sport_keys: selected, source: 'cache', fallback: fallback };
      }
      cachedFallbackCandidate = cached;
      unknownConfiguredFromCache = unknownConfigured;
    } else {
      logResolution('selected_cached', cached, 'none', { source: 'cache' });
      return { sport_keys: cached, source: 'cache', fallback: 'none' };
    }
  }

  const catalogResp = callOddsApi(buildOddsApiUrl_(config, '/sports', { all: 'true' }), { debug: config.VERBOSE_LOGGING });
  if (!catalogResp.ok) {
    if (cachedFallbackCandidate.length) {
      logResolution('catalog_fetch_failed_using_cached', cachedFallbackCandidate, 'catalog_fetch_failed_using_cached', {
        source: 'cache',
        configured_sport_keys: configuredSportKeys,
        unknown_configured_sport_keys: unknownConfiguredFromCache,
      });
      return { sport_keys: cachedFallbackCandidate, source: 'cache', fallback: 'catalog_fetch_failed_using_cached' };
    }

    logResolution('catalog_fetch_failed', [], 'catalog_fetch_failed', {
      source: 'fallback',
      configured_sport_keys: configuredSportKeys,
      unknown_configured_sport_keys: configuredSportKeys,
    });
    return { sport_keys: [], source: 'fallback', fallback: 'catalog_fetch_failed' };
  }

  const activeWtaKeys = selectActiveWtaSportKeys_(catalogResp.payload || []);
  if (activeWtaKeys.length) {
    const configuredKnown = configuredSportKeys.filter(function (key) { return activeWtaKeys.indexOf(key) >= 0; });
    const unknownConfigured = configuredSportKeys.filter(function (key) { return activeWtaKeys.indexOf(key) < 0; });
    const selectedSportKeys = dedupeSportKeys_(configuredKnown.concat(activeWtaKeys));
    const fallback = unknownConfigured.length ? 'unknown_sport_fallback_resolved' : 'none';
    setCached(cacheKey, activeWtaKeys, cacheTtlSec);
    logResolution('selected_from_catalog', selectedSportKeys, fallback, {
      source: 'catalog',
      configured_sport_keys: configuredSportKeys,
      unknown_configured_sport_keys: unknownConfigured,
    });
    return { sport_keys: selectedSportKeys, source: 'catalog', fallback: fallback };
  }

  logResolution('none_active_fallback', [], 'none_active_wta_keys');
  return { sport_keys: [], source: 'fallback', fallback: 'none_active_wta_keys' };
}

function parseConfiguredSportKeys_(rawValue) {
  const raw = String(rawValue || '').trim();
  if (!raw) return [];
  return dedupeSportKeys_(raw.split(',').map(function (token) {
    return String(token || '').trim();
  }).filter(function (token) { return !!token; }));
}

function dedupeSportKeys_(sportKeys) {
  const seen = {};
  const deduped = [];
  (sportKeys || []).forEach(function (key) {
    const normalized = String(key || '').trim();
    if (!normalized || seen[normalized]) return;
    seen[normalized] = true;
    deduped.push(normalized);
  });
  return deduped;
}

function selectActiveWtaSportKeys_(sportsCatalog) {
  const activeKeys = [];
  (sportsCatalog || []).forEach((sport) => {
    const key = String((sport && sport.key) || '').trim();
    const active = sport && sport.active === true;
    if (active && /^tennis_wta_/i.test(key)) {
      activeKeys.push(key);
    }
  });
  return activeKeys;
}

function mergeOddsApiPayloads_(payloads) {
  const mergedByEventId = {};
  (payloads || []).forEach((list) => {
    (list || []).forEach((event) => {
      const eventId = String((event && event.id) || '').trim();
      if (!eventId) return;
      if (!mergedByEventId[eventId]) mergedByEventId[eventId] = event;
    });
  });
  return Object.keys(mergedByEventId).map((eventId) => mergedByEventId[eventId]);
}

function ensureArrayPayload_(value) {
  if (Array.isArray(value)) return value;
  if (!value || typeof value !== 'object') return null;

  const listLikeKeys = ['data', 'events', 'results', 'payload', 'items'];
  for (let i = 0; i < listLikeKeys.length; i += 1) {
    const key = listLikeKeys[i];
    if (Array.isArray(value[key])) return value[key];
  }

  return null;
}

function logOddsSportKeyResolution_(selectionMode, sportKeys, fallbackBehavior, extra) {
  const extras = extra || {};
  Logger.log(JSON.stringify(sanitizeForLog_({
    event: 'odds_sport_keys_resolved',
    mode: selectionMode,
    source: extras.source || '',
    selected_sport_keys: sportKeys || [],
    selected_sport_key_count: Number(extras.selected_sport_key_count || (sportKeys || []).length),
    configured_sport_keys: extras.configured_sport_keys || [],
    unknown_configured_sport_keys: extras.unknown_configured_sport_keys || [],
    fallback_behavior: fallbackBehavior || 'none',
  })));
}

function getCachedOddsSportKeys_(cacheKey) {
  try {
    const raw = CacheService.getScriptCache().get(cacheKey);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed.sport_keys) ? parsed.sport_keys : [];
  } catch (e) {
    return [];
  }
}

function setCachedOddsSportKeys_(cacheKey, sportKeys, ttlSec) {
  CacheService.getScriptCache().put(cacheKey, JSON.stringify({ sport_keys: sportKeys || [] }), ttlSec);
}

function normalizeSportKeyToken_(value) {
  return String(value || '').toLowerCase().trim().replace(/[^a-z0-9_]+/g, '_');
}

function buildOddsApiSportUrl_(config, sportKey, endpoint, query) {
  return buildOddsApiUrl_(config, '/sports/' + encodeURIComponent(sportKey) + '/' + endpoint, query);
}

function buildOddsApiUrl_(config, path, query) {
  const baseUrl = String(config.ODDS_API_BASE_URL || '').replace(/\/+$/, '');
  const qp = Object.assign({}, query || {}, { apiKey: config.ODDS_API_KEY });
  const parts = [];
  Object.keys(qp).forEach((key) => {
    if (qp[key] === undefined || qp[key] === null || qp[key] === '') return;
    parts.push(encodeURIComponent(key) + '=' + encodeURIComponent(String(qp[key])));
  });
  return baseUrl + path + (parts.length ? ('?' + parts.join('&')) : '');
}

function validateOddsApiOddsQuery_(query) {
  const q = query || {};
  const errors = [];

  const markets = String(q.markets || '').trim();
  const regions = String(q.regions || '').trim();
  const oddsFormat = String(q.oddsFormat || '').trim().toLowerCase();
  const commenceTimeFrom = String(q.commenceTimeFrom || '').trim();
  const commenceTimeTo = String(q.commenceTimeTo || '').trim();

  if (!markets) errors.push('markets_required');
  if (!regions) errors.push('regions_required');
  if (!oddsFormat) errors.push('odds_format_required');
  if (oddsFormat && ['american', 'decimal'].indexOf(oddsFormat) < 0) errors.push('odds_format_invalid');

  const diagnostics = buildOddsApiTimeWindowDiagnostics_(commenceTimeFrom, commenceTimeTo);
  if (!diagnostics.from_present) errors.push('commence_time_from_required');
  if (!diagnostics.to_present) errors.push('commence_time_to_required');
  if (diagnostics.from_present && !diagnostics.from_is_rfc3339_utc) errors.push('commence_time_from_not_rfc3339_utc');
  if (diagnostics.to_present && !diagnostics.to_is_rfc3339_utc) errors.push('commence_time_to_not_rfc3339_utc');
  if (diagnostics.from_parsed_ms === null && diagnostics.from_present && diagnostics.from_is_rfc3339_utc) errors.push('commence_time_from_invalid');
  if (diagnostics.to_parsed_ms === null && diagnostics.to_present && diagnostics.to_is_rfc3339_utc) errors.push('commence_time_to_invalid');
  if (diagnostics.has_valid_window_bounds && !diagnostics.from_before_to) errors.push('commence_time_window_invalid');
  if (diagnostics.has_valid_window_bounds && diagnostics.duration_minutes < 0) errors.push('commence_time_duration_negative');

  const timeErrors = {
    commence_time_from_required: true,
    commence_time_to_required: true,
    commence_time_from_not_rfc3339_utc: true,
    commence_time_to_not_rfc3339_utc: true,
    commence_time_from_invalid: true,
    commence_time_to_invalid: true,
    commence_time_window_invalid: true,
    commence_time_duration_negative: true,
  };

  const reasonCode = errors.some(function (err) { return !!timeErrors[err]; }) ? 'invalid_time_window' : 'invalid_query_params';
  const detailCode = errors.length ? errors[0] : '';
  return {
    ok: errors.length === 0,
    reason_code: reasonCode,
    detail: detailCode,
    detail_code: detailCode,
    errors: errors,
    diagnostics: diagnostics,
  };
}

function buildOddsApiOddsQuery_(config, startMs, endMs) {
  const normalizedWindow = normalizeOddsWindowMs_(startMs, endMs);
  const preflight = validateOddsFetchWindowMs_(normalizedWindow.start_ms, normalizedWindow.end_ms);
  const query = {
    regions: config.ODDS_REGIONS,
    markets: config.ODDS_MARKETS,
    oddsFormat: config.ODDS_ODDS_FORMAT,
    commenceTimeFrom: preflight.ok ? new Date(preflight.start_ms).toISOString() : '',
    commenceTimeTo: preflight.ok ? new Date(preflight.end_ms).toISOString() : '',
  };

  const diagnostics = preflight.ok
    ? buildOddsApiTimeWindowDiagnostics_(query.commenceTimeFrom, query.commenceTimeTo)
    : {
      start_ms: preflight.start_ms,
      end_ms: preflight.end_ms,
      duration_minutes: preflight.duration_minutes,
      expected_timezone: 'UTC',
      timezone_conversion_inverted: preflight.start_ms !== null && preflight.end_ms !== null && preflight.start_ms >= preflight.end_ms,
      known_good_cli_pattern: {
        sport_key: 'tennis_wta_indian_wells',
        endpoint: '/v4/sports/{sport_key}/odds',
        required_params: ['regions', 'markets', 'oddsFormat', 'commenceTimeFrom', 'commenceTimeTo'],
        commenceTimeFrom_format: 'RFC3339_UTC',
        commenceTimeTo_format: 'RFC3339_UTC',
        ordering_rule: 'commenceTimeFrom_before_commenceTimeTo',
        http_status: 200,
      },
      known_good_cli_match: false,
      known_good_cli_mismatches: ['window_ms_preflight_failed'],
    };

  return {
    ok: preflight.ok,
    reason_code: preflight.ok ? '' : 'invalid_time_window',
    detail: preflight.detail_code,
    detail_code: preflight.detail_code,
    errors: preflight.errors,
    diagnostics: diagnostics,
    query: query,
    window_start_ms: preflight.start_ms,
    window_end_ms: preflight.end_ms,
    query_params: buildOddsApiDiagnosticQueryParams_(query, {
      start_ms: preflight.start_ms,
      end_ms: preflight.end_ms,
      duration_minutes: preflight.duration_minutes,
      preflight_detail_code: preflight.detail_code,
    }),
  };
}

function normalizeOddsWindowMs_(startMs, endMs) {
  const start = Number(startMs);
  const end = Number(endMs);
  const hasStart = Number.isFinite(start);
  const hasEnd = Number.isFinite(end);
  if (!hasStart || !hasEnd) {
    return {
      start_ms: hasStart ? start : startMs,
      end_ms: hasEnd ? end : endMs,
    };
  }

  const normalizedStart = Math.floor(start / 1000) * 1000;
  let normalizedEnd = Math.floor(end / 1000) * 1000;
  if (normalizedEnd < normalizedStart + 60000) normalizedEnd = normalizedStart + 60000;

  return {
    start_ms: normalizedStart,
    end_ms: normalizedEnd,
  };
}

function validateOddsFetchWindowMs_(startMs, endMs) {
  const start = Number(startMs);
  const end = Number(endMs);
  const hasStart = Number.isFinite(start);
  const hasEnd = Number.isFinite(end);
  const durationMinutes = hasStart && hasEnd ? ((end - start) / 60000) : null;
  const errors = [];

  if (!hasStart) errors.push('refresh_window_start_required');
  if (!hasEnd) errors.push('refresh_window_end_required');
  if (hasStart && hasEnd && end < start) errors.push('refresh_window_duration_negative');
  if (hasStart && hasEnd && start >= end) errors.push('refresh_window_from_must_be_before_to');

  return {
    ok: errors.length === 0,
    detail_code: errors.length ? errors[0] : '',
    errors: errors,
    start_ms: hasStart ? start : null,
    end_ms: hasEnd ? end : null,
    duration_minutes: durationMinutes,
  };
}

function buildOddsApiDiagnosticQueryParams_(query, extra) {
  const q = query || {};
  const diagnostics = buildOddsApiTimeWindowDiagnostics_(String(q.commenceTimeFrom || ''), String(q.commenceTimeTo || ''));
  const knownGoodComparison = compareOddsApiQueryToKnownGoodCliPattern_(q, diagnostics);
  return Object.assign({
    markets: String(q.markets || ''),
    regions: String(q.regions || ''),
    oddsFormat: String(q.oddsFormat || ''),
    commenceTimeFrom: String(q.commenceTimeFrom || ''),
    commenceTimeTo: String(q.commenceTimeTo || ''),
    duration_minutes: diagnostics.duration_minutes,
    known_good_cli_pattern: knownGoodComparison.pattern,
    known_good_cli_match: knownGoodComparison.mismatches.length === 0,
    known_good_cli_mismatches: knownGoodComparison.mismatches,
  }, extra || {});
}

function compareOddsApiQueryToKnownGoodCliPattern_(query, diagnostics) {
  const q = query || {};
  const d = diagnostics || buildOddsApiTimeWindowDiagnostics_(String(q.commenceTimeFrom || ''), String(q.commenceTimeTo || ''));
  const pattern = d.known_good_cli_pattern;
  const mismatches = (d.known_good_cli_mismatches || []).slice();
  if (!String(q.regions || '').trim()) mismatches.push('regions_required');
  if (!String(q.markets || '').trim()) mismatches.push('markets_required');
  if (!String(q.oddsFormat || '').trim()) mismatches.push('oddsFormat_required');
  return {
    pattern: pattern,
    mismatches: dedupeArray_(mismatches),
  };
}

function dedupeArray_(values) {
  const seen = {};
  const out = [];
  (values || []).forEach(function (value) {
    const key = String(value || '');
    if (!key || seen[key]) return;
    seen[key] = true;
    out.push(key);
  });
  return out;
}

function buildOddsApiTimeWindowDiagnostics_(commenceTimeFrom, commenceTimeTo) {
  const fromValue = String(commenceTimeFrom || '').trim();
  const toValue = String(commenceTimeTo || '').trim();
  const rfc3339UtcRegex = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z$/;

  const fromIsRfc3339Utc = !!fromValue && rfc3339UtcRegex.test(fromValue);
  const toIsRfc3339Utc = !!toValue && rfc3339UtcRegex.test(toValue);
  const fromParsedMs = fromIsRfc3339Utc ? parseUtcIsoMillis_(fromValue) : null;
  const toParsedMs = toIsRfc3339Utc ? parseUtcIsoMillis_(toValue) : null;
  const hasValidWindowBounds = fromParsedMs !== null && toParsedMs !== null;
  const durationMs = hasValidWindowBounds ? (toParsedMs - fromParsedMs) : null;
  const durationMinutes = durationMs === null ? null : (durationMs / 60000);

  const knownGoodPattern = {
    sport_key: 'tennis_wta_indian_wells',
    endpoint: '/v4/sports/{sport_key}/odds',
    required_params: ['regions', 'markets', 'oddsFormat', 'commenceTimeFrom', 'commenceTimeTo'],
    commenceTimeFrom_format: 'RFC3339_UTC',
    commenceTimeTo_format: 'RFC3339_UTC',
    ordering_rule: 'commenceTimeFrom_before_commenceTimeTo',
    http_status: 200,
  };
  const knownGoodMismatches = [];
  if (!fromIsRfc3339Utc) knownGoodMismatches.push('commenceTimeFrom_format');
  if (!toIsRfc3339Utc) knownGoodMismatches.push('commenceTimeTo_format');
  if (hasValidWindowBounds && fromParsedMs >= toParsedMs) knownGoodMismatches.push('window_ordering');

  return {
    commenceTimeFrom: fromValue,
    commenceTimeTo: toValue,
    from_present: !!fromValue,
    to_present: !!toValue,
    from_is_rfc3339_utc: fromIsRfc3339Utc,
    to_is_rfc3339_utc: toIsRfc3339Utc,
    from_parsed_ms: fromParsedMs,
    to_parsed_ms: toParsedMs,
    has_valid_window_bounds: hasValidWindowBounds,
    from_before_to: hasValidWindowBounds ? fromParsedMs < toParsedMs : false,
    duration_minutes: durationMinutes,
    timezone_conversion_inverted: hasValidWindowBounds ? fromParsedMs >= toParsedMs : false,
    known_good_cli_pattern: knownGoodPattern,
    known_good_cli_match: knownGoodMismatches.length === 0,
    known_good_cli_mismatches: knownGoodMismatches,
  };
}

function parseUtcIsoMillis_(isoUtc) {
  const parsed = new Date(String(isoUtc || ''));
  if (Number.isNaN(parsed.getTime())) return null;
  if (parsed.toISOString() !== String(isoUtc)) return null;
  return parsed.getTime();
}

function parseQueryParamsFromUrl_(url) {
  const rawUrl = String(url || '');
  const queryIndex = rawUrl.indexOf('?');
  if (queryIndex < 0) return {};
  const queryString = rawUrl.slice(queryIndex + 1);
  const params = {};

  (queryString ? queryString.split('&') : []).forEach(function (pair) {
    const raw = String(pair || '');
    if (!raw) return;
    const equalsIndex = raw.indexOf('=');
    const key = decodeURIComponent((equalsIndex >= 0 ? raw.slice(0, equalsIndex) : raw).replace(/\+/g, ' '));
    const value = decodeURIComponent((equalsIndex >= 0 ? raw.slice(equalsIndex + 1) : '').replace(/\+/g, ' '));
    params[key] = value;
  });

  return params;
}

function classifyOddsApiClientErrorReason_(statusCode, bodyText, url) {
  const status = Number(statusCode || 0);
  if (status < 400 || status >= 500) return 'api_http_' + status;

  const text = String(bodyText || '').toLowerCase();
  const hasSportHint = /invalid[^\n]*sport|sport[^\n]*not found|unknown[^\n]*sport/.test(text);
  const hasWindowHint = /commencetimefrom|commencetimeto|time window|date range|start[^\n]*before[^\n]*end/.test(text);
  const hasParamHint = /regions|markets|oddsformat|query param|parameter|invalid query|missing required/.test(text);

  if (hasSportHint && /invalid|unknown|not found/.test(text)) return 'invalid_sport_key';
  if (hasWindowHint) return 'invalid_time_window';
  if (hasParamHint) return 'invalid_query_params';
  return 'unknown_client_error';
}

function isTransientOddsHttpStatus_(statusCode) {
  const status = Number(statusCode || 0);
  return status === 408 || status === 425 || status === 429 || status === 500 || status === 502 || status === 503 || status === 504;
}

function computeBoundedBackoffMs_(baseMs, maxMs, retryIndex) {
  const base = Math.max(0, Number(baseMs || 0));
  const max = Math.max(base, Number(maxMs || 0));
  const exponent = Math.max(0, Number(retryIndex || 0));
  const expDelay = Math.min(max, base * Math.pow(2, exponent));
  const jitter = expDelay > 0 ? Math.floor(Math.random() * (Math.floor(expDelay * 0.3) + 1)) : 0;
  return Math.min(max, expDelay + jitter);
}

function fetchOddsApiWithRetry_(url, requestOptions, opts) {
  const maxRetries = Math.max(0, Number((opts && opts.max_retries) || 0));
  const backoffBaseMs = Math.max(0, Number((opts && opts.backoff_base_ms) || 0));
  const backoffMaxMs = Math.max(backoffBaseMs, Number((opts && opts.backoff_max_ms) || 0));

  let attempts = 0;
  let lastStatus = 0;
  for (let retry = 0; retry <= maxRetries; retry += 1) {
    attempts += 1;
    let response;
    try {
      response = UrlFetchApp.fetch(url, requestOptions || {});
    } catch (e) {
      if (retry >= maxRetries) {
        return { ok: false, reason_code: 'api_transport_error', api_call_count: attempts, error: e && e.message };
      }
      const delayMs = computeBoundedBackoffMs_(backoffBaseMs, backoffMaxMs, retry);
      if (delayMs > 0) Utilities.sleep(delayMs);
      continue;
    }

    const status = Number(response.getResponseCode() || 0);
    lastStatus = status;
    if (status >= 200 && status < 300) {
      return { ok: true, response: response, api_call_count: attempts, status_code: status };
    }

    if (!isTransientOddsHttpStatus_(status) || retry >= maxRetries) {
      return { ok: true, response: response, api_call_count: attempts, status_code: status };
    }

    const delayMs = computeBoundedBackoffMs_(backoffBaseMs, backoffMaxMs, retry);
    if (delayMs > 0) Utilities.sleep(delayMs);
  }

  return { ok: false, reason_code: lastStatus ? ('api_http_' + lastStatus) : 'api_transport_error', api_call_count: attempts, status_code: lastStatus };
}

function callOddsApi_(url, opts) {
  const debugEnabled = !!(opts && opts.debug);
  if (debugEnabled) {
    const resolvedSportKeys = opts && Array.isArray(opts.resolved_sport_keys) ? opts.resolved_sport_keys : [];
    Logger.log(JSON.stringify({
      event: 'odds_api_request',
      method: 'GET',
      url: sanitizeForLog_(url),
      resolved_sport_keys: resolvedSportKeys,
      request_url_path: buildOddsApiRequestPathForLog_(url),
    }));
  }

  const failureResult = function (reasonCode, creditHeaders, apiCallCount) {
    return {
      ok: false,
      status_code: 0,
      payload: [],
      reason_code: reasonCode,
      api_credit_usage: 0,
      api_call_count: Number(apiCallCount || 0),
      credit_headers: creditHeaders || {},
    };
  };

  const logApiException = function (eventName, error) {
    if (!debugEnabled) return;
    const errorText = sanitizeForLog_(error && error.message ? error.message : String(error || 'unknown_error'));
    Logger.log(JSON.stringify({
      event: eventName,
      url: sanitizeForLog_(url),
      error: errorText,
    }));
  };

  const fetchResult = fetchOddsApiWithRetry_(url, { muteHttpExceptions: true }, {
    max_retries: Number(opts && opts.max_retries !== undefined ? opts.max_retries : 3),
    backoff_base_ms: Number(opts && opts.backoff_base_ms !== undefined ? opts.backoff_base_ms : 250),
    backoff_max_ms: Number(opts && opts.backoff_max_ms !== undefined ? opts.backoff_max_ms : 3000),
  });

  if (!fetchResult.ok || !fetchResult.response) {
    logApiException('odds_api_transport_error', { message: fetchResult.error || fetchResult.reason_code || 'api_transport_error' });
    return failureResult(fetchResult.reason_code || 'api_transport_error', {}, Number(fetchResult.api_call_count || 0));
  }

  const resp = fetchResult.response;
  const status = Number(fetchResult.status_code || 0);
  const normalizedHeaders = buildLowercaseHeaderMap_(resp.getAllHeaders() || {});
  const creditHeaders = normalizeCreditHeaders_(normalizedHeaders);
  const hasCreditHeaders = hasCreditHeaders_(creditHeaders);

  if (debugEnabled) {
    Logger.log(JSON.stringify({
      event: 'odds_api_response',
      url: sanitizeForLog_(url),
      status_code: status,
      has_credit_headers: hasCreditHeaders,
    }));
  }

  const responseBody = resp.getContentText() || '';

  if (status < 200 || status >= 300) {
    const rawQueryParams = parseQueryParamsFromUrl_(url);
    const diagnosticQueryParams = buildOddsApiDiagnosticQueryParams_(rawQueryParams);
    const diagnosticEvent = {
      event: 'odds_api_non_2xx_response',
      url: sanitizeForLog_(url),
      status_code: status,
      reason_code: classifyOddsApiClientErrorReason_(status, responseBody, url),
      query_params: diagnosticQueryParams,
      response_body: sanitizeForLog_(responseBody),
    };
    Logger.log(JSON.stringify(sanitizeForLog_(diagnosticEvent)));

    return {
      ok: false,
      status_code: status,
      payload: [],
      reason_code: classifyOddsApiClientErrorReason_(status, responseBody, url),
      api_credit_usage: creditHeaders.requests_last || 0,
      api_call_count: Number(fetchResult.api_call_count || 0),
      credit_headers: creditHeaders,
    };
  }

  let parsedPayload;
  try {
    parsedPayload = JSON.parse(responseBody || '[]');
  } catch (e) {
    logApiException('odds_api_parse_error', e);
    return failureResult('api_parse_error', creditHeaders, Number(fetchResult.api_call_count || 0));
  }

  const normalizedPayload = ensureArrayPayload_(parsedPayload);
  if (!normalizedPayload) {
    return {
      ok: false,
      status_code: status,
      payload: [],
      reason_code: 'api_unexpected_payload_shape',
      api_credit_usage: creditHeaders.requests_last || 0,
      api_call_count: Number(fetchResult.api_call_count || 0),
      credit_headers: creditHeaders,
    };
  }

  return {
    ok: true,
    status_code: status,
    payload: normalizedPayload,
    api_credit_usage: creditHeaders.requests_last || 0,
    api_call_count: Number(fetchResult.api_call_count || 0),
    credit_headers: creditHeaders,
    reason_code: hasCreditHeaders ? 'api_ok' : 'credit_header_missing',
  };
}

function buildLowercaseHeaderMap_(headers) {
  const source = headers || {};
  const normalized = {};
  Object.keys(source).forEach(function (key) {
    normalized[String(key || '').toLowerCase()] = source[key];
  });
  return normalized;
}

function parseHeaderNumber_(value) {
  if (value === undefined || value === null) return null;
  if (Array.isArray(value)) {
    if (!value.length) return null;
    return parseHeaderNumber_(value[0]);
  }
  if (typeof value === 'number') return value;
  const trimmed = String(value).trim();
  if (!trimmed) return NaN;
  return Number(trimmed);
}

function normalizeCreditHeaders_(headers) {
  const normalized = buildLowercaseHeaderMap_(headers || {});
  const hasUsedHeader = (
    Object.prototype.hasOwnProperty.call(normalized, 'x-requests-used')
    || Object.prototype.hasOwnProperty.call(normalized, 'requests_used')
  );
  const hasRemainingHeader = Object.prototype.hasOwnProperty.call(normalized, 'x-requests-remaining')
    || Object.prototype.hasOwnProperty.call(normalized, 'requests_remaining');
  const hasLastHeader = Object.prototype.hasOwnProperty.call(normalized, 'x-requests-last')
    || Object.prototype.hasOwnProperty.call(normalized, 'requests_last');

  const rawUsed = hasUsedHeader ? (normalized['x-requests-used'] !== undefined ? normalized['x-requests-used'] : normalized.requests_used) : null;
  const rawRemaining = hasRemainingHeader ? (normalized['x-requests-remaining'] !== undefined ? normalized['x-requests-remaining'] : normalized.requests_remaining) : null;
  const rawLast = hasLastHeader ? (normalized['x-requests-last'] !== undefined ? normalized['x-requests-last'] : normalized.requests_last) : null;

  const parsedUsed = parseHeaderNumber_(rawUsed);
  const parsedRemaining = parseHeaderNumber_(rawRemaining);
  const parsedLast = parseHeaderNumber_(rawLast);
  const anyNumericValue = Number.isFinite(parsedUsed) || Number.isFinite(parsedRemaining) || Number.isFinite(parsedLast);

  return {
    requests_used: parsedUsed,
    requests_remaining: parsedRemaining,
    requests_last: parsedLast,
    has_requests_used: hasUsedHeader,
    has_requests_remaining: hasRemainingHeader,
    has_requests_last: hasLastHeader,
    has_credit_headers: hasUsedHeader || hasRemainingHeader || hasLastHeader,
    has_usable_values: anyNumericValue,
  };
}

function hasCreditHeaders_(creditHeaders) {
  return !!(creditHeaders && creditHeaders.has_credit_headers === true);
}

function hasUsableCreditHeaders_(creditHeaders) {
  return !!(creditHeaders && creditHeaders.has_usable_values === true);
}

function buildOddsApiRequestPathForLog_(url) {
  const sanitized = sanitizeStringForLog_(String(url || ''));
  const noOrigin = sanitized.replace(/^https?:\/\/[^/]+/i, '');
  return noOrigin || sanitized;
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
    player_1_hold_pct: pickEventValue_(event, ['player_1_hold_pct']),
    player_2_hold_pct: pickEventValue_(event, ['player_2_hold_pct']),
    player_1_break_pct: pickEventValue_(event, ['player_1_break_pct']),
    player_2_break_pct: pickEventValue_(event, ['player_2_break_pct']),
    player_1_form_score: pickEventValue_(event, ['player_1_form_score', 'player_1_recent_form']),
    player_2_form_score: pickEventValue_(event, ['player_2_form_score', 'player_2_recent_form']),
    h2h_p1_wins: pickEventValue_(event, ['h2h_p1_wins']),
    h2h_p2_wins: pickEventValue_(event, ['h2h_p2_wins']),
    h2h_total_matches: pickEventValue_(event, ['h2h_total_matches']),
    surface: pickEventValue_(event, ['surface', 'court_surface']),
    stats_source: pickEventValue_(event, ['stats_source']),
    h2h_source: pickEventValue_(event, ['h2h_source']),
    stats_as_of: pickEventValue_(event, ['stats_as_of', 'as_of_time']),
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
    player_1_hold_pct: pickEventValue_(event, ['player_1_hold_pct']),
    player_2_hold_pct: pickEventValue_(event, ['player_2_hold_pct']),
    player_1_break_pct: pickEventValue_(event, ['player_1_break_pct']),
    player_2_break_pct: pickEventValue_(event, ['player_2_break_pct']),
    player_1_form_score: pickEventValue_(event, ['player_1_form_score', 'player_1_recent_form']),
    player_2_form_score: pickEventValue_(event, ['player_2_form_score', 'player_2_recent_form']),
    h2h_p1_wins: pickEventValue_(event, ['h2h_p1_wins']),
    h2h_p2_wins: pickEventValue_(event, ['h2h_p2_wins']),
    h2h_total_matches: pickEventValue_(event, ['h2h_total_matches']),
    surface: pickEventValue_(event, ['surface', 'court_surface']),
    stats_source: pickEventValue_(event, ['stats_source']),
    h2h_source: pickEventValue_(event, ['h2h_source']),
    stats_as_of: pickEventValue_(event, ['stats_as_of', 'as_of_time']),
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
    opening_price: Number.isFinite(Number(event.opening_price)) ? Number(event.opening_price) : '',
    evaluation_price: Number.isFinite(Number(event.evaluation_price)) ? Number(event.evaluation_price) : (Number.isFinite(Number(event.price)) ? Number(event.price) : ''),
    price_delta_bps: Number.isFinite(Number(event.price_delta_bps)) ? Number(event.price_delta_bps) : '',
    open_timestamp: event.open_timestamp ? event.open_timestamp.toISOString() : (event.provider_odds_updated_time ? event.provider_odds_updated_time.toISOString() : ''),
    open_timestamp_epoch_ms: event.open_timestamp ? event.open_timestamp.getTime() : (event.provider_odds_updated_time ? event.provider_odds_updated_time.getTime() : ''),
    opening_lag_minutes: Number.isFinite(Number(event.opening_lag_minutes)) ? Number(event.opening_lag_minutes) : '',
    opening_lag_evaluated_at: event.opening_lag_evaluated_at ? event.opening_lag_evaluated_at.toISOString() : '',
    decision_gate_status: event.decision_gate_status || '',
    is_actionable: event.is_actionable !== false,
    reason_code: event.reason_code || '',
    open_timestamp_type: event.open_timestamp_type || '',
    open_timestamp_source: event.open_timestamp_source || '',
    opening_lag_policy_tier: event.opening_lag_policy_tier || '',
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
    player_1_hold_pct: pickEventValue_(event, ['player_1_hold_pct']),
    player_2_hold_pct: pickEventValue_(event, ['player_2_hold_pct']),
    player_1_break_pct: pickEventValue_(event, ['player_1_break_pct']),
    player_2_break_pct: pickEventValue_(event, ['player_2_break_pct']),
    player_1_form_score: pickEventValue_(event, ['player_1_form_score', 'player_1_recent_form']),
    player_2_form_score: pickEventValue_(event, ['player_2_form_score', 'player_2_recent_form']),
    h2h_p1_wins: pickEventValue_(event, ['h2h_p1_wins']),
    h2h_p2_wins: pickEventValue_(event, ['h2h_p2_wins']),
    h2h_total_matches: pickEventValue_(event, ['h2h_total_matches']),
    surface: pickEventValue_(event, ['surface', 'court_surface']),
    stats_source: pickEventValue_(event, ['stats_source']),
    h2h_source: pickEventValue_(event, ['h2h_source']),
    stats_as_of: pickEventValue_(event, ['stats_as_of', 'as_of_time']),
  };
}

function deserializeOddsEvent_(event) {
  const openingLagEvaluatedAt = event.opening_lag_evaluated_at ? new Date(event.opening_lag_evaluated_at) : null;
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
    opening_price: Number.isFinite(Number(event.opening_price)) ? Number(event.opening_price) : null,
    evaluation_price: Number.isFinite(Number(event.evaluation_price)) ? Number(event.evaluation_price) : (Number.isFinite(Number(event.price)) ? Number(event.price) : null),
    price_delta_bps: Number.isFinite(Number(event.price_delta_bps)) ? Number(event.price_delta_bps) : null,
    open_timestamp: event.open_timestamp ? new Date(event.open_timestamp) : (event.provider_odds_updated_time ? new Date(event.provider_odds_updated_time) : null),
    open_timestamp_epoch_ms: Number(event.open_timestamp_epoch_ms || ''),
    opening_lag_minutes: Number.isFinite(Number(event.opening_lag_minutes)) ? Number(event.opening_lag_minutes) : null,
    opening_lag_evaluated_at: openingLagEvaluatedAt && !Number.isNaN(openingLagEvaluatedAt.getTime()) ? openingLagEvaluatedAt : null,
    decision_gate_status: event.decision_gate_status || '',
    is_actionable: event.is_actionable !== false,
    reason_code: event.reason_code || '',
    open_timestamp_type: event.open_timestamp_type || '',
    open_timestamp_source: event.open_timestamp_source || '',
    opening_lag_policy_tier: event.opening_lag_policy_tier || '',
    commence_time: new Date(event.commence_time),
    competition: event.competition,
    tournament: event.tournament || '',
    event_name: event.event_name || '',
    sport_title: event.sport_title || '',
    home_team: event.home_team || '',
    away_team: event.away_team || '',
    player_1: event.player_1,
    player_2: event.player_2,
    player_1_hold_pct: pickEventValue_(event, ['player_1_hold_pct']),
    player_2_hold_pct: pickEventValue_(event, ['player_2_hold_pct']),
    player_1_break_pct: pickEventValue_(event, ['player_1_break_pct']),
    player_2_break_pct: pickEventValue_(event, ['player_2_break_pct']),
    player_1_form_score: pickEventValue_(event, ['player_1_form_score', 'player_1_recent_form']),
    player_2_form_score: pickEventValue_(event, ['player_2_form_score', 'player_2_recent_form']),
    h2h_p1_wins: pickEventValue_(event, ['h2h_p1_wins']),
    h2h_p2_wins: pickEventValue_(event, ['h2h_p2_wins']),
    h2h_total_matches: pickEventValue_(event, ['h2h_total_matches']),
    surface: pickEventValue_(event, ['surface', 'court_surface']),
    stats_source: pickEventValue_(event, ['stats_source']),
    h2h_source: pickEventValue_(event, ['h2h_source']),
    stats_as_of: pickEventValue_(event, ['stats_as_of', 'as_of_time']),
  };
}

function pickEventValue_(event, keys) {
  const source = event || {};
  const keyList = keys || [];
  for (let i = 0; i < keyList.length; i += 1) {
    const key = keyList[i];
    if (source[key] !== undefined && source[key] !== null) return source[key];
  }
  return '';
}
