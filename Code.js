const SHEETS = {
  CONFIG: 'Config',
  RUN_LOG: 'Run_Log',
  RAW_ODDS: 'Raw_Odds',
  RAW_SCHEDULE: 'Raw_Schedule',
  MATCH_MAP: 'Match_Map',
  SIGNALS: 'Signals',
  STATE: 'State',
};

const DEFAULT_CONFIG = {
  RUN_ENABLED: 'true',
  LOOKAHEAD_HOURS: '36',
  ODDS_API_BASE_URL: 'https://api.the-odds-api.com/v4',
  ODDS_API_KEY: '',
  ODDS_SPORT_KEY: 'tennis_wta',
  ODDS_MARKETS: 'h2h',
  ODDS_REGIONS: 'us',
  ODDS_ODDS_FORMAT: 'american',
  ODDS_CACHE_TTL_SEC: '300',
  SCHEDULE_BUFFER_BEFORE_MIN: '180',
  SCHEDULE_BUFFER_AFTER_MIN: '180',
  MATCH_TIME_TOLERANCE_MIN: '45',
  MATCH_FALLBACK_EXPANSION_MIN: '120',
  ALLOW_WTA_125: 'false',
  GRAND_SLAM_ALIASES_JSON: '["australian open","roland garros","french open","wimbledon","us open"]',
  WTA_1000_ALIASES_JSON: '["wta 1000","wta-1000","masters 1000"]',
  WTA_500_ALIASES_JSON: '["wta 500","wta-500"]',
  VERBOSE_LOGGING: 'true',
  DUPLICATE_DEBOUNCE_MS: '90000',
  PLAYER_ALIAS_MAP_JSON: '{}',
  MODEL_VERSION: 'wta_mvp_v1',
  EDGE_THRESHOLD_MICRO: '0.015',
  EDGE_THRESHOLD_SMALL: '0.03',
  EDGE_THRESHOLD_MED: '0.05',
  EDGE_THRESHOLD_STRONG: '0.08',
  STAKE_UNITS_MICRO: '0.25',
  STAKE_UNITS_SMALL: '0.5',
  STAKE_UNITS_MED: '1',
  STAKE_UNITS_STRONG: '1.5',
  SIGNAL_COOLDOWN_MIN: '180',
  MINUTES_BEFORE_START_CUTOFF: '60',
  STALE_ODDS_WINDOW_MIN: '60',
};

const PROPS = {
  PIPELINE_TRIGGER_SIGNATURE: 'PIPELINE_TRIGGER_SIGNATURE',
  LAST_PIPELINE_RUN_TS: 'LAST_PIPELINE_RUN_TS',
  DUPLICATE_PREVENTED_COUNT: 'DUPLICATE_PREVENTED_COUNT',
};

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('WTA Pipeline Ops')
    .addItem('Setup / Verify Tabs', 'menuSetupVerifyTabs')
    .addItem('Run Pipeline Now', 'menuRunPipelineNow')
    .addSeparator()
    .addItem('Install Triggers', 'menuInstallTriggers')
    .addItem('Remove Triggers', 'menuRemoveTriggers')
    .addSeparator()
    .addItem('Diagnostics / Health Check', 'menuDiagnosticsHealthCheck')
    .addToUi();
}

function menuSetupVerifyTabs() {
  ensureTabsAndConfig_();
  SpreadsheetApp.getUi().alert('Setup complete: WTA foundation tabs/headers verified.');
}

function menuRunPipelineNow() {
  runEdgeBoard();
  SpreadsheetApp.getUi().alert('Pipeline run complete. Check Run_Log and State.');
}

function menuInstallTriggers() {
  installOrUpdateTriggers();
  SpreadsheetApp.getUi().alert('Trigger install/update completed. Check Run_Log.');
}

function menuRemoveTriggers() {
  removePipelineTriggers();
  SpreadsheetApp.getUi().alert('Pipeline triggers removed.');
}

function menuDiagnosticsHealthCheck() {
  const report = diagnosticsHealthCheck();
  SpreadsheetApp.getUi().alert('Diagnostics complete', JSON.stringify(report, null, 2), SpreadsheetApp.getUi().ButtonSet.OK);
}

function diagnosticsHealthCheck() {
  ensureTabsAndConfig_();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const scriptProps = PropertiesService.getScriptProperties();
  const tabStatus = {};

  Object.keys(SHEETS).forEach((key) => {
    tabStatus[SHEETS[key]] = !!ss.getSheetByName(SHEETS[key]);
  });

  const triggerCount = ScriptApp.getProjectTriggers().filter((t) => t.getHandlerFunction() === 'runEdgeBoard').length;
  const report = {
    checked_at: new Date().toISOString(),
    tabs_present: tabStatus,
    run_edgeboard_triggers: triggerCount,
    trigger_signature: scriptProps.getProperty(PROPS.PIPELINE_TRIGGER_SIGNATURE) || '',
    duplicate_prevented_count: Number(scriptProps.getProperty(PROPS.DUPLICATE_PREVENTED_COUNT) || 0),
  };

  appendLogRow_({
    row_type: 'ops',
    run_id: buildRunId_(),
    stage: 'diagnosticsHealthCheck',
    status: 'ok',
    reason_code: 'diagnostics_ok',
    message: JSON.stringify(report),
  });

  return report;
}

function installOrUpdateTriggers() {
  ensureTabsAndConfig_();

  const spec = {
    version: 1,
    functionName: 'runEdgeBoard',
    type: 'clock',
    everyMinutes: 15,
  };

  const signature = JSON.stringify(spec);
  const scriptProps = PropertiesService.getScriptProperties();
  const existingSignature = scriptProps.getProperty(PROPS.PIPELINE_TRIGGER_SIGNATURE);
  const existingPipelineTriggers = ScriptApp.getProjectTriggers().filter((t) => t.getHandlerFunction() === spec.functionName);

  if (existingSignature === signature && existingPipelineTriggers.length > 0) {
    appendLogRow_({
      row_type: 'ops',
      run_id: buildRunId_(),
      stage: 'installOrUpdateTriggers',
      status: 'success',
      reason_code: 'trigger_noop',
      message: 'Trigger signature unchanged and trigger already exists.',
      trigger_event: 'trigger_noop',
    });
    return;
  }

  existingPipelineTriggers.forEach((trigger) => ScriptApp.deleteTrigger(trigger));
  ScriptApp.newTrigger(spec.functionName).timeBased().everyMinutes(spec.everyMinutes).create();
  scriptProps.setProperty(PROPS.PIPELINE_TRIGGER_SIGNATURE, signature);

  appendLogRow_({
    row_type: 'ops',
    run_id: buildRunId_(),
    stage: 'installOrUpdateTriggers',
    status: 'success',
    reason_code: 'trigger_reinstalled',
    message: 'Pipeline trigger installed/refreshed with 15-minute schedule.',
    trigger_event: 'trigger_reinstalled',
  });
}

function removePipelineTriggers() {
  ScriptApp.getProjectTriggers().forEach((trigger) => {
    if (trigger.getHandlerFunction() === 'runEdgeBoard') ScriptApp.deleteTrigger(trigger);
  });

  appendLogRow_({
    row_type: 'ops',
    run_id: buildRunId_(),
    stage: 'removePipelineTriggers',
    status: 'success',
    reason_code: 'trigger_removed',
    message: 'Removed runEdgeBoard triggers.',
    trigger_event: 'trigger_removed',
  });
}

function runEdgeBoard() {
  ensureTabsAndConfig_();

  const runId = buildRunId_();
  const startedAt = new Date();
  const scriptProps = PropertiesService.getScriptProperties();
  const lock = LockService.getScriptLock();

  if (!tryLock_(lock, 5000)) {
    const prevented = incrementDuplicatePreventedCount_();
    appendLogRow_({
      row_type: 'summary',
      run_id: runId,
      stage: 'runEdgeBoard',
      started_at: startedAt,
      ended_at: new Date(),
      status: 'skipped',
      reason_code: 'run_locked_skip',
      message: 'Skipped due to script lock contention.',
      lock_event: 'run_locked_skip',
      duplicate_suppressed: prevented,
    });
    return;
  }

  try {
    const config = getConfig_();
    if (!config.RUN_ENABLED) {
      appendLogRow_({
        row_type: 'summary',
        run_id: runId,
        stage: 'runEdgeBoard',
        started_at: startedAt,
        ended_at: new Date(),
        status: 'skipped',
        reason_code: 'run_disabled_skip',
        message: 'RUN_ENABLED is false.',
      });
      return;
    }

    const nowMs = Date.now();
    const debounceMs = config.DUPLICATE_DEBOUNCE_MS;
    const lastRunTs = Number(scriptProps.getProperty(PROPS.LAST_PIPELINE_RUN_TS) || 0);
    if (nowMs - lastRunTs < debounceMs) {
      const prevented = incrementDuplicatePreventedCount_();
      appendLogRow_({
        row_type: 'summary',
        run_id: runId,
        stage: 'runEdgeBoard',
        started_at: startedAt,
        ended_at: new Date(),
        status: 'skipped',
        reason_code: 'run_debounced_skip',
        message: 'Skipped by debounce window.',
        debounce_event: 'run_debounced_skip',
        duplicate_suppressed: prevented,
      });
      return;
    }

    scriptProps.setProperty(PROPS.LAST_PIPELINE_RUN_TS, String(nowMs));

    const oddsStage = stageFetchOdds(runId, config);
    appendStageLog_(runId, oddsStage.summary);

    const scheduleStage = stageFetchSchedule(runId, config, oddsStage.events);
    appendStageLog_(runId, scheduleStage.summary);

    const matchStage = stageMatchEvents(runId, config, oddsStage.events, scheduleStage.events);
    appendStageLog_(runId, matchStage.summary);

    const signalStage = stageGenerateSignals(runId, config, oddsStage.events, matchStage.rows);
    appendStageLog_(runId, signalStage.summary);

    const persistStage = stagePersist(runId, {
      odds: oddsStage.rows,
      schedule: scheduleStage.rows,
      matchMap: matchStage.rows,
      signals: signalStage.rows,
    });
    appendStageLog_(runId, persistStage.summary);

    const combinedReasonCodes = mergeReasonCounts_([
      oddsStage.summary.reason_codes,
      scheduleStage.summary.reason_codes,
      matchStage.summary.reason_codes,
      signalStage.summary.reason_codes,
      persistStage.summary.reason_codes,
    ]);

    const verbosePayload = {
      run_id: runId,
      started_at: startedAt.toISOString(),
      ended_at: new Date().toISOString(),
      config_snapshot: config,
      stage_summaries: [
        oddsStage.summary,
        scheduleStage.summary,
        matchStage.summary,
        signalStage.summary,
        persistStage.summary,
      ],
      canonicalization_examples: {
        competition: scheduleStage.canonicalExamples.slice(0, 25),
        players: matchStage.canonicalizationExamples.slice(0, 25),
      },
      unresolved_competitions: scheduleStage.unresolvedCompetitions.slice(0, 50),
      sample_unmatched_cases: matchStage.unmatched.slice(0, 20),
      top_rejection_reasons: getTopReasonCodes_(combinedReasonCodes, 10),
      reason_codes: combinedReasonCodes,
    };

    setStateValue_('LAST_RUN_VERBOSE_JSON', JSON.stringify(verbosePayload, null, 2));

    appendLogRow_({
      row_type: 'summary',
      run_id: runId,
      stage: 'runEdgeBoard',
      started_at: startedAt,
      ended_at: new Date(),
      status: 'success',
      reason_code: 'run_success',
      message: 'Pipeline run completed.',
      fetched_odds: oddsStage.events.length,
      fetched_schedule: scheduleStage.events.length,
      allowed_tournaments: scheduleStage.allowedCount,
      matched: matchStage.matchedCount,
      unmatched: matchStage.unmatchedCount,
      signals_found: signalStage.sentCount,
      rejection_codes: JSON.stringify(combinedReasonCodes),
      stage_summaries: JSON.stringify(verbosePayload.stage_summaries),
      cooldown_suppressed: signalStage.cooldownSuppressedCount,
      duplicate_suppressed: signalStage.duplicateSuppressedCount,
    });
  } catch (error) {
    appendLogRow_({
      row_type: 'summary',
      run_id: runId,
      stage: 'runEdgeBoard',
      started_at: startedAt,
      ended_at: new Date(),
      status: 'failed',
      reason_code: 'run_exception',
      message: String(error && error.message ? error.message : error),
      exception: String(error && error.message ? error.message : error),
      stack: String(error && error.stack ? error.stack : ''),
    });
    throw error;
  } finally {
    lock.releaseLock();
  }
}

function stageFetchOdds(runId, config) {
  const start = Date.now();
  const source = 'the_odds_api';
  const lookaheadMs = config.LOOKAHEAD_HOURS * 60 * 60 * 1000;
  const now = Date.now();
  const cacheTtlSec = Math.max(30, config.ODDS_CACHE_TTL_SEC);
  const cacheResult = getCachedPayload_('ODDS_WINDOW_PAYLOAD');
  let adapter;

  if (cacheResult && now - cacheResult.cached_at_ms <= cacheTtlSec * 1000) {
    adapter = {
      events: cacheResult.events,
      reason_code: 'odds_cache_hit',
      api_credit_usage: 0,
      api_call_count: 0,
      credit_headers: {},
    };
  } else {
    adapter = fetchOddsWindowFromOddsApi_(config, now, now + lookaheadMs);
    if (adapter.events && adapter.events.length) {
      setCachedPayload_('ODDS_WINDOW_PAYLOAD', adapter.events);
    } else if (adapter.reason_code !== 'odds_api_success') {
      const stale = getStateJson_('ODDS_WINDOW_STALE_PAYLOAD');
      if (stale && stale.events && stale.events.length) {
        adapter = {
          events: stale.events.map(deserializeOddsEvent_),
          reason_code: 'odds_stale_fallback',
          api_credit_usage: adapter.api_credit_usage,
          api_call_count: adapter.api_call_count,
          credit_headers: adapter.credit_headers,
        };
      }
    }
  }

  const raw = adapter.events || [];
  const filtered = raw.filter((event) => event.commence_time.getTime() >= now && event.commence_time.getTime() <= now + lookaheadMs);

  if (filtered.length) {
    setStateValue_('ODDS_WINDOW_STALE_PAYLOAD', JSON.stringify({
      stored_at: new Date().toISOString(),
      events: filtered.map(serializeOddsEvent_),
    }));
  }

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
      within_window: filtered.length,
      outside_window: raw.length - filtered.length,
      odds_rows_emitted: filtered.length,
      events_missing_h2h_outcomes: adapter.events_missing_h2h_outcomes || 0,
      bookmakers_without_h2h_market: adapter.bookmakers_without_h2h_market || 0,
    },
  });

  setStateValue_('LAST_ODDS_API_CREDITS', JSON.stringify({
    run_id: runId,
    observed_at: new Date().toISOString(),
    api_call_count: adapter.api_call_count || 0,
    credit_headers: adapter.credit_headers || {},
  }));

  return { events: filtered, rows, summary };
}

function stageFetchSchedule(runId, config, oddsEvents) {
  const start = Date.now();
  const source = 'the_odds_api_schedule';
  const window = deriveScheduleWindowFromOdds_(oddsEvents, config);
  const scheduleResp = window ? fetchScheduleFromOddsApi_(config, window) : {
    events: [],
    reason_code: 'schedule_window_empty',
    api_credit_usage: 0,
    api_call_count: 0,
    credit_headers: {},
  };
  const inWindow = scheduleResp.events || [];

  const reasonCounts = {};
  const canonicalExamples = [];
  const unresolvedCompetitions = [];
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

  const summary = buildStageSummary_(runId, 'stageFetchSchedule', start, {
    input_count: inWindow.length,
    output_count: inWindow.length,
    provider: source,
    api_credit_usage: scheduleResp.api_credit_usage,
    reason_codes: reasonCounts,
  });

  summary.reason_codes[scheduleResp.reason_code] = (summary.reason_codes[scheduleResp.reason_code] || 0) + 1;

  setStateValue_('LAST_SCHEDULE_API_CREDITS', JSON.stringify({
    run_id: runId,
    observed_at: new Date().toISOString(),
    api_call_count: scheduleResp.api_call_count || 0,
    credit_headers: scheduleResp.credit_headers || {},
  }));

  return {
    events: allowedEvents,
    rows,
    summary,
    canonicalExamples,
    unresolvedCompetitions,
    allowedCount: allowedEvents.length,
  };
}

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

function stageGenerateSignals(runId, config, oddsEvents, matchRows) {
  const start = Date.now();
  const nowMs = Date.now();
  const rows = [];
  const reasonCounts = {
    sent: 0,
    duplicate_suppressed: 0,
    cooldown_suppressed: 0,
    edge_below_threshold: 0,
    too_close_to_start_skip: 0,
    stale_odds_skip: 0,
  };
  const signalState = getSignalState_();
  const seenHashesThisRun = {};
  const matchByOddsEventId = {};
  matchRows.forEach((row) => {
    matchByOddsEventId[row.odds_event_id] = row;
  });

  oddsEvents.forEach((event) => {
    const match = matchByOddsEventId[event.event_id];
    if (!match || !match.schedule_event_id) return;

    const impliedProbability = oddsPriceToImpliedProbability_(event.price);
    if (impliedProbability === null) return;

    const startCutoffMs = config.MINUTES_BEFORE_START_CUTOFF * 60000;
    if (event.commence_time.getTime() <= nowMs + startCutoffMs) {
      rows.push(buildSignalRow_(runId, config, event, match, {
        notification_outcome: 'too_close_to_start_skip',
        model_probability: estimateFairProbability_(impliedProbability, match.competition_tier),
        market_implied_probability: impliedProbability,
        edge_value: 0,
        edge_tier: 'NONE',
        stake_units: 0,
        signal_hash: buildSignalHash_(event.event_id, event.market, event.outcome, config.MODEL_VERSION),
      }));
      reasonCounts.too_close_to_start_skip += 1;
      return;
    }

    const staleThresholdMs = config.STALE_ODDS_WINDOW_MIN * 60000;
    if (nowMs - event.odds_updated_time.getTime() > staleThresholdMs) {
      rows.push(buildSignalRow_(runId, config, event, match, {
        notification_outcome: 'stale_odds_skip',
        model_probability: estimateFairProbability_(impliedProbability, match.competition_tier),
        market_implied_probability: impliedProbability,
        edge_value: 0,
        edge_tier: 'NONE',
        stake_units: 0,
        signal_hash: buildSignalHash_(event.event_id, event.market, event.outcome, config.MODEL_VERSION),
      }));
      reasonCounts.stale_odds_skip += 1;
      return;
    }

    const modelProbability = estimateFairProbability_(impliedProbability, match.competition_tier);
    const edgeValue = roundNumber_(modelProbability - impliedProbability, 4);
    const edgeTierAndStake = classifyEdgeAndStake_(edgeValue, config);
    const signalHash = buildSignalHash_(event.event_id, event.market, event.outcome, config.MODEL_VERSION);

    if (edgeTierAndStake.edge_tier === 'NONE') {
      rows.push(buildSignalRow_(runId, config, event, match, {
        notification_outcome: 'edge_below_threshold',
        model_probability: modelProbability,
        market_implied_probability: impliedProbability,
        edge_value: edgeValue,
        edge_tier: edgeTierAndStake.edge_tier,
        stake_units: edgeTierAndStake.stake_units,
        signal_hash: signalHash,
      }));
      reasonCounts.edge_below_threshold += 1;
      return;
    }

    const notifyDecision = maybeNotifySignal_(signalState, seenHashesThisRun, signalHash, nowMs, config.SIGNAL_COOLDOWN_MIN);
    reasonCounts[notifyDecision.outcome] = (reasonCounts[notifyDecision.outcome] || 0) + 1;

    rows.push(buildSignalRow_(runId, config, event, match, {
      notification_outcome: notifyDecision.outcome,
      model_probability: modelProbability,
      market_implied_probability: impliedProbability,
      edge_value: edgeValue,
      edge_tier: edgeTierAndStake.edge_tier,
      stake_units: edgeTierAndStake.stake_units,
      signal_hash: signalHash,
    }));
  });

  setSignalState_(signalState);
  setStateValue_('LAST_SIGNAL_SNAPSHOTS', JSON.stringify({
    run_id: runId,
    generated_at: new Date().toISOString(),
    model_version: config.MODEL_VERSION,
    signals: rows.map((row) => ({
      event_id: row.odds_event_id,
      schedule_event_id: row.schedule_event_id,
      side: row.side,
      market: row.market,
      model_probability: row.model_probability,
      market_implied_probability: row.market_implied_probability,
      edge_value: row.edge_value,
      edge_tier: row.edge_tier,
      timestamp: row.created_at,
      commence_time: row.commence_time,
      odds_updated_time: row.odds_updated_time,
      model_version: row.model_version,
      notification_outcome: row.notification_outcome,
    })),
  }));

  setStateValue_('LAST_SIGNAL_DECISIONS', JSON.stringify({
    run_id: runId,
    generated_at: new Date().toISOString(),
    reason_counts: reasonCounts,
  }));

  const summary = buildStageSummary_(runId, 'stageGenerateSignals', start, {
    input_count: matchRows.length,
    output_count: rows.length,
    provider: 'internal_signal_builder',
    api_credit_usage: 0,
    reason_codes: reasonCounts,
  });

  return {
    rows,
    summary,
    sentCount: reasonCounts.sent || 0,
    cooldownSuppressedCount: reasonCounts.cooldown_suppressed || 0,
    duplicateSuppressedCount: reasonCounts.duplicate_suppressed || 0,
  };
}

function stagePersist(runId, payload) {
  const start = Date.now();

  upsertSheetRows_(SHEETS.RAW_ODDS, [
    'key', 'event_id', 'bookmaker', 'bookmaker_keys_considered', 'market', 'outcome', 'price', 'odds_timestamp', 'odds_updated_time',
    'odds_updated_epoch_ms', 'provider_odds_updated_time', 'ingestion_timestamp', 'commence_time',
    'commence_epoch_ms', 'competition', 'player_1', 'player_2', 'source', 'updated_at',
  ], payload.odds);

  upsertSheetRows_(SHEETS.RAW_SCHEDULE, [
    'key', 'event_id', 'match_id', 'start_time', 'start_epoch_ms', 'competition', 'player_1', 'player_2',
    'canonical_tier', 'is_allowed', 'reason_code', 'source', 'updated_at',
  ], payload.schedule);

  upsertSheetRows_(SHEETS.MATCH_MAP, [
    'key', 'odds_event_id', 'schedule_event_id', 'match_type',
    'rejection_code', 'time_diff_min', 'competition_tier', 'updated_at',
  ], payload.matchMap);

  upsertSheetRows_(SHEETS.SIGNALS, [
    'key', 'run_id', 'odds_event_id', 'schedule_event_id',
    'market', 'side', 'bookmaker', 'competition_tier', 'model_version',
    'model_probability', 'market_implied_probability', 'edge_value', 'edge_tier', 'stake_units',
    'signal_hash', 'notification_outcome', 'reason_code', 'created_at',
  ], payload.signals);

  const total = payload.odds.length + payload.schedule.length + payload.matchMap.length + payload.signals.length;
  const summary = buildStageSummary_(runId, 'stagePersist', start, {
    input_count: total,
    output_count: total,
    provider: 'google_sheets',
    api_credit_usage: 0,
    reason_codes: {
      raw_odds_upserts: payload.odds.length,
      raw_schedule_upserts: payload.schedule.length,
      match_map_upserts: payload.matchMap.length,
      signals_upserts: payload.signals.length,
    },
  });

  return { summary };
}

function ensureTabsAndConfig_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  Object.keys(SHEETS).forEach((k) => ensureSheet_(ss, SHEETS[k]));

  ensureHeaders_(SHEETS.CONFIG, ['key', 'value']);
  ensureHeaders_(SHEETS.RUN_LOG, [
    'row_type', 'run_id', 'stage', 'started_at', 'ended_at', 'status', 'reason_code', 'message',
    'fetched_odds', 'fetched_schedule', 'allowed_tournaments', 'matched', 'unmatched', 'signals_found',
    'rejection_codes', 'cooldown_suppressed', 'duplicate_suppressed',
    'lock_event', 'debounce_event', 'trigger_event', 'exception', 'stack', 'stage_summaries',
  ]);
  ensureHeaders_(SHEETS.RAW_ODDS, [
    'key', 'event_id', 'bookmaker', 'bookmaker_keys_considered', 'market', 'outcome', 'price', 'odds_timestamp', 'odds_updated_time',
    'odds_updated_epoch_ms', 'provider_odds_updated_time', 'ingestion_timestamp', 'commence_time',
    'commence_epoch_ms', 'competition', 'player_1', 'player_2', 'source', 'updated_at',
  ]);
  ensureHeaders_(SHEETS.RAW_SCHEDULE, [
    'key', 'event_id', 'match_id', 'start_time', 'start_epoch_ms', 'competition', 'player_1', 'player_2',
    'canonical_tier', 'is_allowed', 'reason_code', 'source', 'updated_at',
  ]);
  ensureHeaders_(SHEETS.MATCH_MAP, [
    'key', 'odds_event_id', 'schedule_event_id', 'match_type',
    'rejection_code', 'time_diff_min', 'competition_tier', 'updated_at',
  ]);
  ensureHeaders_(SHEETS.SIGNALS, [
    'key', 'run_id', 'odds_event_id', 'schedule_event_id',
    'market', 'side', 'bookmaker', 'competition_tier', 'model_version',
    'model_probability', 'market_implied_probability', 'edge_value', 'edge_tier', 'stake_units',
    'signal_hash', 'notification_outcome', 'reason_code', 'created_at',
  ]);
  ensureHeaders_(SHEETS.STATE, ['key', 'value', 'updated_at']);

  ensureConfigDefaults_();
}

function ensureSheet_(ss, name) {
  let sh = ss.getSheetByName(name);
  if (!sh) sh = ss.insertSheet(name);
  return sh;
}

function ensureHeaders_(sheetName, headers) {
  const sh = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(sheetName);
  const firstRow = sh.getRange(1, 1, 1, headers.length).getValues()[0];
  const needsUpdate = headers.some((h, i) => firstRow[i] !== h);

  if (needsUpdate) {
    sh.getRange(1, 1, 1, headers.length).setValues([headers]);
  }

  if (sh.getFrozenRows() !== 1) sh.setFrozenRows(1);
}

function ensureConfigDefaults_() {
  const sh = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(SHEETS.CONFIG);
  const values = sh.getDataRange().getValues();
  const existing = {};
  for (let i = 1; i < values.length; i += 1) {
    existing[values[i][0]] = values[i][1];
  }

  Object.keys(DEFAULT_CONFIG).forEach((key) => {
    if (existing[key] === undefined || existing[key] === '') {
      sh.appendRow([key, DEFAULT_CONFIG[key]]);
    }
  });
}

function getConfig_() {
  const sh = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(SHEETS.CONFIG);
  const values = sh.getDataRange().getValues();
  const config = {};

  for (let i = 1; i < values.length; i += 1) {
    const key = String(values[i][0] || '').trim();
    if (!key) continue;
    config[key] = values[i][1];
  }

  return {
    RUN_ENABLED: toBoolean_(config.RUN_ENABLED, true),
    LOOKAHEAD_HOURS: toNumber_(config.LOOKAHEAD_HOURS, 36),
    ODDS_SPORT_KEY: String(config.ODDS_SPORT_KEY || 'tennis_wta'),
    ODDS_API_BASE_URL: String(config.ODDS_API_BASE_URL || 'https://api.the-odds-api.com/v4'),
    ODDS_API_KEY: String(config.ODDS_API_KEY || ''),
    ODDS_MARKETS: String(config.ODDS_MARKETS || 'h2h'),
    ODDS_REGIONS: String(config.ODDS_REGIONS || 'us'),
    ODDS_ODDS_FORMAT: String(config.ODDS_ODDS_FORMAT || 'american'),
    ODDS_CACHE_TTL_SEC: toNumber_(config.ODDS_CACHE_TTL_SEC, 300),
    SCHEDULE_BUFFER_BEFORE_MIN: toNumber_(config.SCHEDULE_BUFFER_BEFORE_MIN, 180),
    SCHEDULE_BUFFER_AFTER_MIN: toNumber_(config.SCHEDULE_BUFFER_AFTER_MIN, 180),
    MATCH_TIME_TOLERANCE_MIN: toNumber_(config.MATCH_TIME_TOLERANCE_MIN, 45),
    MATCH_FALLBACK_EXPANSION_MIN: toNumber_(config.MATCH_FALLBACK_EXPANSION_MIN, 120),
    ALLOW_WTA_125: toBoolean_(config.ALLOW_WTA_125, false),
    GRAND_SLAM_ALIASES_JSON: String(config.GRAND_SLAM_ALIASES_JSON || DEFAULT_CONFIG.GRAND_SLAM_ALIASES_JSON),
    WTA_1000_ALIASES_JSON: String(config.WTA_1000_ALIASES_JSON || DEFAULT_CONFIG.WTA_1000_ALIASES_JSON),
    WTA_500_ALIASES_JSON: String(config.WTA_500_ALIASES_JSON || DEFAULT_CONFIG.WTA_500_ALIASES_JSON),
    VERBOSE_LOGGING: toBoolean_(config.VERBOSE_LOGGING, true),
    DUPLICATE_DEBOUNCE_MS: toNumber_(config.DUPLICATE_DEBOUNCE_MS, 90000),
    PLAYER_ALIAS_MAP_JSON: String(config.PLAYER_ALIAS_MAP_JSON || '{}'),
    MODEL_VERSION: String(config.MODEL_VERSION || 'wta_mvp_v1'),
    EDGE_THRESHOLD_MICRO: toNumber_(config.EDGE_THRESHOLD_MICRO, 0.015),
    EDGE_THRESHOLD_SMALL: toNumber_(config.EDGE_THRESHOLD_SMALL, 0.03),
    EDGE_THRESHOLD_MED: toNumber_(config.EDGE_THRESHOLD_MED, 0.05),
    EDGE_THRESHOLD_STRONG: toNumber_(config.EDGE_THRESHOLD_STRONG, 0.08),
    STAKE_UNITS_MICRO: toNumber_(config.STAKE_UNITS_MICRO, 0.25),
    STAKE_UNITS_SMALL: toNumber_(config.STAKE_UNITS_SMALL, 0.5),
    STAKE_UNITS_MED: toNumber_(config.STAKE_UNITS_MED, 1),
    STAKE_UNITS_STRONG: toNumber_(config.STAKE_UNITS_STRONG, 1.5),
    SIGNAL_COOLDOWN_MIN: toNumber_(config.SIGNAL_COOLDOWN_MIN, 180),
    MINUTES_BEFORE_START_CUTOFF: toNumber_(config.MINUTES_BEFORE_START_CUTOFF, 60),
    STALE_ODDS_WINDOW_MIN: toNumber_(config.STALE_ODDS_WINDOW_MIN, 60),
  };
}

function appendStageLog_(runId, summary) {
  appendLogRow_({
    row_type: 'stage',
    run_id: runId,
    stage: summary.stage,
    started_at: summary.started_at,
    ended_at: summary.ended_at,
    status: 'success',
    reason_code: 'stage_completed',
    message: JSON.stringify({
      input_count: summary.input_count,
      output_count: summary.output_count,
      provider: summary.provider,
      reason_codes: summary.reason_codes,
    }),
  });
}

function appendLogRow_(entry) {
  const sh = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(SHEETS.RUN_LOG);
  sh.appendRow([
    entry.row_type || 'summary',
    entry.run_id || '',
    entry.stage || '',
    toIso_(entry.started_at),
    toIso_(entry.ended_at),
    entry.status || '',
    entry.reason_code || '',
    entry.message || '',
    entry.fetched_odds || 0,
    entry.fetched_schedule || 0,
    entry.allowed_tournaments || 0,
    entry.matched || 0,
    entry.unmatched || 0,
    entry.signals_found || 0,
    entry.rejection_codes || '{}',
    entry.cooldown_suppressed || 0,
    entry.duplicate_suppressed || 0,
    entry.lock_event || '',
    entry.debounce_event || '',
    entry.trigger_event || '',
    entry.exception || '',
    entry.stack || '',
    entry.stage_summaries || '[]',
  ]);
}

function setStateValue_(key, value) {
  upsertSheetRows_(SHEETS.STATE, ['key', 'value', 'updated_at'], [{
    key,
    value,
    updated_at: new Date().toISOString(),
  }]);
}

function upsertSheetRows_(sheetName, headers, rows) {
  if (!rows || !rows.length) return;
  const sh = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(sheetName);
  ensureHeaders_(sheetName, headers);

  const lastRow = sh.getLastRow();
  const existing = lastRow > 1 ? sh.getRange(2, 1, lastRow - 1, headers.length).getValues() : [];
  const keyToRowIdx = {};

  existing.forEach((row, idx) => {
    keyToRowIdx[String(row[0])] = idx;
  });

  rows.forEach((obj) => {
    const newRow = headers.map((h) => obj[h]);
    const key = String(newRow[0]);
    if (Object.prototype.hasOwnProperty.call(keyToRowIdx, key)) {
      existing[keyToRowIdx[key]] = newRow;
    } else {
      keyToRowIdx[key] = existing.length;
      existing.push(newRow);
    }
  });

  if (existing.length) {
    sh.getRange(2, 1, existing.length, headers.length).setValues(existing);
  }
}

function incrementDuplicatePreventedCount_() {
  const scriptProps = PropertiesService.getScriptProperties();
  const current = Number(scriptProps.getProperty(PROPS.DUPLICATE_PREVENTED_COUNT) || 0);
  const next = current + 1;
  scriptProps.setProperty(PROPS.DUPLICATE_PREVENTED_COUNT, String(next));
  return next;
}

function buildStageSummary_(runId, stage, startMs, opts) {
  const endMs = Date.now();
  const summary = {
    run_id: runId,
    stage,
    started_at: new Date(startMs).toISOString(),
    ended_at: new Date(endMs).toISOString(),
    duration_ms: endMs - startMs,
    input_count: opts.input_count,
    output_count: opts.output_count,
    provider: opts.provider,
    api_credit_usage: opts.api_credit_usage,
    reason_codes: opts.reason_codes || {},
  };
  Logger.log(JSON.stringify(summary));
  return summary;
}

function buildSignalRow_(runId, config, event, match, detail) {
  return {
    key: detail.signal_hash,
    run_id: runId,
    odds_event_id: event.event_id,
    schedule_event_id: match.schedule_event_id,
    market: event.market,
    side: event.outcome,
    bookmaker: event.bookmaker,
    competition_tier: match.competition_tier,
    model_version: config.MODEL_VERSION,
    model_probability: detail.model_probability,
    market_implied_probability: detail.market_implied_probability,
    edge_value: detail.edge_value,
    edge_tier: detail.edge_tier,
    stake_units: detail.stake_units,
    signal_hash: detail.signal_hash,
    notification_outcome: detail.notification_outcome,
    reason_code: detail.notification_outcome,
    commence_time: event.commence_time.toISOString(),
    odds_updated_time: event.odds_updated_time.toISOString(),
    created_at: new Date().toISOString(),
  };
}

function getSignalState_() {
  const existing = getStateJson_('SIGNAL_GUARD_STATE');
  if (!existing || typeof existing !== 'object') return { sent_hashes: {} };
  return {
    sent_hashes: existing.sent_hashes || {},
  };
}

function setSignalState_(state) {
  setStateValue_('SIGNAL_GUARD_STATE', JSON.stringify({
    updated_at: new Date().toISOString(),
    sent_hashes: state.sent_hashes || {},
  }));
}

function maybeNotifySignal_(state, seenHashesThisRun, signalHash, nowMs, cooldownMin) {
  const lastSent = Number((state.sent_hashes || {})[signalHash] || 0);

  if (seenHashesThisRun[signalHash]) {
    return { outcome: 'duplicate_suppressed' };
  }

  seenHashesThisRun[signalHash] = true;

  if (lastSent > 0 && nowMs - lastSent < cooldownMin * 60000) {
    return { outcome: 'cooldown_suppressed' };
  }

  state.sent_hashes[signalHash] = nowMs;
  return { outcome: 'sent' };
}

function buildSignalHash_(eventId, market, side, modelVersion) {
  return [eventId || '', market || '', side || '', modelVersion || ''].join('|');
}

function oddsPriceToImpliedProbability_(price) {
  const p = Number(price);
  if (!Number.isFinite(p) || p === 0) return null;
  if (p > 0) return roundNumber_(100 / (p + 100), 4);
  return roundNumber_(Math.abs(p) / (Math.abs(p) + 100), 4);
}

function estimateFairProbability_(marketProbability, competitionTier) {
  const tierBump = {
    GRAND_SLAM: 0.012,
    WTA_1000: 0.01,
    WTA_500: 0.008,
    WTA_125: 0.005,
  };
  const underdogBump = marketProbability < 0.5 ? 0.01 : -0.005;
  const fair = marketProbability + underdogBump + (tierBump[competitionTier] || 0.005);
  return roundNumber_(Math.max(0.02, Math.min(0.98, fair)), 4);
}

function classifyEdgeAndStake_(edgeValue, config) {
  if (edgeValue >= config.EDGE_THRESHOLD_STRONG) {
    return { edge_tier: 'STRONG', stake_units: config.STAKE_UNITS_STRONG };
  }
  if (edgeValue >= config.EDGE_THRESHOLD_MED) {
    return { edge_tier: 'MED', stake_units: config.STAKE_UNITS_MED };
  }
  if (edgeValue >= config.EDGE_THRESHOLD_SMALL) {
    return { edge_tier: 'SMALL', stake_units: config.STAKE_UNITS_SMALL };
  }
  if (edgeValue >= config.EDGE_THRESHOLD_MICRO) {
    return { edge_tier: 'MICRO', stake_units: config.STAKE_UNITS_MICRO };
  }
  return { edge_tier: 'NONE', stake_units: 0 };
}

function roundNumber_(value, decimals) {
  const factor = Math.pow(10, Number(decimals || 0));
  return Math.round(Number(value) * factor) / factor;
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
  const creditHeaders = {
    requests_used: Number(headers['x-requests-used'] || headers['X-Requests-Used'] || 0),
    requests_remaining: Number(headers['x-requests-remaining'] || headers['X-Requests-Remaining'] || 0),
    requests_last: Number(headers['x-requests-last'] || headers['X-Requests-Last'] || 0),
  };

  if (status < 200 || status >= 300) {
    return {
      ok: false,
      payload: [],
      reason_code: 'api_http_' + status,
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
  };
}

function getCachedPayload_(key) {
  try {
    const raw = CacheService.getScriptCache().get(key);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    return {
      cached_at_ms: parsed.cached_at_ms,
      events: (parsed.events || []).map(deserializeOddsEvent_),
    };
  } catch (e) {
    return null;
  }
}

function setCachedPayload_(key, events) {
  CacheService.getScriptCache().put(key, JSON.stringify({
    cached_at_ms: Date.now(),
    events: events.map(serializeOddsEvent_),
  }), 21600);
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

function getStateJson_(key) {
  const sh = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(SHEETS.STATE);
  const values = sh.getDataRange().getValues();
  for (let i = 1; i < values.length; i += 1) {
    if (String(values[i][0]) === key) {
      try {
        return JSON.parse(values[i][1]);
      } catch (e) {
        return null;
      }
    }
  }
  return null;
}

function mergeReasonCounts_(reasonMaps) {
  const merged = {};
  reasonMaps.forEach((map) => {
    Object.keys(map || {}).forEach((k) => {
      merged[k] = (merged[k] || 0) + map[k];
    });
  });
  return merged;
}



function getTopReasonCodes_(reasonMap, topN) {
  return Object.keys(reasonMap || {})
    .map((k) => ({ reason_code: k, count: reasonMap[k] }))
    .sort((a, b) => b.count - a.count)
    .slice(0, topN || 10);
}

function buildRunId_() {
  return Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyyMMdd'T'HHmmss") + '_' + Utilities.getUuid().slice(0, 8);
}

function tryLock_(lock, timeoutMs) {
  try {
    lock.tryLock(timeoutMs);
    return lock.hasLock();
  } catch (e) {
    return false;
  }
}

function toBoolean_(value, fallback) {
  if (value === true || value === false) return value;
  if (value === undefined || value === null || value === '') return fallback;
  const lowered = String(value).toLowerCase().trim();
  return lowered === 'true' || lowered === '1' || lowered === 'yes';
}

function toNumber_(value, fallback) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function toIso_(value) {
  if (!value) return '';
  if (value instanceof Date) return value.toISOString();
  return String(value);
}
