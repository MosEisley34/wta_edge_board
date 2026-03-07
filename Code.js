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
  VERBOSE_LOGGING: 'true',
  DUPLICATE_DEBOUNCE_MS: '90000',
  PLAYER_ALIAS_MAP_JSON: '{}',
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

    const signalStage = stageGenerateSignals(runId, matchStage.rows);
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
      signals_found: signalStage.signalsFound,
      rejection_codes: JSON.stringify(combinedReasonCodes),
      stage_summaries: JSON.stringify(verbosePayload.stage_summaries),
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
    key: [event.event_id, event.bookmaker, event.market, event.outcome].join('|'),
    event_id: event.event_id,
    bookmaker: event.bookmaker,
    market: event.market,
    outcome: event.outcome,
    price: event.price,
    commence_time: event.commence_time.toISOString(),
    commence_epoch_ms: event.commence_time.getTime(),
    competition: event.competition,
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
  const rows = [];
  const allowedEvents = [];

  inWindow.forEach((event) => {
    const canonical = canonicalizeCompetition(event.competition);
    const decision = isAllowedTournament(canonical, config);
    reasonCounts[decision.reason_code] = (reasonCounts[decision.reason_code] || 0) + 1;
    canonicalExamples.push({
      raw_name: event.competition,
      canonical_tier: canonical,
      reason_code: decision.reason_code,
    });

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

function stageGenerateSignals(runId, matchRows) {
  const start = Date.now();
  const rows = [];
  let signalsFound = 0;

  matchRows.forEach((row) => {
    if (!row.schedule_event_id) return;
    const signal = {
      key: row.key,
      run_id: runId,
      odds_event_id: row.odds_event_id,
      schedule_event_id: row.schedule_event_id,
      signal_type: 'candidate_edge',
      signal_score: 1,
      reason_code: row.match_type,
      created_at: new Date().toISOString(),
    };
    rows.push(signal);
    signalsFound += 1;
  });

  const summary = buildStageSummary_(runId, 'stageGenerateSignals', start, {
    input_count: matchRows.length,
    output_count: rows.length,
    provider: 'internal_signal_builder',
    api_credit_usage: 0,
    reason_codes: {
      signal_created: signalsFound,
      signal_not_created: matchRows.length - signalsFound,
    },
  });

  return { rows, signalsFound, summary };
}

function stagePersist(runId, payload) {
  const start = Date.now();

  upsertSheetRows_(SHEETS.RAW_ODDS, [
    'key', 'event_id', 'bookmaker', 'market', 'outcome', 'price', 'commence_time',
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
    'signal_type', 'signal_score', 'reason_code', 'created_at',
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
    'key', 'event_id', 'bookmaker', 'market', 'outcome', 'price', 'commence_time',
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
    'signal_type', 'signal_score', 'reason_code', 'created_at',
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
    VERBOSE_LOGGING: toBoolean_(config.VERBOSE_LOGGING, true),
    DUPLICATE_DEBOUNCE_MS: toNumber_(config.DUPLICATE_DEBOUNCE_MS, 90000),
    PLAYER_ALIAS_MAP_JSON: String(config.PLAYER_ALIAS_MAP_JSON || '{}'),
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

function canonicalizeCompetition(name) {
  if (!name) return 'UNKNOWN';
  const norm = String(name).toLowerCase().replace(/\s+/g, ' ').trim();

  if (/(australian open|roland garros|french open|wimbledon|us open)/.test(norm)) return 'GRAND_SLAM';
  if (/wta\s*1000/.test(norm)) return 'WTA_1000';
  if (/wta\s*500/.test(norm)) return 'WTA_500';
  if (/wta\s*125/.test(norm)) return 'WTA_125';
  if (/wta/.test(norm)) return 'OTHER';
  return 'UNKNOWN';
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

  return {
    events: (fetched.payload || []).map((event) => {
      const outcome = (((event.bookmakers || [])[0] || {}).markets || [])[0];
      const firstOutcome = ((outcome || {}).outcomes || [])[0] || {};
      return {
        event_id: event.id,
        bookmaker: ((event.bookmakers || [])[0] || {}).key || '',
        market: ((outcome || {}).key) || config.ODDS_MARKETS,
        outcome: firstOutcome.name || '',
        price: Number(firstOutcome.price || ''),
        commence_time: new Date(event.commence_time),
        competition: event.sport_title || '',
        player_1: event.home_team || '',
        player_2: event.away_team || '',
      };
    }),
    reason_code: 'odds_api_success',
    api_credit_usage: fetched.api_credit_usage,
    api_call_count: 1,
    credit_headers: fetched.credit_headers,
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
    market: event.market,
    outcome: event.outcome,
    price: event.price,
    commence_time: event.commence_time.toISOString(),
    competition: event.competition,
    player_1: event.player_1,
    player_2: event.player_2,
  };
}

function deserializeOddsEvent_(event) {
  return {
    event_id: event.event_id,
    bookmaker: event.bookmaker,
    market: event.market,
    outcome: event.outcome,
    price: event.price,
    commence_time: new Date(event.commence_time),
    competition: event.competition,
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
