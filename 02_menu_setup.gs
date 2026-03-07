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
  const checkedAt = localAndUtcTimestamps_(new Date());
  const report = {
    checked_at: checkedAt.local,
    checked_at_utc: checkedAt.utc,
    tabs_present: tabStatus,
    run_edgeboard_triggers: triggerCount,
    trigger_signature: scriptProps.getProperty(PROPS.PIPELINE_TRIGGER_SIGNATURE) || '',
    duplicate_prevented_count: Number(scriptProps.getProperty(PROPS.DUPLICATE_PREVENTED_COUNT) || 0),
    timezone: TIMESTAMP_TIMEZONE.ID,
    timezone_offset: TIMESTAMP_TIMEZONE.OFFSET,
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
  ensureHeaders_(SHEETS.RAW_PLAYER_STATS, [
    'key', 'event_id', 'player_canonical_name', 'source', 'feature_timestamp', 'feature_values', 'has_stats', 'updated_at',
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
