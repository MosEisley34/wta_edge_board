function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('WTA Pipeline Ops')
    .addItem('Setup / Verify Tabs', 'menuSetupVerifyTabs')
    .addItem('Deduplicate Config Sheet', 'menuDedupeConfigSheet')
    .addItem('Run Pipeline Now', 'menuRunPipelineNow')
    .addSeparator()
    .addItem('Re-create / Reset Workbook', 'menuRecreateWorkbook')
    .addSeparator()
    .addItem('Install Triggers', 'menuInstallTriggers')
    .addItem('Remove Triggers', 'menuRemoveTriggers')
    .addSeparator()
    .addItem('Diagnostics / Health Check', 'menuDiagnosticsHealthCheck')
    .addSeparator()
    .addItem('Validate TennisAbstract Sources', 'menuValidateTennisAbstractSources')
    .addItem('Refresh H2H Cache', 'menuRefreshH2hCache')
    .addItem('Preview Player Stats Mapping', 'menuPreviewPlayerStatsMapping')
    .addToUi();
}

function menuSetupVerifyTabs() {
  ensureTabsAndConfig_();
  SpreadsheetApp.getUi().alert('Setup complete: WTA foundation tabs/headers verified.');
}

function menuDedupeConfigSheet() {
  dedupeConfigSheetMenuAction_();
  SpreadsheetApp.getUi().alert('Config sheet deduplicated.');
}

function menuRunPipelineNow() {
  try {
    runEdgeBoard();
    SpreadsheetApp.getUi().alert('Pipeline run complete. Check Run_Log and State.');
  } catch (error) {
    const errorMessage = String(error && error.message ? error.message : error);
    if (errorMessage.indexOf('dedupeConfigSheet_()') >= 0) {
      SpreadsheetApp.getUi().alert(
        'Pipeline run failed',
        'Why this fails: duplicate config keys are ambiguous.\nHow to fix safely: run dedupeConfigSheet_() exactly once, then run pipeline again.',
        SpreadsheetApp.getUi().ButtonSet.OK,
      );
    }
    throw error;
  }
}

function menuRecreateWorkbook() {
  const ui = SpreadsheetApp.getUi();
  const response = ui.alert(
    'Re-create / Reset Workbook',
    'This will delete and recreate all managed pipeline tabs, clear script state/properties, and remove pipeline triggers. Continue?',
    ui.ButtonSet.YES_NO,
  );

  if (response !== ui.Button.YES) {
    appendLogRow_({
      row_type: 'ops',
      run_id: buildRunId_(),
      stage: 'menuRecreateWorkbook',
      status: 'skipped',
      reason_code: 'recreate_cancelled',
      message: 'Workbook recreate was cancelled by user.',
    });
    return;
  }

  recreateWorkbook_();
  ui.alert('Workbook reset complete. Tabs, defaults, and headers have been recreated.');
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


function menuValidateTennisAbstractSources() {
  const report = runProviderHealthCheck_({ forceH2hRefresh: false, includeMappingPreview: true });
  SpreadsheetApp.getUi().alert('TennisAbstract source validation complete. Status: ' + report.status + '. Check ProviderHealth tab.');
}

function menuRefreshH2hCache() {
  const report = runProviderHealthCheck_({ forceH2hRefresh: true, includeMappingPreview: false });
  SpreadsheetApp.getUi().alert('H2H cache refresh complete. Status: ' + report.status + '. Check ProviderHealth tab.');
}

function menuPreviewPlayerStatsMapping() {
  const report = runProviderHealthCheck_({ forceH2hRefresh: false, includeMappingPreview: true });
  SpreadsheetApp.getUi().alert('Player stats mapping preview complete. Status: ' + report.status + '. Check ProviderHealth tab.');
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


function runProviderHealthCheck_(options) {
  ensureTabsAndConfig_();
  const opts = options || {};
  const config = getConfig_();
  const now = new Date();

  const leadersValidation = validateTaLeadersSource_(config);
  const h2hValidation = validateTaH2hSource_(config, !!opts.forceH2hRefresh);
  const samplePlayers = buildProviderSamplePlayers_(leadersValidation.structured_rows || []);

  const checks = [leadersValidation.status, h2hValidation.status];
  if (samplePlayers.length === 0) checks.push('warn');

  const finalStatus = checks.indexOf('fail') >= 0 ? 'fail' : (checks.indexOf('warn') >= 0 ? 'warn' : 'pass');
  const stamp = localAndUtcTimestamps_(now);
  const rows = [
    ['last_run_timestamp', stamp.local, finalStatus, ''],
    ['leaders_js_url_detected', leadersValidation.leaders_js_url || '', leadersValidation.status, leadersValidation.reason_code || ''],
    ['matchmx_row_count', leadersValidation.matchmx_row_count, leadersValidation.status, leadersValidation.reason_code || ''],
    ['h2h_pair_count', h2hValidation.h2h_pair_count, h2hValidation.status, h2hValidation.reason_code || ''],
    ['sample_parsed_players', samplePlayers.join(', '), samplePlayers.length ? 'pass' : 'warn', samplePlayers.length ? '' : 'no_sample_players'],
  ];

  writeProviderHealthRows_(rows, stamp.local);

  appendLogRow_({
    row_type: 'ops',
    run_id: buildRunId_(),
    stage: 'providerHealthCheck',
    status: finalStatus,
    reason_code: finalStatus === 'pass' ? 'provider_health_ok' : 'provider_health_attention',
    message: JSON.stringify({
      leaders_status: leadersValidation.status,
      leaders_reason_code: leadersValidation.reason_code,
      h2h_status: h2hValidation.status,
      h2h_reason_code: h2hValidation.reason_code,
      sample_players: samplePlayers,
      include_mapping_preview: !!opts.includeMappingPreview,
      force_h2h_refresh: !!opts.forceH2hRefresh,
    }),
  });

  return {
    status: finalStatus,
    checked_at: stamp.local,
    checked_at_utc: stamp.utc,
    leaders_js_url_detected: leadersValidation.leaders_js_url || '',
    matchmx_row_count: Number(leadersValidation.matchmx_row_count || 0),
    h2h_pair_count: Number(h2hValidation.h2h_pair_count || 0),
    sample_parsed_players: samplePlayers,
  };
}

function validateTaLeadersSource_(config) {
  const leadersUrl = String(config.PLAYER_STATS_TA_LEADERS_URL || DEFAULT_CONFIG.PLAYER_STATS_TA_LEADERS_URL).trim();
  const headers = {
    Accept: 'text/html',
    'User-Agent': String(config.PLAYER_STATS_FETCH_USER_AGENT || DEFAULT_CONFIG.PLAYER_STATS_FETCH_USER_AGENT),
  };

  const pageFetch = playerStatsFetchWithRetry_(leadersUrl, {
    method: 'get',
    muteHttpExceptions: true,
    headers: headers,
    followRedirects: true,
    validateHttpsCertificates: true,
  }, config);

  if (!pageFetch.ok) {
    return { status: 'fail', reason_code: pageFetch.reason_code || 'ta_leaders_page_fetch_failed', leaders_js_url: '', matchmx_row_count: 0, structured_rows: [] };
  }

  const html = String(pageFetch.response.getContentText() || '');
  const leadersJsUrl = extractLeadersJsUrl_(html, leadersUrl);
  if (!leadersJsUrl) {
    return { status: 'fail', reason_code: 'ta_leaders_js_url_missing', leaders_js_url: '', matchmx_row_count: 0, structured_rows: [] };
  }

  sleepTennisAbstractRequestGap_(config);
  const jsFetch = playerStatsFetchWithRetry_(leadersJsUrl, {
    method: 'get',
    muteHttpExceptions: true,
    headers: headers,
    followRedirects: true,
    validateHttpsCertificates: true,
  }, config);

  if (!jsFetch.ok) {
    return { status: 'fail', reason_code: jsFetch.reason_code || 'ta_leaders_js_fetch_failed', leaders_js_url: leadersJsUrl, matchmx_row_count: 0, structured_rows: [] };
  }

  const jsPayload = String(jsFetch.response.getContentText() || '');
  const structuredRows = extractMatchMxRows_(jsPayload);
  const rowCount = structuredRows.length;
  const status = rowCount > 0 ? 'pass' : 'warn';

  return {
    status: status,
    reason_code: rowCount > 0 ? 'ta_matchmx_ok' : 'ta_matchmx_empty',
    leaders_js_url: leadersJsUrl,
    matchmx_row_count: rowCount,
    structured_rows: structuredRows,
  };
}

function validateTaH2hSource_(config, forceRefresh) {
  const runtimeConfig = Object.assign({}, config, { PLAYER_STATS_FORCE_REFRESH: !!forceRefresh });
  const dataset = getTaH2hDataset_(runtimeConfig);
  const pairCount = Number(dataset && dataset.rows ? dataset.rows.length : 0);
  if (pairCount <= 0) {
    const meta = getStateJson_('PLAYER_STATS_H2H_LAST_FETCH_META') || {};
    const reason = String(meta.last_failure_reason || meta.source_type || 'ta_h2h_parse_failed');
    return { status: 'warn', reason_code: reason, h2h_pair_count: 0 };
  }

  return {
    status: 'pass',
    reason_code: forceRefresh ? 'ta_h2h_refreshed' : 'ta_h2h_ok',
    h2h_pair_count: pairCount,
  };
}

function buildProviderSamplePlayers_(matchMxRows) {
  const samples = [];
  const seen = {};

  (matchMxRows || []).forEach(function (row) {
    const canonical = canonicalizePlayerName_(row.player_name || '', {});
    if (!canonical || seen[canonical]) return;
    seen[canonical] = true;
    samples.push(canonical);
  });

  return samples.slice(0, 8);
}

function writeProviderHealthRows_(rows, updatedAt) {
  const sh = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(SHEETS.PROVIDER_HEALTH);
  const headers = ['metric', 'value', 'status', 'details', 'updated_at'];
  sh.clearContents();
  sh.getRange(1, 1, 1, headers.length).setValues([headers]);

  const output = (rows || []).map(function (row) {
    return [row[0], row[1], row[2], row[3], updatedAt];
  });

  if (output.length) {
    sh.getRange(2, 1, output.length, headers.length).setValues(output);
  }

  if (sh.getFrozenRows() !== 1) sh.setFrozenRows(1);
}

function ensureTabsAndConfig_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  Object.keys(SHEETS).forEach((k) => ensureSheet_(ss, SHEETS[k]));

  ensureHeaders_(SHEETS.CONFIG, ['key', 'value']);
  ensureHeaders_(SHEETS.RUN_LOG, RUN_LOG_HEADERS);
  ensureHeaders_(SHEETS.RAW_ODDS, [
    'key', 'event_id', 'bookmaker', 'bookmaker_keys_considered', 'market', 'outcome', 'price', 'odds_timestamp', 'odds_updated_time',
    'odds_updated_epoch_ms', 'provider_odds_updated_time', 'ingestion_timestamp', 'commence_time',
    'commence_epoch_ms', 'competition', 'player_1', 'player_2',
    'player_1_hold_pct', 'player_2_hold_pct', 'player_1_break_pct', 'player_2_break_pct',
    'player_1_form_score', 'player_2_form_score',
    'h2h_p1_wins', 'h2h_p2_wins', 'h2h_total_matches',
    'surface', 'stats_source', 'h2h_source', 'stats_as_of',
    'source', 'updated_at',
  ]);
  ensureHeaders_(SHEETS.RAW_SCHEDULE, [
    'key', 'event_id', 'match_id', 'start_time', 'start_epoch_ms', 'competition', 'player_1', 'player_2',
    'player_1_hold_pct', 'player_2_hold_pct', 'player_1_break_pct', 'player_2_break_pct',
    'player_1_form_score', 'player_2_form_score',
    'h2h_p1_wins', 'h2h_p2_wins', 'h2h_total_matches',
    'surface', 'stats_source', 'h2h_source', 'stats_as_of',
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
  ensureHeaders_(SHEETS.PROVIDER_HEALTH, ['metric', 'value', 'status', 'details', 'updated_at']);

  ensureConfigDefaults_();
}

function ensureSheet_(ss, name) {
  let sh = ss.getSheetByName(name);
  if (!sh) sh = ss.insertSheet(name);
  return sh;
}

function ensureHeaders_(sheetName, headers) {
  const sh = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(sheetName);
  const existingHeaderWidth = Math.max(1, sh.getLastColumn() || 1);
  const existingHeaders = sh.getRange(1, 1, 1, existingHeaderWidth).getValues()[0];

  if (!existingHeaders[0]) {
    sh.getRange(1, 1, 1, headers.length).setValues([headers]);
  } else {
    const hasSamePrefix = headers.slice(0, existingHeaders.length).every((h, i) => existingHeaders[i] === h);
    const hasAllColumns = headers.every((h) => existingHeaders.indexOf(h) !== -1);

    if (hasSamePrefix && !hasAllColumns) {
      sh.getRange(1, existingHeaders.length + 1, 1, headers.length - existingHeaders.length)
        .setValues([headers.slice(existingHeaders.length)]);
    } else if (!hasSamePrefix) {
      sh.getRange(1, 1, 1, headers.length).setValues([headers]);
    }
  }

  if (sh.getFrozenRows() !== 1) sh.setFrozenRows(1);
}

function ensureConfigDefaults_() {
  const sh = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(SHEETS.CONFIG);
  const values = sh.getDataRange().getValues();
  const parsed = parseConfigRows_(values, {
    mode: 'warn_last_wins',
    context: 'ensureConfigDefaults_',
    logger: function (msg) { Logger.log(msg); },
  });

  if (parsed.duplicate_keys.length) {
    dedupeConfigSheet_(sh, {
      precedence: 'last_wins',
      preserve_row_order: true,
      include_missing_defaults: false,
      log_summary: true,
    });
  }

  const existing = parsed.config;
  Object.keys(DEFAULT_CONFIG).forEach((key) => {
    if (existing[key] === undefined || existing[key] === '') {
      sh.appendRow([key, DEFAULT_CONFIG[key]]);
    }
  });
}

function dedupeConfigSheet_(sheet, options) {
  const auditedKeys = {
    ODDS_SPORT_KEY: true,
    PLAYER_STATS_API_BASE_URL: true,
    PLAYER_STATS_API_KEY: true,
    PLAYER_STATS_SCRAPE_URLS: true,
  };
  const sh = sheet || SpreadsheetApp.getActiveSpreadsheet().getSheetByName(SHEETS.CONFIG);
  const opts = options || {};
  const precedence = String(opts.precedence || 'last_wins');
  const preserveRowOrder = opts.preserve_row_order !== false;
  const includeMissingDefaults = opts.include_missing_defaults !== false;
  const logSummary = !!opts.log_summary;
  const values = sh.getDataRange().getValues();

  if (!values.length) {
    ensureHeaders_(SHEETS.CONFIG, ['key', 'value']);
    return {
      rewritten: false,
      duplicate_keys: [],
      removed_row_count: 0,
      final_row_count: 1,
      added_default_count: 0,
    };
  }

  const header = values[0];
  const body = values.slice(1);
  const rowsByKey = {};
  const rowNumbersByKey = {};
  const firstRowIndexByKey = {};
  const lastRowIndexByKey = {};

  body.forEach(function (row, idx) {
    const key = String(row[0] || '').trim();
    if (!key) return;
    if (!rowsByKey[key]) rowsByKey[key] = [];
    if (!rowNumbersByKey[key]) rowNumbersByKey[key] = [];
    rowsByKey[key].push(row);
    rowNumbersByKey[key].push(idx + 2);
    if (firstRowIndexByKey[key] === undefined) firstRowIndexByKey[key] = idx;
    lastRowIndexByKey[key] = idx;
  });

  const duplicateKeys = Object.keys(rowsByKey).filter(function (key) {
    return rowsByKey[key].length > 1;
  });

  const keys = Object.keys(rowsByKey);
  keys.sort(function (a, b) {
    if (!preserveRowOrder) return a.localeCompare(b);
    const ai = precedence === 'first_wins' ? firstRowIndexByKey[a] : lastRowIndexByKey[a];
    const bi = precedence === 'first_wins' ? firstRowIndexByKey[b] : lastRowIndexByKey[b];
    return ai - bi;
  });

  const dedupedBody = keys.map(function (key) {
    const bucket = rowsByKey[key];
    return precedence === 'first_wins' ? bucket[0] : bucket[bucket.length - 1];
  });

  const removedRowsByKey = {};
  const keptRowByKey = {};
  duplicateKeys.forEach(function (key) {
    const rowNumbers = rowNumbersByKey[key] || [];
    const kept = precedence === 'first_wins' ? rowNumbers[0] : rowNumbers[rowNumbers.length - 1];
    keptRowByKey[key] = kept;
    removedRowsByKey[key] = rowNumbers.filter(function (rowNumber) {
      return rowNumber !== kept;
    });
  });

  let addedDefaultCount = 0;
  if (includeMissingDefaults) {
    const existing = {};
    dedupedBody.forEach(function (row) {
      existing[String(row[0] || '').trim()] = true;
    });
    Object.keys(DEFAULT_CONFIG).forEach(function (key) {
      if (!existing[key]) {
        dedupedBody.push([key, DEFAULT_CONFIG[key]]);
        addedDefaultCount += 1;
      }
    });
  }

  const output = [header].concat(dedupedBody);
  sh.clearContents();
  sh.getRange(1, 1, output.length, header.length).setValues(output);
  if (sh.getFrozenRows() !== 1) sh.setFrozenRows(1);

  const verifyParsed = parseConfigRows_(output, {
    mode: 'error',
    context: 'dedupeConfigSheet_ post_verify',
  });
  if (verifyParsed.duplicate_keys.length) {
    throw new Error('dedupeConfigSheet_ post_verify failed: duplicate keys remain: ' + verifyParsed.duplicate_keys.join(', '));
  }

  const watchedRemovedRowsByKey = {};
  const watchedKeptRowByKey = {};
  Object.keys(auditedKeys).forEach(function (key) {
    watchedRemovedRowsByKey[key] = removedRowsByKey[key] || [];
    watchedKeptRowByKey[key] = keptRowByKey[key] || null;
  });

  const summary = {
    rewritten: true,
    duplicate_keys: duplicateKeys,
    removed_row_count: body.length - keys.length,
    final_row_count: output.length,
    added_default_count: addedDefaultCount,
    removed_rows_by_key: removedRowsByKey,
    kept_row_by_key: keptRowByKey,
    verified_unique_keys: true,
    watched_removed_rows_by_key: watchedRemovedRowsByKey,
    watched_kept_row_by_key: watchedKeptRowByKey,
  };

  appendLogRow_({
    row_type: 'ops',
    stage: 'config_dedupe',
    status: 'ok',
    reason_code: 'config_dedupe_applied',
    message: JSON.stringify({
      precedence: precedence,
      duplicate_keys: duplicateKeys,
      removed_rows_by_key: removedRowsByKey,
      kept_row_by_key: keptRowByKey,
      watched_removed_rows_by_key: watchedRemovedRowsByKey,
      watched_kept_row_by_key: watchedKeptRowByKey,
      removed_row_count: summary.removed_row_count,
    }),
  });

  if (logSummary) {
    Logger.log('[Config] dedupeConfigSheet_ summary: ' + JSON.stringify(summary));
  }

  return summary;
}

function dedupeConfigSheetMenuAction_() {
  ensureTabsAndConfig_();
  const summary = dedupeConfigSheet_(null, {
    precedence: 'last_wins',
    preserve_row_order: true,
    include_missing_defaults: true,
    log_summary: true,
  });

  SpreadsheetApp.getActiveSpreadsheet().toast(
    'Config dedupe complete: removed ' + summary.removed_row_count + ' duplicate rows; defaults added ' + summary.added_default_count + '.',
    'WTA Edge Board',
    8
  );
}

function recreateWorkbook_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const managedTabs = Object.keys(SHEETS).map((k) => SHEETS[k]);
  const placeholderName = '__WTA_RESET_PLACEHOLDER__';
  const createdPlaceholder = !ss.getSheetByName(placeholderName);

  removePipelineTriggers();

  if (createdPlaceholder && ss.getSheets().length <= managedTabs.length) {
    ensureSheet_(ss, placeholderName);
  }

  managedTabs.forEach((name) => {
    const sh = ss.getSheetByName(name);
    if (sh) ss.deleteSheet(sh);
  });

  const placeholder = ss.getSheetByName(placeholderName);
  if (placeholder && ss.getSheets().length > 1) {
    ss.deleteSheet(placeholder);
  }

  const scriptProps = PropertiesService.getScriptProperties();
  const allProps = scriptProps.getProperties();
  Object.keys(allProps || {}).forEach((key) => {
    scriptProps.deleteProperty(key);
  });

  const scriptCache = CacheService.getScriptCache();
  scriptCache.removeAll([
    'ODDS_WINDOW_PAYLOAD',
    'SCHEDULE_WINDOW_CACHE',
  ]);

  ensureTabsAndConfig_();

  const placeholderAfterSetup = ss.getSheetByName(placeholderName);
  if (placeholderAfterSetup && ss.getSheets().length > 1) {
    ss.deleteSheet(placeholderAfterSetup);
  }

  appendLogRow_({
    row_type: 'ops',
    run_id: buildRunId_(),
    stage: 'recreateWorkbook_',
    status: 'success',
    reason_code: 'recreate_completed',
    message: 'Workbook tabs were deleted/recreated, triggers removed, and script state cleared.',
  });
}
