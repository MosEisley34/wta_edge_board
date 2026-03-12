#!/usr/bin/env node
const fs = require('fs');
const vm = require('vm');

const FILES = [
  '01_constants_config.gs',
  '02_menu_setup.gs',
  '03_triggers_orchestration.gs',
  '04_odds_schedule_adapters.gs',
  '05_matching_normalization.gs',
  '05_player_stats_adapter.gs',
  '06_signals_risk_controls.gs',
  '07_shared_utils_state_logging.gs',
];

function loadContext() {
  const context = {
    console,
    JSON,
    Date,
    Math,
    Number,
    String,
    Boolean,
    Array,
    Object,
    RegExp,
    Error,
    parseInt,
    parseFloat,
    isNaN,
    Utilities: {
      formatDate: (d) => new Date(d).toISOString(),
      sleep: () => {},
      newBlob: (s) => ({ getBytes: () => Buffer.from(String(s), 'utf8') }),
    },
    Session: { getScriptTimeZone: () => 'UTC' },
    Logger: { log: () => {} },
    UrlFetchApp: { fetch: () => { throw new Error('fetch disabled in sample capture'); } },
    PropertiesService: { getScriptProperties: () => ({ getProperty: () => '', setProperty: () => {}, deleteProperty: () => {} }) },
    LockService: { getScriptLock: () => ({ waitLock: () => {}, releaseLock: () => {} }) },
    SpreadsheetApp: { getActiveSpreadsheet: () => ({ getSheetByName: () => null }), getActive: () => ({ getSheetByName: () => null }) },
  };

  context.global = context;
  vm.createContext(context);
  FILES.forEach((file) => vm.runInContext(fs.readFileSync(file, 'utf8'), context, { filename: file }));
  return context;
}

function main() {
  const outPath = process.argv[2] || 'docs/baselines/pipeline_log_sample_3h.json';
  const runs = Number(process.argv[3] || 36);
  const stepMinutes = Number(process.argv[4] || 5);
  const logProfile = String(process.argv[5] || 'compact').toLowerCase() === 'verbose' ? 'verbose' : 'compact';
  const logVerbosity = logProfile === 'verbose' ? 3 : 1;

  const context = loadContext();
  const logs = [];
  let nowMs = Date.parse('2026-03-01T12:00:00Z');

  context.Date.now = () => nowMs;
  context.assertPipelineRuntimeBudget_ = () => {};
  context.ensureTabsAndConfig_ = () => {};
  context.preflightConfigUniqueness_ = () => ({ ok: true });
  context.buildRunId_ = () => `sim-${nowMs}`;
  context.tryLock_ = () => true;
  context.appendLogRow_ = (entry) => logs.push(JSON.parse(JSON.stringify(entry)));
  context.logDiagnosticEvent_ = () => {};
  context.setStateValue_ = () => {};
  context.getStateJson_ = () => ({});
  context.appendRunLifecycleStatus_ = () => {};
  context.releaseRunLifecycleLease_ = () => {};
  context.acquireRunLifecycleLease_ = () => ({ acquired: true });
  context.markRunLifecycleCompleted_ = () => {};
  context.updateBootstrapEmptyCycleState_ = () => ({ reason_code: '', warning_needed: false, bootstrap_empty_cycle_count: 0, bootstrap_empty_cycle_threshold: 3 });
  context.updateEmptyProductiveOutputState_ = () => ({ reason_code: '', warning_needed: false, consecutive_count: 0, threshold: 3 });
  context.computeCreditBurnRateState_ = () => ({ status: 'ok', window: '24h', projected_daily_credits: 0 });
  context.maybeNotifyCreditBurnRate_ = () => ({ notified: false });
  context.PropertiesService.getScriptProperties = () => ({ getProperty: () => '', setProperty: () => {}, deleteProperty: () => {} });
  context.LockService.getScriptLock = () => ({ releaseLock: () => {} });
  context.localAndUtcTimestamps_ = () => ({ local: new Date(nowMs).toISOString(), utc: new Date(nowMs).toISOString() });

  context.getConfig_ = () => ({
    RUN_ENABLED: true,
    LOG_PROFILE: logProfile,
    LOG_VERBOSITY_LEVEL: logVerbosity,
    VERBOSE_LOGGING: logProfile === 'verbose',
    DUPLICATE_DEBOUNCE_MS: 0,
    PIPELINE_MAX_RUNTIME_MS: 330000,
    MODEL_MODE: 'hybrid',
    DISABLE_SOFASCORE: false,
    REQUIRE_OPENING_LINE_PROXIMITY: true,
    MAX_OPENING_LAG_MINUTES: 30,
    ALLOW_WTA_125: true,
    ALLOW_WTA_250: true,
    LOOKAHEAD_HOURS: 24,
  });

  context.resolveOddsWindowForPipeline_ = () => ({
    should_fetch_odds: true,
    decision_reason_code: 'odds_refresh_test',
    decision_message: 'window open',
    bootstrap_mode: false,
    transitioned_from_bootstrap_to_active_window: false,
    current_refresh_mode: 'active',
    previous_refresh_mode: 'active',
    bootstrap_window_hours: 0,
    bootstrap_cached_payload_has_events: false,
    bootstrap_cached_payload_source: '',
    odds_fetch_window: {},
    selected_source: 'odds_api',
  });

  context.stageFetchOdds = (runId) => ({
    events: [{ event_id: 'e1', competition: 'WTA 500', player_1: 'A', player_2: 'B', market: 'h2h', outcome: 'A', bookmaker: 'bk', price: -110, commence_time: new Date(nowMs + 3600000), odds_updated_time: new Date(nowMs) }],
    rows: [{ event_id: 'e1' }],
    summary: context.buildStageSummary_(runId, 'stageFetchOdds', nowMs - 1000, { input_count: 2, output_count: 1, provider: 'odds_api', api_credit_usage: 1, reason_codes: { odds_fetched: 1, market_h2h: 1 } }),
    selected_source: 'odds_api',
  });

  context.stageFetchSchedule = (runId) => ({
    events: [{ event_id: 's1', competition: 'WTA 500', player_1: 'A', player_2: 'B' }],
    rows: [{ event_id: 's1' }],
    summary: context.buildStageSummary_(runId, 'stageFetchSchedule', nowMs - 800, { input_count: 2, output_count: 1, provider: 'sofascore', api_credit_usage: 0, reason_codes: { schedule_fetched: 1, competition_allowed: 1 }, reason_metadata: { resolver: 'canonical' } }),
    canonicalExamples: [],
    unresolvedCompetitions: [],
    unresolvedCompetitionCounts: {},
    topUnresolvedCompetitions: [],
    allowedCount: 1,
  });

  context.stageMatchEvents = (runId) => ({
    rows: [{ key: 'm1', odds_event_id: 'e1', schedule_event_id: 's1' }],
    summary: context.buildStageSummary_(runId, 'stageMatchEvents', nowMs - 700, { input_count: 1, output_count: 1, provider: 'internal', api_credit_usage: 0, reason_codes: { matched_exact: 1, long_reason_code_key_for_schedule_enrichment_h2h_missing: 1 } }),
    matchedCount: 1,
    unmatchedCount: 0,
    rejectedCount: 0,
    diagnosticRecordsWritten: 0,
    unmatched: [],
    canonicalizationExamples: [],
  });

  context.stageFetchPlayerStats = (runId) => ({
    rows: [{ event_id: 'e1' }],
    byOddsEventId: { e1: { feature_values: {} } },
    summary: context.buildStageSummary_(runId, 'stageFetchPlayerStats', nowMs - 500, { input_count: 1, output_count: 1, provider: 'tennisabstract', api_credit_usage: 0, reason_codes: { stats_loaded: 1, provider_returned_null_features: 1, very_long_reason_code_key_player_stats_incomplete_profile: 1 }, reason_metadata: { players_with_non_null_stats: 1 } }),
  });

  context.stageGenerateSignals = (runId) => ({
    rows: [{ key: 'sig1' }],
    sentCount: 1,
    cooldownSuppressedCount: 0,
    duplicateSuppressedCount: 0,
    summary: context.buildStageSummary_(runId, 'stageGenerateSignals', nowMs - 300, { input_count: 1, output_count: 1, provider: 'internal_model', api_credit_usage: 0, reason_codes: { signals_generated: 1, signal_edge_above_threshold: 1 } }),
  });

  context.stagePersist = (runId) => ({
    summary: context.buildStageSummary_(runId, 'stagePersist', nowMs - 200, { input_count: 5, output_count: 5, provider: 'google_sheets', api_credit_usage: 0, reason_codes: { raw_odds_upserts: 1, raw_schedule_upserts: 1, raw_player_stats_upserts: 1, match_map_upserts: 1, signals_upserts: 1 } }),
  });

  for (let i = 0; i < runs; i += 1) {
    context.runEdgeBoard();
    nowMs += stepMinutes * 60 * 1000;
  }

  fs.mkdirSync(require('path').dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(logs, null, 2));
  console.log(`wrote ${logs.length} rows to ${outPath} (profile=${logProfile})`);
}

main();
