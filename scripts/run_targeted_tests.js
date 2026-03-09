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
  '08_odds_sport_key_resolver_tests.gs',
  '09_config_duplicate_tests.gs',
  '10_player_stats_h2h_parser_tests.gs',
];

const GROUPS = {
  sport_key_resolver: [
    'testResolveActiveWtaSportKeys_unknownConfiguredKey_discoversActiveFallback_',
    'testResolveActiveWtaSportKeys_returnsCatalogKeysWhenPresent_',
  ],
  freshness_timestamps: [
    'testFetchOddsWindowFromOddsApi_usesOutcomeMarketBookmakerLastUpdateWithoutEventTimestamp_',
    'testStageFetchOdds_persistsProviderAndOddsUpdatedTimeWhenEventTimestampMissing_',
  ],
  cadence_tiers: [
    'testResolveOddsWindowForPipeline_tieredCadence_mediumTierBoundaryFetchesAfterCadenceElapsed_',
    'testResolveOddsWindowForPipeline_tieredCadence_highTierBoundaryUsesHighCadence_',
    'testResolveOddsWindowForPipeline_tieredCadence_outsideWindowBehaviorUnchanged_',
  ],
  h2h_partial_coverage: [
    'testGetTaH2hCoverageForCanonicalPair_partialCoverageReason_',
  ],
  manual_pipeline_outside_window: [
    'testRunEdgeBoard_marksIdleOutsideOddsWindowForScheduleOnlyRun_',
    'testRunEdgeBoard_scheduleOnlyWatchdogNoticeIsLowSeverityExpectedIdleAndNoFailureEscalation_',
  ],
  manual_pipeline_inside_window: [
    'testRunEdgeBoard_statsStageExecutesForOddsDrivenRun_',
    'testRunEdgeBoard_degradesWhenOddsPresentButNoMatches_',
  ],
};

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
    },
    Session: { getScriptTimeZone: () => 'UTC' },
    PropertiesService: {
      getScriptProperties: () => ({
        getProperty: () => null,
        setProperty: () => {},
        deleteProperty: () => {},
      }),
    },
    LockService: {
      getScriptLock: () => ({
        waitLock: () => {},
        releaseLock: () => {},
      }),
    },
    UrlFetchApp: {
      fetch: () => {
        throw new Error('UrlFetchApp.fetch stub invoked unexpectedly in this test harness');
      },
    },

    SpreadsheetApp: {
      getActive: () => ({
        getSheetByName: () => ({ getDataRange: () => ({ getValues: () => [] }) }),
      }),
      getActiveSpreadsheet: () => ({
        getSheetByName: () => ({ getDataRange: () => ({ getValues: () => [] }) }),
      }),
    },

  };
  context.global = context;
  vm.createContext(context);
  for (const file of FILES) {
    vm.runInContext(fs.readFileSync(file, 'utf8'), context, { filename: file });
  }
  return context;
}

function runGroup(name, context) {
  const tests = GROUPS[name];
  if (!tests) {
    throw new Error(`Unknown group: ${name}`);
  }
  let passed = 0;
  const failures = [];
  for (const testName of tests) {
    try {
      if (typeof context[testName] !== 'function') {
        throw new Error('missing test function');
      }
      context[testName]();
      console.log(`PASS ${testName}`);
      passed += 1;
    } catch (error) {
      console.log(`FAIL ${testName}`);
      failures.push({ testName, error });
    }
  }
  return { name, total: tests.length, passed, failures };
}

function main() {
  const arg = process.argv[2] || 'all';
  const context = loadContext();
  const targetGroups = arg === 'all' ? Object.keys(GROUPS) : [arg];
  const results = targetGroups.map((group) => runGroup(group, context));

  console.log('\nSummary');
  let failed = 0;
  for (const result of results) {
    console.log(`- ${result.name}: ${result.passed}/${result.total} passed`);
    failed += result.failures.length;
    for (const failure of result.failures) {
      console.log(`  • ${failure.testName}: ${failure.error && failure.error.stack ? failure.error.stack : failure.error}`);
    }
  }

  if (failed > 0) {
    process.exitCode = 1;
  }
}

main();
