function testResolveActiveWtaSportKeys_returnsCatalogKeysWhenPresent_() {
  const calls = { cacheSet: null, logs: [] };
  const config = {
    ODDS_CACHE_TTL_SEC: 180,
    ODDS_SPORT_KEY: 'tennis_wta',
    ODDS_API_BASE_URL: 'https://api.the-odds-api.com/v4',
    ODDS_API_KEY: 'test',
  };

  const result = resolveActiveWtaSportKeys_(config, {
    getCachedOddsSportKeys: function () { return []; },
    setCachedOddsSportKeys: function (cacheKey, keys, ttlSec) { calls.cacheSet = { cacheKey: cacheKey, keys: keys, ttlSec: ttlSec }; },
    callOddsApi: function () {
      return {
        ok: true,
        payload: [
          { key: 'tennis_wta_us_open', active: true },
          { key: 'tennis_wta_wimbledon', active: true },
          { key: 'tennis_atp_us_open', active: true },
        ],
      };
    },
    logOddsSportKeyResolution: function (mode, keys, fallback) { calls.logs.push({ mode: mode, keys: keys, fallback: fallback }); },
  });

  assertEquals_('catalog', result.source);
  assertEquals_('none', result.fallback);
  assertArrayEquals_(['tennis_wta_us_open', 'tennis_wta_wimbledon'], result.sport_keys);
  assertEquals_('ODDS_ACTIVE_WTA_SPORT_KEYS', calls.cacheSet.cacheKey);
  assertEquals_(180, calls.cacheSet.ttlSec);
}

function testResolveActiveWtaSportKeys_fallsBackWhenAbsent_() {
  const config = {
    ODDS_CACHE_TTL_SEC: 300,
    ODDS_SPORT_KEY: '',
    ODDS_API_BASE_URL: 'https://api.the-odds-api.com/v4',
    ODDS_API_KEY: 'test',
  };

  const result = resolveActiveWtaSportKeys_(config, {
    getCachedOddsSportKeys: function () { return []; },
    setCachedOddsSportKeys: function () {},
    callOddsApi: function () {
      return {
        ok: true,
        payload: [
          { key: 'soccer_epl', active: true },
          { key: 'tennis_atp_us_open', active: true },
        ],
      };
    },
    logOddsSportKeyResolution: function () {},
  });

  assertEquals_('fallback', result.source);
  assertEquals_('none_active_wta_keys', result.fallback);
  assertArrayEquals_(['UNKNOWN_SPORT'], result.sport_keys);
}

function testResolveActiveWtaSportKeys_ignoresInactiveWtaKeys_() {
  const config = {
    ODDS_CACHE_TTL_SEC: 300,
    ODDS_SPORT_KEY: 'wta_manual_fallback',
    ODDS_API_BASE_URL: 'https://api.the-odds-api.com/v4',
    ODDS_API_KEY: 'test',
  };

  const result = resolveActiveWtaSportKeys_(config, {
    getCachedOddsSportKeys: function () { return []; },
    setCachedOddsSportKeys: function () {},
    callOddsApi: function () {
      return {
        ok: true,
        payload: [
          { key: 'tennis_wta_us_open', active: false },
          { key: 'tennis_wta_wimbledon', active: false },
        ],
      };
    },
    logOddsSportKeyResolution: function () {},
  });

  assertEquals_('fallback', result.source);
  assertEquals_('none_active_wta_keys', result.fallback);
  assertArrayEquals_(['wta_manual_fallback'], result.sport_keys);
}

function assertEquals_(expected, actual) {
  if (expected !== actual) {
    throw new Error('Assertion failed. Expected: ' + expected + ', actual: ' + actual);
  }
}

function assertArrayEquals_(expected, actual) {
  const left = JSON.stringify(expected || []);
  const right = JSON.stringify(actual || []);
  if (left !== right) {
    throw new Error('Assertion failed. Expected array: ' + left + ', actual array: ' + right);
  }
}

function testNormalizeAndDeduplicateScheduleEvents_dedupesByEventIdAndCommenceTime_() {
  const payloadLists = [
    [
      {
        id: 'evt_1',
        commence_time: '2025-01-01T12:00:00Z',
        tournament: 'WTA 500',
        home_team: 'A',
        away_team: 'B',
      },
    ],
    [
      {
        id: 'evt_1',
        commence_time: '2025-01-01T12:00:00Z',
        tournament: 'WTA Duplicate',
        home_team: 'A',
        away_team: 'B',
      },
      {
        id: 'evt_1',
        commence_time: '2025-01-01T14:00:00Z',
        tournament: 'WTA Shifted',
        home_team: 'A',
        away_team: 'B',
      },
    ],
  ];

  const events = normalizeAndDeduplicateScheduleEvents_(payloadLists);
  assertEquals_(2, events.length);

  const dedupeKeys = events
    .map(function (event) { return buildScheduleEventDedupeKey_(event); })
    .sort();
  assertArrayEquals_([
    'evt_1|1735732800000',
    'evt_1|1735740000000',
  ], dedupeKeys);
}

function testFetchScheduleFromOddsApi_marksNoActiveWtaKeysReason_() {
  const originalResolver = resolveActiveWtaSportKeys_;
  const originalCaller = callOddsApi_;

  resolveActiveWtaSportKeys_ = function () {
    return {
      sport_keys: ['UNKNOWN_SPORT'],
      source: 'fallback',
      fallback: 'none_active_wta_keys',
    };
  };

  callOddsApi_ = function () {
    return {
      ok: false,
      status_code: 404,
      payload: [],
      reason_code: 'api_http_404',
      api_credit_usage: 1,
      api_call_count: 1,
      credit_headers: { requests_last: 1 },
    };
  };

  try {
    const result = fetchScheduleFromOddsApi_({
      ODDS_API_KEY: 'test',
      ODDS_API_BASE_URL: 'https://api.the-odds-api.com/v4',
    }, {
      startIso: '2025-01-01T00:00:00Z',
      endIso: '2025-01-02T00:00:00Z',
    });

    assertEquals_('schedule_no_active_wta_keys', result.reason_code);
    assertEquals_(0, result.events.length);
  } finally {
    resolveActiveWtaSportKeys_ = originalResolver;
    callOddsApi_ = originalCaller;
  }
}

function testFetchScheduleFromOddsApi_marksActiveKeysNoEventsReason_() {
  const originalResolver = resolveActiveWtaSportKeys_;
  const originalCaller = callOddsApi_;

  resolveActiveWtaSportKeys_ = function () {
    return {
      sport_keys: ['tennis_wta_us_open'],
      source: 'catalog',
      fallback: 'none',
    };
  };

  callOddsApi_ = function () {
    return {
      ok: true,
      status_code: 200,
      payload: [],
      reason_code: 'api_success',
      api_credit_usage: 1,
      api_call_count: 1,
      credit_headers: { requests_last: 1 },
    };
  };

  try {
    const result = fetchScheduleFromOddsApi_({
      ODDS_API_KEY: 'test',
      ODDS_API_BASE_URL: 'https://api.the-odds-api.com/v4',
    }, {
      startIso: '2025-01-01T00:00:00Z',
      endIso: '2025-01-02T00:00:00Z',
    });

    assertEquals_('schedule_active_keys_no_events', result.reason_code);
    assertEquals_(0, result.events.length);
  } finally {
    resolveActiveWtaSportKeys_ = originalResolver;
    callOddsApi_ = originalCaller;
  }
}

function testSanitizeStringForLog_redactsQuerySecrets_() {
  const raw = 'https://api.the-odds-api.com/v4/sports?apiKey=abc123&regions=us&token=xyz';
  const sanitized = sanitizeStringForLog_(raw);
  assertEquals_('https://api.the-odds-api.com/v4/sports?apiKey=[REDACTED]&regions=us&token=[REDACTED]', sanitized);
}

function testSanitizeForLog_redactsSensitiveObjectFields_() {
  const payload = {
    apiKey: 'abc123',
    nested: {
      Authorization: 'Bearer topsecret',
      url: 'https://x.test/path?api_key=xyz',
    },
  };

  const sanitized = sanitizeForLog_(payload);
  assertEquals_('[REDACTED]', sanitized.apiKey);
  assertEquals_('[REDACTED]', sanitized.nested.Authorization);
  assertEquals_('https://x.test/path?api_key=[REDACTED]', sanitized.nested.url);
}


function testCallOddsApi_transportThrow_returnsNormalizedFailure_() {
  const originalFetch = UrlFetchApp.fetch;

  UrlFetchApp.fetch = function () {
    throw new Error('transport failed apiKey=secret123');
  };

  try {
    const result = callOddsApi_('https://api.the-odds-api.com/v4/sports?apiKey=secret123', { debug: true });
    assertEquals_(false, result.ok);
    assertEquals_(0, result.status_code);
    assertArrayEquals_([], result.payload);
    assertEquals_('api_transport_error', result.reason_code);
    assertEquals_(0, result.api_credit_usage);
    assertEquals_(1, result.api_call_count);
    assertArrayEquals_([], Object.keys(result.credit_headers || {}));
  } finally {
    UrlFetchApp.fetch = originalFetch;
  }
}

function testCallOddsApi_malformedJson_returnsParseFailure_() {
  const originalFetch = UrlFetchApp.fetch;

  UrlFetchApp.fetch = function () {
    return {
      getResponseCode: function () { return 200; },
      getAllHeaders: function () {
        return {
          'x-requests-used': '10',
          'x-requests-remaining': '490',
          'x-requests-last': '1',
        };
      },
      getContentText: function () { return '{bad json'; },
    };
  };

  try {
    const result = callOddsApi_('https://api.the-odds-api.com/v4/sports?apiKey=secret123', { debug: true });
    assertEquals_(false, result.ok);
    assertEquals_(0, result.status_code);
    assertArrayEquals_([], result.payload);
    assertEquals_('api_parse_error', result.reason_code);
    assertEquals_(0, result.api_credit_usage);
    assertEquals_(1, result.api_call_count);
    assertEquals_(1, result.credit_headers.requests_last);
    assertEquals_(10, result.credit_headers.requests_used);
    assertEquals_(490, result.credit_headers.requests_remaining);
  } finally {
    UrlFetchApp.fetch = originalFetch;
  }
}

function testRunEdgeBoard_debounceSkipStillWorks_() {
  const harness = createRunEdgeBoardTestHarness_({
    nowMs: 1_000_000,
    lastRunTs: 999_500,
    debounceMs: 1_000,
  });

  try {
    runEdgeBoard();
    assertEquals_(1, harness.duplicatePreventedCount);
    assertEquals_(0, harness.setPropertyCalls.length);
    assertEquals_(1, harness.releaseLockCalls);
    assertEquals_('run_debounced_skip', harness.summaryReasonCode());
  } finally {
    harness.restore();
  }
}

function testRunEdgeBoard_crashDoesNotSetDebounceTimestamp_() {
  const harness = createRunEdgeBoardTestHarness_({
    nowMs: 1_000_000,
    lastRunTs: 0,
    debounceMs: 1_000,
    throwDuringOrchestration: true,
  });

  try {
    let threw = false;
    try {
      runEdgeBoard();
    } catch (error) {
      threw = true;
      assertEquals_('stage crash', String(error && error.message ? error.message : error));
    }
    assertEquals_(true, threw);
    assertEquals_(0, harness.setPropertyCalls.length);
    assertEquals_(1, harness.releaseLockCalls);
    assertEquals_('run_exception', harness.summaryReasonCode());
  } finally {
    harness.restore();
  }
}

function createRunEdgeBoardTestHarness_(options) {
  const opts = options || {};
  const originalDateNow = Date.now;
  const originalEnsureTabsAndConfig = ensureTabsAndConfig_;
  const originalBuildRunId = buildRunId_;
  const originalTryLock = tryLock_;
  const originalGetConfig = getConfig_;
  const originalAppendLogRow = appendLogRow_;
  const originalIncrementDuplicatePreventedCount = incrementDuplicatePreventedCount_;
  const originalResolveOddsWindowForPipeline = resolveOddsWindowForPipeline_;
  const originalSetStateValue = setStateValue_;
  const originalGetScriptProperties = PropertiesService.getScriptProperties;
  const originalGetScriptLock = LockService.getScriptLock;

  const logs = [];
  const setPropertyCalls = [];
  let duplicatePreventedCount = 0;
  let releaseLockCalls = 0;

  Date.now = function () { return Number(opts.nowMs || 0); };

  ensureTabsAndConfig_ = function () {};
  buildRunId_ = function () { return 'test-run'; };
  tryLock_ = function () { return true; };
  getConfig_ = function () {
    return {
      RUN_ENABLED: true,
      DUPLICATE_DEBOUNCE_MS: Number(opts.debounceMs || 0),
    };
  };
  appendLogRow_ = function (row) { logs.push(row); };
  incrementDuplicatePreventedCount_ = function () {
    duplicatePreventedCount += 1;
    return duplicatePreventedCount;
  };
  resolveOddsWindowForPipeline_ = function () {
    if (opts.throwDuringOrchestration) throw new Error('stage crash');
    throw new Error('unexpected test path: orchestration should not execute in this harness');
  };
  setStateValue_ = function () {};

  PropertiesService.getScriptProperties = function () {
    return {
      getProperty: function (key) {
        if (key === PROPS.LAST_PIPELINE_RUN_TS) return String(Number(opts.lastRunTs || 0));
        return '';
      },
      setProperty: function (key, value) {
        setPropertyCalls.push({ key: key, value: value });
      },
    };
  };

  LockService.getScriptLock = function () {
    return {
      releaseLock: function () { releaseLockCalls += 1; },
    };
  };

  return {
    setPropertyCalls: setPropertyCalls,
    logs: logs,
    restore: function () {
      Date.now = originalDateNow;
      ensureTabsAndConfig_ = originalEnsureTabsAndConfig;
      buildRunId_ = originalBuildRunId;
      tryLock_ = originalTryLock;
      getConfig_ = originalGetConfig;
      appendLogRow_ = originalAppendLogRow;
      incrementDuplicatePreventedCount_ = originalIncrementDuplicatePreventedCount;
      resolveOddsWindowForPipeline_ = originalResolveOddsWindowForPipeline;
      setStateValue_ = originalSetStateValue;
      PropertiesService.getScriptProperties = originalGetScriptProperties;
      LockService.getScriptLock = originalGetScriptLock;
    },
    summaryReasonCode: function () {
      for (let i = logs.length - 1; i >= 0; i -= 1) {
        if (logs[i].row_type === 'summary' && logs[i].stage === 'runEdgeBoard') {
          return logs[i].reason_code || '';
        }
      }
      return '';
    },
    get duplicatePreventedCount() { return duplicatePreventedCount; },
    get releaseLockCalls() { return releaseLockCalls; },
  };
}

function testStageGenerateSignals_notifyDisabledDoesNotMarkSentHash_() {
  const originalDateNow = Date.now;
  const originalGetSignalState = getSignalState_;
  const originalSetSignalState = setSignalState_;
  const originalAppendLogRow = appendLogRow_;
  const originalSetStateValue = setStateValue_;
  const originalLocalAndUtcTimestamps = localAndUtcTimestamps_;

  const nowMs = Date.parse('2026-01-01T00:00:00.000Z');
  const captured = {
    state: null,
    logs: [],
  };

  Date.now = function () { return nowMs; };
  getSignalState_ = function () { return { sent_hashes: {} }; };
  setSignalState_ = function (state) { captured.state = JSON.parse(JSON.stringify(state || {})); };
  appendLogRow_ = function (entry) { captured.logs.push(entry); };
  setStateValue_ = function () {};
  localAndUtcTimestamps_ = function () {
    return {
      local: '2026-01-01T00:00:00-07:00',
      utc: '2026-01-01T07:00:00.000Z',
    };
  };

  try {
    const event = {
      event_id: 'evt_notify_disabled',
      market: 'h2h',
      outcome: 'player_a',
      bookmaker: 'book_x',
      price: 150,
      commence_time: new Date(nowMs + (5 * 60 * 60 * 1000)),
      odds_updated_time: new Date(nowMs),
    };
    const match = {
      odds_event_id: 'evt_notify_disabled',
      schedule_event_id: 'sched_1',
      competition_tier: 'WTA_500',
    };
    const config = {
      MODEL_VERSION: 'test_model_v1',
      EDGE_THRESHOLD_MICRO: 0.001,
      EDGE_THRESHOLD_SMALL: 0.03,
      EDGE_THRESHOLD_MED: 0.05,
      EDGE_THRESHOLD_STRONG: 0.08,
      STAKE_UNITS_MICRO: 0.25,
      STAKE_UNITS_SMALL: 0.5,
      STAKE_UNITS_MED: 1,
      STAKE_UNITS_STRONG: 1.5,
      SIGNAL_COOLDOWN_MIN: 180,
      MINUTES_BEFORE_START_CUTOFF: 60,
      STALE_ODDS_WINDOW_MIN: 60,
      NOTIFY_ENABLED: false,
      NOTIFY_TEST_MODE: false,
      DISCORD_WEBHOOK: '',
    };

    const result = stageGenerateSignals('run_notify_disabled', config, [event], [match], {});

    assertEquals_(1, result.rows.length);
    assertEquals_('notify_disabled', result.rows[0].notification_outcome);
    assertEquals_(0, Object.keys((captured.state && captured.state.sent_hashes) || {}).length);
    assertEquals_(0, result.sentCount);

    const notifyLog = captured.logs.filter(function (entry) {
      return entry.stage === 'signalNotifyDelivery';
    })[0] || null;

    assertEquals_(true, !!notifyLog);
    assertEquals_('notify_disabled', notifyLog.reason_code);
  } finally {
    Date.now = originalDateNow;
    getSignalState_ = originalGetSignalState;
    setSignalState_ = originalSetSignalState;
    appendLogRow_ = originalAppendLogRow;
    setStateValue_ = originalSetStateValue;
    localAndUtcTimestamps_ = originalLocalAndUtcTimestamps;
  }
}
