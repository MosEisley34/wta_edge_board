function testResolveActiveWtaSportKeys_returnsCatalogKeysWhenPresent_() {
  const calls = { cacheSet: null, logs: [] };
  const config = {
    ODDS_CACHE_TTL_SEC: 180,
    ODDS_SPORT_KEY: 'tennis_wta_us_open',
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

function testResolveActiveWtaSportKeys_unknownConfiguredKey_discoversActiveFallback_() {
  const calls = { logs: [] };
  const config = {
    ODDS_CACHE_TTL_SEC: 180,
    ODDS_SPORT_KEY: 'tennis_wta_unknown',
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
          { key: 'tennis_wta_indian_wells', active: true },
          { key: 'tennis_wta_wimbledon', active: true },
        ],
      };
    },
    logOddsSportKeyResolution: function (mode, keys, fallback, extra) {
      calls.logs.push({ mode: mode, keys: keys, fallback: fallback, extra: extra || {} });
    },
  });

  assertEquals_('catalog', result.source);
  assertEquals_('unknown_sport_fallback_resolved', result.fallback);
  assertArrayEquals_(['tennis_wta_indian_wells', 'tennis_wta_wimbledon'], result.sport_keys);
  assertEquals_('selected_from_catalog', calls.logs[0].mode);
  assertEquals_('unknown_sport_fallback_resolved', calls.logs[0].fallback);
  assertEquals_(2, Number(calls.logs[0].extra.selected_sport_key_count || calls.logs[0].keys.length));
}


function testResolveActiveWtaSportKeys_catalogFetchFailed_usesCachedWhenConfiguredUnknown_() {
  const calls = { logs: [] };
  const config = {
    ODDS_CACHE_TTL_SEC: 180,
    ODDS_SPORT_KEY: 'tennis_wta_unknown',
    ODDS_API_BASE_URL: 'https://api.the-odds-api.com/v4',
    ODDS_API_KEY: 'test',
  };

  const result = resolveActiveWtaSportKeys_(config, {
    getCachedOddsSportKeys: function () { return ['tennis_wta_us_open', 'tennis_wta_wimbledon']; },
    setCachedOddsSportKeys: function () {},
    callOddsApi: function () { return { ok: false }; },
    logOddsSportKeyResolution: function (mode, keys, fallback, extra) {
      calls.logs.push({ mode: mode, keys: keys, fallback: fallback, extra: extra || {} });
    },
  });

  assertEquals_('cache', result.source);
  assertEquals_('catalog_fetch_failed_using_cached', result.fallback);
  assertArrayEquals_(['tennis_wta_us_open', 'tennis_wta_wimbledon'], result.sport_keys);
  assertEquals_('catalog_fetch_failed_using_cached', calls.logs[0].mode);
  assertArrayEquals_(['tennis_wta_unknown'], calls.logs[0].extra.unknown_configured_sport_keys);
}

function testResolveActiveWtaSportKeys_catalogFetchFailed_withoutCache_returnsEmpty_() {
  const calls = { logs: [] };
  const config = {
    ODDS_CACHE_TTL_SEC: 180,
    ODDS_SPORT_KEY: 'tennis_wta_unknown',
    ODDS_API_BASE_URL: 'https://api.the-odds-api.com/v4',
    ODDS_API_KEY: 'test',
  };

  const result = resolveActiveWtaSportKeys_(config, {
    getCachedOddsSportKeys: function () { return []; },
    setCachedOddsSportKeys: function () {},
    callOddsApi: function () { return { ok: false }; },
    logOddsSportKeyResolution: function (mode, keys, fallback, extra) {
      calls.logs.push({ mode: mode, keys: keys, fallback: fallback, extra: extra || {} });
    },
  });

  assertEquals_('fallback', result.source);
  assertEquals_('catalog_fetch_failed', result.fallback);
  assertArrayEquals_([], result.sport_keys);
  assertEquals_('catalog_fetch_failed', calls.logs[0].mode);
  assertArrayEquals_(['tennis_wta_unknown'], calls.logs[0].extra.unknown_configured_sport_keys);
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
  assertArrayEquals_([], result.sport_keys);
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
  assertArrayEquals_([], result.sport_keys);
}


function testFetchOddsWindowFromOddsApi_invalidWindow_recoversWithFallbackWindow_() {
  const originalCaller = callOddsApiWithSportKeyFallback_;
  const originalNow = Date.now;
  const calls = [];

  Date.now = function () { return 1735689600123; };
  callOddsApiWithSportKeyFallback_ = function (config, opts) {
    calls.push(opts);
    if (calls.length === 1) {
      return {
        ok: false,
        reason_code: 'invalid_time_window',
        api_credit_usage: 1,
        api_call_count: 2,
        credit_headers: {},
      };
    }
    return {
      ok: true,
      status_code: 200,
      payload: [{
        id: 'evt_1',
        commence_time: '2025-01-01T03:00:00Z',
        bookmakers: [{
          key: 'book_a',
          markets: [{
            key: 'h2h',
            outcomes: [
              { name: 'Player A', price: 1.9 },
              { name: 'Player B', price: 2.0 },
            ],
          }],
        }],
      }],
      reason_code: 'api_success',
      api_credit_usage: 3,
      api_call_count: 4,
      credit_headers: { requests_last: 4 },
      selected_sport_keys: ['tennis_wta_us_open', 'tennis_wta_wimbledon'],
      selected_sport_key_count: 2,
      selected_sport_key_source: 'catalog',
      selected_sport_key_fallback: 'none',
      window_request_start_ms: 0,
      window_request_end_ms: 0,
    };
  };

  try {
    const result = fetchOddsWindowFromOddsApi_({
      ODDS_API_KEY: 'test',
      ODDS_REGIONS: 'us',
      ODDS_MARKETS: 'h2h',
      ODDS_ODDS_FORMAT: 'decimal',
      ODDS_BOOTSTRAP_LOOKAHEAD_HOURS: 1,
    }, 1735689601123, 1735689602123);

    assertEquals_('invalid_time_window_recovered', result.reason_code);
    assertEquals_(2, calls.length);
    assertEquals_(2, result.events.length);
    assertEquals_(4, result.api_credit_usage);
    assertEquals_(6, result.api_call_count);
    assertEquals_('2025-01-01T00:00:00.000Z', calls[1].query.commenceTimeFrom);
    assertEquals_('2025-01-01T06:00:00.000Z', calls[1].query.commenceTimeTo);
    assertEquals_(1735689600000, result.window_request_start_ms);
    assertEquals_(1735711200000, result.window_request_end_ms);
  } finally {
    callOddsApiWithSportKeyFallback_ = originalCaller;
    Date.now = originalNow;
  }
}

function testFetchOddsWindowFromOddsApi_usesOutcomeMarketBookmakerLastUpdateWithoutEventTimestamp_() {
  const originalCaller = callOddsApiWithSportKeyFallback_;

  callOddsApiWithSportKeyFallback_ = function () {
    return {
      ok: true,
      status_code: 200,
      payload: [{
        id: 'evt_timestamp_fallback',
        commence_time: '2025-01-01T03:00:00.000Z',
        bookmakers: [{
          key: 'book_a',
          last_update: '2025-01-01T00:06:00.000Z',
          markets: [{
            key: 'h2h',
            last_update: '2025-01-01T00:05:00.000Z',
            outcomes: [
              { name: 'Player A', price: 2.1, last_update: '2025-01-01T00:04:00.000Z' },
              { name: 'Player B', price: 1.8 },
            ],
          }],
        }],
      }],
      reason_code: 'api_success',
      api_credit_usage: 1,
      api_call_count: 1,
      credit_headers: {},
      selected_sport_keys: ['tennis_wta_us_open'],
      selected_sport_key_count: 1,
      selected_sport_key_source: 'catalog',
      selected_sport_key_fallback: 'none',
      window_request_start_ms: 0,
      window_request_end_ms: 0,
    };
  };

  try {
    const result = fetchOddsWindowFromOddsApi_({
      ODDS_API_KEY: 'test',
      ODDS_REGIONS: 'us',
      ODDS_MARKETS: 'h2h',
      ODDS_ODDS_FORMAT: 'decimal',
      ODDS_BOOTSTRAP_LOOKAHEAD_HOURS: 1,
    }, Date.parse('2025-01-01T00:00:00.000Z'), Date.parse('2025-01-01T06:00:00.000Z'));

    assertEquals_(2, result.events.length);
    assertEquals_('2025-01-01T00:04:00.000Z', result.events[0].provider_odds_updated_time.toISOString());
    assertEquals_('2025-01-01T00:04:00.000Z', result.events[0].odds_updated_time.toISOString());
    assertEquals_('2025-01-01T00:05:00.000Z', result.events[1].provider_odds_updated_time.toISOString());
    assertEquals_('2025-01-01T00:05:00.000Z', result.events[1].odds_updated_time.toISOString());
  } finally {
    callOddsApiWithSportKeyFallback_ = originalCaller;
  }
}

function testStageFetchOdds_persistsProviderAndOddsUpdatedTimeWhenEventTimestampMissing_() {
  const originalFetchOdds = fetchOddsWindowFromOddsApi_;
  const originalGetCachedPayload = getCachedPayload_;
  const originalGetCreditAwareRuntimeConfig = getCreditAwareRuntimeConfig_;
  const originalSetCachedPayload = setCachedPayload_;
  const originalSetStateValue = setStateValue_;
  const originalGetStateJson = getStateJson_;
  const originalUpdateCreditState = updateCreditStateFromHeaders_;
  const originalLocalAndUtcTimestamps = localAndUtcTimestamps_;
  const originalLogDiagnosticEvent = logDiagnosticEvent_;

  const writes = {};

  fetchOddsWindowFromOddsApi_ = function () {
    const providerTime = new Date('2025-01-01T00:05:00.000Z');
    const ingestionTime = new Date('2025-01-01T00:07:00.000Z');
    return {
      events: [{
        event_id: 'evt_stage_fetch_odds',
        bookmaker: 'book_a',
        bookmaker_keys_considered: ['book_a'],
        market: 'h2h',
        outcome: 'Player A',
        price: 2.1,
        provider_odds_updated_time: providerTime,
        ingestion_timestamp: ingestionTime,
        odds_updated_time: providerTime,
        commence_time: new Date('2025-01-01T03:00:00.000Z'),
        competition: 'WTA Test',
        tournament: 'WTA Test',
        event_name: 'A vs B',
        sport_title: 'Tennis',
        home_team: 'Player A',
        away_team: 'Player B',
        player_1: 'Player A',
        player_2: 'Player B',
      }],
      reason_code: 'odds_api_success',
      api_credit_usage: 1,
      api_call_count: 1,
      credit_headers: {},
      selected_source: 'fresh_api',
      window_meta: {
        cached_at_ms: Date.parse('2025-01-01T00:08:00.000Z'),
      },
    };
  };
  getCachedPayload_ = function () { return null; };
  getCreditAwareRuntimeConfig_ = function () { return { odds_window_cache_ttl_min: 10, odds_window_refresh_min: 1, mode: 'normal' }; };
  setCachedPayload_ = function () {};
  setStateValue_ = function (key, value) { writes[key] = value; };
  getStateJson_ = function () { return null; };
  updateCreditStateFromHeaders_ = function () { return { header_present: false }; };
  localAndUtcTimestamps_ = function () { return { local: '2025-01-01T00:00:00-07:00', utc: '2025-01-01T07:00:00.000Z' }; };
  logDiagnosticEvent_ = function () {};

  try {
    const result = stageFetchOdds('run_odds_persist_time', {
      LOOKAHEAD_HOURS: 6,
      ODDS_WINDOW_FORCE_REFRESH: true,
    }, {
      startMs: Date.parse('2025-01-01T00:00:00.000Z'),
      endMs: Date.parse('2025-01-01T06:00:00.000Z'),
    });

    assertEquals_(1, result.rows.length);
    assertEquals_('2025-01-01T00:05:00.000Z', result.rows[0].provider_odds_updated_time);
    assertEquals_('2025-01-01T00:05:00.000Z', result.rows[0].odds_updated_time);

    const stalePayload = JSON.parse(writes.ODDS_WINDOW_STALE_PAYLOAD || '{}');
    assertEquals_('2025-01-01T00:05:00.000Z', stalePayload.events[0].provider_odds_updated_time || '');
    assertEquals_('2025-01-01T00:05:00.000Z', stalePayload.events[0].odds_updated_time || '');
  } finally {
    fetchOddsWindowFromOddsApi_ = originalFetchOdds;
    getCachedPayload_ = originalGetCachedPayload;
    getCreditAwareRuntimeConfig_ = originalGetCreditAwareRuntimeConfig;
    setCachedPayload_ = originalSetCachedPayload;
    setStateValue_ = originalSetStateValue;
    getStateJson_ = originalGetStateJson;
    updateCreditStateFromHeaders_ = originalUpdateCreditState;
    localAndUtcTimestamps_ = originalLocalAndUtcTimestamps;
    logDiagnosticEvent_ = originalLogDiagnosticEvent;
  }
}

function testFetchOddsWindowFromOddsApi_invalidWindow_retryFailure_() {
  const originalCaller = callOddsApiWithSportKeyFallback_;
  const originalNow = Date.now;
  const calls = [];

  Date.now = function () { return 1735689600123; };
  callOddsApiWithSportKeyFallback_ = function (config, opts) {
    calls.push(opts);
    if (calls.length === 1) {
      return {
        ok: false,
        reason_code: 'invalid_time_window',
        api_credit_usage: 1,
        api_call_count: 2,
        credit_headers: {},
      };
    }
    return {
      ok: false,
      reason_code: 'api_http_422',
      detail_code: 'commence_time_window_invalid',
      api_credit_usage: 3,
      api_call_count: 4,
      credit_headers: { requests_last: 4 },
    };
  };

  try {
    const result = fetchOddsWindowFromOddsApi_({
      ODDS_API_KEY: 'test',
      ODDS_REGIONS: 'us',
      ODDS_MARKETS: 'h2h',
      ODDS_ODDS_FORMAT: 'decimal',
      ODDS_BOOTSTRAP_LOOKAHEAD_HOURS: 1,
    }, 1735689601123, 1735689602123);

    assertEquals_('invalid_time_window_retry_failed', result.reason_code);
    assertEquals_(2, calls.length);
    assertEquals_(0, result.events.length);
    assertEquals_(4, result.credit_headers.requests_last);
  } finally {
    callOddsApiWithSportKeyFallback_ = originalCaller;
    Date.now = originalNow;
  }
}

function testFetchOddsWindowFromOddsApi_invalidWindow_onlyRetriesOnce_() {
  const originalCaller = callOddsApiWithSportKeyFallback_;
  const originalNow = Date.now;
  let callCount = 0;

  Date.now = function () { return 1735689600123; };
  callOddsApiWithSportKeyFallback_ = function () {
    callCount += 1;
    return {
      ok: false,
      reason_code: 'invalid_time_window',
      api_credit_usage: 1,
      api_call_count: 1,
      credit_headers: {},
    };
  };

  try {
    const result = fetchOddsWindowFromOddsApi_({
      ODDS_API_KEY: 'test',
      ODDS_REGIONS: 'us',
      ODDS_MARKETS: 'h2h',
      ODDS_ODDS_FORMAT: 'decimal',
      ODDS_BOOTSTRAP_LOOKAHEAD_HOURS: 24,
    }, 1735689601123, 1735689602123);

    assertEquals_('invalid_time_window_retry_failed', result.reason_code);
    assertEquals_(2, callCount);
  } finally {
    callOddsApiWithSportKeyFallback_ = originalCaller;
    Date.now = originalNow;
  }
}

function testFetchOddsWindowFromOddsApi_invalidWindow_recoversWithRelaxedQuery_() {
  const originalCaller = callOddsApiWithSportKeyFallback_;
  const originalNow = Date.now;
  const calls = [];

  Date.now = function () { return 1735689600123; };
  callOddsApiWithSportKeyFallback_ = function (config, opts) {
    calls.push(opts);
    if (calls.length < 3) {
      return {
        ok: false,
        reason_code: 'invalid_time_window',
        api_credit_usage: 1,
        api_call_count: 1,
        credit_headers: {},
      };
    }
    return {
      ok: true,
      status_code: 200,
      payload: [{ id: 'evt_1', commence_time: '2025-01-01T03:00:00Z', bookmakers: [] }],
      api_credit_usage: 1,
      api_call_count: 1,
      credit_headers: { requests_last: 1 },
      selected_sport_keys: ['tennis_wta_us_open'],
      selected_sport_key_count: 1,
      selected_sport_key_source: 'cache',
      selected_sport_key_fallback: 'none',
    };
  };

  try {
    const result = fetchOddsWindowFromOddsApi_({
      ODDS_API_KEY: 'test',
      ODDS_REGIONS: 'us',
      ODDS_MARKETS: 'h2h',
      ODDS_ODDS_FORMAT: 'decimal',
      ODDS_BOOTSTRAP_LOOKAHEAD_HOURS: 18,
    }, 1735689601123, 1735689602123);

    assertEquals_('invalid_time_window_recovered_relaxed_query', result.reason_code);
    assertEquals_(3, calls.length);
  } finally {
    callOddsApiWithSportKeyFallback_ = originalCaller;
    Date.now = originalNow;
  }
}


function testBuildOddsApiOddsQuery_normalizesToSecondAndMinWindow_() {
  const result = buildOddsApiOddsQuery_({
    ODDS_REGIONS: 'us',
    ODDS_MARKETS: 'h2h',
    ODDS_ODDS_FORMAT: 'decimal',
  }, 1735689600123, 1735689600456);

  assertEquals_(true, result.ok);
  assertEquals_('2025-01-01T00:00:00.000Z', result.query.commenceTimeFrom);
  assertEquals_('2025-01-01T00:01:00.000Z', result.query.commenceTimeTo);
  assertEquals_(1735689600000, result.window_start_ms);
  assertEquals_(1735689660000, result.window_end_ms);
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

function assertTrue_(condition, message) {
  if (!condition) {
    throw new Error('Assertion failed. ' + String(message || 'expected true'));
  }
}


function assertContains_(haystack, needle) {
  if (String(haystack || '').indexOf(String(needle || '')) < 0) {
    throw new Error('Assertion failed. Expected text to include "' + needle + '" but was: ' + haystack);
  }
}

function assertDoesNotContain_(haystack, needle) {
  if (String(haystack || '').indexOf(String(needle || '')) >= 0) {
    throw new Error('Assertion failed. Expected text to exclude "' + needle + '" but was: ' + haystack);
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
      sport_keys: [],
      source: 'fallback',
      fallback: 'none_active_wta_keys',
    };
  };

  let callCount = 0;
  callOddsApi_ = function () { callCount += 1; throw new Error('should not be called'); };

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
    assertEquals_(0, callCount);
  } finally {
    resolveActiveWtaSportKeys_ = originalResolver;
    callOddsApi_ = originalCaller;
  }
}

function testFetchScheduleFromOddsApi_unknownConfiguredKey_usesDiscoveredFallback_() {
  const originalResolver = resolveActiveWtaSportKeys_;
  const originalCall = callOddsApi_;
  const calls = [];

  resolveActiveWtaSportKeys_ = function () {
    return {
      sport_keys: ['tennis_wta_indian_wells', 'tennis_wta_wimbledon'],
      source: 'catalog',
      fallback: 'unknown_sport_fallback_resolved',
    };
  };
  callOddsApi_ = function (url) {
    calls.push(String(url || ''));
    return {
      ok: true,
      status_code: 200,
      payload: [{
        id: 'evt_sched_1',
        commence_time: '2025-01-02T00:00:00Z',
        home_team: 'Player A',
        away_team: 'Player B',
        tournament: 'Indian Wells',
      }],
      reason_code: 'api_success',
      api_credit_usage: 1,
      api_call_count: 1,
      credit_headers: {},
    };
  };

  try {
    const result = fetchScheduleFromOddsApi_({
      ODDS_API_KEY: 'test',
      ODDS_API_BASE_URL: 'https://api.the-odds-api.com/v4',
      ODDS_SPORT_KEY: 'tennis_wta_unknown',
    }, {
      startIso: '2025-01-01T00:00:00.000Z',
      endIso: '2025-01-03T00:00:00.000Z',
    });

    assertEquals_('schedule_api_success_sport_key_fallback', result.reason_code);
    assertArrayEquals_(['tennis_wta_indian_wells', 'tennis_wta_wimbledon'], result.resolved_sport_keys);
    assertEquals_(2, calls.length);
  } finally {
    resolveActiveWtaSportKeys_ = originalResolver;
    callOddsApi_ = originalCall;
  }
}

function testCallOddsApiWithSportKeyFallback_noActiveWtaKeys_returnsDedicatedReason_() {
  const originalResolver = resolveActiveWtaSportKeys_;
  const originalCaller = callOddsApi_;

  resolveActiveWtaSportKeys_ = function () {
    return {
      sport_keys: [],
      source: 'fallback',
      fallback: 'none_active_wta_keys',
    };
  };

  callOddsApi_ = function () { throw new Error('should not be called'); };

  try {
    const result = callOddsApiWithSportKeyFallback_({
      ODDS_API_KEY: 'test',
      ODDS_API_BASE_URL: 'https://api.the-odds-api.com/v4',
    }, {
      endpoint: 'odds',
      query: {
        regions: 'us',
      },
    });

    assertEquals_(false, result.ok);
    assertEquals_('odds_no_active_wta_keys', result.reason_code);
    assertEquals_(0, result.api_call_count);
    assertArrayEquals_([], result.selected_sport_keys);
  } finally {
    resolveActiveWtaSportKeys_ = originalResolver;
    callOddsApi_ = originalCaller;
  }
}

function testBuildOddsApiRequestPathForLog_stripsOriginAndSecrets_() {
  const path = buildOddsApiRequestPathForLog_('https://api.the-odds-api.com/v4/sports/tennis_wta_indian_wells/events?apiKey=abc123&regions=us');
  assertDoesNotContain_(path, 'abc123');
  assertEquals_('/v4/sports/tennis_wta_indian_wells/events?apiKey=ab***23&regions=us', path);
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

    assertEquals_('schedule_no_games_in_window', result.reason_code);
    assertEquals_(0, result.events.length);
  } finally {
    resolveActiveWtaSportKeys_ = originalResolver;
    callOddsApi_ = originalCaller;
  }
}

function testComputeCanonicalTimeWindow_referenceClockHandlesUtcBoundary_() {
  const referenceMs = Date.parse('2025-03-09T07:55:00.000Z');
  const window = computeCanonicalTimeWindow_({
    reference_ms: referenceMs,
    lookahead_hours: 4,
    buffer_before_min: 90,
    buffer_after_min: 45,
  });

  assertEquals_('2025-03-09T06:25:00.000Z', window.start_iso);
  assertEquals_('2025-03-09T12:40:00.000Z', window.end_iso);
  assertEquals_('reference_clock', window.source);
}

function testComputeCanonicalTimeWindow_eventTimesHandlesLocalOffsetInputs_() {
  const eventTimes = [
    new Date('2025-03-08T23:50:00-08:00').getTime(),
    new Date('2025-03-09T01:15:00-08:00').getTime(),
  ];
  const window = computeCanonicalTimeWindow_({
    reference_ms: Date.parse('2025-03-09T08:00:00.000Z'),
    event_times_ms: eventTimes,
    buffer_before_min: 30,
    buffer_after_min: 60,
  });

  assertEquals_('2025-03-09T07:20:00.000Z', window.start_iso);
  assertEquals_('2025-03-09T10:15:00.000Z', window.end_iso);
  assertEquals_('event_times', window.source);
}

function testSanitizeStringForLog_redactsQuerySecrets_() {
  const raw = 'https://api.the-odds-api.com/v4/sports?apiKey=abc123&regions=us&token=xyz';
  const sanitized = sanitizeStringForLog_(raw);
  assertDoesNotContain_(sanitized, 'abc123');
  assertDoesNotContain_(sanitized, 'token=xyz');
  assertTrue_(sanitized.indexOf('apiKey=ab***23') >= 0, 'apiKey should be masked in prefix/suffix form');
  assertTrue_(sanitized.indexOf('token=x***z') >= 0, 'token should be masked in prefix/suffix form');
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
  assertDoesNotContain_(sanitized.apiKey, 'abc123');
  assertDoesNotContain_(sanitized.nested.Authorization, 'topsecret');
  assertEquals_('ab***23', sanitized.apiKey);
  assertEquals_('Bear***ret', sanitized.nested.Authorization);
  assertEquals_('https://x.test/path?api_key=x***z', sanitized.nested.url);
}

function testSetStateValue_masksVerboseJsonSecretsBeforePersist_() {
  const originalUpsertSheetRows = upsertSheetRows_;
  const captured = { row: null };

  upsertSheetRows_ = function (sheetName, headers, rows) {
    if (sheetName === SHEETS.STATE) captured.row = rows[0];
  };

  try {
    const rawOddsKey = 'odds_secret_12345';
    const rawWebhook = 'https://discord.com/api/webhooks/123456789012345678/AAABBBCCCDDDEEEFFF';
    setStateValue_('LAST_RUN_VERBOSE_JSON', JSON.stringify({
      config_snapshot: {
        ODDS_API_KEY: rawOddsKey,
        DISCORD_WEBHOOK: rawWebhook,
      },
      exception: 'transport failed token=tok_super_secret',
    }));

    const stored = String((captured.row && captured.row.value) || '');
    assertDoesNotContain_(stored, rawOddsKey);
    assertDoesNotContain_(stored, rawWebhook);
    assertDoesNotContain_(stored, 'tok_super_secret');
    assertTrue_(stored.indexOf('odds***345') >= 0, 'ODDS_API_KEY should be persisted as masked');
    assertTrue_(stored.indexOf('http***FFF') >= 0, 'DISCORD_WEBHOOK should be persisted as masked');
    assertTrue_(stored.indexOf('token=tok_***ret') >= 0, 'token query payload should be masked');
  } finally {
    upsertSheetRows_ = originalUpsertSheetRows;
  }
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


function testCallOddsApi_mixedCaseHeaders_areNormalized_() {
  const originalFetch = UrlFetchApp.fetch;

  UrlFetchApp.fetch = function () {
    return {
      getResponseCode: function () { return 200; },
      getAllHeaders: function () {
        return {
          'X-Requests-Used': 21,
          'x-REQUESTS-remaining': '479',
          'X-REQUESTS-LAST': '2',
        };
      },
      getContentText: function () { return '[]'; },
    };
  };

  try {
    const result = callOddsApi_('https://api.the-odds-api.com/v4/sports?apiKey=secret123', { debug: true });
    assertEquals_(true, result.ok);
    assertEquals_('api_ok', result.reason_code);
    assertEquals_(21, result.credit_headers.requests_used);
    assertEquals_(479, result.credit_headers.requests_remaining);
    assertEquals_(2, result.credit_headers.requests_last);
    assertEquals_(true, result.credit_headers.has_credit_headers);
    assertEquals_(2, result.api_credit_usage);
  } finally {
    UrlFetchApp.fetch = originalFetch;
  }
}

function testCallOddsApi_missingCreditHeaders_marksCreditHeaderMissing_() {
  const originalFetch = UrlFetchApp.fetch;

  UrlFetchApp.fetch = function () {
    return {
      getResponseCode: function () { return 200; },
      getAllHeaders: function () { return { 'content-type': 'application/json' }; },
      getContentText: function () { return '[]'; },
    };
  };

  try {
    const result = callOddsApi_('https://api.the-odds-api.com/v4/sports?apiKey=secret123', { debug: true });
    assertEquals_(true, result.ok);
    assertEquals_('credit_header_missing', result.reason_code);
    assertEquals_(null, result.credit_headers.requests_used);
    assertEquals_(null, result.credit_headers.requests_remaining);
    assertEquals_(null, result.credit_headers.requests_last);
    assertEquals_(false, result.credit_headers.has_credit_headers);
  } finally {
    UrlFetchApp.fetch = originalFetch;
  }
}

function testCallOddsApi_objectPayloadWithListLikeField_isNormalized_() {
  const originalFetch = UrlFetchApp.fetch;

  UrlFetchApp.fetch = function () {
    return {
      getResponseCode: function () { return 200; },
      getAllHeaders: function () {
        return {
          'x-requests-used': '11',
          'x-requests-remaining': '489',
          'x-requests-last': '1',
        };
      },
      getContentText: function () {
        return JSON.stringify({
          events: [
            { id: 'evt_obj_1', commence_time: '2025-01-01T12:00:00Z' },
          ],
        });
      },
    };
  };

  try {
    const result = callOddsApi_('https://api.the-odds-api.com/v4/sports?apiKey=secret123', { debug: true });
    assertEquals_(true, result.ok);
    assertEquals_(200, result.status_code);
    assertEquals_(1, result.payload.length);
    assertEquals_('evt_obj_1', result.payload[0].id);
    assertEquals_('api_ok', result.reason_code);
  } finally {
    UrlFetchApp.fetch = originalFetch;
  }
}

function testCallOddsApi_stringPayload_returnsUnexpectedShapeFailure_() {
  const originalFetch = UrlFetchApp.fetch;

  UrlFetchApp.fetch = function () {
    return {
      getResponseCode: function () { return 200; },
      getAllHeaders: function () {
        return {
          'x-requests-used': '12',
          'x-requests-remaining': '488',
          'x-requests-last': '1',
        };
      },
      getContentText: function () { return JSON.stringify('not_an_array'); },
    };
  };

  try {
    const result = callOddsApi_('https://api.the-odds-api.com/v4/sports?apiKey=secret123', { debug: true });
    assertEquals_(false, result.ok);
    assertEquals_(200, result.status_code);
    assertArrayEquals_([], result.payload);
    assertEquals_('api_unexpected_payload_shape', result.reason_code);
    assertEquals_(1, result.api_credit_usage);
    assertEquals_(1, result.api_call_count);
  } finally {
    UrlFetchApp.fetch = originalFetch;
  }
}

function testFetchOddsWindowFromOddsApi_nonArrayPayloadDoesNotThrow_() {
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
      payload: 'unexpected-string-shape',
      reason_code: 'api_ok',
      api_credit_usage: 1,
      api_call_count: 1,
      credit_headers: { requests_last: 1 },
    };
  };

  try {
    const result = fetchOddsWindowFromOddsApi_({
      ODDS_API_KEY: 'test',
      ODDS_API_BASE_URL: 'https://api.the-odds-api.com/v4',
      ODDS_REGIONS: 'us',
      ODDS_MARKETS: 'h2h',
      ODDS_ODDS_FORMAT: 'american',
    }, Date.parse('2025-01-01T00:00:00Z'), Date.parse('2025-01-02T00:00:00Z'));

    assertEquals_('odds_api_success', result.reason_code);
    assertEquals_(0, result.events.length);
  } finally {
    resolveActiveWtaSportKeys_ = originalResolver;
    callOddsApi_ = originalCaller;
  }
}

function testValidateOddsApiOddsQuery_validWindowAndParams_() {
  const result = validateOddsApiOddsQuery_({
    regions: 'us',
    markets: 'h2h',
    oddsFormat: 'american',
    commenceTimeFrom: '2025-01-01T00:00:00.000Z',
    commenceTimeTo: '2025-01-02T00:00:00.000Z',
  });

  assertEquals_(true, result.ok);
  assertEquals_('', result.detail);
  assertEquals_(0, result.errors.length);
  assertEquals_(true, result.diagnostics.known_good_cli_match);
  assertEquals_(1440, result.diagnostics.duration_minutes);
}

function testValidateOddsApiOddsQuery_invalidWindowFormatsTimeReason_() {
  const result = validateOddsApiOddsQuery_({
    regions: 'us',
    markets: 'h2h',
    oddsFormat: 'american',
    commenceTimeFrom: 'not-a-time',
    commenceTimeTo: '2025-01-02T00:00:00.000Z',
  });

  assertEquals_(false, result.ok);
  assertEquals_('invalid_time_window', result.reason_code);
  assertEquals_('commence_time_from_not_rfc3339_utc', result.detail);
  assertEquals_('commence_time_from_not_rfc3339_utc', result.detail_code);
  assertEquals_(true, result.errors.indexOf('commence_time_from_not_rfc3339_utc') >= 0);
}

function testValidateOddsApiOddsQuery_startAfterEndIsInvalidWindow_() {
  const result = validateOddsApiOddsQuery_({
    regions: 'us',
    markets: 'h2h',
    oddsFormat: 'american',
    commenceTimeFrom: '2025-01-03T00:00:00.000Z',
    commenceTimeTo: '2025-01-02T00:00:00.000Z',
  });

  assertEquals_(false, result.ok);
  assertEquals_('invalid_time_window', result.reason_code);
  assertEquals_('commence_time_window_invalid', result.detail);
  assertEquals_('commence_time_window_invalid', result.detail_code);
  assertEquals_(true, result.errors.indexOf('commence_time_window_invalid') >= 0);
  assertEquals_(true, result.diagnostics.known_good_cli_mismatches.indexOf('window_ordering') >= 0);
}

function testValidateOddsApiOddsQuery_missingTimeBoundsAreRequired_() {
  const result = validateOddsApiOddsQuery_({
    regions: 'us',
    markets: 'h2h',
    oddsFormat: 'american',
    commenceTimeFrom: '',
    commenceTimeTo: '',
  });

  assertEquals_(false, result.ok);
  assertEquals_('invalid_time_window', result.reason_code);
  assertEquals_('commence_time_from_required', result.detail);
  assertEquals_('commence_time_from_required', result.detail_code);
  assertEquals_(true, result.errors.indexOf('commence_time_from_required') >= 0);
  assertEquals_(true, result.errors.indexOf('commence_time_to_required') >= 0);
}

function testValidateOddsApiOddsQuery_rejectsNonUtcOffsetTimes_() {
  const result = validateOddsApiOddsQuery_({
    regions: 'us',
    markets: 'h2h',
    oddsFormat: 'american',
    commenceTimeFrom: '2025-01-01T00:00:00-08:00',
    commenceTimeTo: '2025-01-01T01:00:00-08:00',
  });

  assertEquals_(false, result.ok);
  assertEquals_('invalid_time_window', result.reason_code);
  assertEquals_('commence_time_from_not_rfc3339_utc', result.detail);
  assertEquals_('commence_time_from_not_rfc3339_utc', result.detail_code);
  assertEquals_(true, result.errors.indexOf('commence_time_from_not_rfc3339_utc') >= 0);
  assertEquals_(true, result.errors.indexOf('commence_time_to_not_rfc3339_utc') >= 0);
}

function testValidateOddsApiOddsQuery_missingParamsReturnsInvalidQueryParams_() {
  const result = validateOddsApiOddsQuery_({
    regions: '',
    markets: '',
    oddsFormat: 'fractional',
    commenceTimeFrom: '2025-01-01T00:00:00.000Z',
    commenceTimeTo: '2025-01-02T00:00:00.000Z',
  });

  assertEquals_(false, result.ok);
  assertEquals_('invalid_query_params', result.reason_code);
  assertEquals_(true, result.errors.indexOf('markets_required') >= 0);
  assertEquals_(true, result.errors.indexOf('regions_required') >= 0);
  assertEquals_(true, result.errors.indexOf('odds_format_invalid') >= 0);
}

function testBuildOddsApiDiagnosticQueryParams_returnsExpectedSubset_() {
  const diagnostic = buildOddsApiDiagnosticQueryParams_({
    markets: 'h2h',
    regions: 'us',
    oddsFormat: 'american',
    commenceTimeFrom: '2025-01-01T00:00:00.000Z',
    commenceTimeTo: '2025-01-01T04:00:00.000Z',
    apiKey: 'secret',
  });

  assertEquals_('h2h', diagnostic.markets);
  assertEquals_('us', diagnostic.regions);
  assertEquals_('american', diagnostic.oddsFormat);
  assertEquals_('2025-01-01T00:00:00.000Z', diagnostic.commenceTimeFrom);
  assertEquals_('2025-01-01T04:00:00.000Z', diagnostic.commenceTimeTo);
  assertEquals_(240, diagnostic.duration_minutes);
  assertEquals_(true, diagnostic.known_good_cli_match);
  assertArrayEquals_([], diagnostic.known_good_cli_mismatches);
  assertEquals_('/v4/sports/{sport_key}/odds', diagnostic.known_good_cli_pattern.endpoint);
  assertEquals_(undefined, diagnostic.apiKey);
}

function testBuildOddsApiDiagnosticQueryParams_surfacesKnownGoodCliDiffs_() {
  const diagnostic = buildOddsApiDiagnosticQueryParams_({
    markets: 'h2h',
    regions: 'us',
    oddsFormat: 'american',
    commenceTimeFrom: '2025-01-01T02:00:00.000Z',
    commenceTimeTo: '2025-01-01T01:00:00.000Z',
  });

  assertEquals_(-60, diagnostic.duration_minutes);
  assertEquals_(false, diagnostic.known_good_cli_match);
  assertEquals_(true, diagnostic.known_good_cli_mismatches.indexOf('window_ordering') >= 0);
}

function testBuildOddsApiOddsQuery_preflightValidatesInvertedMsWindow_() {
  const built = buildOddsApiOddsQuery_({
    ODDS_REGIONS: 'us',
    ODDS_MARKETS: 'h2h',
    ODDS_ODDS_FORMAT: 'american',
  }, Date.parse('2025-01-02T00:00:00.000Z'), Date.parse('2025-01-01T00:00:00.000Z'));

  assertEquals_(false, built.ok);
  assertEquals_('invalid_time_window', built.reason_code);
  assertEquals_('refresh_window_duration_negative', built.detail);
  assertEquals_('refresh_window_duration_negative', built.detail_code);
  assertEquals_(true, built.errors.indexOf('refresh_window_duration_negative') >= 0);
  assertEquals_(true, built.errors.indexOf('refresh_window_from_must_be_before_to') >= 0);
  assertEquals_(-1440, built.query_params.duration_minutes);
}

function testBuildOddsApiOddsQuery_serializesUtcIsoForKnownGoodCliPattern_() {
  const startMs = Date.parse('2025-03-01T00:00:00.000Z');
  const endMs = Date.parse('2025-03-01T12:00:00.000Z');
  const built = buildOddsApiOddsQuery_({
    ODDS_REGIONS: 'us',
    ODDS_MARKETS: 'h2h',
    ODDS_ODDS_FORMAT: 'american',
  }, startMs, endMs);

  assertEquals_(true, built.ok);
  assertEquals_('2025-03-01T00:00:00.000Z', built.query.commenceTimeFrom);
  assertEquals_('2025-03-01T12:00:00.000Z', built.query.commenceTimeTo);
  assertEquals_(720, built.query_params.duration_minutes);
  assertEquals_(true, built.query_params.known_good_cli_match);
  assertArrayEquals_([], built.query_params.known_good_cli_mismatches);
}

function testResolveOddsWindowForPipeline_bootstrapWindowSerializationIsUtcAndOrdered_() {
  const originalFetchScheduleFromOddsApi = fetchScheduleFromOddsApi_;
  const originalUpdateCreditStateFromHeaders = updateCreditStateFromHeaders_;
  const originalGetCachedPayload = getCachedPayload_;
  const originalGetStateJson = getStateJson_;
  const originalGetCreditAwareRuntimeConfig = getCreditAwareRuntimeConfig_;

  fetchScheduleFromOddsApi_ = function () {
    return {
      events: [],
      reason_code: 'schedule_no_games_in_window',
      api_credit_usage: 0,
      api_call_count: 1,
      credit_headers: {},
    };
  };
  updateCreditStateFromHeaders_ = function () {};
  getCachedPayload_ = function () { return null; };
  getStateJson_ = function () { return null; };
  getCreditAwareRuntimeConfig_ = function () { return { mode: 'OFF', snapshot: {} }; };

  const nowMs = Date.parse('2025-03-09T09:55:00.000Z');
  try {
    const decision = resolveOddsWindowForPipeline_({
      ODDS_NO_GAMES_BEHAVIOR: 'SKIP',
      ODDS_BOOTSTRAP_LOOKAHEAD_HOURS: 12,
      LOOKAHEAD_HOURS: 24,
      ODDS_WINDOW_PRE_FIRST_MIN: 45,
      ODDS_WINDOW_POST_LAST_MIN: 180,
      API_CREDIT_HARD_LIMIT: 0,
      CREDIT_USAGE_ENFORCEMENT_MODE: 'OFF',
    }, nowMs);

    const built = buildOddsApiOddsQuery_({
      ODDS_REGIONS: 'us',
      ODDS_MARKETS: 'h2h',
      ODDS_ODDS_FORMAT: 'american',
    }, decision.refresh_window_start_ms, decision.refresh_window_end_ms);

    assertEquals_(true, built.ok);
    assertEquals_(true, /Z$/.test(built.query.commenceTimeFrom));
    assertEquals_(true, /Z$/.test(built.query.commenceTimeTo));
    assertEquals_(true, built.query.commenceTimeFrom < built.query.commenceTimeTo);
    assertEquals_(false, built.diagnostics.timezone_conversion_inverted);
  } finally {
    fetchScheduleFromOddsApi_ = originalFetchScheduleFromOddsApi;
    updateCreditStateFromHeaders_ = originalUpdateCreditStateFromHeaders;
    getCachedPayload_ = originalGetCachedPayload;
    getStateJson_ = originalGetStateJson;
    getCreditAwareRuntimeConfig_ = originalGetCreditAwareRuntimeConfig;
  }
}

function testResolveOddsWindowForPipeline_bootstrapWindowHasPositiveUtcDuration_() {
  const originalFetchScheduleFromOddsApi = fetchScheduleFromOddsApi_;
  const originalUpdateCreditStateFromHeaders = updateCreditStateFromHeaders_;
  const originalGetCachedPayload = getCachedPayload_;
  const originalGetStateJson = getStateJson_;
  const originalGetCreditAwareRuntimeConfig = getCreditAwareRuntimeConfig_;

  fetchScheduleFromOddsApi_ = function () {
    return {
      events: [],
      reason_code: 'schedule_no_games_in_window',
      api_credit_usage: 0,
      api_call_count: 1,
      credit_headers: {},
    };
  };
  updateCreditStateFromHeaders_ = function () {};
  getCachedPayload_ = function () { return null; };
  getStateJson_ = function () { return null; };
  getCreditAwareRuntimeConfig_ = function () { return { mode: 'OFF', snapshot: {} }; };

  const nowMs = Date.parse('2025-03-01T00:00:00.000Z');
  try {
    const decision = resolveOddsWindowForPipeline_({
      ODDS_NO_GAMES_BEHAVIOR: 'SKIP',
      ODDS_BOOTSTRAP_LOOKAHEAD_HOURS: 6,
      LOOKAHEAD_HOURS: 12,
      ODDS_WINDOW_PRE_FIRST_MIN: 45,
      ODDS_WINDOW_POST_LAST_MIN: 180,
      API_CREDIT_HARD_LIMIT: 0,
      CREDIT_USAGE_ENFORCEMENT_MODE: 'OFF',
    }, nowMs);

    assertEquals_(true, decision.bootstrap_mode);
    assertEquals_(nowMs, decision.refresh_window_start_ms);
    assertEquals_(nowMs + (12 * 60 * 60000), decision.refresh_window_end_ms);

    const query = {
      regions: 'us',
      markets: 'h2h',
      oddsFormat: 'american',
      commenceTimeFrom: new Date(decision.refresh_window_start_ms).toISOString(),
      commenceTimeTo: new Date(decision.refresh_window_end_ms).toISOString(),
    };
    const validation = validateOddsApiOddsQuery_(query);
    assertEquals_(true, validation.ok);
    assertEquals_(720, validation.diagnostics.duration_minutes);
  } finally {
    fetchScheduleFromOddsApi_ = originalFetchScheduleFromOddsApi;
    updateCreditStateFromHeaders_ = originalUpdateCreditStateFromHeaders;
    getCachedPayload_ = originalGetCachedPayload;
    getStateJson_ = originalGetStateJson;
    getCreditAwareRuntimeConfig_ = originalGetCreditAwareRuntimeConfig;
  }
}

function testResolveOddsWindowForPipeline_bootstrapModeNoEligibleRows_() {
  const originalFetchScheduleFromOddsApi = fetchScheduleFromOddsApi_;
  const originalUpdateCreditStateFromHeaders = updateCreditStateFromHeaders_;
  const originalGetCachedPayload = getCachedPayload_;
  const originalGetStateJson = getStateJson_;
  const originalGetCreditAwareRuntimeConfig = getCreditAwareRuntimeConfig_;

  fetchScheduleFromOddsApi_ = function () {
    return {
      events: [],
      reason_code: 'schedule_no_games_in_window',
      api_credit_usage: 0,
      api_call_count: 1,
      credit_headers: {},
    };
  };
  updateCreditStateFromHeaders_ = function () {};
  getCachedPayload_ = function () { return null; };
  getStateJson_ = function () { return null; };
  getCreditAwareRuntimeConfig_ = function () { return { mode: 'OFF', snapshot: {} }; };

  try {
    const nowMs = Date.parse('2025-03-01T00:00:00.000Z');
    const decision = resolveOddsWindowForPipeline_({
      ODDS_NO_GAMES_BEHAVIOR: 'SKIP',
      ODDS_BOOTSTRAP_LOOKAHEAD_HOURS: 12,
      LOOKAHEAD_HOURS: 24,
      API_CREDIT_HARD_LIMIT: 0,
      CREDIT_USAGE_ENFORCEMENT_MODE: 'OFF',
    }, nowMs);

    assertEquals_(true, decision.should_fetch_odds);
    assertEquals_(true, decision.bootstrap_mode);
    assertEquals_('odds_refresh_bootstrap_fetch', decision.decision_reason_code);
    assertEquals_('bootstrap', decision.current_refresh_mode);
  } finally {
    fetchScheduleFromOddsApi_ = originalFetchScheduleFromOddsApi;
    updateCreditStateFromHeaders_ = originalUpdateCreditStateFromHeaders;
    getCachedPayload_ = originalGetCachedPayload;
    getStateJson_ = originalGetStateJson;
    getCreditAwareRuntimeConfig_ = originalGetCreditAwareRuntimeConfig;
  }
}

function testClassifyOddsApiClientErrorReason_splitsClientErrorCodes_() {
  assertEquals_(
    'invalid_sport_key',
    classifyOddsApiClientErrorReason_(422, '{"message":"invalid sport key tennis_wta_unknown"}', 'https://api.test/v4/sports/tennis_wta_unknown/odds')
  );
  assertEquals_(
    'invalid_time_window',
    classifyOddsApiClientErrorReason_(422, '{"message":"commenceTimeFrom must be before commenceTimeTo"}', 'https://api.test/v4/sports/tennis_wta_us_open/odds')
  );
  assertEquals_(
    'invalid_query_params',
    classifyOddsApiClientErrorReason_(422, '{"message":"invalid query parameter markets"}', 'https://api.test/v4/sports/tennis_wta_us_open/odds')
  );
  assertEquals_(
    'unknown_client_error',
    classifyOddsApiClientErrorReason_(422, '{"message":"unprocessable entity"}', 'https://api.test/v4/sports/tennis_wta_us_open/odds')
  );
}

function testCallOddsApi_non2xxReturnsBodyDerivedReasonCode_() {
  const originalFetch = UrlFetchApp.fetch;

  UrlFetchApp.fetch = function () {
    return {
      getResponseCode: function () { return 422; },
      getAllHeaders: function () { return {}; },
      getContentText: function () {
        return '{"message":"invalid query parameter markets"}';
      },
    };
  };

  try {
    const result = callOddsApi_('https://api.the-odds-api.com/v4/sports/tennis_wta_us_open/odds?apiKey=secret&regions=us&markets=', { debug: true });
    assertEquals_(false, result.ok);
    assertEquals_(422, result.status_code);
    assertEquals_('invalid_query_params', result.reason_code);
  } finally {
    UrlFetchApp.fetch = originalFetch;
  }
}

function testRunEdgeBoard_debounceSkipStillWorks_() {
  const harness = createRunEdgeBoardTestHarness_({
    nowMs: 1000000,
    lastRunTs: 999500,
    debounceMs: 1000,
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
    nowMs: 1000000,
    lastRunTs: 0,
    debounceMs: 1000,
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

function testRunEdgeBoard_degradesWhenOddsPresentButNoMatches_() {
  const harness = createRunEdgeBoardTestHarness_({
    nowMs: 1000000,
    lastRunTs: 0,
    debounceMs: 1000,
    orchestrationScenario: {
      oddsEvents: [{ event_id: 'odds_1' }],
      oddsRows: [{ key: 'odds_1|book|h2h|p1' }],
      scheduleEvents: [],
      scheduleReasonCodes: { schedule_enrichment_no_schedule_events: 1 },
      matchedCount: 0,
      unmatchedCount: 1,
      rejectedCount: 1,
      diagnosticRecordsWritten: 1,
      unmatched: [{
        odds_event_id: 'odds_1',
        competition: 'WTA 500 Doha',
        player_1: 'Player One',
        player_2: 'Player Two',
        commence_time: '2025-03-01T12:00:00.000Z',
        rejection_code: 'no_player_match',
      }],
      matchReasonCodes: { no_player_match: 1, rejected_count: 1 },
      signalRows: [],
      sentCount: 0,
    },
  });

  try {
    runEdgeBoard();
    assertEquals_('run_health_no_matches_from_odds', harness.summaryReasonCode());

    let summary = null;
    let healthWarning = null;
    for (let i = 0; i < harness.logs.length; i += 1) {
      const row = harness.logs[i];
      if (row.row_type === 'summary' && row.stage === 'runEdgeBoard') summary = row;
      if (row.row_type === 'ops' && row.stage === 'run_health_guard') healthWarning = row;
    }

    assertEquals_('degraded', summary.status);
    assertEquals_('run_health_no_matches_from_odds', summary.reason_code);
    assertEquals_(1, summary.fetched_odds);
    assertEquals_(0, summary.matched);

    const warningPayload = JSON.parse(healthWarning.message || '{}');
    assertEquals_('run_health_no_matches_from_odds', warningPayload.reason_code);
    assertEquals_(1, warningPayload.stage_skipped_reason_counts.schedule_enrichment_no_schedule_events);
    assertEquals_(1, warningPayload.stage_skipped_reason_counts.no_player_match);
    assertEquals_('no_player_match', warningPayload.dominant_blocker_categories[0].category);
    assertEquals_(1, warningPayload.dominant_blocker_categories[0].count);
    assertEquals_('odds_1', warningPayload.sample_unmatched_events[0].odds_event_id);
    assertEquals_('no_player_match', warningPayload.sample_unmatched_events[0].rejection_code);
    assertEquals_(0, warningPayload.opening_lag_blocked_count);
    assertEquals_(0, warningPayload.schedule_only_seed_count);
  } finally {
    harness.restore();
  }
}


function testRunEdgeBoard_marksIdleOutsideOddsWindowForScheduleOnlyRun_() {
  const harness = createRunEdgeBoardTestHarness_({
    nowMs: 1000000,
    lastRunTs: 0,
    debounceMs: 1000,
    orchestrationScenario: {
      oddsEvents: [],
      oddsRows: [],
      oddsReasonCodes: { odds_refresh_skipped_outside_window: 1 },
      scheduleEvents: [{ event_id: 'sch_1' }],
      scheduleRows: [{ event_id: 'sch_1' }],
      matchedCount: 0,
      unmatchedCount: 0,
      rejectedCount: 0,
      diagnosticRecordsWritten: 0,
      matchReasonCodes: {},
      signalRows: [],
      sentCount: 0,
    },
  });

  try {
    runEdgeBoard();
    assertEquals_('odds_refresh_skipped_outside_window', harness.summaryReasonCode());

    let summary = null;
    let healthWarning = null;
    for (let i = 0; i < harness.logs.length; i += 1) {
      const row = harness.logs[i];
      if (row.row_type === 'summary' && row.stage === 'runEdgeBoard') summary = row;
      if (row.row_type === 'ops' && row.stage === 'run_health_guard') healthWarning = row;
    }

    assertEquals_('success', summary.status);
    assertEquals_('odds_refresh_skipped_outside_window', summary.reason_code);
    assertEquals_(0, summary.fetched_odds);
    assertEquals_(1, summary.fetched_schedule);
    assertEquals_(null, healthWarning);

    const verbose = JSON.parse(harness.stateWrites.LAST_RUN_VERBOSE_JSON || '{}');
    assertEquals_('idle_outside_odds_window', verbose.run_health.status);
    assertEquals_('odds_refresh_skipped_outside_window', verbose.run_health.reason_code);
    assertTrue_(
      ['run_health_expected_idle_outside_odds_window', 'odds_refresh_skipped_outside_window'].indexOf(String(verbose.run_health.diagnostics.reason_code || '')) >= 0,
      'expected idle-outside-window diagnostic reason code'
    );
    assertEquals_(0, verbose.run_health.diagnostics.matched);
    assertEquals_(0, verbose.run_health.diagnostics.signals_found);
  } finally {
    harness.restore();
  }
}

function testRunEdgeBoard_writesCompactVerboseStateWhenLogProfileCompact_() {
  const harness = createRunEdgeBoardTestHarness_({
    nowMs: 1000000,
    lastRunTs: 0,
    debounceMs: 1000,
    logProfile: 'compact',
    orchestrationScenario: {
      oddsEvents: [{ event_id: 'odds_1' }],
      oddsRows: [{ key: 'odds_1|book|h2h|p1' }],
      scheduleEvents: [{ event_id: 'sch_1' }],
      scheduleRows: [{ event_id: 'sch_1' }],
      matchRows: [{ odds_event_id: 'odds_1', schedule_event_id: 'sch_1' }],
      matchedCount: 1,
      matchReasonCodes: { primary_match: 1, matched_count: 1 },
      signalRows: [],
      sentCount: 0,
    },
  });

  try {
    runEdgeBoard();
    const verbose = JSON.parse(harness.stateWrites.LAST_RUN_VERBOSE_JSON || '{}');
    assertEquals_('compact', verbose.log_profile);
    assertEquals_(1, Number((verbose.reason_codes || {}).primary_match || 0));
    assertEquals_(undefined, verbose.run_health);
  } finally {
    harness.restore();
  }
}



function testRunEdgeBoard_statsStageExecutesForOddsDrivenRun_() {
  const harness = createRunEdgeBoardTestHarness_({
    nowMs: 1000000,
    lastRunTs: 0,
    debounceMs: 1000,
    orchestrationScenario: {
      oddsEvents: [{ event_id: 'odds_1' }],
      oddsRows: [{ key: 'odds_1|book|h2h|p1' }],
      scheduleEvents: [{ event_id: 'sch_1' }],
      scheduleRows: [{ event_id: 'sch_1' }],
      matchRows: [{ odds_event_id: 'odds_1', schedule_event_id: 'sch_1' }],
      matchedCount: 1,
      matchReasonCodes: { primary_match: 1, matched_count: 1 },
      playerStatsStageResult: {
        rows: [{ key: 'odds_1|player a|ts' }],
        byOddsEventId: { odds_1: { source: 'player_stats_provider_v1' } },
        summary: {
          reason_codes: { stats_enriched: 2 },
          reason_metadata: { players_with_non_null_stats: 2 },
        },
      },
      signalRows: [],
      sentCount: 0,
    },
  });

  try {
    runEdgeBoard();
    assertEquals_(1, harness.playerStatsCallCount);
  } finally {
    harness.restore();
  }
}

function testRunEdgeBoard_degradesWhenMatchedButStatsCoverageIsZero_() {
  const harness = createRunEdgeBoardTestHarness_({
    nowMs: 1000000,
    lastRunTs: 0,
    debounceMs: 1000,
    orchestrationScenario: {
      oddsEvents: [{ event_id: 'odds_1' }],
      oddsRows: [{ key: 'odds_1|book|h2h|p1' }],
      scheduleEvents: [{ event_id: 'sch_1' }],
      scheduleRows: [{ event_id: 'sch_1' }],
      matchRows: [{ odds_event_id: 'odds_1', schedule_event_id: 'sch_1' }],
      matchedCount: 1,
      unmatchedCount: 0,
      rejectedCount: 0,
      diagnosticRecordsWritten: 1,
      matchReasonCodes: { primary_match: 1, matched_count: 1 },
      playerStatsStageResult: {
        rows: [],
        byOddsEventId: {},
        summary: {
          reason_codes: { provider_returned_empty: 1 },
          reason_metadata: { players_with_non_null_stats: 0 },
        },
      },
      signalRows: [],
      sentCount: 0,
    },
  });

  try {
    runEdgeBoard();
    assertEquals_('stats_zero_coverage', harness.summaryReasonCode());

    let summary = null;
    let healthWarning = null;
    for (let i = 0; i < harness.logs.length; i += 1) {
      const row = harness.logs[i];
      if (row.row_type === 'summary' && row.stage === 'runEdgeBoard') summary = row;
      if (row.row_type === 'ops' && row.stage === 'run_health_guard') healthWarning = row;
    }

    assertEquals_('degraded', summary.status);
    assertEquals_('stats_zero_coverage', summary.reason_code);

    const warningPayload = JSON.parse(healthWarning.message || '{}');
    assertEquals_('stats_zero_coverage', warningPayload.reason_code);
    assertEquals_(1, warningPayload.matched);
    assertEquals_(0, warningPayload.players_with_non_null_stats);

    const verbose = JSON.parse(harness.stateWrites.LAST_RUN_VERBOSE_JSON || '{}');
    assertEquals_(1, Number((verbose.reason_codes || {}).stats_zero_coverage || 0));
  } finally {
    harness.restore();
  }
}

function testRunEdgeBoard_statsStageSkippedForScheduleOnlyNoOdds_() {
  const harness = createRunEdgeBoardTestHarness_({
    nowMs: 1000000,
    lastRunTs: 0,
    debounceMs: 1000,
    orchestrationScenario: {
      oddsEvents: [],
      oddsRows: [],
      oddsReasonCodes: { odds_refresh_skipped_outside_window: 1 },
      scheduleEvents: [{ event_id: 'sch_1' }],
      scheduleRows: [{ event_id: 'sch_1' }],
      matchRows: [{ odds_event_id: 'sch_1', schedule_event_id: 'sch_1', match_type: 'schedule_seed_no_odds' }],
      matchedCount: 0,
      unmatchedCount: 0,
      rejectedCount: 0,
      diagnosticRecordsWritten: 1,
      matchReasonCodes: { schedule_seed_no_odds: 1, diagnostic_records_written: 1 },
      signalRows: [],
      sentCount: 0,
    },
  });

  try {
    runEdgeBoard();
    assertEquals_(0, harness.playerStatsCallCount);

    const statsSummary = harness.logs.filter(function (row) {
      return row.row_type === 'summary' && row.stage === 'stageFetchPlayerStats';
    })[0];
    assertEquals_('skipped_schedule_only_no_odds', statsSummary.reason_code);
    assertEquals_('skipped', statsSummary.status);
  } finally {
    harness.restore();
  }
}

function testRunEdgeBoard_mergesMixedReasonValueTypesWithoutTypeCoercion_() {
  const harness = createRunEdgeBoardTestHarness_({
    nowMs: 1000000,
    lastRunTs: 0,
    debounceMs: 1000,
    orchestrationScenario: {
      oddsEvents: [],
      oddsRows: [],
      oddsReasonCodes: { odds_refresh_skipped_outside_window: 1 },
      scheduleEvents: [{ event_id: 'sch_1' }],
      scheduleRows: [{ event_id: 'sch_1' }],
      scheduleReasonCodes: { schedule_seed_no_odds: 1, upstream_gate_reason: 'schedule_seed_no_odds' },
      matchedCount: 0,
      unmatchedCount: 0,
      rejectedCount: 0,
      diagnosticRecordsWritten: 0,
      matchReasonCodes: { upstream_gate_reason: 'unspecified' },
      signalRows: [],
      sentCount: 0,
    },
  });

  try {
    runEdgeBoard();

    const verbose = JSON.parse(harness.stateWrites.LAST_RUN_VERBOSE_JSON || '{}');
    assertEquals_(1, verbose.reason_codes.odds_refresh_skipped_outside_window || 0);
    assertEquals_(1, verbose.reason_codes.schedule_seed_no_odds || 0);
    assertEquals_(undefined, verbose.reason_codes.upstream_gate_reason);
    assertEquals_('schedule_seed_no_odds', (verbose.reason_metadata || {}).upstream_gate_reason || '');
    assertEquals_('schedule_seed_no_odds', verbose.upstream_gate_reason || '');
  } finally {
    harness.restore();
  }
}


function testUpdateEmptyProductiveOutputState_warnsAtThreshold_() {
  const originalGetStateJson = getStateJson_;
  const originalSetStateValue = setStateValue_;
  const originalLocalAndUtcTimestamps = localAndUtcTimestamps_;

  const writes = {};

  getStateJson_ = function () {
    return { consecutive_count: 2 };
  };
  setStateValue_ = function (key, value) {
    writes[key] = value;
  };
  localAndUtcTimestamps_ = function () {
    return {
      local: '2025-03-01T00:00:00-07:00',
      utc: '2025-03-01T07:00:00.000Z',
    };
  };

  try {
    const result = updateEmptyProductiveOutputState_('run_test', {
      fetched_odds: 4,
      fetched_schedule: 0,
      signals_found: 0,
    }, {
      EMPTY_PRODUCTIVE_OUTPUT_THRESHOLD: 3,
      SCHEDULE_ONLY_STREAK_NOTICE_THRESHOLD: 3,
    });

    assertEquals_(3, result.consecutive_count);
    assertEquals_(true, result.warning_needed);
    assertEquals_('productive_output_empty_streak_detected', result.reason_code);
    assertEquals_(0, result.schedule_only_consecutive_count);
    assertEquals_(false, result.schedule_only_notice_needed);

    const stored = JSON.parse(writes.EMPTY_PRODUCTIVE_OUTPUT_STATE || '{}');
    assertEquals_(3, stored.consecutive_count);
    assertEquals_(0, stored.schedule_only_consecutive_count);
    assertEquals_(4, stored.fetched_odds);
    assertEquals_(0, stored.fetched_schedule);
    assertEquals_(0, stored.signals_found);
  } finally {
    getStateJson_ = originalGetStateJson;
    setStateValue_ = originalSetStateValue;
    localAndUtcTimestamps_ = originalLocalAndUtcTimestamps;
  }
}



function testUpdateEmptyProductiveOutputState_scheduleOnlyNoticesAtThreshold_() {
  const originalGetStateJson = getStateJson_;
  const originalSetStateValue = setStateValue_;
  const originalLocalAndUtcTimestamps = localAndUtcTimestamps_;

  const writes = {};

  getStateJson_ = function () {
    return {
      consecutive_count: 5,
      schedule_only_consecutive_count: 2,
    };
  };
  setStateValue_ = function (key, value) {
    writes[key] = value;
  };
  localAndUtcTimestamps_ = function () {
    return {
      local: '2025-03-01T00:00:00-07:00',
      utc: '2025-03-01T07:00:00.000Z',
    };
  };

  try {
    const result = updateEmptyProductiveOutputState_('run_test', {
      fetched_odds: 0,
      fetched_schedule: 7,
      signals_found: 0,
    }, {
      EMPTY_PRODUCTIVE_OUTPUT_THRESHOLD: 3,
      SCHEDULE_ONLY_STREAK_NOTICE_THRESHOLD: 3,
    });

    assertEquals_(0, result.consecutive_count);
    assertEquals_(false, result.warning_needed);
    assertEquals_('', result.reason_code);
    assertEquals_(3, result.schedule_only_consecutive_count);
    assertEquals_(true, result.schedule_only_notice_needed);
    assertEquals_('schedule_only_streak_detected', result.schedule_only_reason_code);

    const stored = JSON.parse(writes.EMPTY_PRODUCTIVE_OUTPUT_STATE || '{}');
    assertEquals_(0, stored.consecutive_count);
    assertEquals_(3, stored.schedule_only_consecutive_count);
    assertEquals_(0, stored.fetched_odds);
    assertEquals_(7, stored.fetched_schedule);
  } finally {
    getStateJson_ = originalGetStateJson;
    setStateValue_ = originalSetStateValue;
    localAndUtcTimestamps_ = originalLocalAndUtcTimestamps;
  }
}



function testUpdateEmptyProductiveOutputState_scheduleOnlyNoticeBelowThresholdMinusOne_() {
  const originalGetStateJson = getStateJson_;
  const originalSetStateValue = setStateValue_;
  const originalLocalAndUtcTimestamps = localAndUtcTimestamps_;

  getStateJson_ = function () {
    return {
      consecutive_count: 5,
      schedule_only_consecutive_count: 1,
    };
  };
  setStateValue_ = function () {};
  localAndUtcTimestamps_ = function () {
    return {
      local: '2025-03-01T00:00:00-07:00',
      utc: '2025-03-01T07:00:00.000Z',
    };
  };

  try {
    const result = updateEmptyProductiveOutputState_('run_test', {
      fetched_odds: 0,
      fetched_schedule: 7,
      signals_found: 0,
    }, {
      EMPTY_PRODUCTIVE_OUTPUT_THRESHOLD: 3,
      SCHEDULE_ONLY_STREAK_NOTICE_THRESHOLD: 3,
    });

    assertEquals_(2, result.schedule_only_consecutive_count);
    assertEquals_(false, result.schedule_only_notice_needed);
    assertEquals_('', result.schedule_only_reason_code);
  } finally {
    getStateJson_ = originalGetStateJson;
    setStateValue_ = originalSetStateValue;
    localAndUtcTimestamps_ = originalLocalAndUtcTimestamps;
  }
}

function testUpdateEmptyProductiveOutputState_scheduleOnlyNoticeAboveThresholdPlusOne_() {
  const originalGetStateJson = getStateJson_;
  const originalSetStateValue = setStateValue_;
  const originalLocalAndUtcTimestamps = localAndUtcTimestamps_;

  getStateJson_ = function () {
    return {
      consecutive_count: 5,
      schedule_only_consecutive_count: 3,
    };
  };
  setStateValue_ = function () {};
  localAndUtcTimestamps_ = function () {
    return {
      local: '2025-03-01T00:00:00-07:00',
      utc: '2025-03-01T07:00:00.000Z',
    };
  };

  try {
    const result = updateEmptyProductiveOutputState_('run_test', {
      fetched_odds: 0,
      fetched_schedule: 7,
      signals_found: 0,
    }, {
      EMPTY_PRODUCTIVE_OUTPUT_THRESHOLD: 3,
      SCHEDULE_ONLY_STREAK_NOTICE_THRESHOLD: 3,
    });

    assertEquals_(4, result.schedule_only_consecutive_count);
    assertEquals_(true, result.schedule_only_notice_needed);
    assertEquals_('schedule_only_streak_detected', result.schedule_only_reason_code);
  } finally {
    getStateJson_ = originalGetStateJson;
    setStateValue_ = originalSetStateValue;
    localAndUtcTimestamps_ = originalLocalAndUtcTimestamps;
  }
}

function testRunEdgeBoard_scheduleOnlyWatchdogNoticeIsLowSeverityExpectedIdleAndNoFailureEscalation_() {
  const harness = createRunEdgeBoardTestHarness_({
    nowMs: 1000000,
    lastRunTs: 0,
    debounceMs: 1000,
    orchestrationScenario: {
      oddsEvents: [],
      oddsRows: [],
      oddsReasonCodes: { odds_refresh_skipped_outside_window: 1 },
      scheduleEvents: [{ event_id: 'sch_1' }],
      scheduleRows: [{ event_id: 'sch_1' }],
      matchedCount: 0,
      unmatchedCount: 0,
      rejectedCount: 0,
      diagnosticRecordsWritten: 0,
      matchReasonCodes: {},
      signalRows: [],
      sentCount: 0,
      productiveOutputState: {
        reason_code: '',
        warning_needed: false,
        consecutive_count: 0,
        threshold: 3,
        schedule_only_consecutive_count: 3,
        schedule_only_threshold: 3,
        schedule_only_notice_needed: true,
        schedule_only_reason_code: 'schedule_only_streak_detected',
      },
    },
  });

  try {
    runEdgeBoard();

    const summary = harness.logs.filter(function (row) {
      return row.row_type === 'summary' && row.stage === 'runEdgeBoard';
    })[0];
    const scheduleOnlyNotice = harness.logs.filter(function (row) {
      return row.row_type === 'ops' && row.stage === 'schedule_only_watchdog';
    })[0];
    const healthWarning = harness.logs.filter(function (row) {
      return row.row_type === 'ops' && row.stage === 'run_health_guard';
    })[0] || null;

    assertEquals_('success', summary.status);
    assertEquals_('odds_refresh_skipped_outside_window', summary.reason_code);
    assertEquals_('notice', scheduleOnlyNotice.status);
    assertEquals_(null, healthWarning);

    const noticePayload = JSON.parse(scheduleOnlyNotice.message || '{}');
    assertEquals_('low', noticePayload.notice_severity);
    assertEquals_(true, noticePayload.expected_idle);
    assertEquals_('outside_window', noticePayload.odds_window_context);
    assertTrue_(String(noticePayload.message || '').indexOf('should not be treated as a pipeline failure') >= 0);
  } finally {
    harness.restore();
  }
}


function testRunEdgeBoard_compactSummaryReasonMapsKeepVerboseReasonMaps_() {
  const harness = createRunEdgeBoardTestHarness_({
    nowMs: 1000000,
    lastRunTs: 0,
    debounceMs: 1000,
    orchestrationScenario: {
      oddsEvents: [{ event_id: 'odds_1' }],
      oddsRows: [{ key: 'odds_1|h2h|p1' }],
      oddsReasonCodes: { odds_present: 1, zeroed_reason: 0 },
      scheduleEvents: [{ event_id: 'sch_1' }],
      scheduleRows: [{ event_id: 'sch_1' }],
      scheduleReasonCodes: { schedule_present: 1, schedule_zeroed: 0 },
      matchedCount: 0,
      unmatchedCount: 1,
      rejectedCount: 1,
      diagnosticRecordsWritten: 0,
      matchReasonCodes: { no_schedule_candidates: 1, match_zeroed: 0 },
      signalRows: [],
      sentCount: 0,
    },
  });

  try {
    runEdgeBoard();

    const summary = harness.logs.filter(function (row) {
      return row.row_type === 'summary' && row.stage === 'runEdgeBoard';
    })[0];
    const compactReasonEnvelope = JSON.parse(summary.rejection_codes || '{}');
    const compactStageSummaryEnvelope = JSON.parse(summary.stage_summaries || '{}');
    const compactReasonCodes = compactReasonEnvelope.reason_codes || {};
    const compactStageSummaries = compactStageSummaryEnvelope.stage_summaries || [];
    const verbose = JSON.parse(harness.stateWrites.LAST_RUN_VERBOSE_JSON || '{}');

    assertEquals_(REASON_CODE_ALIAS_SCHEMA_ID, compactReasonEnvelope.schema_id);
    assertEquals_(REASON_CODE_ALIAS_SCHEMA_ID, compactStageSummaryEnvelope.schema_id);

    assertEquals_(1, compactReasonCodes.odds_present || 0);
    assertEquals_(undefined, compactReasonCodes.zeroed_reason);
    assertEquals_(undefined, compactReasonCodes.schedule_zeroed);
    assertEquals_(undefined, compactReasonCodes.match_zeroed);

    assertEquals_(1, Number((compactStageSummaries[0].reason_codes || {}).odds_present || 0));
    assertEquals_(undefined, (compactStageSummaries[0].reason_codes || {}).zeroed_reason);
    assertEquals_(1, Number((compactStageSummaries[1].reason_codes || {}).schedule_present || 0));
    assertEquals_(undefined, (compactStageSummaries[1].reason_codes || {}).schedule_zeroed);

    assertEquals_(0, Number((verbose.reason_codes || {}).zeroed_reason || 0));
    assertEquals_(0, Number((verbose.reason_codes || {}).schedule_zeroed || 0));
    assertEquals_(0, Number((verbose.reason_codes || {}).match_zeroed || 0));
    assertEquals_(0, Number((verbose.stage_summaries[0].reason_codes || {}).zeroed_reason || 0));
    assertEquals_(0, Number((verbose.stage_summaries[1].reason_codes || {}).schedule_zeroed || 0));
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
  const originalStageFetchOdds = stageFetchOdds;
  const originalStageFetchSchedule = stageFetchSchedule;
  const originalStageMatchEvents = stageMatchEvents;
  const originalStageFetchPlayerStats = stageFetchPlayerStats;
  const originalStageGenerateSignals = stageGenerateSignals;
  const originalStagePersist = stagePersist;
  const originalAppendStageLog = appendStageLog_;
  const originalLogDiagnosticEvent = logDiagnosticEvent_;
  const originalLocalAndUtcTimestamps = localAndUtcTimestamps_;
  const originalMergeReasonCounts = mergeReasonCounts_;
  const originalGetTopReasonCodes = getTopReasonCodes_;
  const originalUpdateBootstrapEmptyCycleState = updateBootstrapEmptyCycleState_;
  const originalUpdateEmptyProductiveOutputState = updateEmptyProductiveOutputState_;
  const originalSetStateValue = setStateValue_;
  const originalGetScriptProperties = PropertiesService.getScriptProperties;
  const originalGetScriptLock = LockService.getScriptLock;

  const logs = [];
  const setPropertyCalls = [];
  const stateWrites = {};
  let duplicatePreventedCount = 0;
  let releaseLockCalls = 0;
  let playerStatsCallCount = 0;

  Date.now = function () { return Number(opts.nowMs || 0); };

  ensureTabsAndConfig_ = function () {};
  buildRunId_ = function () { return 'test-run'; };
  tryLock_ = function () { return true; };
  getConfig_ = function () {
    return {
      RUN_ENABLED: true,
      DUPLICATE_DEBOUNCE_MS: Number(opts.debounceMs || 0),
      LOG_PROFILE: String(opts.logProfile || 'verbose'),
      LOG_VERBOSITY_LEVEL: Number(opts.logVerbosityLevel || 2),
      VERBOSE_LOGGING: true,
    };
  };
  appendLogRow_ = function (row) { logs.push(row); };
  incrementDuplicatePreventedCount_ = function () {
    duplicatePreventedCount += 1;
    return duplicatePreventedCount;
  };
  resolveOddsWindowForPipeline_ = function () {
    if (opts.throwDuringOrchestration) throw new Error('stage crash');
    if (opts.orchestrationScenario) {
      return {
        should_fetch_odds: true,
        odds_fetch_window: {},
        selected_source: 'fallback_static_window',
        decision_reason_code: 'odds_refresh_test',
        bootstrap_mode: false,
        transitioned_from_bootstrap_to_active_window: false,
      };
    }
    throw new Error('unexpected test path: orchestration should not execute in this harness');
  };
  setStateValue_ = function (key, value) { stateWrites[key] = value; };

  if (opts.orchestrationScenario) {
    appendStageLog_ = function (runId, summary) {
      logs.push({
        row_type: 'summary',
        run_id: runId,
        stage: summary.stage,
        status: summary.status,
        reason_code: summary.reason_code,
      });
    };
    logDiagnosticEvent_ = function () {};
    localAndUtcTimestamps_ = function () {
      return { local: '2025-03-01T00:00:00-07:00', utc: '2025-03-01T07:00:00.000Z' };
    };
    mergeReasonCounts_ = function (maps) {
      const out = {};
      (maps || []).forEach(function (map) {
        Object.keys(map || {}).forEach(function (key) {
          const value = Number((map || {})[key]);
          if (!Number.isFinite(value)) return;
          out[key] = Number(out[key] || 0) + value;
        });
      });
      return out;
    };
    getTopReasonCodes_ = function () { return []; };
    updateBootstrapEmptyCycleState_ = function () {
      return { reason_code: '', warning_needed: false };
    };
    updateEmptyProductiveOutputState_ = function () {
      if (opts.orchestrationScenario && opts.orchestrationScenario.productiveOutputState) {
        return JSON.parse(JSON.stringify(opts.orchestrationScenario.productiveOutputState));
      }
      return { reason_code: '', warning_needed: false, consecutive_count: 0, threshold: 3 };
    };

    stageFetchOdds = function () {
      return {
        events: (opts.orchestrationScenario.oddsEvents || []).slice(),
        rows: (opts.orchestrationScenario.oddsRows || []).slice(),
        summary: { reason_codes: Object.assign({}, opts.orchestrationScenario.oddsReasonCodes || {}) },
        selected_source: 'fallback_static_window',
      };
    };
    stageFetchSchedule = function () {
      return {
        events: (opts.orchestrationScenario.scheduleEvents || []).slice(),
        rows: (opts.orchestrationScenario.scheduleRows || []).slice(),
        summary: { reason_codes: Object.assign({}, opts.orchestrationScenario.scheduleReasonCodes || {}) },
        canonicalExamples: [],
        unresolvedCompetitions: [],
        unresolvedCompetitionCounts: {},
        topUnresolvedCompetitions: [],
        allowedCount: Number(opts.orchestrationScenario.allowedCount || 0),
      };
    };
    stageMatchEvents = function () {
      return {
        rows: (opts.orchestrationScenario.matchRows || []).slice(),
        summary: { reason_codes: Object.assign({}, opts.orchestrationScenario.matchReasonCodes || {}) },
        matchedCount: Number(opts.orchestrationScenario.matchedCount || 0),
        unmatchedCount: Number(opts.orchestrationScenario.unmatchedCount || 0),
        rejectedCount: Number(opts.orchestrationScenario.rejectedCount || 0),
        diagnosticRecordsWritten: Number(opts.orchestrationScenario.diagnosticRecordsWritten || 0),
        unmatched: (opts.orchestrationScenario.unmatched || []).slice(),
        canonicalizationExamples: [],
      };
    };
    stageFetchPlayerStats = function () {
      playerStatsCallCount += 1;
      if (opts.orchestrationScenario.playerStatsStageResult) {
        return JSON.parse(JSON.stringify(opts.orchestrationScenario.playerStatsStageResult));
      }
      return { rows: [], byOddsEventId: {}, summary: { reason_codes: {} } };
    };
    stageGenerateSignals = function () {
      return {
        rows: (opts.orchestrationScenario.signalRows || []).slice(),
        sentCount: Number(opts.orchestrationScenario.sentCount || 0),
        cooldownSuppressedCount: 0,
        duplicateSuppressedCount: 0,
        summary: { reason_codes: {} },
      };
    };
    stagePersist = function () {
      return { summary: { reason_codes: {} } };
    };
  }

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
      stageFetchOdds = originalStageFetchOdds;
      stageFetchSchedule = originalStageFetchSchedule;
      stageMatchEvents = originalStageMatchEvents;
      stageFetchPlayerStats = originalStageFetchPlayerStats;
      stageGenerateSignals = originalStageGenerateSignals;
      stagePersist = originalStagePersist;
      appendStageLog_ = originalAppendStageLog;
      logDiagnosticEvent_ = originalLogDiagnosticEvent;
      localAndUtcTimestamps_ = originalLocalAndUtcTimestamps;
      mergeReasonCounts_ = originalMergeReasonCounts;
      getTopReasonCodes_ = originalGetTopReasonCodes;
      updateBootstrapEmptyCycleState_ = originalUpdateBootstrapEmptyCycleState;
      updateEmptyProductiveOutputState_ = originalUpdateEmptyProductiveOutputState;
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
    get playerStatsCallCount() { return playerStatsCallCount; },
    get stateWrites() { return stateWrites; },
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

function testStageGenerateSignals_statsZeroCoverageMarksFallbackOnlyAndSuppressesNotify_() {
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
      event_id: 'evt_fallback_only',
      market: 'h2h',
      outcome: 'player_a',
      bookmaker: 'book_x',
      price: 150,
      commence_time: new Date(nowMs + (5 * 60 * 60 * 1000)),
      odds_updated_time: new Date(nowMs),
    };
    const match = {
      odds_event_id: 'evt_fallback_only',
      schedule_event_id: 'sched_1',
      competition_tier: 'WTA_500',
    };
    const stats = {
      evt_fallback_only: {
        player_a: {
          has_stats: true,
          features: { ranking: 8, recent_form: 0.75, surface_win_rate: 0.65, hold_pct: 0.71, break_pct: 0.39 },
        },
        player_b: {
          has_stats: true,
          features: { ranking: 28, recent_form: 0.48, surface_win_rate: 0.51, hold_pct: 0.6, break_pct: 0.29 },
        },
      },
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
      NOTIFY_ENABLED: true,
      NOTIFY_TEST_MODE: false,
      DISCORD_WEBHOOK: 'https://hooks.slack.com/services/mock',
    };

    const result = stageGenerateSignals('run_fallback_only', config, [event], [match], stats, {
      upstream_gate_reason: 'stats_zero_coverage',
    });

    assertEquals_(1, result.rows.length);
    assertEquals_('fallback_only', result.rows[0].notification_outcome);
    assertEquals_('fallback_only', result.rows[0].signal_delivery_mode);
    assertEquals_(1, result.summary.reason_codes.fallback_only || 0);
    assertEquals_(0, Object.keys((captured.state && captured.state.sent_hashes) || {}).length);

    const notifyLogs = captured.logs.filter(function (entry) {
      return entry.stage === 'signalNotifyDelivery';
    });
    assertEquals_(0, notifyLogs.length);
  } finally {
    Date.now = originalDateNow;
    getSignalState_ = originalGetSignalState;
    setSignalState_ = originalSetSignalState;
    appendLogRow_ = originalAppendLogRow;
    setStateValue_ = originalSetStateValue;
    localAndUtcTimestamps_ = originalLocalAndUtcTimestamps;
  }
}

function testStageGenerateSignals_staleGuardUsesProviderDerivedOddsUpdatedTime_() {
  const originalDateNow = Date.now;
  const originalGetSignalState = getSignalState_;
  const originalSetSignalState = setSignalState_;
  const originalSetStateValue = setStateValue_;
  const originalLocalAndUtcTimestamps = localAndUtcTimestamps_;

  const nowMs = Date.parse('2026-01-01T00:10:00.000Z');

  Date.now = function () { return nowMs; };
  getSignalState_ = function () { return { sent_hashes: {} }; };
  setSignalState_ = function () {};
  setStateValue_ = function () {};
  appendLogRow_ = function () {};
  localAndUtcTimestamps_ = function () {
    return {
      local: '2026-01-01T00:10:00-07:00',
      utc: '2026-01-01T07:10:00.000Z',
    };
  };

  try {
    const event = {
      event_id: 'evt_provider_time_stale_guard',
      market: 'h2h',
      outcome: 'player_a',
      bookmaker: 'book_x',
      price: 150,
      commence_time: new Date(nowMs + (5 * 60 * 60 * 1000)),
      odds_updated_time: new Date('2026-01-01T00:05:30.000Z'),
      provider_odds_updated_time: new Date('2026-01-01T00:05:30.000Z'),
    };
    const match = {
      odds_event_id: 'evt_provider_time_stale_guard',
      schedule_event_id: 'sched_provider_time_stale_guard',
      competition_tier: 'WTA_500',
    };
    const stats = {
      evt_provider_time_stale_guard: {
        player_a: {
          has_stats: true,
          features: { ranking: 10, recent_form: 0.6, surface_win_rate: 0.58, hold_pct: 0.64, break_pct: 0.36 },
        },
        player_b: {
          has_stats: true,
          features: { ranking: 12, recent_form: 0.59, surface_win_rate: 0.57, hold_pct: 0.63, break_pct: 0.35 },
        },
      },
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
      STALE_ODDS_WINDOW_MIN: 10,
      NOTIFY_ENABLED: false,
      NOTIFY_TEST_MODE: false,
      DISCORD_WEBHOOK: '',
      SIGNAL_DECISION_SAMPLE_LIMIT: 10,
    };

    const result = stageGenerateSignals('run_provider_time_stale_guard', config, [event], [match], stats);

    assertEquals_(1, result.rows.length);
    assertTrue_(result.rows[0].notification_outcome !== 'stale_odds_skip');
  } finally {
    Date.now = originalDateNow;
    getSignalState_ = originalGetSignalState;
    setSignalState_ = originalSetSignalState;
    setStateValue_ = originalSetStateValue;
    localAndUtcTimestamps_ = originalLocalAndUtcTimestamps;
  }
}

function testStageGenerateSignals_recordsFullyUnmatchedCandidates_() {
  const originalDateNow = Date.now;
  const originalGetSignalState = getSignalState_;
  const originalSetSignalState = setSignalState_;
  const originalSetStateValue = setStateValue_;
  const originalLocalAndUtcTimestamps = localAndUtcTimestamps_;

  const nowMs = Date.parse('2026-01-01T00:00:00.000Z');
  const stateWrites = {};

  Date.now = function () { return nowMs; };
  getSignalState_ = function () { return { sent_hashes: {} }; };
  setSignalState_ = function () {};
  setStateValue_ = function (key, value) { stateWrites[key] = value; };
  localAndUtcTimestamps_ = function () {
    return {
      local: '2026-01-01T00:00:00-07:00',
      utc: '2026-01-01T07:00:00.000Z',
    };
  };

  try {
    const events = [
      {
        event_id: 'evt_missing_match_1', market: 'h2h', outcome: 'player_a', bookmaker: 'book_x', price: 150,
        commence_time: new Date(nowMs + (5 * 60 * 60 * 1000)), odds_updated_time: new Date(nowMs),
      },
      {
        event_id: 'evt_missing_match_2', market: 'h2h', outcome: 'player_b', bookmaker: 'book_x', price: 120,
        commence_time: new Date(nowMs + (6 * 60 * 60 * 1000)), odds_updated_time: new Date(nowMs),
      },
    ];
    const config = {
      MODEL_VERSION: 'test_model_v1', EDGE_THRESHOLD_MICRO: 0.001, EDGE_THRESHOLD_SMALL: 0.03, EDGE_THRESHOLD_MED: 0.05,
      EDGE_THRESHOLD_STRONG: 0.08, STAKE_UNITS_MICRO: 0.25, STAKE_UNITS_SMALL: 0.5, STAKE_UNITS_MED: 1,
      STAKE_UNITS_STRONG: 1.5, SIGNAL_COOLDOWN_MIN: 180, MINUTES_BEFORE_START_CUTOFF: 60, STALE_ODDS_WINDOW_MIN: 60,
      NOTIFY_ENABLED: false, NOTIFY_TEST_MODE: false, DISCORD_WEBHOOK: '', SIGNAL_DECISION_SAMPLE_LIMIT: 10,
    };

    const result = stageGenerateSignals('run_missing_match', config, events, [], {});
    const decisionState = JSON.parse(stateWrites.LAST_SIGNAL_DECISIONS || '{}');

    assertEquals_(0, result.rows.length);
    assertEquals_(2, result.summary.reason_codes.missing_match || 0);
    assertEquals_(2, decisionState.input_count || 0);
    assertEquals_(2, (decisionState.reason_counts && decisionState.reason_counts.missing_match) || 0);
    assertEquals_(2, decisionState.all_drop_reasons || 0);
    assertEquals_(0, decisionState.sent_count || 0);
    assertEquals_(true, !!(decisionState.invariant && decisionState.invariant.sent_plus_drop_reasons_equals_input));
    assertEquals_(2, (decisionState.sampled_candidate_rows || []).length);
    assertEquals_('missing_match', (decisionState.sampled_candidate_rows[0] || {}).decision_reason_code || '');
  } finally {
    Date.now = originalDateNow;
    getSignalState_ = originalGetSignalState;
    setSignalState_ = originalSetSignalState;
    setStateValue_ = originalSetStateValue;
    localAndUtcTimestamps_ = originalLocalAndUtcTimestamps;
  }
}

function testStageGenerateSignals_recordsFullyMissingStatsCandidates_() {
  const originalDateNow = Date.now;
  const originalGetSignalState = getSignalState_;
  const originalSetSignalState = setSignalState_;
  const originalSetStateValue = setStateValue_;
  const originalLocalAndUtcTimestamps = localAndUtcTimestamps_;

  const nowMs = Date.parse('2026-01-01T00:00:00.000Z');
  const stateWrites = {};

  Date.now = function () { return nowMs; };
  getSignalState_ = function () { return { sent_hashes: {} }; };
  setSignalState_ = function () {};
  setStateValue_ = function (key, value) { stateWrites[key] = value; };
  localAndUtcTimestamps_ = function () {
    return {
      local: '2026-01-01T00:00:00-07:00',
      utc: '2026-01-01T07:00:00.000Z',
    };
  };

  try {
    const events = [
      {
        event_id: 'evt_missing_stats_1', market: 'h2h', outcome: 'player_a', bookmaker: 'book_x', price: 150,
        commence_time: new Date(nowMs + (5 * 60 * 60 * 1000)), odds_updated_time: new Date(nowMs),
      },
      {
        event_id: 'evt_missing_stats_2', market: 'h2h', outcome: 'player_b', bookmaker: 'book_x', price: 170,
        commence_time: new Date(nowMs + (7 * 60 * 60 * 1000)), odds_updated_time: new Date(nowMs),
      },
    ];
    const matches = [
      { odds_event_id: 'evt_missing_stats_1', schedule_event_id: 'sched_missing_stats_1', competition_tier: 'WTA_500' },
      { odds_event_id: 'evt_missing_stats_2', schedule_event_id: 'sched_missing_stats_2', competition_tier: 'WTA_500' },
    ];
    const config = {
      MODEL_VERSION: 'test_model_v1', EDGE_THRESHOLD_MICRO: 0.001, EDGE_THRESHOLD_SMALL: 0.03, EDGE_THRESHOLD_MED: 0.05,
      EDGE_THRESHOLD_STRONG: 0.08, STAKE_UNITS_MICRO: 0.25, STAKE_UNITS_SMALL: 0.5, STAKE_UNITS_MED: 1,
      STAKE_UNITS_STRONG: 1.5, SIGNAL_COOLDOWN_MIN: 180, MINUTES_BEFORE_START_CUTOFF: 60, STALE_ODDS_WINDOW_MIN: 60,
      NOTIFY_ENABLED: false, NOTIFY_TEST_MODE: false, DISCORD_WEBHOOK: '', SIGNAL_DECISION_SAMPLE_LIMIT: 10,
    };

    const result = stageGenerateSignals('run_missing_stats', config, events, matches, {});
    const decisionState = JSON.parse(stateWrites.LAST_SIGNAL_DECISIONS || '{}');

    assertEquals_(0, result.rows.length);
    assertEquals_(2, result.summary.reason_codes.missing_stats || 0);
    assertEquals_(2, (decisionState.reason_counts && decisionState.reason_counts.missing_stats) || 0);
    assertEquals_(2, decisionState.all_drop_reasons || 0);
    assertEquals_(0, decisionState.sent_count || 0);
    assertEquals_(true, !!(decisionState.invariant && decisionState.invariant.sent_plus_drop_reasons_equals_input));
    assertEquals_('missing_stats', (decisionState.sampled_candidate_rows[0] || {}).decision_reason_code || '');
  } finally {
    Date.now = originalDateNow;
    getSignalState_ = originalGetSignalState;
    setSignalState_ = originalSetSignalState;
    setStateValue_ = originalSetStateValue;
    localAndUtcTimestamps_ = originalLocalAndUtcTimestamps;
  }
}

function testStageGenerateSignals_scoresNullFeaturesFallbackAndTracksReason_() {
  const originalDateNow = Date.now;
  const originalGetSignalState = getSignalState_;
  const originalSetSignalState = setSignalState_;
  const originalSetStateValue = setStateValue_;
  const originalLocalAndUtcTimestamps = localAndUtcTimestamps_;

  const nowMs = Date.parse('2026-01-01T00:00:00.000Z');
  const stateWrites = {};

  Date.now = function () { return nowMs; };
  getSignalState_ = function () { return { sent_hashes: {} }; };
  setSignalState_ = function () {};
  setStateValue_ = function (key, value) { stateWrites[key] = value; };
  localAndUtcTimestamps_ = function () {
    return {
      local: '2026-01-01T00:00:00-07:00',
      utc: '2026-01-01T07:00:00.000Z',
    };
  };

  try {
    const events = [{
      event_id: 'evt_null_features_1', market: 'h2h', outcome: 'player_a', bookmaker: 'book_x', price: 150,
      commence_time: new Date(nowMs + (5 * 60 * 60 * 1000)), odds_updated_time: new Date(nowMs),
    }];
    const matches = [
      { odds_event_id: 'evt_null_features_1', schedule_event_id: 'sched_null_features_1', competition_tier: 'WTA_500' },
    ];
    const stats = {
      evt_null_features_1: {
        stats_fallback_mode: 'null_features',
        player_a: {
          has_stats: false,
          stats_fallback_mode: 'null_features',
          features: { ranking: null, recent_form: null, surface_win_rate: null, hold_pct: null, break_pct: null },
        },
        player_b: {
          has_stats: false,
          stats_fallback_mode: 'null_features',
          features: { ranking: null, recent_form: null, surface_win_rate: null, hold_pct: null, break_pct: null },
        },
      },
    };
    const config = {
      MODEL_VERSION: 'test_model_v1', EDGE_THRESHOLD_MICRO: 0.001, EDGE_THRESHOLD_SMALL: 0.03, EDGE_THRESHOLD_MED: 0.05,
      EDGE_THRESHOLD_STRONG: 0.08, STAKE_UNITS_MICRO: 0.25, STAKE_UNITS_SMALL: 0.5, STAKE_UNITS_MED: 1,
      STAKE_UNITS_STRONG: 1.5, SIGNAL_COOLDOWN_MIN: 180, MINUTES_BEFORE_START_CUTOFF: 60, STALE_ODDS_WINDOW_MIN: 60,
      NOTIFY_ENABLED: false, NOTIFY_TEST_MODE: false, DISCORD_WEBHOOK: '', SIGNAL_DECISION_SAMPLE_LIMIT: 10,
    };

    const result = stageGenerateSignals('run_null_features_fallback', config, events, matches, stats);
    const decisionState = JSON.parse(stateWrites.LAST_SIGNAL_DECISIONS || '{}');

    assertEquals_(1, result.rows.length);
    assertEquals_(0, result.summary.reason_codes.missing_stats || 0);
    assertEquals_(1, result.summary.reason_codes.null_features_fallback_scored || 0);
    assertTrue_((decisionState.sampled_candidate_rows[0] || {}).decision_reason_code !== 'missing_stats', 'null-feature fallback should not be dropped as missing stats');
  } finally {
    Date.now = originalDateNow;
    getSignalState_ = originalGetSignalState;
    setSignalState_ = originalSetSignalState;
    setStateValue_ = originalSetStateValue;
    localAndUtcTimestamps_ = originalLocalAndUtcTimestamps;
  }
}


function testStageGenerateSignals_recordsMixedUpstreamOutcomesAndDecisionSamples_() {
  const originalDateNow = Date.now;
  const originalGetSignalState = getSignalState_;
  const originalSetSignalState = setSignalState_;
  const originalSetStateValue = setStateValue_;
  const originalLocalAndUtcTimestamps = localAndUtcTimestamps_;

  const nowMs = Date.parse('2026-01-01T00:00:00.000Z');
  const stateWrites = {};

  Date.now = function () { return nowMs; };
  getSignalState_ = function () { return { sent_hashes: {} }; };
  setSignalState_ = function () {};
  setStateValue_ = function (key, value) { stateWrites[key] = value; };
  localAndUtcTimestamps_ = function () {
    return {
      local: '2026-01-01T00:00:00-07:00',
      utc: '2026-01-01T07:00:00.000Z',
    };
  };

  try {
    const events = [
      { event_id: 'evt_mixed_missing_match', market: 'h2h', outcome: 'player_a', bookmaker: 'book_x', price: 150, commence_time: new Date(nowMs + (4 * 60 * 60 * 1000)), odds_updated_time: new Date(nowMs) },
      { event_id: 'evt_mixed_missing_stats', market: 'h2h', outcome: 'player_b', bookmaker: 'book_x', price: 180, commence_time: new Date(nowMs + (5 * 60 * 60 * 1000)), odds_updated_time: new Date(nowMs) },
      { event_id: 'evt_mixed_invalid_features', market: 'h2h', outcome: 'player_a', bookmaker: 'book_x', price: null, commence_time: new Date(nowMs + (6 * 60 * 60 * 1000)), odds_updated_time: new Date(nowMs) },
    ];
    const matches = [
      { odds_event_id: 'evt_mixed_missing_stats', schedule_event_id: 'sched_mixed_missing_stats', competition_tier: 'WTA_500' },
      { odds_event_id: 'evt_mixed_invalid_features', schedule_event_id: 'sched_mixed_invalid_features', competition_tier: 'WTA_500' },
    ];
    const stats = {
      evt_mixed_invalid_features: {
        player_a: { has_stats: true, features: { ranking: 10, recent_form: 0.6, surface_win_rate: 0.58, hold_pct: 0.64, break_pct: 0.36 } },
        player_b: { has_stats: true, features: { ranking: 12, recent_form: 0.59, surface_win_rate: 0.57, hold_pct: 0.63, break_pct: 0.35 } },
      },
    };
    const config = {
      MODEL_VERSION: 'test_model_v1', EDGE_THRESHOLD_MICRO: 0.001, EDGE_THRESHOLD_SMALL: 0.03, EDGE_THRESHOLD_MED: 0.05,
      EDGE_THRESHOLD_STRONG: 0.08, STAKE_UNITS_MICRO: 0.25, STAKE_UNITS_SMALL: 0.5, STAKE_UNITS_MED: 1,
      STAKE_UNITS_STRONG: 1.5, SIGNAL_COOLDOWN_MIN: 180, MINUTES_BEFORE_START_CUTOFF: 60, STALE_ODDS_WINDOW_MIN: 60,
      NOTIFY_ENABLED: false, NOTIFY_TEST_MODE: false, DISCORD_WEBHOOK: '', SIGNAL_DECISION_SAMPLE_LIMIT: 10,
    };

    const result = stageGenerateSignals('run_mixed_diagnostics', config, events, matches, stats);
    const decisionState = JSON.parse(stateWrites.LAST_SIGNAL_DECISIONS || '{}');

    assertEquals_(0, result.rows.length);
    assertEquals_(1, result.summary.reason_codes.missing_match || 0);
    assertEquals_(1, result.summary.reason_codes.missing_stats || 0);
    assertEquals_(1, result.summary.reason_codes.invalid_features || 0);
    assertEquals_(3, decisionState.all_drop_reasons || 0);
    assertEquals_(3, (decisionState.sampled_candidate_rows || []).length);
    assertEquals_('missing_match', (decisionState.sampled_candidate_rows[0] || {}).decision_reason_code || '');
    assertEquals_('missing_stats', (decisionState.sampled_candidate_rows[1] || {}).decision_reason_code || '');
    assertEquals_('invalid_features', (decisionState.sampled_candidate_rows[2] || {}).decision_reason_code || '');
    assertEquals_(true, !!(decisionState.invariant && decisionState.invariant.sent_plus_drop_reasons_equals_input));
  } finally {
    Date.now = originalDateNow;
    getSignalState_ = originalGetSignalState;
    setSignalState_ = originalSetSignalState;
    setStateValue_ = originalSetStateValue;
    localAndUtcTimestamps_ = originalLocalAndUtcTimestamps;
  }
}


function testStageGenerateSignals_zeroInputIncludesUpstreamGateReason_() {
  const originalSetSignalState = setSignalState_;
  const originalGetSignalState = getSignalState_;
  const originalSetStateValue = setStateValue_;
  const originalLocalAndUtcTimestamps = localAndUtcTimestamps_;

  const stateWrites = {};
  getSignalState_ = function () { return { sent_hashes: {} }; };
  setSignalState_ = function () {};
  setStateValue_ = function (key, value) { stateWrites[key] = value; };
  localAndUtcTimestamps_ = function () {
    return {
      local: '2026-01-01T00:00:00-07:00',
      utc: '2026-01-01T07:00:00.000Z',
    };
  };

  try {
    const config = {
      MODEL_VERSION: 'test_model_v1', EDGE_THRESHOLD_MICRO: 0.001, EDGE_THRESHOLD_SMALL: 0.03, EDGE_THRESHOLD_MED: 0.05,
      EDGE_THRESHOLD_STRONG: 0.08, STAKE_UNITS_MICRO: 0.25, STAKE_UNITS_SMALL: 0.5, STAKE_UNITS_MED: 1,
      STAKE_UNITS_STRONG: 1.5, SIGNAL_COOLDOWN_MIN: 180, MINUTES_BEFORE_START_CUTOFF: 60, STALE_ODDS_WINDOW_MIN: 60,
      NOTIFY_ENABLED: false, NOTIFY_TEST_MODE: false, DISCORD_WEBHOOK: '', SIGNAL_DECISION_SAMPLE_LIMIT: 10,
    };
    const upstreamGateReason = deriveSignalUpstreamGateReason_(
      { events: [], summary: { reason_codes: { odds_refresh_skipped_outside_window: 1 } } },
      { summary: { reason_codes: {} } },
      { matchedCount: 0, summary: { reason_codes: { schedule_seed_no_odds: 1 } } }
    );

    const result = stageGenerateSignals('run_zero_input', config, [], [], {}, {
      upstream_gate_reason: upstreamGateReason.reason,
      upstream_gate_inputs: upstreamGateReason.inputs,
    });
    const decisionState = JSON.parse(stateWrites.LAST_SIGNAL_DECISIONS || '{}');

    assertEquals_(0, result.rows.length);
    assertEquals_(0, result.summary.input_count || 0);
    assertEquals_('odds_refresh_skipped_outside_window', (result.summary.reason_metadata || {}).upstream_gate_reason || '');
    assertEquals_('odds_refresh_skipped_outside_window', decisionState.upstream_gate_reason || '');
    assertEquals_('odds_refresh_skipped_outside_window', ((decisionState.explanatory_metadata || {}).upstream_gate_reason || ''));
    assertEquals_('odds_refresh_skipped_outside_window', (((decisionState.explanatory_metadata || {}).upstream_gate_inputs || {}).odds_stage_reason || ''));
    assertEquals_(true, !!(((decisionState.invariant || {}).zero_input_has_explanatory_metadata)));
  } finally {
    setSignalState_ = originalSetSignalState;
    getSignalState_ = originalGetSignalState;
    setStateValue_ = originalSetStateValue;
    localAndUtcTimestamps_ = originalLocalAndUtcTimestamps;
  }
}


function testDeriveSignalUpstreamGateReason_prioritizesConcreteUpstreamStageReason_() {
  const reason = deriveSignalUpstreamGateReason_(
    { events: [], summary: { reason_codes: { source_credit_saver_skip: 1 } } },
    { summary: { reason_codes: { schedule_fetch_skipped_outside_window_credit_saver: 1 } } },
    { matchedCount: 0, summary: { reason_codes: { schedule_seed_no_odds: 1 } } },
    { summary: { reason_metadata: { players_with_non_null_stats: 0 } } }
  );

  assertEquals_('source_credit_saver_skip', reason.reason || '');
  assertEquals_('source_credit_saver_skip', (reason.inputs || {}).odds_stage_reason || '');
  assertEquals_('schedule_fetch_skipped_outside_window_credit_saver', (reason.inputs || {}).schedule_stage_reason || '');
  assertEquals_('schedule_seed_no_odds', (reason.inputs || {}).match_stage_reason || '');
}

function testStageGenerateSignals_thresholdRejectionIncludedInDecisionDiagnostics_() {
  const originalDateNow = Date.now;
  const originalGetSignalState = getSignalState_;
  const originalSetSignalState = setSignalState_;
  const originalSetStateValue = setStateValue_;
  const originalLocalAndUtcTimestamps = localAndUtcTimestamps_;

  const nowMs = Date.parse('2026-01-01T00:00:00.000Z');
  const stateWrites = {};

  Date.now = function () { return nowMs; };
  getSignalState_ = function () { return { sent_hashes: {} }; };
  setSignalState_ = function () {};
  setStateValue_ = function (key, value) { stateWrites[key] = value; };
  localAndUtcTimestamps_ = function () {
    return {
      local: '2026-01-01T00:00:00-07:00',
      utc: '2026-01-01T07:00:00.000Z',
    };
  };

  try {
    const event = {
      event_id: 'evt_threshold_reject',
      market: 'h2h',
      outcome: 'player_a',
      bookmaker: 'book_x',
      price: 1.95,
      commence_time: new Date(nowMs + (5 * 60 * 60 * 1000)),
      odds_updated_time: new Date(nowMs),
    };
    const match = {
      odds_event_id: 'evt_threshold_reject',
      schedule_event_id: 'sched_threshold_reject',
      competition_tier: 'WTA_500',
    };
    const stats = {
      evt_threshold_reject: {
        player_a: {
          has_stats: true,
          features: { ranking: 10, recent_form: 0.6, surface_win_rate: 0.58, hold_pct: 0.64, break_pct: 0.36 },
        },
        player_b: {
          has_stats: true,
          features: { ranking: 12, recent_form: 0.59, surface_win_rate: 0.57, hold_pct: 0.63, break_pct: 0.35 },
        },
      },
    };
    const config = {
      MODEL_VERSION: 'test_model_v1',
      EDGE_THRESHOLD_MICRO: 0.2,
      EDGE_THRESHOLD_SMALL: 0.3,
      EDGE_THRESHOLD_MED: 0.4,
      EDGE_THRESHOLD_STRONG: 0.5,
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
      SIGNAL_DECISION_SAMPLE_LIMIT: 10,
    };

    const result = stageGenerateSignals('run_threshold_reject', config, [event], [match], stats);
    const decisionState = JSON.parse(stateWrites.LAST_SIGNAL_DECISIONS || '{}');
    const decisionRow = (decisionState.sampled_decisions || [])[0] || {};

    assertEquals_(1, result.rows.length);
    assertEquals_('edge_below_threshold', result.rows[0].notification_outcome);
    assertEquals_(1, result.summary.reason_codes.edge_below_threshold || 0);
    assertEquals_(1, decisionState.processed_count || 0);
    assertEquals_(1, (decisionState.reason_counts && decisionState.reason_counts.edge_below_threshold) || 0);
    assertEquals_('edge_below_threshold', decisionRow.decision_reason_code || '');
    assertEquals_(true, !!(decisionRow.detail && decisionRow.detail.scored));
  } finally {
    Date.now = originalDateNow;
    getSignalState_ = originalGetSignalState;
    setSignalState_ = originalSetSignalState;
    setStateValue_ = originalSetStateValue;
    localAndUtcTimestamps_ = originalLocalAndUtcTimestamps;
  }
}



function testStageGenerateSignals_richPlayerStatsInfluenceEdgeAndTier_() {
  const originalDateNow = Date.now;
  const originalGetSignalState = getSignalState_;
  const originalSetSignalState = setSignalState_;
  const originalSetStateValue = setStateValue_;
  const originalLocalAndUtcTimestamps = localAndUtcTimestamps_;

  const nowMs = Date.parse('2026-01-01T00:00:00.000Z');

  Date.now = function () { return nowMs; };
  getSignalState_ = function () { return { sent_hashes: {} }; };
  setSignalState_ = function () {};
  setStateValue_ = function () {};
  appendLogRow_ = function () {};
  localAndUtcTimestamps_ = function () {
    return {
      local: '2026-01-01T00:00:00-07:00',
      utc: '2026-01-01T07:00:00.000Z',
    };
  };

  try {
    const events = [
      {
        event_id: 'evt_edge_baseline',
        market: 'h2h',
        outcome: 'player_a',
        bookmaker: 'book_x',
        price: 150,
        commence_time: new Date(nowMs + (5 * 60 * 60 * 1000)),
        odds_updated_time: new Date(nowMs),
      },
      {
        event_id: 'evt_edge_rich',
        market: 'h2h',
        outcome: 'player_a',
        bookmaker: 'book_x',
        price: 150,
        commence_time: new Date(nowMs + (5 * 60 * 60 * 1000)),
        odds_updated_time: new Date(nowMs),
      },
    ];
    const matches = [
      { odds_event_id: 'evt_edge_baseline', schedule_event_id: 'sched_edge_baseline', competition_tier: 'WTA_500' },
      { odds_event_id: 'evt_edge_rich', schedule_event_id: 'sched_edge_rich', competition_tier: 'WTA_500' },
    ];
    const baselineFeaturesA = { ranking: 10, recent_form: 0.6, surface_win_rate: 0.58, hold_pct: 0.64, break_pct: 0.36 };
    const baselineFeaturesB = { ranking: 12, recent_form: 0.59, surface_win_rate: 0.57, hold_pct: 0.63, break_pct: 0.35 };
    const stats = {
      evt_edge_baseline: {
        player_a: { has_stats: true, features: baselineFeaturesA },
        player_b: { has_stats: true, features: baselineFeaturesB },
      },
      evt_edge_rich: {
        player_a: {
          has_stats: true,
          features: {
            ranking: 10,
            recent_form: 0.6,
            surface_win_rate: 0.58,
            hold_pct: 0.64,
            break_pct: 0.36,
            first_serve_in_pct: 0.69,
            first_serve_points_won_pct: 0.74,
            second_serve_points_won_pct: 0.56,
            return_points_won_pct: 0.45,
            bp_saved_pct: 0.67,
            bp_conv_pct: 0.46,
            dr: 1.18,
            tpw_pct: 0.54,
          },
        },
        player_b: {
          has_stats: true,
          features: {
            ranking: 12,
            recent_form: 0.59,
            surface_win_rate: 0.57,
            hold_pct: 0.63,
            break_pct: 0.35,
            first_serve_in_pct: 0.57,
            first_serve_points_won_pct: 0.65,
            second_serve_points_won_pct: 0.48,
            return_points_won_pct: 0.37,
            bp_saved_pct: 0.56,
            bp_conv_pct: 0.36,
            dr: 0.95,
            tpw_pct: 0.48,
          },
        },
      },
    };
    const config = {
      MODEL_VERSION: 'test_model_v1',
      EDGE_THRESHOLD_MICRO: 0.02,
      EDGE_THRESHOLD_SMALL: 0.03,
      EDGE_THRESHOLD_MED: 0.08,
      EDGE_THRESHOLD_STRONG: 0.12,
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
      SIGNAL_DECISION_SAMPLE_LIMIT: 10,
    };

    const result = stageGenerateSignals('run_rich_stats_impact', config, events, matches, stats);
    const baselineRow = (result.rows || []).filter(function (row) { return row.odds_event_id === 'evt_edge_baseline'; })[0] || {};
    const richRow = (result.rows || []).filter(function (row) { return row.odds_event_id === 'evt_edge_rich'; })[0] || {};

    assertEquals_(2, result.rows.length);
    assertEquals_('MICRO', baselineRow.edge_tier || '');
    assertEquals_('SMALL', richRow.edge_tier || '');
    assertTrue_(Number(richRow.edge_value || 0) > Number(baselineRow.edge_value || 0), 'rich stats should increase edge value versus baseline stats');
  } finally {
    Date.now = originalDateNow;
    getSignalState_ = originalGetSignalState;
    setSignalState_ = originalSetSignalState;
    setStateValue_ = originalSetStateValue;
    localAndUtcTimestamps_ = originalLocalAndUtcTimestamps;
  }
}

function testStageGenerateSignals_surfaceSpecificDeltasShiftEdgeDirection_() {
  const originalDateNow = Date.now;
  const originalGetSignalState = getSignalState_;
  const originalSetSignalState = setSignalState_;
  const originalSetStateValue = setStateValue_;
  const originalLocalAndUtcTimestamps = localAndUtcTimestamps_;

  const nowMs = Date.parse('2026-01-01T00:00:00.000Z');
  Date.now = function () { return nowMs; };
  getSignalState_ = function () { return { sent_hashes: {} }; };
  setSignalState_ = function () {};
  setStateValue_ = function () {};
  localAndUtcTimestamps_ = function () {
    return {
      local: '2026-01-01T00:00:00-07:00',
      utc: '2026-01-01T07:00:00.000Z',
    };
  };

  try {
    const event = {
      event_id: 'evt_surface_direction',
      market: 'h2h',
      outcome: 'player_a',
      bookmaker: 'book_x',
      price: 150,
      commence_time: new Date(nowMs + (5 * 60 * 60 * 1000)),
      odds_updated_time: new Date(nowMs),
    };
    const match = { odds_event_id: 'evt_surface_direction', schedule_event_id: 'sched_surface_direction', competition_tier: 'WTA_500' };
    const baselineA = { ranking: 10, recent_form: 0.6, surface_win_rate: 0.58, hold_pct: 0.64, break_pct: 0.36 };
    const baselineB = { ranking: 12, recent_form: 0.59, surface_win_rate: 0.57, hold_pct: 0.63, break_pct: 0.35 };

    const baseline = stageGenerateSignals('run_surface_baseline', {
      MODEL_VERSION: 'test_model_v1',
      EDGE_THRESHOLD_MICRO: 0,
      EDGE_THRESHOLD_SMALL: 0.03,
      EDGE_THRESHOLD_MED: 0.08,
      EDGE_THRESHOLD_STRONG: 0.12,
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
      SIGNAL_DECISION_SAMPLE_LIMIT: 10,
    }, [event], [match], {
      evt_surface_direction: {
        player_a: { has_stats: true, features: baselineA },
        player_b: { has_stats: true, features: baselineB },
      },
    });

    const withSurfaceProfiles = stageGenerateSignals('run_surface_profiles', {
      MODEL_VERSION: 'test_model_v1',
      EDGE_THRESHOLD_MICRO: 0,
      EDGE_THRESHOLD_SMALL: 0.03,
      EDGE_THRESHOLD_MED: 0.08,
      EDGE_THRESHOLD_STRONG: 0.12,
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
      SIGNAL_DECISION_SAMPLE_LIMIT: 10,
    }, [event], [match], {
      evt_surface_direction: {
        player_a: { has_stats: true, features: Object.assign({}, baselineA, {
          recent_form_last_10: 0.72,
          surface_hold_pct: 0.7,
          surface_break_pct: 0.4,
          surface_recent_form: 0.71,
        }) },
        player_b: { has_stats: true, features: Object.assign({}, baselineB, {
          recent_form_last_10: 0.45,
          surface_hold_pct: 0.56,
          surface_break_pct: 0.25,
          surface_recent_form: 0.43,
        }) },
      },
    });

    const baselineEdge = Number((baseline.rows[0] || {}).edge_value || 0);
    const surfaceEdge = Number((withSurfaceProfiles.rows[0] || {}).edge_value || 0);
    assertTrue_(surfaceEdge > baselineEdge, 'opposing surface profiles should increase edge toward player_a');
  } finally {
    Date.now = originalDateNow;
    getSignalState_ = originalGetSignalState;
    setSignalState_ = originalSetSignalState;
    setStateValue_ = originalSetStateValue;
    localAndUtcTimestamps_ = originalLocalAndUtcTimestamps;
  }
}


function testCombinePlayerStatsFeatureBump_fullFeatureBundlePreservesRawBehavior_() {
  const reasonCodes = {};
  const bundle = {
    stats_confidence: 1,
    player_a: {
      has_stats: true,
      features: {
        ranking: 8,
        recent_form: 0.66,
        recent_form_last_10: 0.68,
        surface_win_rate: 0.62,
        hold_pct: 0.69,
        break_pct: 0.38,
        surface_hold_pct: 0.71,
        surface_break_pct: 0.4,
        surface_recent_form: 0.67,
        first_serve_in_pct: 0.67,
        first_serve_points_won_pct: 0.73,
        second_serve_points_won_pct: 0.55,
        return_points_won_pct: 0.44,
        bp_saved_pct: 0.66,
        bp_conv_pct: 0.47,
        dr: 1.15,
        tpw_pct: 0.53,
      },
    },
    player_b: {
      has_stats: true,
      features: {
        ranking: 16,
        recent_form: 0.57,
        recent_form_last_10: 0.55,
        surface_win_rate: 0.54,
        hold_pct: 0.62,
        break_pct: 0.32,
        surface_hold_pct: 0.63,
        surface_break_pct: 0.31,
        surface_recent_form: 0.52,
        first_serve_in_pct: 0.61,
        first_serve_points_won_pct: 0.66,
        second_serve_points_won_pct: 0.48,
        return_points_won_pct: 0.39,
        bp_saved_pct: 0.58,
        bp_conv_pct: 0.37,
        dr: 0.97,
        tpw_pct: 0.48,
      },
    },
  };

  const expectedRawBump = roundNumber_(
    ((((bundle.player_b.features.ranking || 0) - (bundle.player_a.features.ranking || 0)) / 300) * 0.2)
    + (((bundle.player_a.features.recent_form || 0) - (bundle.player_b.features.recent_form || 0)) * 0.17)
    + (((bundle.player_a.features.recent_form_last_10 || 0) - (bundle.player_b.features.recent_form_last_10 || 0)) * 0.03)
    + (((bundle.player_a.features.surface_win_rate || 0) - (bundle.player_b.features.surface_win_rate || 0)) * 0.14)
    + ((((bundle.player_a.features.hold_pct || 0) - (bundle.player_b.features.hold_pct || 0)) + ((bundle.player_a.features.break_pct || 0) - (bundle.player_b.features.break_pct || 0))) * 0.13)
    + ((((bundle.player_a.features.surface_hold_pct || 0) - (bundle.player_b.features.surface_hold_pct || 0)) + ((bundle.player_a.features.surface_break_pct || 0) - (bundle.player_b.features.surface_break_pct || 0))) * 0.02)
    + (((bundle.player_a.features.surface_recent_form || 0) - (bundle.player_b.features.surface_recent_form || 0)) * 0.01)
    + (((bundle.player_a.features.first_serve_in_pct || 0) - (bundle.player_b.features.first_serve_in_pct || 0)) * 0.07)
    + (((bundle.player_a.features.first_serve_points_won_pct || 0) - (bundle.player_b.features.first_serve_points_won_pct || 0)) * 0.07)
    + (((bundle.player_a.features.second_serve_points_won_pct || 0) - (bundle.player_b.features.second_serve_points_won_pct || 0)) * 0.06)
    + (((bundle.player_a.features.return_points_won_pct || 0) - (bundle.player_b.features.return_points_won_pct || 0)) * 0.04)
    + (((bundle.player_a.features.bp_saved_pct || 0) - (bundle.player_b.features.bp_saved_pct || 0)) * 0.03)
    + (((bundle.player_a.features.bp_conv_pct || 0) - (bundle.player_b.features.bp_conv_pct || 0)) * 0.02)
    + (((bundle.player_a.features.dr || 0) - (bundle.player_b.features.dr || 0)) * 0.005)
    + (((bundle.player_a.features.tpw_pct || 0) - (bundle.player_b.features.tpw_pct || 0)) * 0.005),
    4
  );

  const actualBump = combinePlayerStatsFeatureBump_(bundle, reasonCodes);

  assertEquals_(expectedRawBump, actualBump);
  assertEquals_(1, reasonCodes.full_confidence_stats_scored || 0);
}

function testCombinePlayerStatsFeatureBump_nullHeavyBundleDampensInsteadOfDropping_() {
  const reasonCodes = {};
  const bundle = {
    player_a: {
      has_stats: true,
      features: {
        ranking: 12,
        recent_form: 0.64,
        surface_win_rate: 0.6,
        hold_pct: 0.67,
        break_pct: 0.36,
      },
    },
    player_b: {
      has_stats: true,
      features: {
        ranking: 25,
        recent_form: 0.55,
        surface_win_rate: 0.52,
        hold_pct: 0.61,
        break_pct: 0.31,
      },
    },
  };

  const dampened = combinePlayerStatsFeatureBump_(bundle, reasonCodes);
  const raw = combinePlayerStatsFeatureBump_(Object.assign({}, bundle, { stats_confidence: 1 }), {});

  assertTrue_(dampened > 0, 'null-heavy bundle should still contribute non-zero bump');
  assertTrue_(dampened < raw, 'null-heavy bundle bump should be dampened below full-confidence equivalent');
  assertTrue_((reasonCodes.low_confidence_stats_scored || 0) >= 1, 'low-confidence path should increment reason counts');
}


function testGetConfig_allowWta250Missing_usesDefaultTrue_() {
  const originalGetActiveSpreadsheet = SpreadsheetApp.getActiveSpreadsheet;

  SpreadsheetApp.getActiveSpreadsheet = function () {
    return {
      getSheetByName: function () {
        return {
          getDataRange: function () {
            return {
              getValues: function () {
                return [
                  ['key', 'value'],
                  ['RUN_ENABLED', 'true'],
                  ['ALLOW_WTA_125', 'false'],
                ];
              },
            };
          },
        };
      },
    };
  };

  try {
    const config = getConfig_();
    assertEquals_(true, config.ALLOW_WTA_250);
  } finally {
    SpreadsheetApp.getActiveSpreadsheet = originalGetActiveSpreadsheet;
  }
}

function testStageFetchSchedule_staleWithEvents_keepsCachedStaleFallbackSource_() {
  const result = runStageFetchScheduleScenario_({
    cacheResult: {
      cached_at_ms: Date.parse('2025-03-01T00:00:00.000Z'),
      events: [],
    },
    stalePayload: {
      event_count: 2,
      events: [{ event_id: 'evt_stale_1' }, { event_id: 'evt_stale_2' }],
    },
    fetchResponses: [],
  });

  assertEquals_('cached_stale_fallback', result.stage.selected_source);
  assertEquals_(0, result.fetchCalls.length);
  assertEquals_(0, result.lastMeta.live_fetch_happened ? 1 : 0);
  assertEquals_(0, result.lastMeta.stale_fallback_empty_forced_live ? 1 : 0);
  assertEquals_(0, result.creditHeadersCaptured.length);
}

function testStageFetchSchedule_staleEmpty_forcesLiveAndPersistsFlagsOnSuccess_() {
  const liveHeaders = {
    x_requests_used: '10',
    x_requests_remaining: '490',
  };
  const result = runStageFetchScheduleScenario_({
    cacheResult: {
      cached_at_ms: Date.parse('2025-03-01T00:00:00.000Z'),
      events: [],
    },
    stalePayload: {
      event_count: 0,
      events: [],
    },
    fetchResponses: [{
      events: [],
      reason_code: 'schedule_api_success',
      api_credit_usage: 1,
      api_call_count: 1,
      credit_headers: liveHeaders,
    }],
  });

  assertEquals_('fresh_api', result.stage.selected_source);
  assertEquals_(1, result.fetchCalls.length);
  assertEquals_(1, result.lastMeta.live_fetch_happened ? 1 : 0);
  assertEquals_(1, result.lastMeta.stale_fallback_empty_forced_live ? 1 : 0);
  assertEquals_(1, result.creditHeadersCaptured.length);
  assertEquals_('490', String(result.creditHeadersCaptured[0].x_requests_remaining || ''));
}

function testStageFetchSchedule_staleEmpty_forcesLiveAndPersistsFlagsOnLiveEmpty_() {
  const liveHeaders = {
    x_requests_used: '11',
    x_requests_remaining: '489',
  };
  const result = runStageFetchScheduleScenario_({
    cacheResult: {
      cached_at_ms: Date.parse('2025-03-01T00:00:00.000Z'),
      events: [],
    },
    stalePayload: {
      event_count: 0,
      events: [],
    },
    fetchResponses: [{
      events: [],
      reason_code: 'schedule_no_games_in_window',
      api_credit_usage: 1,
      api_call_count: 1,
      credit_headers: liveHeaders,
    }],
  });

  assertEquals_('fresh_api', result.stage.selected_source);
  assertEquals_(1, result.stage.summary.reason_codes.schedule_no_games_in_window || 0);
  assertEquals_(1, result.fetchCalls.length);
  assertEquals_(1, result.lastMeta.live_fetch_happened ? 1 : 0);
  assertEquals_(1, result.lastMeta.stale_fallback_empty_forced_live ? 1 : 0);
  assertEquals_(1, result.creditHeadersCaptured.length);
  assertEquals_('489', String(result.creditHeadersCaptured[0].x_requests_remaining || ''));
}

function testStageFetchSchedule_invalidWindowMismatch_autoExpandsOnce_() {
  const oddsEvents = [{
    event_id: 'odds_1',
    commence_time: new Date('2025-03-01T03:00:00.000Z'),
    player_1: 'Player One',
    player_2: 'Player Two',
  }];
  const scheduleEvents = [{
    event_id: 'sched_1',
    match_id: 'sched_1',
    start_time: new Date('2025-03-01T03:00:00.000Z'),
    competition: 'WTA 500',
    player_1: 'Player One',
    player_2: 'Player Two',
  }];
  const result = runStageFetchScheduleScenario_({
    oddsEvents: oddsEvents,
    fetchResponses: [{
      events: [],
      reason_code: 'invalid_time_window',
      api_credit_usage: 1,
      api_call_count: 1,
      credit_headers: { x_requests_remaining: '490' },
    }, {
      events: scheduleEvents,
      reason_code: 'schedule_api_success',
      api_credit_usage: 1,
      api_call_count: 1,
      credit_headers: { x_requests_remaining: '489' },
    }],
  });

  assertEquals_(2, result.fetchCalls.length);
  assertEquals_(1, result.stage.summary.reason_codes.odds_schedule_window_mismatch_high_severity || 0);
  assertEquals_(1, result.stage.summary.reason_codes.invalid_time_window_auto_expanded || 0);
  assertEquals_(1, result.stage.summary.reason_codes.expanded_window_fallback_used || 0);
}


function testStageFetchSchedule_h2hMissingSummarySeparatesExpectedVsPipelineFailures_() {
  const result = runStageFetchScheduleScenario_({
    fetchResponses: [{
      events: [],
      reason_code: 'schedule_api_success',
      api_credit_usage: 1,
      api_call_count: 1,
      credit_headers: { x_requests_remaining: '490' },
    }],
    enrichmentResult: {
      events: [],
      reason_code: 'schedule_enrichment_ta_completed',
      stats_reason_code: 'ta_leaders_ok',
      h2h_reason_code: '',
      h2h_impact: null,
      canonical_player_count: 2,
      stats_rows_applied: 0,
      h2h_rows_applied: 0,
      h2h_missing: 3,
      h2h_pairs_requested: 3,
      h2h_pairs_found: 0,
      h2h_missing_reason_codes: {
        h2h_player_not_in_matrix: 1,
        ta_h2h_fetch_failed: 1,
        h2h_missing: 1,
      },
      h2h_missing_classification: {
        expected_missing_source_coverage: 1,
        pipeline_failure: 1,
        source_partial_coverage: 0,
        source_dataset_unavailable: 0,
        invalid_h2h_pair: 0,
        generic_h2h_missing: 1,
        unclassified: 0,
      },
      failed: false,
      error: '',
    },
  });

  assertEquals_(1, result.stage.summary.reason_codes.schedule_enrichment_h2h_missing_expected_source_coverage || 0);
  assertEquals_(1, result.stage.summary.reason_codes.schedule_enrichment_h2h_missing_pipeline_failure || 0);
  assertEquals_(1, result.stage.summary.reason_codes.schedule_enrichment_h2h_missing_generic_h2h_missing || 0);
  assertEquals_(0, result.stage.summary.reason_codes.schedule_enrichment_h2h_missing_unclassified || 0);
}

function testBuildCanonicalSchedulePlayers_filtersPastEventsAndDedupes_() {
  const nowMs = Date.parse('2025-03-01T00:00:00.000Z');
  const players = buildCanonicalSchedulePlayers_([
    {
      start_time: new Date('2025-02-28T23:59:59.000Z'),
      player_1: 'Past A',
      player_2: 'Past B',
    },
    {
      start_time: new Date('2025-03-01T01:00:00.000Z'),
      player_1: 'Future One',
      player_2: 'Future Two',
    },
    {
      start_time: new Date('2025-03-01T02:00:00.000Z'),
      player_1: 'Future One',
      player_2: 'Future Three',
    },
  ], nowMs);

  assertArrayEquals_(['future one', 'future two', 'future three'], players);
}

function testEnrichScheduleEventsFromTennisAbstract_nonFatalOnFailure_() {
  const originalFetchPlayerStatsBatch = fetchPlayerStatsBatch_;

  fetchPlayerStatsBatch_ = function () {
    throw new Error('stats provider blew up');
  };

  try {
    const event = {
      event_id: 'sched_1',
      start_time: new Date('2025-03-01T03:00:00.000Z'),
      player_1: 'Player One',
      player_2: 'Player Two',
    };
    const result = enrichScheduleEventsFromTennisAbstract_({}, [event]);

    assertEquals_('schedule_enrichment_ta_failed_non_fatal', result.reason_code);
    assertEquals_(1, result.events.length);
    assertEquals_('sched_1', result.events[0].event_id);
    assertEquals_(true, result.failed);
    assertTrue_(String(result.error || '').indexOf('stats provider blew up') >= 0, 'error should be captured');
  } finally {
    fetchPlayerStatsBatch_ = originalFetchPlayerStatsBatch;
  }
}

function testEnrichScheduleEventsFromTennisAbstract_h2hEmptyTableAddsReasonAndImpact_() {
  const originalFetchPlayerStatsBatch = fetchPlayerStatsBatch_;
  const originalGetStateJson = getStateJson_;
  const originalGetTaH2hRowForCanonicalPair = getTaH2hRowForCanonicalPair_;

  fetchPlayerStatsBatch_ = function () {
    return { stats_by_player: {}, reason_code: 'ta_leaders_ok', source: 'ta_leaders' };
  };
  getStateJson_ = function (key) {
    if (key === 'PLAYER_STATS_H2H_LAST_FETCH_META') {
      return {
        last_failure_reason: 'h2h_source_empty_table',
        source_type: 'fresh_h2h_empty_table',
        row_count: 0,
      };
    }
    return {};
  };
  getTaH2hRowForCanonicalPair_ = function () { return null; };

  try {
    const result = enrichScheduleEventsFromTennisAbstract_({}, [{
      event_id: 'sched_h2h_empty',
      start_time: new Date('2025-03-01T03:00:00.000Z'),
      player_1: 'Player One',
      player_2: 'Player Two',
    }]);

    assertEquals_('schedule_enrichment_ta_completed', result.reason_code);
    assertEquals_('h2h_source_empty_table', result.h2h_reason_code);
    assertEquals_(true, !!result.h2h_impact);
    assertEquals_(true, result.h2h_impact.h2h_features_unavailable);
    assertEquals_('h2h_features_only', result.h2h_impact.model_fallback_scope);
    assertEquals_('fresh_h2h_empty_table', result.h2h_impact.h2h_source_type);
    assertEquals_(0, result.h2h_impact.h2h_row_count);
    assertEquals_(1, result.h2h_missing);
  } finally {
    fetchPlayerStatsBatch_ = originalFetchPlayerStatsBatch;
    getStateJson_ = originalGetStateJson;
    getTaH2hRowForCanonicalPair_ = originalGetTaH2hRowForCanonicalPair;
  }
}


function testEnrichScheduleEventsFromTennisAbstract_h2hMixedCoverageTracksReasonsAndMetrics_() {
  const originalFetchPlayerStatsBatch = fetchPlayerStatsBatch_;
  const originalGetStateJson = getStateJson_;
  const originalGetTaH2hCoverageForCanonicalPair = getTaH2hCoverageForCanonicalPair_;

  fetchPlayerStatsBatch_ = function () {
    return { stats_by_player: {}, reason_code: 'ta_leaders_ok', source: 'ta_leaders' };
  };
  getStateJson_ = function () { return {}; };
  getTaH2hCoverageForCanonicalPair_ = function (config, playerA, playerB) {
    const key = String(playerA || '') + '::' + String(playerB || '');
    if (key === 'iga swiatek::aryna sabalenka') {
      return { row: { wins_a: 2, wins_b: 1 }, reason_code: '', reason_metadata: { matched_pair_verified: true } };
    }
    if (key === 'iga swiatek::coco gauff') {
      return {
        row: null,
        reason_code: 'h2h_partial_coverage',
        reason_metadata: {
          debug_sample: {
            requested_pair_keys: ['iga swiatek||coco gauff', 'coco gauff||iga swiatek'],
            nearest_candidate_keys: ['iga swiatek||aryna sabalenka'],
            edit_distance_top_matches: [{ key: 'iga swiatek||aryna sabalenka', distance: 11 }],
          },
        },
      };
    }
    return {
      row: null,
      reason_code: 'h2h_player_not_in_matrix',
      reason_metadata: {
        debug_sample: {
          requested_pair_keys: ['iga swiatek||outside player', 'outside player||iga swiatek'],
          nearest_candidate_keys: ['iga swiatek||aryna sabalenka', 'coco gauff||aryna sabalenka'],
          edit_distance_top_matches: [{ key: 'iga swiatek||aryna sabalenka', distance: 8 }],
        },
      },
    };
  };

  try {
    const result = enrichScheduleEventsFromTennisAbstract_({}, [{
      event_id: 'sched_h2h_found',
      start_time: new Date('2025-03-01T03:00:00.000Z'),
      player_1: 'Iga Swiatek',
      player_2: 'Aryna Sabalenka',
    }, {
      event_id: 'sched_h2h_partial',
      start_time: new Date('2025-03-01T04:00:00.000Z'),
      player_1: 'Iga Swiatek',
      player_2: 'Coco Gauff',
    }, {
      event_id: 'sched_h2h_outside',
      start_time: new Date('2025-03-01T05:00:00.000Z'),
      player_1: 'Iga Swiatek',
      player_2: 'Outside Player',
    }]);

    assertEquals_('schedule_enrichment_ta_completed', result.reason_code);
    assertEquals_(3, result.h2h_pairs_requested);
    assertEquals_(1, result.h2h_pairs_found);
    assertEquals_(1, result.h2h_rows_applied);
    assertEquals_(2, result.h2h_missing);
    assertEquals_(1, result.h2h_missing_reason_codes.h2h_partial_coverage || 0);
    assertEquals_(1, result.h2h_missing_reason_codes.h2h_player_not_in_matrix || 0);
    assertEquals_(1, result.h2h_missing_classification.expected_missing_source_coverage || 0);
    assertEquals_(0, result.h2h_missing_classification.pipeline_failure || 0);
    assertEquals_(1, result.h2h_missing_classification.source_partial_coverage || 0);
    assertEquals_(0, result.h2h_missing_classification.source_dataset_unavailable || 0);
    assertEquals_(0, result.h2h_missing_classification.invalid_h2h_pair || 0);
    assertEquals_(0, result.h2h_missing_classification.generic_h2h_missing || 0);
    assertEquals_(0, result.h2h_missing_classification.unclassified || 0);
    assertEquals_(2, result.events[0].h2h_p1_wins);
    assertEquals_(1, result.events[0].h2h_p2_wins);
    assertEquals_(2, (result.h2h_lookup_debug_samples || []).length);
    assertEquals_('h2h_partial_coverage', result.h2h_lookup_debug_samples[0].reason_code);
    assertEquals_('iga swiatek||coco gauff', result.h2h_lookup_debug_samples[0].schedule_key);
    assertEquals_('h2h_player_not_in_matrix', result.h2h_lookup_debug_samples[1].reason_code);
    assertEquals_('iga swiatek||outside player', result.h2h_lookup_debug_samples[1].schedule_key);

  } finally {
    fetchPlayerStatsBatch_ = originalFetchPlayerStatsBatch;
    getStateJson_ = originalGetStateJson;
    getTaH2hCoverageForCanonicalPair_ = originalGetTaH2hCoverageForCanonicalPair;
  }
}


function testEnrichScheduleEventsFromTennisAbstract_h2hPipelineFailuresClassifiedSeparately_() {
  const originalDateNow = Date.now;
  const originalFetchPlayerStatsBatch = fetchPlayerStatsBatch_;
  const originalGetStateJson = getStateJson_;
  const originalGetTaH2hCoverageForCanonicalPair = getTaH2hCoverageForCanonicalPair_;

  Date.now = function () { return Date.parse('2025-03-01T00:00:00.000Z'); };
  fetchPlayerStatsBatch_ = function () {
    return { stats_by_player: {}, reason_code: 'ta_leaders_ok', source: 'ta_leaders' };
  };
  getStateJson_ = function () { return {}; };
  getTaH2hCoverageForCanonicalPair_ = function (config, playerA, playerB) {
    const key = String(playerA || '') + '::' + String(playerB || '');
    if (key === 'iga swiatek::aryna sabalenka') {
      return { row: null, reason_code: 'ta_h2h_parse_failed' };
    }
    return { row: null, reason_code: 'ta_h2h_fetch_failed' };
  };

  try {
    const result = enrichScheduleEventsFromTennisAbstract_({}, [{
      event_id: 'sched_h2h_parse_fail',
      start_time: new Date('2025-03-01T03:00:00.000Z'),
      player_1: 'Iga Swiatek',
      player_2: 'Aryna Sabalenka',
    }, {
      event_id: 'sched_h2h_fetch_fail',
      start_time: new Date('2025-03-01T04:00:00.000Z'),
      player_1: 'Iga Swiatek',
      player_2: 'Coco Gauff',
    }]);

    assertEquals_('schedule_enrichment_ta_completed', result.reason_code);
    assertEquals_(2, result.h2h_missing);
    assertEquals_(1, result.h2h_missing_reason_codes.ta_h2h_parse_failed || 0);
    assertEquals_(1, result.h2h_missing_reason_codes.ta_h2h_fetch_failed || 0);
    assertEquals_(0, result.h2h_missing_classification.expected_missing_source_coverage || 0);
    assertEquals_(2, result.h2h_missing_classification.pipeline_failure || 0);
    assertEquals_(0, result.h2h_missing_classification.source_partial_coverage || 0);
    assertEquals_(0, result.h2h_missing_classification.source_dataset_unavailable || 0);
    assertEquals_(0, result.h2h_missing_classification.invalid_h2h_pair || 0);
    assertEquals_(0, result.h2h_missing_classification.generic_h2h_missing || 0);
    assertEquals_(0, result.h2h_missing_classification.unclassified || 0);
  } finally {
    Date.now = originalDateNow;
    fetchPlayerStatsBatch_ = originalFetchPlayerStatsBatch;
    getStateJson_ = originalGetStateJson;
    getTaH2hCoverageForCanonicalPair_ = originalGetTaH2hCoverageForCanonicalPair;
  }
}


function testClassifyScheduleEnrichmentH2hMissingReason_specificBucketsAndFallback_() {
  assertEquals_('expected_missing_source_coverage', classifyScheduleEnrichmentH2hMissingReason_('h2h_player_not_in_matrix'));
  assertEquals_('pipeline_failure', classifyScheduleEnrichmentH2hMissingReason_('ta_h2h_fetch_failed'));
  assertEquals_('pipeline_failure', classifyScheduleEnrichmentH2hMissingReason_('ta_h2h_parse_failed'));
  assertEquals_('source_partial_coverage', classifyScheduleEnrichmentH2hMissingReason_('h2h_partial_coverage'));
  assertEquals_('source_dataset_unavailable', classifyScheduleEnrichmentH2hMissingReason_('h2h_dataset_unavailable'));
  assertEquals_('invalid_h2h_pair', classifyScheduleEnrichmentH2hMissingReason_('h2h_pair_invalid'));
  assertEquals_('generic_h2h_missing', classifyScheduleEnrichmentH2hMissingReason_('h2h_missing'));
  assertEquals_('unclassified', classifyScheduleEnrichmentH2hMissingReason_('h2h_missing_mystery'));
}

function runStageFetchScheduleScenario_(options) {
  const opts = options || {};
  const originalDateNow = Date.now;
  const originalDeriveScheduleWindowFromOdds = deriveScheduleWindowFromOdds_;
  const originalGetCreditAwareRuntimeConfig = getCreditAwareRuntimeConfig_;
  const originalGetCachedSchedulePayload = getCachedSchedulePayload_;
  const originalGetStateJson = getStateJson_;
  const originalFetchScheduleFromOddsApi = fetchScheduleFromOddsApi_;
  const originalSetCachedSchedulePayload = setCachedSchedulePayload_;
  const originalBuildCompetitionTierResolverConfig = buildCompetitionTierResolverConfig_;
  const originalResolveCompetitionTier = resolveCompetitionTier_;
  const originalIsAllowedTournament = isAllowedTournament;
  const originalBuildStageSummary = buildStageSummary_;
  const originalUpdateCreditStateFromHeaders = updateCreditStateFromHeaders_;
  const originalLocalAndUtcTimestamps = localAndUtcTimestamps_;
  const originalSetStateValue = setStateValue_;
  const originalEnrichScheduleEventsFromTennisAbstract = enrichScheduleEventsFromTennisAbstract_;

  const fetchResponses = (opts.fetchResponses || []).slice();
  const fetchCalls = [];
  const stateWrites = {};
  const creditHeadersCaptured = [];

  Date.now = function () { return Date.parse('2025-03-01T00:10:00.000Z'); };
  deriveScheduleWindowFromOdds_ = function () {
    return {
      startIso: '2025-03-01T00:00:00.000Z',
      endIso: '2025-03-01T06:00:00.000Z',
    };
  };
  getCreditAwareRuntimeConfig_ = function () {
    return {
      odds_window_cache_ttl_min: 1,
      odds_window_refresh_min: 30,
      mode: 'normal',
      schedule_refresh_non_critical_enabled: false,
      match_fallback_expansion_min: 90,
    };
  };
  getCachedSchedulePayload_ = function () { return opts.cacheResult || null; };
  getStateJson_ = function (key) {
    if (key === 'SCHEDULE_WINDOW_STALE_PAYLOAD') return opts.stalePayload || {};
    if (key === 'SCHEDULE_WINDOW_LAST_FETCH_META') return opts.lastFetchMeta || {};
    if (opts.stateJson && Object.prototype.hasOwnProperty.call(opts.stateJson, key)) return opts.stateJson[key];
    return {};
  };
  fetchScheduleFromOddsApi_ = function () {
    fetchCalls.push(true);
    if (!fetchResponses.length) throw new Error('missing fetch response');
    return fetchResponses.shift();
  };
  setCachedSchedulePayload_ = function () {};
  buildCompetitionTierResolverConfig_ = function () { return {}; };
  resolveCompetitionTier_ = function () {
    return {
      canonical_tier: 'UNKNOWN',
      matched_by: '',
      matched_field: '',
      raw_fields: [],
    };
  };
  isAllowedTournament = function () {
    return {
      allowed: true,
      reason_code: 'allowed',
    };
  };
  buildStageSummary_ = function (runId, stage, start, values) {
    return {
      run_id: runId,
      stage: stage,
      reason_code: values && values.reason_code ? values.reason_code : '',
      reason_codes: Object.assign({}, (values && values.reason_codes) || {}),
    };
  };
  updateCreditStateFromHeaders_ = function (runId, headers) {
    const normalized = Object.assign({}, headers || {});
    const remaining = Number(normalized.requests_remaining || normalized.x_requests_remaining);
    creditHeadersCaptured.push(normalized);
    return {
      header_present: Object.keys(normalized).length > 0,
      remaining: Number.isFinite(remaining) ? remaining : null,
    };
  };
  localAndUtcTimestamps_ = function () {
    return {
      local: '2025-03-01T00:10:00-07:00',
      utc: '2025-03-01T07:10:00.000Z',
    };
  };
  setStateValue_ = function (key, value) {
    stateWrites[key] = value;
  };
  enrichScheduleEventsFromTennisAbstract_ = function (config, events) {
    if (opts.enrichmentResult) return opts.enrichmentResult;
    return {
      events: events,
      reason_code: 'schedule_enrichment_test_passthrough',
      stats_reason_code: '',
      canonical_player_count: 0,
      stats_rows_applied: 0,
      h2h_rows_applied: 0,
      h2h_missing: 0,
      h2h_missing_reason_codes: {},
      h2h_missing_classification: {
        expected_missing_source_coverage: 0,
        pipeline_failure: 0,
        source_partial_coverage: 0,
        source_dataset_unavailable: 0,
        invalid_h2h_pair: 0,
        generic_h2h_missing: 0,
        unclassified: 0,
      },
      failed: false,
      error: '',
    };
  };

  try {
    const stage = stageFetchSchedule('run_stage_fetch_schedule_test', {}, opts.oddsEvents || []);
    return {
      stage: stage,
      fetchCalls: fetchCalls,
      lastMeta: JSON.parse(stateWrites.SCHEDULE_WINDOW_LAST_FETCH_META || '{}'),
      creditHeadersCaptured: creditHeadersCaptured,
      stateWrites: stateWrites,
    };
  } finally {
    Date.now = originalDateNow;
    deriveScheduleWindowFromOdds_ = originalDeriveScheduleWindowFromOdds;
    getCreditAwareRuntimeConfig_ = originalGetCreditAwareRuntimeConfig;
    getCachedSchedulePayload_ = originalGetCachedSchedulePayload;
    getStateJson_ = originalGetStateJson;
    fetchScheduleFromOddsApi_ = originalFetchScheduleFromOddsApi;
    setCachedSchedulePayload_ = originalSetCachedSchedulePayload;
    buildCompetitionTierResolverConfig_ = originalBuildCompetitionTierResolverConfig;
    resolveCompetitionTier_ = originalResolveCompetitionTier;
    isAllowedTournament = originalIsAllowedTournament;
    buildStageSummary_ = originalBuildStageSummary;
    updateCreditStateFromHeaders_ = originalUpdateCreditStateFromHeaders;
    localAndUtcTimestamps_ = originalLocalAndUtcTimestamps;
    setStateValue_ = originalSetStateValue;
    enrichScheduleEventsFromTennisAbstract_ = originalEnrichScheduleEventsFromTennisAbstract;
  }
}


function testResolveActiveWtaSportKeys_forceDiscoveryBypassesCache_() {
  let catalogCalls = 0;
  const result = resolveActiveWtaSportKeys_({
    ODDS_CACHE_TTL_SEC: 300,
    ODDS_API_BASE_URL: 'https://api.the-odds-api.com/v4',
    ODDS_API_KEY: 'test',
  }, {
    getCachedOddsSportKeys: function () { return ['tennis_wta_cached_only']; },
    setCachedOddsSportKeys: function () {},
    callOddsApi: function () {
      catalogCalls += 1;
      return {
        ok: true,
        payload: [
          { key: 'tennis_wta_catalog_1', active: true },
        ],
      };
    },
    logOddsSportKeyResolution: function () {},
  }, {
    force_discovery: true,
  });

  assertEquals_(1, catalogCalls);
  assertArrayEquals_(['tennis_wta_catalog_1'], result.sport_keys);
  assertEquals_('catalog', result.source);
}

function testStageFetchPlayerStats_realStatsPathEnrichesBothPlayers_() {
  const result = runStageFetchPlayerStatsScenario_({
    batchResult: {
      stats_by_player: {
        'player one': { ranking: 9, recent_form: 0.72, surface_win_rate: 0.63, hold_pct: 0.68, break_pct: 0.37 },
        'player two': { ranking: 28, recent_form: 0.51, surface_win_rate: 0.45, hold_pct: 0.62, break_pct: 0.31 },
      },
      provider_available: true,
      api_credit_usage: 2,
      reason_code: 'player_stats_api_success',
    },
  });

  assertEquals_(2, result.fetchPlayers.length);
  assertEquals_('player one', result.fetchPlayers[0]);
  assertEquals_('player two', result.fetchPlayers[1]);
  assertEquals_(2, result.stage.rows.length);
  assertEquals_(2, result.stage.summary.reason_codes.stats_enriched);
  assertEquals_(0, result.stage.summary.reason_codes.stats_missing_player_a);
  assertEquals_(0, result.stage.summary.reason_codes.stats_missing_player_b);

  const bundle = result.stage.byOddsEventId.odds_evt_1;
  assertTrue_(bundle.player_a.has_stats, 'player a should be enriched');
  assertTrue_(bundle.player_b.has_stats, 'player b should be enriched');
  assertEquals_(9, bundle.player_a.features.ranking);
  assertEquals_(28, bundle.player_b.features.ranking);
}

function testStageFetchPlayerStats_partialMissingIncrementsReasonCodes_() {
  const result = runStageFetchPlayerStatsScenario_({
    batchResult: {
      stats_by_player: {
        'player one': { ranking: 11, recent_form: 0.6, surface_win_rate: 0.58, hold_pct: 0.66, break_pct: 0.33 },
      },
      provider_available: true,
      api_credit_usage: 1,
      reason_code: 'player_stats_api_success',
    },
  });

  assertEquals_(1, result.stage.summary.reason_codes.stats_enriched);
  assertEquals_(0, result.stage.summary.reason_codes.stats_missing_player_a);
  assertEquals_(1, result.stage.summary.reason_codes.stats_missing_player_b);
  assertEquals_(0, result.stage.summary.reason_codes.stats_fallback_model_used);

  const bundle = result.stage.byOddsEventId.odds_evt_1;
  assertTrue_(bundle.player_a.has_stats, 'player a should have provider stats');
  assertEquals_(false, bundle.player_b.has_stats);
  assertEquals_(null, bundle.player_b.features.ranking);
}

function testStageFetchPlayerStats_providerUnavailableFallsBackDeterministically_() {
  const first = runStageFetchPlayerStatsScenario_({
    batchResult: {
      stats_by_player: {},
      provider_available: false,
      api_credit_usage: 0,
      reason_code: 'player_stats_provider_not_configured',
    },
  });
  const second = runStageFetchPlayerStatsScenario_({
    batchResult: {
      stats_by_player: {},
      provider_available: false,
      api_credit_usage: 0,
      reason_code: 'player_stats_provider_not_configured',
    },
  });

  assertEquals_(2, first.stage.summary.reason_codes.stats_fallback_model_used);
  assertEquals_(JSON.stringify(first.stage.byOddsEventId), JSON.stringify(second.stage.byOddsEventId));
  assertEquals_(JSON.stringify(first.stage.rows), JSON.stringify(second.stage.rows));
}

function runStageFetchPlayerStatsScenario_(options) {
  const opts = options || {};
  const originalFetchPlayerStatsBatch = fetchPlayerStatsBatch_;
  const originalBuildStageSummary = buildStageSummary_;
  const originalGetStateJson = getStateJson_;
  const originalDeserializeScheduleEvent = deserializeScheduleEvent_;

  const defaultOddsEvents = [{
    event_id: 'odds_evt_1',
    player_1: 'Player One',
    player_2: 'Player Two',
    odds_updated_time: new Date('2025-03-01T00:00:00.000Z'),
    commence_time: new Date('2025-03-01T03:00:00.000Z'),
    market: 'h2h',
    outcome: 'Player One',
    bookmaker: 'book_a',
    price: 1.9,
  }];
  const defaultMatchRows = [{
    odds_event_id: 'odds_evt_1',
    schedule_event_id: 'sched_evt_1',
    competition_tier: 'WTA_500',
  }];
  let fetchPlayers = [];

  fetchPlayerStatsBatch_ = function (config, canonicalPlayers) {
    fetchPlayers = canonicalPlayers.slice();
    return opts.batchResult || {
      stats_by_player: {},
      provider_available: true,
      api_credit_usage: 0,
      reason_code: 'player_stats_api_success',
    };
  };
  buildStageSummary_ = function (runId, stage, startMs, values) {
    return {
      run_id: runId,
      stage: stage,
      input_count: values.input_count,
      output_count: values.output_count,
      provider: values.provider,
      reason_codes: values.reason_codes,
      api_credit_usage: values.api_credit_usage,
    };
  };
  getStateJson_ = function (key) {
    if (key === 'SCHEDULE_WINDOW_STALE_PAYLOAD') return opts.schedulePayload || null;
    return null;
  };
  deserializeScheduleEvent_ = function (event) { return event; };

  try {
    const oddsEvents = opts.oddsEvents || defaultOddsEvents;
    const matchRows = opts.matchRows || defaultMatchRows;
    return {
      stage: stageFetchPlayerStats('run_test_player_stats', {}, oddsEvents, matchRows),
      fetchPlayers: fetchPlayers,
    };
  } finally {
    Date.now = originalDateNow;
    fetchPlayerStatsBatch_ = originalFetchPlayerStatsBatch;
    buildStageSummary_ = originalBuildStageSummary;
    getStateJson_ = originalGetStateJson;
    deserializeScheduleEvent_ = originalDeserializeScheduleEvent;
  }
}

function testStageFetchPlayerStats_scheduleSeedWritesRowsWithoutOdds_() {
  const result = runStageFetchPlayerStatsScenario_({
    oddsEvents: [],
    matchRows: [],
    schedulePayload: {
      events: [{ event_id: 'sched_evt_1', player_1: 'Player One', player_2: 'Player Two' }],
    },
    batchResult: {
      stats_by_player: {
        'player one': { ranking: 15, recent_form: 0.55, surface_win_rate: 0.58, hold_pct: 0.66, break_pct: 0.31 },
        'player two': { ranking: 22, recent_form: 0.52, surface_win_rate: 0.54, hold_pct: 0.64, break_pct: 0.29 },
      },
      provider_available: true,
      api_credit_usage: 1,
      reason_code: 'player_stats_api_success',
    },
  });

  assertEquals_(2, result.stage.rows.length);
  assertEquals_(true, result.stage.byOddsEventId.sched_evt_1.synthetic_schedule_seed);
  assertEquals_(2, result.fetchPlayers.length);
}

function testStageFetchPlayerStats_noMatchedEventsTracksSkipReason_() {
  const result = runStageFetchPlayerStatsScenario_({
    oddsEvents: [{
      event_id: 'odds_evt_1',
      player_1: 'Player One',
      player_2: 'Player Two',
      odds_updated_time: new Date('2025-03-01T00:00:00.000Z'),
    }],
    matchRows: [],
  });

  assertEquals_(0, result.stage.summary.output_count);
  assertEquals_(1, result.stage.summary.reason_codes.skipped_no_matched_events);
  assertEquals_(1, result.stage.summary.reason_codes.skipped_no_player_keys);
}

function testStageFetchPlayerStats_providerAvailableButEmptyTracksReason_() {
  const result = runStageFetchPlayerStatsScenario_({
    batchResult: {
      stats_by_player: {},
      provider_available: true,
      api_credit_usage: 1,
      reason_code: 'player_stats_api_success_empty',
    },
  });

  assertEquals_(2, result.stage.summary.output_count);
  assertEquals_(1, result.stage.summary.reason_codes.provider_returned_empty);
  assertEquals_(0, result.stage.summary.reason_codes.stats_enriched);
  assertEquals_(0, result.stage.summary.reason_codes.stats_fallback_model_used);
}

function testStageFetchPlayerStats_providerReturnedNullFeaturesMarksFallbackMetadata_() {
  const result = runStageFetchPlayerStatsScenario_({
    batchResult: {
      stats_by_player: {
        'player one': { ranking: null, recent_form: null, surface_win_rate: null, hold_pct: null, break_pct: null },
        'player two': { ranking: null, recent_form: null, surface_win_rate: null, hold_pct: null, break_pct: null },
      },
      provider_available: true,
      api_credit_usage: 1,
      reason_code: 'provider_returned_null_features',
    },
  });

  const bundle = result.stage.byOddsEventId.odds_evt_1;
  assertEquals_(1, result.stage.summary.reason_codes.provider_returned_null_features || 0);
  assertEquals_('null_features', bundle.stats_fallback_mode || '');
  assertEquals_('null_features', bundle.player_a.stats_fallback_mode || '');
  assertEquals_('null_features', bundle.player_b.stats_fallback_mode || '');
  assertEquals_('player_stats_provider_v1', bundle.player_a.provenance || '');
  assertEquals_('player_stats_provider_v1', bundle.player_b.provenance || '');
}


function testStageFetchPlayerStats_successfulEnrichmentTracksReason_() {
  const result = runStageFetchPlayerStatsScenario_({
    batchResult: {
      stats_by_player: {
        'player one': { ranking: 9, recent_form: 0.72, surface_win_rate: 0.63, hold_pct: 0.68, break_pct: 0.37 },
        'player two': { ranking: 28, recent_form: 0.51, surface_win_rate: 0.45, hold_pct: 0.62, break_pct: 0.31 },
      },
      provider_available: true,
      api_credit_usage: 2,
      reason_code: 'player_stats_api_success',
    },
  });

  assertEquals_(2, result.stage.summary.output_count);
  assertEquals_(2, result.stage.summary.reason_codes.stats_enriched);
  assertEquals_(0, result.stage.summary.reason_codes.provider_returned_empty);
}

function testNormalizePlayerStatsResponse_mapsCoreFields_() {
  const normalized = normalizePlayerStatsResponse_({
    data: [{
      player_name: 'Player One',
      rank: 14,
      recent_win_rate: 62,
      surfaceWinRate: 0.58,
      hold_percentage: 71,
      break_percentage: 39,
    }],
  }, ['Player One']);

  assertEquals_(14, normalized['player one'].ranking);
  assertEquals_(0.62, normalized['player one'].recent_form);
  assertEquals_(0.58, normalized['player one'].surface_win_rate);
  assertEquals_(0.71, normalized['player one'].hold_pct);
  assertEquals_(0.39, normalized['player one'].break_pct);
}

function testNormalizePlayerStatsResponse_mapsRecentAndSurfaceSpecificFields_() {
  const normalized = normalizePlayerStatsResponse_({
    data: [{
      player_name: 'Player One',
      rank: 14,
      recent_win_rate: 62,
      form_last_10: 70,
      surfaceWinRate: 0.58,
      hold_percentage: 71,
      break_percentage: 39,
      surface_hold_pct: 74,
      surface_break_pct: 42,
      surface_recent_form: 67,
    }],
  }, ['Player One']);

  assertEquals_(0.7, normalized['player one'].recent_form_last_10);
  assertEquals_(0.74, normalized['player one'].surface_hold_pct);
  assertEquals_(0.42, normalized['player one'].surface_break_pct);
  assertEquals_(0.67, normalized['player one'].surface_recent_form);
}

function testFetchPlayerStatsFromProvider_multiSourceMerge_() {
  const originalFetch = UrlFetchApp.fetch;
  const calls = [];
  UrlFetchApp.fetch = function (url, opts) {
    calls.push(url);
    if (url.indexOf('provider-a') >= 0) {
      return {
        getResponseCode: function () { return 200; },
        getContentText: function () { return JSON.stringify({ data: [{ player_name: 'Player One', rank: 10 }] }); },
      };
    }
    return {
      getResponseCode: function () { return 200; },
      getContentText: function () { return JSON.stringify({ data: [{ player_name: 'Player One', hold_pct: 71 }] }); },
    };
  };

  try {
    const result = fetchPlayerStatsFromProvider_({
      PLAYER_STATS_API_BASE_URL: 'https://provider-a.test,https://provider-b.test',
      PLAYER_STATS_API_KEY: '',
    }, ['Player One'], new Date('2025-03-01T00:00:00.000Z'));

    assertEquals_(true, result.ok);
    assertEquals_('player_stats_multi_source_success', result.reason_code);
    assertEquals_(2, result.api_call_count);
    assertEquals_(10, result.stats_by_player['player one'].ranking);
    assertEquals_(0.71, result.stats_by_player['player one'].hold_pct);
    assertEquals_(2, calls.length);
  } finally {
    UrlFetchApp.fetch = originalFetch;
  }
}

function testFetchPlayerStatsBatch_providerUnavailableReturnsNoProvider_() {
  const originalFetchPlayerStatsFromProvider = fetchPlayerStatsFromProvider_;
  const originalGetCachedPlayerStatsPayload = getCachedPlayerStatsPayload_;
  const originalGetStateJson = getStateJson_;
  const originalSetStateValue = setStateValue_;

  fetchPlayerStatsFromProvider_ = function () {
    return {
      ok: false,
      reason_code: 'player_stats_transport_error',
      api_credit_usage: 0,
      api_call_count: 1,
    };
  };
  getCachedPlayerStatsPayload_ = function () { return null; };
  getStateJson_ = function () { return null; };
  setStateValue_ = function () {};

  try {
    const result = fetchPlayerStatsBatch_({
      PLAYER_STATS_CACHE_TTL_MIN: 10,
      PLAYER_STATS_REFRESH_MIN: 5,
      PLAYER_STATS_FORCE_REFRESH: false,
    }, ['Player One'], new Date('2025-03-01T00:00:00.000Z'));

    assertEquals_(false, result.provider_available);
    assertEquals_('provider_unavailable', result.source);
    assertEquals_('player_stats_transport_error', result.reason_code);
  } finally {
    fetchPlayerStatsFromProvider_ = originalFetchPlayerStatsFromProvider;
    getCachedPlayerStatsPayload_ = originalGetCachedPlayerStatsPayload;
    getStateJson_ = originalGetStateJson;
    setStateValue_ = originalSetStateValue;
  }
}

function testFetchPlayerStatsBatch_providerAvailableButDataUnavailableMeta_() {
  const originalFetchPlayerStatsFromProvider = fetchPlayerStatsFromProvider_;
  const originalGetCachedPlayerStatsPayload = getCachedPlayerStatsPayload_;
  const originalGetStateJson = getStateJson_;
  const originalSetStateValue = setStateValue_;
  const writes = {};

  fetchPlayerStatsFromProvider_ = function () {
    return {
      ok: true,
      reason_code: 'player_stats_api_success_empty',
      stats_by_player: {},
      api_credit_usage: 0,
      api_call_count: 1,
      scrape_call_count: 0,
    };
  };
  getCachedPlayerStatsPayload_ = function () { return null; };
  getStateJson_ = function () { return null; };
  setStateValue_ = function (key, value) { writes[key] = value; };

  try {
    const result = fetchPlayerStatsBatch_({
      PLAYER_STATS_CACHE_TTL_MIN: 10,
      PLAYER_STATS_REFRESH_MIN: 5,
      PLAYER_STATS_FORCE_REFRESH: false,
    }, ['Player One'], new Date('2025-03-01T00:00:00.000Z'));

    const meta = JSON.parse(writes.PLAYER_STATS_LAST_FETCH_META || '{}');
    assertEquals_(true, result.provider_available);
    assertEquals_(true, meta.provider_available);
    assertEquals_(false, meta.data_available);
    assertEquals_('provider_returned_empty', meta.last_failure_reason);
    assertEquals_('provider_returned_empty', meta.aggregate_reason_code);
    assertEquals_(0, meta.players_with_non_null_stats);
    assertEquals_(0, meta.players_with_null_only_stats);
  } finally {
    fetchPlayerStatsFromProvider_ = originalFetchPlayerStatsFromProvider;
    getCachedPlayerStatsPayload_ = originalGetCachedPlayerStatsPayload;
    getStateJson_ = originalGetStateJson;
    setStateValue_ = originalSetStateValue;
  }
}


function testFetchPlayerStatsBatch_providerReturnedNullOnlyFeaturesMeta_() {
  const originalFetchPlayerStatsFromProvider = fetchPlayerStatsFromProvider_;
  const originalGetCachedPlayerStatsPayload = getCachedPlayerStatsPayload_;
  const originalGetStateJson = getStateJson_;
  const originalSetStateValue = setStateValue_;
  const writes = {};

  fetchPlayerStatsFromProvider_ = function () {
    return {
      ok: true,
      reason_code: 'player_stats_api_success',
      stats_by_player: {
        'player one': {
          ranking: null,
          recent_form: null,
          surface_win_rate: null,
          hold_pct: null,
          break_pct: null,
        },
      },
      api_credit_usage: 0,
      api_call_count: 1,
      scrape_call_count: 0,
    };
  };
  getCachedPlayerStatsPayload_ = function () { return null; };
  getStateJson_ = function () { return null; };
  setStateValue_ = function (key, value) { writes[key] = value; };

  try {
    const result = fetchPlayerStatsBatch_({
      PLAYER_STATS_CACHE_TTL_MIN: 10,
      PLAYER_STATS_REFRESH_MIN: 5,
      PLAYER_STATS_FORCE_REFRESH: false,
    }, ['Player One'], new Date('2025-03-01T00:00:00.000Z'));

    const meta = JSON.parse(writes.PLAYER_STATS_LAST_FETCH_META || '{}');
    assertEquals_(true, result.provider_available);
    assertEquals_('provider_returned_null_features', result.reason_code);
    assertEquals_(false, meta.has_stats);
    assertEquals_(false, meta.data_available);
    assertEquals_(0, meta.players_with_non_null_stats);
    assertEquals_(1, meta.players_with_null_only_stats);
    assertEquals_('provider_returned_null_features', meta.aggregate_reason_code);
    assertEquals_('provider_returned_null_features', meta.last_failure_reason);
  } finally {
    fetchPlayerStatsFromProvider_ = originalFetchPlayerStatsFromProvider;
    getCachedPlayerStatsPayload_ = originalGetCachedPlayerStatsPayload;
    getStateJson_ = originalGetStateJson;
    setStateValue_ = originalSetStateValue;
  }
}

function testDeriveScheduleWindowFromOdds_noOddsFallsBackToLookaheadWindow_() {
  const originalDateNow = Date.now;
  Date.now = function () { return Date.parse('2025-03-01T12:00:00.000Z'); };

  try {
    const config = {
      LOOKAHEAD_HOURS: 24,
      SCHEDULE_BUFFER_BEFORE_MIN: 120,
      SCHEDULE_BUFFER_AFTER_MIN: 180,
    };
    const window = deriveScheduleWindowFromOdds_([], config);

    assertEquals_('2025-03-01T10:00:00.000Z', window.startIso);
    assertEquals_('2025-03-02T15:00:00.000Z', window.endIso);
  } finally {
    Date.now = originalDateNow;
  }
}

function testStageMatchEvents_withoutOddsSeedsMatchRowsFromSchedule_() {
  const scheduleEvents = [{
    event_id: 'sched_1',
    canonical_tier: 'WTA_500',
    player_1: 'A',
    player_2: 'B',
    start_time: new Date('2025-03-01T12:00:00.000Z'),
  }];

  const stage = stageMatchEvents('run_test', {
    MATCH_TIME_TOLERANCE_MIN: 45,
    MATCH_FALLBACK_EXPANSION_MIN: 120,
    PLAYER_ALIAS_MAP_JSON: '{}',
  }, [], scheduleEvents);

  assertEquals_(1, stage.rows.length);
  assertEquals_('schedule_seed_no_odds', stage.rows[0].match_type);
  assertEquals_('sched_1', stage.rows[0].odds_event_id);
  assertEquals_('sched_1', stage.rows[0].schedule_event_id);
  assertEquals_(1, stage.summary.reason_codes.schedule_seed_no_odds);
}


function testStageMatchEvents_countsFullyUnmatchedOddsAsRejected_() {
  const oddsEvents = [{
    event_id: 'odds_1',
    competition: 'WTA 500 Doha',
    player_1: 'Player One',
    player_2: 'Player Two',
    commence_time: new Date('2025-03-01T12:00:00.000Z'),
  }];

  const scheduleEvents = [{
    event_id: 'sched_1',
    canonical_tier: 'WTA_500',
    player_1: 'Different',
    player_2: 'Names',
    start_time: new Date('2025-03-01T12:05:00.000Z'),
  }];

  const stage = stageMatchEvents('run_test', {
    MATCH_TIME_TOLERANCE_MIN: 45,
    MATCH_FALLBACK_EXPANSION_MIN: 120,
    PLAYER_ALIAS_MAP_JSON: '{}',
  }, oddsEvents, scheduleEvents);

  assertEquals_(0, stage.matchedCount);
  assertEquals_(1, stage.rejectedCount);
  assertEquals_(1, stage.unmatchedCount);
  assertEquals_(1, stage.diagnosticRecordsWritten);
  assertEquals_(0, stage.summary.output_count);
  assertEquals_(1, stage.summary.reason_codes.rejected_count);
  assertEquals_(1, stage.summary.reason_codes.no_player_match);
}

function testStageMatchEvents_countsPartialMatchesAndRejectionsSeparately_() {
  const oddsEvents = [{
    event_id: 'odds_1',
    competition: 'WTA 500 Doha',
    player_1: 'Player One',
    player_2: 'Player Two',
    commence_time: new Date('2025-03-01T12:00:00.000Z'),
  }, {
    event_id: 'odds_2',
    competition: 'WTA 500 Doha',
    player_1: 'Unmatched',
    player_2: 'Pairing',
    commence_time: new Date('2025-03-01T13:00:00.000Z'),
  }];

  const scheduleEvents = [{
    event_id: 'sched_1',
    canonical_tier: 'WTA_500',
    player_1: 'Player One',
    player_2: 'Player Two',
    start_time: new Date('2025-03-01T12:10:00.000Z'),
  }];

  const stage = stageMatchEvents('run_test', {
    MATCH_TIME_TOLERANCE_MIN: 45,
    MATCH_FALLBACK_EXPANSION_MIN: 120,
    PLAYER_ALIAS_MAP_JSON: '{}',
  }, oddsEvents, scheduleEvents);

  assertEquals_(1, stage.matchedCount);
  assertEquals_(1, stage.rejectedCount);
  assertEquals_(1, stage.unmatchedCount);
  assertEquals_(1, stage.diagnosticRecordsWritten);
  assertEquals_(1, stage.summary.output_count);
  assertEquals_(1, stage.summary.reason_codes.matched_count);
  assertEquals_(1, stage.summary.reason_codes.rejected_count);
  assertEquals_(1, stage.summary.reason_codes.primary_match);
  assertEquals_(1, stage.summary.reason_codes.no_player_match);
}

function testExtractLeadersJsUrl_matchesLeadersourceWtaScript_() {
  const html = '<html><script src="/jsmatches/abc_leadersource_latest_wta.js"></script></html>';
  const result = extractLeadersJsUrl_(html, 'https://www.tennisabstract.com/cgi-bin/leaders_wta.cgi?players=top');

  assertEquals_('https://www.tennisabstract.com/jsmatches/abc_leadersource_latest_wta.js', result);
}

function testExtractMatchMxRows_parsesStructuredRowsAndRetSafely_() {
  const payload = [
    'matchmx[0]=["2025-03-01","Doha","Hard","Player One","Opponent A","6-4 6-4",11,62,58,70,40,65,42,61,68,51,47,1.05,53];',
    'matchmx[1]=["2025-03-02","Dubai","Hard","Player One","Opponent B","RET",12,65,59,71,41,66,43,62,69,52,48,1.06,54];',
  ].join('\n');

  const rows = extractMatchMxRows_(payload);

  assertEquals_(2, rows.length);
  assertEquals_('2025-03-01', rows[0].date);
  assertEquals_('Doha', rows[0].event);
  assertEquals_('Hard', rows[0].surface);
  assertEquals_('Player One', rows[0].player_name);
  assertEquals_('Opponent A', rows[0].opponent);
  assertEquals_(11, rows[0].ranking);
  assertEquals_(62, rows[0].recent_form);
  assertEquals_(65, rows[0].bp_saved_pct);
  assertEquals_(1.05, rows[0].dr);
  assertEquals_(null, rows[1].recent_form);
  assertEquals_(null, rows[1].hold_pct);
  assertEquals_(null, rows[1].bp_saved_pct);
}

function testNormalizePlayerStatsResponse_aggregatesMatchMxMetricsWithWindowAndCount_() {
  const rows = [
    {
      date: '2025-03-10',
      player_name: 'Player One',
      ranking: 10,
      recent_form: 70,
      surface_win_rate: 58,
      hold_pct: 71,
      break_pct: 39,
      bp_saved_pct: 63,
      bp_conv_pct: 44,
      first_serve_in_pct: 61,
      first_serve_points_won_pct: 67,
      second_serve_points_won_pct: 50,
      return_points_won_pct: 46,
      dr: 1.08,
      tpw_pct: 54,
      numeric_stats: [],
    },
    {
      date: '2025-02-20',
      player_name: 'Player One',
      ranking: 11,
      recent_form: 60,
      surface_win_rate: 56,
      hold_pct: 69,
      break_pct: 37,
      bp_saved_pct: 61,
      bp_conv_pct: 43,
      first_serve_in_pct: 59,
      first_serve_points_won_pct: 65,
      second_serve_points_won_pct: 49,
      return_points_won_pct: 44,
      dr: 1.02,
      tpw_pct: 52,
      numeric_stats: [],
    },
    {
      date: '2023-01-15',
      player_name: 'Player One',
      ranking: 20,
      recent_form: 40,
      hold_pct: 50,
      break_pct: 30,
      numeric_stats: [],
    },
  ];

  const normalized = normalizePlayerStatsResponse_(rows, ['Player One'], {
    as_of_time: new Date('2025-03-15T00:00:00.000Z'),
    match_window_weeks: 52,
    recent_match_count: 2,
  });

  assertEquals_(0.65, normalized['player one'].recent_form);
  assertEquals_(0.57, normalized['player one'].surface_win_rate);
  assertEquals_(0.7, normalized['player one'].hold_pct);
  assertEquals_(0.38, normalized['player one'].break_pct);
  assertEquals_(0.62, normalized['player one'].bp_saved_pct);
  assertEquals_(0.435, normalized['player one'].bp_conv_pct);
  assertEquals_(0.6, normalized['player one'].first_serve_in_pct);
  assertEquals_(0.66, normalized['player one'].first_serve_points_won_pct);
  assertEquals_(0.495, normalized['player one'].second_serve_points_won_pct);
  assertEquals_(0.45, normalized['player one'].return_points_won_pct);
  assertEquals_(1.05, normalized['player one'].dr);
  assertEquals_(0.53, normalized['player one'].tpw_pct);
  assertEquals_(10, normalized['player one'].ranking);
  assertEquals_(0.65, normalized['player one'].recent_form_last_10);
  assertEquals_(0.7, normalized['player one'].surface_hold_pct);
  assertEquals_(0.38, normalized['player one'].surface_break_pct);
  assertEquals_(0.65, normalized['player one'].surface_recent_form);
}

function testNormalizePlayerStatsResponse_aggregatesSurfaceSpecificMetricsWithFallback_() {
  const rows = [
    { date: '2025-03-11', player_name: 'Player One', surface: 'Hard', recent_form: 80, hold_pct: 74, break_pct: 44, numeric_stats: [] },
    { date: '2025-03-05', player_name: 'Player One', surface: 'Clay', recent_form: 42, hold_pct: 62, break_pct: 28, numeric_stats: [] },
    { date: '2025-02-20', player_name: 'Player One', surface: 'Hard', recent_form: 78, hold_pct: 72, break_pct: 42, numeric_stats: [] },
    { date: '2025-02-10', player_name: 'Player One', surface: 'Hard', recent_form: 76, hold_pct: 70, break_pct: 40, numeric_stats: [] },
  ];

  const withSurfaceSample = normalizePlayerStatsResponse_(rows, ['Player One'], {
    as_of_time: new Date('2025-03-15T00:00:00.000Z'),
    recent_match_count: 4,
    surface: 'Hard',
    surface_match_min_sample: 3,
  });
  assertEquals_(0.72, withSurfaceSample['player one'].hold_pct);
  assertEquals_(0.42, withSurfaceSample['player one'].break_pct);
  assertEquals_(0.72, withSurfaceSample['player one'].surface_hold_pct);
  assertEquals_(0.42, withSurfaceSample['player one'].surface_break_pct);
  assertEquals_(0.78, withSurfaceSample['player one'].surface_recent_form);

  const fallbackToGlobal = normalizePlayerStatsResponse_(rows, ['Player One'], {
    as_of_time: new Date('2025-03-15T00:00:00.000Z'),
    recent_match_count: 4,
    surface: 'Clay',
    surface_match_min_sample: 3,
  });
  assertEquals_(0.7, fallbackToGlobal['player one'].surface_hold_pct);
  assertEquals_(0.385, fallbackToGlobal['player one'].surface_break_pct);
  assertEquals_(0.69, fallbackToGlobal['player one'].surface_recent_form);
}


function testExtractTaH2hMatrixRows_parsesAnchorTuples_() {
  const html = [
    '<html>',
    '<div>Last update: 2025-03-19</div>',
    '<a href="/cgi-bin/wplayer-classic.cgi?player1=Iga+Swiatek&player2=Coco+Gauff">4-1</a>',
    '<a href="/cgi-bin/wplayer-classic.cgi?p1=Aryna+Sabalenka&p2=Elena+Rybakina">2:3</a>',
    '</html>',
  ].join('');

  const rows = extractTaH2hMatrixRows_(html);

  assertEquals_(2, rows.length);
  assertEquals_('iga swiatek', rows[0].player_a);
  assertEquals_('coco gauff', rows[0].player_b);
  assertEquals_(4, rows[0].wins_a);
  assertEquals_(1, rows[0].wins_b);
  assertEquals_('aryna sabalenka', rows[1].player_a);
  assertEquals_('elena rybakina', rows[1].player_b);
  assertEquals_(2, rows[1].wins_a);
  assertEquals_(3, rows[1].wins_b);
}

function testExtractTaH2hSourceUpdatedDate_parsesFallbackValue_() {
  const html = '<html><body><span>Last update: March 5, 2025</span></body></html>';
  const value = extractTaH2hSourceUpdatedDate_(html);

  assertEquals_('2025-03-05', value);
}

function testGetTaH2hRowForCanonicalPair_supportsReverseLookup_() {
  const original = getTaH2hDataset_;
  try {
    getTaH2hDataset_ = function () {
      return {
        source_updated_date: '2025-03-20',
        by_pair: {
          'player one||player two': {
            player_a: 'player one',
            player_b: 'player two',
            wins_a: 6,
            wins_b: 2,
            source_updated_date: '2025-03-20',
          },
        },
      };
    };

    const reverse = getTaH2hRowForCanonicalPair_({}, 'Player Two', 'Player One');

    assertEquals_('player two', reverse.player_a);
    assertEquals_('player one', reverse.player_b);
    assertEquals_(2, reverse.wins_a);
    assertEquals_(6, reverse.wins_b);
    assertEquals_('2025-03-20', reverse.source_updated_date);
  } finally {
    getTaH2hDataset_ = original;
  }
}


function testSetCachedTaLeadersPayload_nearLimitWritesDirectWithoutThrow_() {
  const originalGetScriptCache = CacheService.getScriptCache;
  const originalLoggerLog = Logger.log;
  const originalSetStateValue = setStateValue_;

  const cacheStore = {};
  CacheService.getScriptCache = function () {
    return {
        put: function (key, value) { cacheStore[key] = String(value || ''); },
        get: function (key) { return cacheStore[key] || null; },
        putAll: function (obj) { Object.keys(obj || {}).forEach(function (k) { cacheStore[k] = obj[k]; }); },
        removeAll: function (keys) { (keys || []).forEach(function (k) { delete cacheStore[k]; }); },
        getAll: function (keys) {
          const out = {};
          (keys || []).forEach(function (k) { if (Object.prototype.hasOwnProperty.call(cacheStore, k)) out[k] = cacheStore[k]; });
          return out;
        },
      };
    };
  Logger.log = function () {};
  setStateValue_ = function () {};

  try {
    const baseRows = [];
    for (let i = 0; i < 500; i += 1) {
      baseRows.push({
        date: '2025-03-01',
        event: 'Event ' + i,
        surface: 'Hard',
        player_name: 'Player ' + i,
        opponent: 'Opponent ' + i,
        score: '6-4 6-4',
        ranking: i + 1,
        recent_form: 0.62,
        hold_pct: 0.68,
        break_pct: 0.39,
        numeric_stats: [1, 2, 3, 4, 5],
      });
    }

    const payload = {
      source: 'https://example.test/leaders.js',
      fetched_at: '2025-03-01T00:00:00.000Z',
      cached_at_ms: Date.now(),
      rows: baseRows,
    };

    const result = setCachedTaLeadersPayload_(payload);
    assertTrue_(!!result, 'expected result object');
    assertTrue_(result.ok === true || result.ok === false, 'expected result.ok boolean');
    assertTrue_(result.storage_path === 'direct' || result.storage_path === 'pruned' || result.storage_path === 'chunked_compressed' || result.storage_path === 'normalized_subset' || result.storage_path === 'none', 'expected storage path marker');
  } finally {
    CacheService.getScriptCache = originalGetScriptCache;
    Logger.log = originalLoggerLog;
    setStateValue_ = originalSetStateValue;
  }
}

function testSetCachedTaLeadersPayload_overLimitStorageFailureIsNonFatal_() {
  const originalGetScriptCache = CacheService.getScriptCache;
  const originalLoggerLog = Logger.log;
  const originalSetStateValue = setStateValue_;

  CacheService.getScriptCache = function () {
    return {
        put: function () { throw new Error('cache write failed'); },
        get: function () { return null; },
        putAll: function () { throw new Error('cache bulk write failed'); },
        removeAll: function () {},
        getAll: function () { return {}; },
      };
    };
  Logger.log = function () {};
  setStateValue_ = function () {};

  try {
    const oversizedRows = [];
    for (let i = 0; i < 1500; i += 1) {
      oversizedRows.push({
        date: '2025-03-01',
        event: 'Very Long Event Name ' + i + ' ' + new Array(20).join('x'),
        surface: 'Hard',
        player_name: 'Player ' + i,
        opponent: 'Opponent ' + i,
        score: '6-4 6-4',
        ranking: i + 1,
        recent_form: 0.62,
        surface_win_rate: 0.58,
        hold_pct: 0.68,
        break_pct: 0.39,
        bp_saved_pct: 0.61,
        bp_conv_pct: 0.45,
        first_serve_in_pct: 0.64,
        first_serve_points_won_pct: 0.71,
        second_serve_points_won_pct: 0.49,
        return_points_won_pct: 0.42,
        dr: 1.09,
        tpw_pct: 0.53,
        numeric_stats: [1, 2, 3, 4, 5, 6, 7, 8, 9],
      });
    }

    const payload = {
      source: 'https://example.test/leaders.js',
      fetched_at: '2025-03-01T00:00:00.000Z',
      cached_at_ms: Date.now(),
      rows: oversizedRows,
    };

    let threw = false;
    let result = null;
    try {
      result = setCachedTaLeadersPayload_(payload);
    } catch (e) {
      threw = true;
    }

    assertEquals_(false, threw);
    assertTrue_(!!result, 'expected result object');
    assertEquals_(false, result.ok);
  } finally {
    CacheService.getScriptCache = originalGetScriptCache;
    Logger.log = originalLoggerLog;
    setStateValue_ = originalSetStateValue;
  }
}

function testFetchPlayerStatsFromLeadersSource_reasonCodes_() {
  const originalFetch = UrlFetchApp.fetch;
  const originalGetStateJson = getStateJson_;

  try {
    getStateJson_ = function () { return null; };
    UrlFetchApp.fetch = function () {
      throw new Error('network error');
    };

    const fetchFailed = fetchPlayerStatsFromLeadersSource_(['Player One'], {}, new Date('2025-03-01T00:00:00.000Z'));
    assertEquals_(false, fetchFailed.ok);
    assertEquals_('ta_leaders_page_fetch_failed', fetchFailed.reason_code);

    UrlFetchApp.fetch = function (url) {
      if (url.indexOf('leaders_wta.cgi') >= 0) {
        return {
          getResponseCode: function () { return 200; },
          getContentText: function () { return '<html>No matching js</html>'; },
        };
      }
      return {
        getResponseCode: function () { return 404; },
        getContentText: function () { return ''; },
      };
    };

    const missingJs = fetchPlayerStatsFromLeadersSource_(['Player One'], {}, new Date('2025-03-01T00:00:00.000Z'));
    assertEquals_(false, missingJs.ok);
    assertEquals_('ta_leaders_js_url_missing', missingJs.reason_code);

    UrlFetchApp.fetch = function (url) {
      if (url.indexOf('leaders_wta.cgi') >= 0) {
        return {
          getResponseCode: function () { return 200; },
          getContentText: function () { return '<script src="/jsmatches/test_leadersource_wta.js"></script>'; },
        };
      }
      return {
        getResponseCode: function () { return 200; },
        getContentText: function () { return 'matchmx[0]=["2025-03-01","Doha","Hard","Player One","Opponent A","6-4 6-4",14,62,58,71,39];'; },
      };
    };

    const ok = fetchPlayerStatsFromLeadersSource_(['Player One'], {}, new Date('2025-03-01T00:00:00.000Z'));
    assertEquals_(true, ok.ok);
    assertEquals_('ta_matchmx_ok', ok.reason_code);
    assertEquals_(14, ok.stats_by_player['player one'].ranking);
  } finally {
    UrlFetchApp.fetch = originalFetch;
    getStateJson_ = originalGetStateJson;
  }
}

function testFetchPlayerStatsFromLeadersSource_escalatesWhenFreshParseFailsAndStaleNullOnly_() {
  const originalFetch = UrlFetchApp.fetch;
  const originalGetStateJson = getStateJson_;
  const originalLogDiagnosticEvent = logDiagnosticEvent_;
  const diagnosticEvents = [];

  try {
    getStateJson_ = function (key) {
      if (key === 'PLAYER_STATS_TA_LEADERS_STALE_PAYLOAD') {
        return {
          rows: [{ player_name: 'Player One', score: '6-4 6-4', ranking: null, hold_pct: null, break_pct: null }],
        };
      }
      return null;
    };
    logDiagnosticEvent_ = function (config, eventType, payload) {
      diagnosticEvents.push({ event_type: eventType, payload: payload });
    };
    UrlFetchApp.fetch = function (url) {
      if (url.indexOf('leaders_wta.cgi') >= 0) {
        return {
          getResponseCode: function () { return 200; },
          getContentText: function () { return '<script src="/jsmatches/test_leadersource_wta.js"></script>'; },
        };
      }
      return {
        getResponseCode: function () { return 200; },
        getContentText: function () { return 'var x = 1;'; },
      };
    };

    const result = fetchPlayerStatsFromLeadersSource_(['Player One'], {}, new Date('2025-03-01T00:00:00.000Z'));

    assertEquals_(false, result.ok);
    assertEquals_('no_usable_stats_payload', result.reason_code);
    assertEquals_('fresh_parse_failed_stale_null_only', result.selection_metadata.selection_reason);
    assertEquals_(1, diagnosticEvents.length);
    assertEquals_('no_usable_stats_payload', diagnosticEvents[0].event_type);
  } finally {
    UrlFetchApp.fetch = originalFetch;
    getStateJson_ = originalGetStateJson;
    logDiagnosticEvent_ = originalLogDiagnosticEvent;
  }
}


function testSummarizeTaLeadersParseDiagnostics_tracksCanonicalizationSamplesAndFieldCounts_() {
  const rows = [
    { player_name: 'Élise Mertens', ranking: 21, hold_pct: 66, break_pct: 39 },
    { player_name: 'Iga Świątek', ranking: 1, hold_pct: 78, break_pct: 51 },
    { player_name: 'Iga Swiatek', ranking: 1, hold_pct: 79, break_pct: 50 },
  ];
  const statsByPlayer = {
    'elise mertens': { ranking: 21, hold_pct: 0.66, break_pct: 0.39 },
    'iga swiatek': { ranking: 1, hold_pct: 0.78, break_pct: 0.51 },
  };

  const diagnostics = summarizeTaLeadersParseDiagnostics_(rows, statsByPlayer);

  assertEquals_(3, diagnostics.parsed_player_key_count);
  assertTrue_(diagnostics.parsed_player_key_samples_before_normalization.indexOf('Élise Mertens') >= 0);
  assertTrue_(diagnostics.parsed_player_key_samples_after_normalization.indexOf('elise mertens') >= 0);
  assertTrue_(diagnostics.parsed_player_key_samples_after_normalization.indexOf('iga swiatek') >= 0);
  assertEquals_(2, diagnostics.non_null_feature_count_by_field.ranking);
  assertEquals_(2, diagnostics.non_null_feature_count_by_field.hold_pct);
  assertEquals_(2, diagnostics.non_null_feature_count_by_field.break_pct);
}


function testExtractMatchMxRows_parsesStructuredAssignmentRowsContainer_() {
  const payload = 'var matchmx={meta:{source:"wta"},rows:[["2025-03-01","Doha","Hard","Iga Swiatek","Aryna Sabalenka","6-4 6-4",1,82,78,71,44],["2025-03-01","Doha","Hard","E. Mertens","Aryna Sabalenka","6-4 6-4",21,62,58,71,39]]};';
  const extracted = extractMatchMxRows_(payload);
  assertEquals_(1, extracted.rows.length);
  assertEquals_('Iga Swiatek', extracted.rows[0].player_name);
  assertEquals_('structured_assignment', extracted.diagnostics.parser_format);
  assertEquals_('matchmx.rows', extracted.diagnostics.row_container_path);
}

function testEvaluateTaLeadersQualityGate_rejectsSingleCharNamesAndLowDistinctPlayers_() {
  const diagnostics = {
    canonical_name_sanity: {
      valid_ratio: 1,
      invalid_single_letter_token: 1,
    },
    canonical_overlap: {
      overlap_with_scheduled_ratio: 0.4,
    },
    non_null_by_feature: {
      ranking: 6,
      hold_pct: 6,
      break_pct: 6,
    },
    non_zero_non_null_feature_total: 6,
    unique_players_parsed: 2,
  };
  const completeness = { players_with_non_null_stats: 4 };
  const result = evaluateTaLeadersQualityGate_(diagnostics, completeness, {});
  assertEquals_(false, result.meets_thresholds);
  assertEquals_('ta_matchmx_name_quality_low', result.reason_code);

  const diagnosticsDistinct = {
    canonical_name_sanity: {
      valid_ratio: 1,
      invalid_single_letter_token: 0,
    },
    canonical_overlap: {
      overlap_with_scheduled_ratio: 0.4,
    },
    non_null_by_feature: {
      ranking: 6,
      hold_pct: 6,
      break_pct: 6,
    },
    non_zero_non_null_feature_total: 6,
    unique_players_parsed: 3,
  };
  const distinctResult = evaluateTaLeadersQualityGate_(diagnosticsDistinct, completeness, {});
  assertEquals_(false, distinctResult.meets_thresholds);
  assertEquals_('ta_matchmx_distinct_players_low', distinctResult.reason_code);
}

function testFetchPlayerStatsFromLeadersSource_failsFastWhenFreshRowsLargeButNoMatchedPlayers_() {
  const originalFetch = UrlFetchApp.fetch;
  const originalGetStateJson = getStateJson_;
  const originalLogDiagnosticEvent = logDiagnosticEvent_;
  const diagnosticEvents = [];

  try {
    getStateJson_ = function () { return null; };
    logDiagnosticEvent_ = function (config, eventType, payload) {
      diagnosticEvents.push({ event_type: eventType, payload: payload });
    };

    UrlFetchApp.fetch = function (url) {
      if (url.indexOf('leaders_wta.cgi') >= 0) {
        return {
          getResponseCode: function () { return 200; },
          getContentText: function () { return '<script src="/jsmatches/test_leadersource_wta.js"></script>'; },
        };
      }
      const rows = [];
      for (let i = 0; i < 520; i += 1) {
        rows.push('matchmx[' + i + ']=["2025-03-01","Doha","Hard","Player ' + i + '","Opponent","6-4 6-4",' + (100 + i) + ',62,58,71,39];');
      }
      return {
        getResponseCode: function () { return 200; },
        getContentText: function () { return rows.join('\n'); },
      };
    };

    const result = fetchPlayerStatsFromLeadersSource_(['Iga Swiatek', 'Elise Mertens'], {}, new Date('2025-03-01T00:00:00.000Z'));

    assertEquals_(false, result.ok);
    assertEquals_('ta_matchmx_unusable_payload', result.reason_code);
    assertEquals_('fresh_unusable_non_null_zero', result.selection_metadata.selection_reason);
    assertEquals_(520, result.selection_metadata.fresh_rows);
    assertEquals_(0, result.selection_metadata.players_with_non_null_stats);
    assertEquals_(1, diagnosticEvents.length);
    assertEquals_('no_usable_stats_payload', diagnosticEvents[0].event_type);
  } finally {
    UrlFetchApp.fetch = originalFetch;
    getStateJson_ = originalGetStateJson;
    logDiagnosticEvent_ = originalLogDiagnosticEvent;
  }
}

function testResolveCompetitionTier_acceptsWtaIndianWellsAsWta1000_() {
  const resolverConfig = buildCompetitionTierResolverConfig_({
    ALLOW_WTA_250: true,
    COMPETITION_SOURCE_FIELDS_JSON: DEFAULT_CONFIG.COMPETITION_SOURCE_FIELDS_JSON,
    GRAND_SLAM_ALIASES_JSON: DEFAULT_CONFIG.GRAND_SLAM_ALIASES_JSON,
    WTA_1000_ALIASES_JSON: DEFAULT_CONFIG.WTA_1000_ALIASES_JSON,
    WTA_500_ALIASES_JSON: DEFAULT_CONFIG.WTA_500_ALIASES_JSON,
    COMPETITION_DENY_ALIASES_JSON: DEFAULT_CONFIG.COMPETITION_DENY_ALIASES_JSON,
    GRAND_SLAM_ALIAS_MAP_JSON: DEFAULT_CONFIG.GRAND_SLAM_ALIAS_MAP_JSON,
    WTA_500_ALIAS_MAP_JSON: DEFAULT_CONFIG.WTA_500_ALIAS_MAP_JSON,
    WTA_1000_ALIAS_MAP_JSON: DEFAULT_CONFIG.WTA_1000_ALIAS_MAP_JSON,
    COMPETITION_DENY_ALIAS_MAP_JSON: DEFAULT_CONFIG.COMPETITION_DENY_ALIAS_MAP_JSON,
  });

  const resolved = resolveCompetitionTier_({ competition: 'WTA Indian Wells' }, resolverConfig);

  assertEquals_('WTA_1000', resolved.canonical_tier);
  assertEquals_('competition', resolved.matched_field);
  assertEquals_('WTA Indian Wells', resolved.matched_value);
}

function testResolveCompetitionTier_acceptsWta1000HyphenAsWta1000_() {
  const resolverConfig = buildCompetitionTierResolverConfig_({
    ALLOW_WTA_250: true,
    COMPETITION_SOURCE_FIELDS_JSON: DEFAULT_CONFIG.COMPETITION_SOURCE_FIELDS_JSON,
    GRAND_SLAM_ALIASES_JSON: DEFAULT_CONFIG.GRAND_SLAM_ALIASES_JSON,
    WTA_1000_ALIASES_JSON: DEFAULT_CONFIG.WTA_1000_ALIASES_JSON,
    WTA_500_ALIASES_JSON: DEFAULT_CONFIG.WTA_500_ALIASES_JSON,
    COMPETITION_DENY_ALIASES_JSON: DEFAULT_CONFIG.COMPETITION_DENY_ALIASES_JSON,
    GRAND_SLAM_ALIAS_MAP_JSON: DEFAULT_CONFIG.GRAND_SLAM_ALIAS_MAP_JSON,
    WTA_500_ALIAS_MAP_JSON: DEFAULT_CONFIG.WTA_500_ALIAS_MAP_JSON,
    WTA_1000_ALIAS_MAP_JSON: DEFAULT_CONFIG.WTA_1000_ALIAS_MAP_JSON,
    COMPETITION_DENY_ALIAS_MAP_JSON: DEFAULT_CONFIG.COMPETITION_DENY_ALIAS_MAP_JSON,
  });

  const resolved = resolveCompetitionTier_({ competition: 'WTA-1000 Miami' }, resolverConfig);

  assertEquals_('WTA_1000', resolved.canonical_tier);
  assertEquals_('competition', resolved.matched_field);
}

function testResolveCompetitionTier_mixedSourceFieldsUsesEventNameWhenCompetitionEmpty_() {
  const resolverConfig = buildCompetitionTierResolverConfig_({
    ALLOW_WTA_250: true,
    COMPETITION_SOURCE_FIELDS_JSON: '["competition","sport_title","event_name"]',
    GRAND_SLAM_ALIASES_JSON: DEFAULT_CONFIG.GRAND_SLAM_ALIASES_JSON,
    WTA_1000_ALIASES_JSON: DEFAULT_CONFIG.WTA_1000_ALIASES_JSON,
    WTA_500_ALIASES_JSON: DEFAULT_CONFIG.WTA_500_ALIASES_JSON,
    COMPETITION_DENY_ALIASES_JSON: DEFAULT_CONFIG.COMPETITION_DENY_ALIASES_JSON,
    GRAND_SLAM_ALIAS_MAP_JSON: DEFAULT_CONFIG.GRAND_SLAM_ALIAS_MAP_JSON,
    WTA_500_ALIAS_MAP_JSON: DEFAULT_CONFIG.WTA_500_ALIAS_MAP_JSON,
    WTA_1000_ALIAS_MAP_JSON: DEFAULT_CONFIG.WTA_1000_ALIAS_MAP_JSON,
    COMPETITION_DENY_ALIAS_MAP_JSON: DEFAULT_CONFIG.COMPETITION_DENY_ALIAS_MAP_JSON,
  });

  const resolved = resolveCompetitionTier_({
    competition: '',
    sport_title: 'Tennis',
    event_name: 'WTA Indian Wells',
  }, resolverConfig);

  assertEquals_('WTA_1000', resolved.canonical_tier);
  assertEquals_('event_name', resolved.matched_field);
  assertEquals_('WTA Indian Wells', resolved.matched_value);
}

function testResolveCompetitionTier_prioritizesSpecificEventOverGenericWtaSource_() {
  const resolverConfig = buildCompetitionTierResolverConfig_({
    ALLOW_WTA_250: true,
    COMPETITION_SOURCE_FIELDS_JSON: '["competition","sport_title","event_name"]',
    GRAND_SLAM_ALIASES_JSON: DEFAULT_CONFIG.GRAND_SLAM_ALIASES_JSON,
    WTA_1000_ALIASES_JSON: DEFAULT_CONFIG.WTA_1000_ALIASES_JSON,
    WTA_500_ALIASES_JSON: DEFAULT_CONFIG.WTA_500_ALIASES_JSON,
    COMPETITION_DENY_ALIASES_JSON: DEFAULT_CONFIG.COMPETITION_DENY_ALIASES_JSON,
    GRAND_SLAM_ALIAS_MAP_JSON: DEFAULT_CONFIG.GRAND_SLAM_ALIAS_MAP_JSON,
    WTA_500_ALIAS_MAP_JSON: DEFAULT_CONFIG.WTA_500_ALIAS_MAP_JSON,
    WTA_1000_ALIAS_MAP_JSON: DEFAULT_CONFIG.WTA_1000_ALIAS_MAP_JSON,
    COMPETITION_DENY_ALIAS_MAP_JSON: DEFAULT_CONFIG.COMPETITION_DENY_ALIAS_MAP_JSON,
  });

  const resolved = resolveCompetitionTier_({
    competition: 'WTA Tour',
    sport_title: 'WTA',
    event_name: 'WTA Indian Wells',
  }, resolverConfig);

  assertEquals_('WTA_1000', resolved.canonical_tier);
  assertEquals_('event_name', resolved.matched_field);
  assertEquals_('WTA Indian Wells', resolved.matched_value);
}

function testResolveCompetitionTier_acceptsIndianWellsVariantInCompetition_() {
  const resolverConfig = buildCompetitionTierResolverConfig_({
    ALLOW_WTA_250: true,
    COMPETITION_SOURCE_FIELDS_JSON: DEFAULT_CONFIG.COMPETITION_SOURCE_FIELDS_JSON,
    GRAND_SLAM_ALIASES_JSON: DEFAULT_CONFIG.GRAND_SLAM_ALIASES_JSON,
    WTA_1000_ALIASES_JSON: DEFAULT_CONFIG.WTA_1000_ALIASES_JSON,
    WTA_500_ALIASES_JSON: DEFAULT_CONFIG.WTA_500_ALIASES_JSON,
    COMPETITION_DENY_ALIASES_JSON: DEFAULT_CONFIG.COMPETITION_DENY_ALIASES_JSON,
    GRAND_SLAM_ALIAS_MAP_JSON: DEFAULT_CONFIG.GRAND_SLAM_ALIAS_MAP_JSON,
    WTA_500_ALIAS_MAP_JSON: DEFAULT_CONFIG.WTA_500_ALIAS_MAP_JSON,
    WTA_1000_ALIAS_MAP_JSON: DEFAULT_CONFIG.WTA_1000_ALIAS_MAP_JSON,
    COMPETITION_DENY_ALIAS_MAP_JSON: DEFAULT_CONFIG.COMPETITION_DENY_ALIAS_MAP_JSON,
  });

  const resolved = resolveCompetitionTier_({ competition: 'BNP Paribas Open - WTA Indian Wells' }, resolverConfig);

  assertEquals_('WTA_1000', resolved.canonical_tier);
  assertEquals_('competition', resolved.matched_field);
}

function testDescribeCompetitionDecision_includesRawCanonicalTierAndReason_() {
  const resolved = {
    canonical_tier: 'WTA_1000',
    matched_field: 'event_name',
    matched_value: 'WTA Indian Wells',
    raw_fields: [
      { field: 'competition', value: 'WTA Tour' },
      { field: 'event_name', value: 'WTA Indian Wells' },
    ],
  };
  const decision = { allowed: true, reason_code: 'allowed_wta1000' };

  const trace = describeCompetitionDecision_(resolved, decision);

  assertEquals_('WTA Indian Wells', trace.raw_competition);
  assertEquals_('wta indian wells', trace.canonical_competition);
  assertEquals_('WTA_1000', trace.resolved_tier);
  assertEquals_('allow', trace.allow_decision);
  assertEquals_('allowed_wta1000', trace.decision_reason);
  assertEquals_('event_name', trace.source_field);
}

function testResolveRejectionSource_prefersMatchedFieldThenFallback_() {
  const fromMatched = resolveRejectionSource_({
    matched_field: 'competition',
    matched_value: 'WTA 750 Demo',
    raw_fields: [{ field: 'competition', value: 'WTA 750 Demo' }],
  });
  assertEquals_('competition', fromMatched.field);
  assertEquals_('WTA 750 Demo', fromMatched.value);

  const fromFallback = resolveRejectionSource_({
    matched_field: '',
    matched_value: '',
    raw_fields: [
      { field: 'competition', value: '' },
      { field: 'sport_title', value: 'WTA Tour' },
      { field: 'event_name', value: 'Qualifier Draw' },
    ],
  });
  assertEquals_('sport_title', fromFallback.field);
  assertEquals_('WTA Tour', fromFallback.value);
}



function testSetStateValue_oversizedPlayerStatsMetaWritesSummary_() {
  const originalUpsert = upsertSheetRows_;
  const originalLoggerLog = Logger.log;
  const originalUtilities = Utilities;
  const writes = [];
  const logs = [];

  upsertSheetRows_ = function (sheetName, headers, rows) {
    writes.push({ sheetName: sheetName, headers: headers, rows: rows });
  };
  Logger.log = function (message) { logs.push(String(message || '')); };
  Utilities = {
    formatDate: function () { return '2025-01-01T00:00:00'; },
    newBlob: function (text) {
      return {
        getBytes: function () {
          return String(text || '').split('').map(function (ch) { return ch.charCodeAt(0) & 255; });
        },
      };
    },
  };

  try {
    const oversized = JSON.stringify({
      cache_key: 'PLAYER_STATS_PAYLOAD|bucket|123',
      source: 'ta_leaders',
      cached_at_ms: 1730000000000,
      stats_by_player: {
        'Player One': { blob: new Array(60000).join('x') },
      },
    });

    setStateValue_('PLAYER_STATS_STALE_PAYLOAD', oversized, {
      source_meta: {
        source: 'ta_leaders',
        reference_key: 'PLAYER_STATS_PAYLOAD|bucket|123',
        storage_path: 'chunked_compressed',
      },
    });

    const stateWrite = writes[0].rows[0];
    const parsed = JSON.parse(stateWrite.value);
    assertEquals_('state_value_summarized_size_guard', parsed.reason_code);
    assertEquals_('PLAYER_STATS_STALE_PAYLOAD', parsed.key);
    assertEquals_('PLAYER_STATS_PAYLOAD|bucket|123', parsed.reference_key);
    assertEquals_('chunked_compressed', parsed.storage_path);
    assertTrue_(logs.join('\n').indexOf('state_value_size_guard_applied') >= 0, 'expected explicit guard log line');
  } finally {
    upsertSheetRows_ = originalUpsert;
    Logger.log = originalLoggerLog;
    Utilities = originalUtilities;
  }
}

function testPersistPlayerStatsMeta_oversizedMetadataSummarizesWithReference_() {
  const originalUpsert = upsertSheetRows_;
  const originalLoggerLog = Logger.log;
  const originalUtilities = Utilities;
  const writes = [];

  upsertSheetRows_ = function (sheetName, headers, rows) {
    writes.push({ sheetName: sheetName, headers: headers, rows: rows });
  };
  Logger.log = function () {};
  Utilities = {
    formatDate: function () { return '2025-01-01T00:00:00'; },
    newBlob: function (text) {
      return {
        getBytes: function () {
          return String(text || '').split('').map(function (ch) { return ch.charCodeAt(0) & 255; });
        },
      };
    },
  };

  try {
    const stats = {};
    for (let i = 0; i < 1200; i += 1) {
      stats['Player ' + i] = {
        hold_pct: 0.6,
        break_pct: 0.4,
        notes: new Array(60).join('meta'),
      };
    }

    persistPlayerStatsMeta_({
      cache_key: 'PLAYER_STATS_PAYLOAD|meta|987',
      cached_at_ms: 1730000000000,
      as_of_time: '2025-03-01T00:00:00.000Z',
      player_count: 1200,
      stats_by_player: stats,
    }, 'fresh_api', 1730000000000, 1200, new Date('2025-03-01T00:00:00.000Z'), {
      api_call_count: 3,
      provider_available: true,
    });

    const staleWrite = writes.filter(function (x) { return x.rows[0].key === 'PLAYER_STATS_STALE_PAYLOAD'; })[0];
    const metaWrite = writes.filter(function (x) { return x.rows[0].key === 'PLAYER_STATS_LAST_FETCH_META'; })[0];
    const staleParsed = JSON.parse(staleWrite.rows[0].value);
    const metaParsed = JSON.parse(metaWrite.rows[0].value);

    assertEquals_('state_value_summarized_size_guard', staleParsed.reason_code);
    assertEquals_('PLAYER_STATS_PAYLOAD|meta|987', staleParsed.reference_key);
    assertEquals_('cache_reference', staleParsed.storage_path);
    assertEquals_('fresh_api', metaParsed.source);
    assertEquals_(1200, metaParsed.player_count);
    assertEquals_(1200, metaParsed.players_with_non_null_stats);
    assertEquals_(0, metaParsed.players_with_null_only_stats);
    assertEquals_(true, metaParsed.has_stats);
  } finally {
    upsertSheetRows_ = originalUpsert;
    Logger.log = originalLoggerLog;
    Utilities = originalUtilities;
  }
}

function testResolveOddsWindowForPipeline_tieredCadence_lowTierSkipsWhenMarketUnchanged_() {
  const originalFetchScheduleFromOddsApi = fetchScheduleFromOddsApi_;
  const originalUpdateCreditStateFromHeaders = updateCreditStateFromHeaders_;
  const originalGetCachedPayload = getCachedPayload_;
  const originalGetStateJson = getStateJson_;
  const originalGetCreditAwareRuntimeConfig = getCreditAwareRuntimeConfig_;

  const nowMs = Date.parse('2025-03-01T00:00:00.000Z');
  const firstStartMs = nowMs + (200 * 60000);

  fetchScheduleFromOddsApi_ = function () {
    return {
      events: [{ start_time: new Date(firstStartMs), competition: 'WTA 500' }],
      reason_code: 'schedule_api_success',
      api_credit_usage: 0,
      api_call_count: 1,
      credit_headers: {},
    };
  };
  updateCreditStateFromHeaders_ = function () {};
  getCachedPayload_ = function () {
    return {
      events: [{ odds_updated_time: new Date(nowMs - (10 * 60000)) }],
    };
  };
  getStateJson_ = function (key) {
    if (key === 'ODDS_WINDOW_STALE_PAYLOAD') return { events: [] };
    if (key === 'ODDS_WINDOW_LAST_FETCH_META') return { cached_at_ms: nowMs - (5 * 60000) };
    return null;
  };
  getCreditAwareRuntimeConfig_ = function () {
    return {
      mode: 'normal',
      snapshot: {},
      odds_refresh_tier_low_upper_min: 240,
      odds_refresh_tier_med_upper_min: 180,
      odds_refresh_tier_high_upper_min: 90,
      odds_refresh_tier_low_interval_min: 20,
      odds_refresh_tier_med_interval_min: 10,
      odds_refresh_tier_high_interval_min: 5,
    };
  };

  try {
    const decision = resolveOddsWindowForPipeline_({
      LOOKAHEAD_HOURS: 36,
      ODDS_WINDOW_PRE_FIRST_MIN: 240,
      ODDS_WINDOW_POST_LAST_MIN: 60,
      ODDS_NO_GAMES_BEHAVIOR: 'SKIP',
    }, nowMs);

    assertEquals_('low', decision.refresh_tier);
    assertEquals_(20, decision.refresh_cadence_min);
    assertEquals_(false, decision.should_fetch_odds);
    assertEquals_('odds_refresh_skipped_tier_cadence_no_market_update', decision.decision_reason_code);
  } finally {
    fetchScheduleFromOddsApi_ = originalFetchScheduleFromOddsApi;
    updateCreditStateFromHeaders_ = originalUpdateCreditStateFromHeaders;
    getCachedPayload_ = originalGetCachedPayload;
    getStateJson_ = originalGetStateJson;
    getCreditAwareRuntimeConfig_ = originalGetCreditAwareRuntimeConfig;
  }
}

function testResolveOddsWindowForPipeline_tieredCadence_mediumTierBoundaryFetchesAfterCadenceElapsed_() {
  const originalFetchScheduleFromOddsApi = fetchScheduleFromOddsApi_;
  const originalUpdateCreditStateFromHeaders = updateCreditStateFromHeaders_;
  const originalGetCachedPayload = getCachedPayload_;
  const originalGetStateJson = getStateJson_;
  const originalGetCreditAwareRuntimeConfig = getCreditAwareRuntimeConfig_;

  const nowMs = Date.parse('2025-03-01T00:00:00.000Z');
  const firstStartMs = nowMs + (180 * 60000);

  fetchScheduleFromOddsApi_ = function () {
    return {
      events: [{ start_time: new Date(firstStartMs), competition: 'WTA 500' }],
      reason_code: 'schedule_api_success',
      api_credit_usage: 0,
      api_call_count: 1,
      credit_headers: {},
    };
  };
  updateCreditStateFromHeaders_ = function () {};
  getCachedPayload_ = function () {
    return {
      events: [{ odds_updated_time: new Date(nowMs - (20 * 60000)) }],
    };
  };
  getStateJson_ = function (key) {
    if (key === 'ODDS_WINDOW_STALE_PAYLOAD') return { events: [] };
    if (key === 'ODDS_WINDOW_LAST_FETCH_META') return { cached_at_ms: nowMs - (11 * 60000) };
    return null;
  };
  getCreditAwareRuntimeConfig_ = function () {
    return {
      mode: 'normal',
      snapshot: {},
      odds_refresh_tier_low_upper_min: 240,
      odds_refresh_tier_med_upper_min: 180,
      odds_refresh_tier_high_upper_min: 90,
      odds_refresh_tier_low_interval_min: 20,
      odds_refresh_tier_med_interval_min: 10,
      odds_refresh_tier_high_interval_min: 5,
    };
  };

  try {
    const decision = resolveOddsWindowForPipeline_({
      LOOKAHEAD_HOURS: 36,
      ODDS_WINDOW_PRE_FIRST_MIN: 240,
      ODDS_WINDOW_POST_LAST_MIN: 60,
      ODDS_NO_GAMES_BEHAVIOR: 'SKIP',
    }, nowMs);

    assertEquals_('medium', decision.refresh_tier);
    assertEquals_(10, decision.refresh_cadence_min);
    assertEquals_(true, decision.should_fetch_odds);
    assertEquals_('odds_refresh_executed_in_window', decision.decision_reason_code);
  } finally {
    fetchScheduleFromOddsApi_ = originalFetchScheduleFromOddsApi;
    updateCreditStateFromHeaders_ = originalUpdateCreditStateFromHeaders;
    getCachedPayload_ = originalGetCachedPayload;
    getStateJson_ = originalGetStateJson;
    getCreditAwareRuntimeConfig_ = originalGetCreditAwareRuntimeConfig;
  }
}

function testResolveOddsWindowForPipeline_tieredCadence_highTierBoundaryUsesHighCadence_() {
  const originalFetchScheduleFromOddsApi = fetchScheduleFromOddsApi_;
  const originalUpdateCreditStateFromHeaders = updateCreditStateFromHeaders_;
  const originalGetCachedPayload = getCachedPayload_;
  const originalGetStateJson = getStateJson_;
  const originalGetCreditAwareRuntimeConfig = getCreditAwareRuntimeConfig_;

  const nowMs = Date.parse('2025-03-01T00:00:00.000Z');
  const firstStartMs = nowMs + (90 * 60000);

  fetchScheduleFromOddsApi_ = function () {
    return {
      events: [{ start_time: new Date(firstStartMs), competition: 'WTA 500' }],
      reason_code: 'schedule_api_success',
      api_credit_usage: 0,
      api_call_count: 1,
      credit_headers: {},
    };
  };
  updateCreditStateFromHeaders_ = function () {};
  getCachedPayload_ = function () {
    return {
      events: [{ odds_updated_time: new Date(nowMs - (30 * 60000)) }],
    };
  };
  getStateJson_ = function (key) {
    if (key === 'ODDS_WINDOW_STALE_PAYLOAD') return { events: [] };
    if (key === 'ODDS_WINDOW_LAST_FETCH_META') return { cached_at_ms: nowMs - (6 * 60000) };
    return null;
  };
  getCreditAwareRuntimeConfig_ = function () {
    return {
      mode: 'normal',
      snapshot: {},
      odds_refresh_tier_low_upper_min: 240,
      odds_refresh_tier_med_upper_min: 180,
      odds_refresh_tier_high_upper_min: 90,
      odds_refresh_tier_low_interval_min: 20,
      odds_refresh_tier_med_interval_min: 10,
      odds_refresh_tier_high_interval_min: 5,
    };
  };

  try {
    const decision = resolveOddsWindowForPipeline_({
      LOOKAHEAD_HOURS: 36,
      ODDS_WINDOW_PRE_FIRST_MIN: 240,
      ODDS_WINDOW_POST_LAST_MIN: 60,
      ODDS_NO_GAMES_BEHAVIOR: 'SKIP',
    }, nowMs);

    assertEquals_('high', decision.refresh_tier);
    assertEquals_(5, decision.refresh_cadence_min);
    assertEquals_(true, decision.should_fetch_odds);
  } finally {
    fetchScheduleFromOddsApi_ = originalFetchScheduleFromOddsApi;
    updateCreditStateFromHeaders_ = originalUpdateCreditStateFromHeaders;
    getCachedPayload_ = originalGetCachedPayload;
    getStateJson_ = originalGetStateJson;
    getCreditAwareRuntimeConfig_ = originalGetCreditAwareRuntimeConfig;
  }
}

function testResolveOddsWindowForPipeline_tieredCadence_fetchesWhenMarketUpdatedAfterLastFetch_() {
  const originalFetchScheduleFromOddsApi = fetchScheduleFromOddsApi_;
  const originalUpdateCreditStateFromHeaders = updateCreditStateFromHeaders_;
  const originalGetCachedPayload = getCachedPayload_;
  const originalGetStateJson = getStateJson_;
  const originalGetCreditAwareRuntimeConfig = getCreditAwareRuntimeConfig_;

  const nowMs = Date.parse('2025-03-01T00:00:00.000Z');
  const firstStartMs = nowMs + (200 * 60000);

  fetchScheduleFromOddsApi_ = function () {
    return {
      events: [{ start_time: new Date(firstStartMs), competition: 'WTA 500' }],
      reason_code: 'schedule_api_success',
      api_credit_usage: 0,
      api_call_count: 1,
      credit_headers: {},
    };
  };
  updateCreditStateFromHeaders_ = function () {};
  getCachedPayload_ = function () {
    return {
      events: [{ odds_updated_time: new Date(nowMs - (2 * 60000)) }],
    };
  };
  getStateJson_ = function (key) {
    if (key === 'ODDS_WINDOW_STALE_PAYLOAD') return { events: [] };
    if (key === 'ODDS_WINDOW_LAST_FETCH_META') return { cached_at_ms: nowMs - (5 * 60000) };
    return null;
  };
  getCreditAwareRuntimeConfig_ = function () {
    return {
      mode: 'normal',
      snapshot: {},
      odds_refresh_tier_low_upper_min: 240,
      odds_refresh_tier_med_upper_min: 180,
      odds_refresh_tier_high_upper_min: 90,
      odds_refresh_tier_low_interval_min: 20,
      odds_refresh_tier_med_interval_min: 10,
      odds_refresh_tier_high_interval_min: 5,
    };
  };

  try {
    const decision = resolveOddsWindowForPipeline_({
      LOOKAHEAD_HOURS: 36,
      ODDS_WINDOW_PRE_FIRST_MIN: 240,
      ODDS_WINDOW_POST_LAST_MIN: 60,
      ODDS_NO_GAMES_BEHAVIOR: 'SKIP',
    }, nowMs);

    assertEquals_('low', decision.refresh_tier);
    assertEquals_(true, decision.should_fetch_odds);
    assertEquals_('odds_refresh_executed_in_window', decision.decision_reason_code);
  } finally {
    fetchScheduleFromOddsApi_ = originalFetchScheduleFromOddsApi;
    updateCreditStateFromHeaders_ = originalUpdateCreditStateFromHeaders;
    getCachedPayload_ = originalGetCachedPayload;
    getStateJson_ = originalGetStateJson;
    getCreditAwareRuntimeConfig_ = originalGetCreditAwareRuntimeConfig;
  }
}

function testResolveOddsWindowForPipeline_tieredCadence_outsideWindowBehaviorUnchanged_() {
  const originalFetchScheduleFromOddsApi = fetchScheduleFromOddsApi_;
  const originalUpdateCreditStateFromHeaders = updateCreditStateFromHeaders_;
  const originalGetCachedPayload = getCachedPayload_;
  const originalGetStateJson = getStateJson_;
  const originalGetCreditAwareRuntimeConfig = getCreditAwareRuntimeConfig_;

  const nowMs = Date.parse('2025-03-01T00:00:00.000Z');
  const firstStartMs = nowMs + (400 * 60000);

  fetchScheduleFromOddsApi_ = function () {
    return {
      events: [{ start_time: new Date(firstStartMs), competition: 'WTA 500' }],
      reason_code: 'schedule_api_success',
      api_credit_usage: 0,
      api_call_count: 1,
      credit_headers: {},
    };
  };
  updateCreditStateFromHeaders_ = function () {};
  getCachedPayload_ = function () { return { events: [] }; };
  getStateJson_ = function () { return null; };
  getCreditAwareRuntimeConfig_ = function () {
    return {
      mode: 'normal',
      snapshot: {},
      odds_refresh_tier_low_upper_min: 240,
      odds_refresh_tier_med_upper_min: 180,
      odds_refresh_tier_high_upper_min: 90,
      odds_refresh_tier_low_interval_min: 20,
      odds_refresh_tier_med_interval_min: 10,
      odds_refresh_tier_high_interval_min: 5,
    };
  };

  try {
    const decision = resolveOddsWindowForPipeline_({
      LOOKAHEAD_HOURS: 36,
      ODDS_WINDOW_PRE_FIRST_MIN: 60,
      ODDS_WINDOW_POST_LAST_MIN: 30,
      ODDS_NO_GAMES_BEHAVIOR: 'SKIP',
    }, nowMs);

    assertEquals_(false, decision.should_fetch_odds);
    assertEquals_('odds_refresh_skipped_outside_window', decision.decision_reason_code);
  } finally {
    fetchScheduleFromOddsApi_ = originalFetchScheduleFromOddsApi;
    updateCreditStateFromHeaders_ = originalUpdateCreditStateFromHeaders;
    getCachedPayload_ = originalGetCachedPayload;
    getStateJson_ = originalGetStateJson;
    getCreditAwareRuntimeConfig_ = originalGetCreditAwareRuntimeConfig;
  }
}
function testStageGenerateSignals_sentNotificationIncludesRationale_() {
  const originalDateNow = Date.now;
  const originalGetSignalState = getSignalState_;
  const originalSetSignalState = setSignalState_;
  const originalSetStateValue = setStateValue_;
  const originalLocalAndUtcTimestamps = localAndUtcTimestamps_;
  const originalPostDiscordWebhook = postDiscordWebhook_;
  const originalAppendLogRow = appendLogRow_;

  const nowMs = Date.parse('2026-01-01T00:00:00.000Z');
  const captured = { payload: null };

  Date.now = function () { return nowMs; };
  getSignalState_ = function () { return { sent_hashes: {} }; };
  setSignalState_ = function () {};
  setStateValue_ = function () {};
  localAndUtcTimestamps_ = function () {
    return {
      local: '2026-01-01T00:00:00-07:00',
      utc: '2026-01-01T07:00:00.000Z',
    };
  };
  postDiscordWebhook_ = function (url, payload) {
    captured.payload = payload;
    return { outcome: 'sent', transport: 'discord_webhook', http_status: 204, response_body_preview: '', test_mode: false };
  };

  try {
    const event = {
      event_id: 'evt_rationale_signal',
      market: 'h2h',
      outcome: 'player_a',
      bookmaker: 'book_x',
      price: 150,
      h2h_p1_wins: 8,
      h2h_p2_wins: 2,
      h2h_total_matches: 10,
      commence_time: new Date(nowMs + (5 * 60 * 60 * 1000)),
      odds_updated_time: new Date(nowMs),
    };
    const match = {
      odds_event_id: 'evt_rationale_signal',
      schedule_event_id: 'sched_rationale_signal',
      competition_tier: 'WTA_500',
    };
    const stats = {
      evt_rationale_signal: {
        stats_confidence: 0.86,
        player_a: {
          has_stats: true,
          features: { ranking: 10, recent_form: 0.72, surface_win_rate: 0.66, hold_pct: 0.69, break_pct: 0.42 },
        },
        player_b: {
          has_stats: true,
          features: { ranking: 35, recent_form: 0.49, surface_win_rate: 0.51, hold_pct: 0.61, break_pct: 0.29 },
        },
      },
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
      NOTIFY_ENABLED: true,
      NOTIFY_TEST_MODE: false,
      DISCORD_WEBHOOK: 'https://discord.example/webhook',
      H2H_BUMP_ENABLED: true,
      H2H_MIN_MATCHES: 3,
      H2H_MAX_ABS_BUMP: 0.05,
    };

    const result = stageGenerateSignals('run_rationale_signal', config, [event], [match], stats);

    assertEquals_(1, result.rows.length);
    assertEquals_('sent', result.rows[0].notification_outcome);
    assertTrue_(!!captured.payload);
    assertContains_(captured.payload.content, '**Why this edge**');
    assertContains_(captured.payload.content, 'Model win probability');
    assertContains_(captured.payload.content, 'Top stat drivers');
    assertContains_(captured.payload.content, 'Head-to-head added');
    assertContains_(captured.payload.content, 'Stats confidence is');
  } finally {
    Date.now = originalDateNow;
    getSignalState_ = originalGetSignalState;
    setSignalState_ = originalSetSignalState;
    setStateValue_ = originalSetStateValue;
    localAndUtcTimestamps_ = originalLocalAndUtcTimestamps;
    postDiscordWebhook_ = originalPostDiscordWebhook;
    appendLogRow_ = originalAppendLogRow;
  }
}

function testBuildSignalRationaleParagraph_fallbackGenericAndLengthBounded_() {
  const generic = buildSignalRationaleParagraph_({
    edge_value: null,
    model_probability: null,
    market_implied_probability: null,
    stats_confidence: null,
    stats_bundle: {
      player_a: { has_stats: false, features: {} },
      player_b: { has_stats: false, features: {} },
      stats_confidence: 0,
    },
    h2h_decision: { applied: false, bump: 0 },
  });

  assertContains_(generic, 'Model signals an edge over current market pricing');
  assertContains_(generic, 'Player feature coverage is limited');
  assertContains_(generic, 'Stats confidence is unavailable');
  assertTrue_(generic.length <= 680, 'generic rationale should be bounded');

  const longRationale = buildSignalRationaleParagraph_({
    edge_value: 0.1234,
    model_probability: 0.7321,
    market_implied_probability: 0.5333,
    stats_confidence: 0.91,
    stats_bundle: {
      stats_confidence: 0.91,
      player_a: {
        has_stats: true,
        features: {
          ranking: 2,
          recent_form: 0.9,
          surface_win_rate: 0.88,
          hold_pct: 0.82,
          break_pct: 0.52,
          first_serve_points_won_pct: 0.8,
          second_serve_points_won_pct: 0.66,
        },
      },
      player_b: {
        has_stats: true,
        features: {
          ranking: 70,
          recent_form: 0.4,
          surface_win_rate: 0.45,
          hold_pct: 0.55,
          break_pct: 0.21,
          first_serve_points_won_pct: 0.58,
          second_serve_points_won_pct: 0.41,
        },
      },
    },
    h2h_decision: { applied: true, bump: 0.05, sample_size: 22 },
  });

  assertTrue_(longRationale.length <= 680, 'long rationale should be trimmed');
}


function testStageGenerateSignals_preActionGuardSuppressesOnLineDrift_() {
  const originalDateNow = Date.now;
  const originalGetSignalState = getSignalState_;
  const originalSetSignalState = setSignalState_;
  const originalSetStateValue = setStateValue_;
  const originalAppendLogRow = appendLogRow_;
  const originalLocalAndUtcTimestamps = localAndUtcTimestamps_;

  const nowMs = Date.parse('2026-01-01T00:10:00.000Z');
  Date.now = function () { return nowMs; };
  getSignalState_ = function () { return { sent_hashes: {} }; };
  setSignalState_ = function () {};
  setStateValue_ = function () {};
  appendLogRow_ = function () {};
  localAndUtcTimestamps_ = function () {
    return { local: '2026-01-01T00:10:00-07:00', utc: '2026-01-01T07:10:00.000Z' };
  };

  try {
    const event = {
      event_id: 'evt_line_drift_guard',
      market: 'h2h',
      outcome: 'player_a',
      bookmaker: 'book_x',
      bookmaker_keys_considered: ['book_x', 'book_y'],
      price: 130,
      open_price: 200,
      commence_time: new Date(nowMs + (5 * 60 * 60 * 1000)),
      odds_updated_time: new Date(nowMs),
      open_timestamp_epoch_ms: nowMs - (10 * 60 * 1000),
    };
    const match = {
      odds_event_id: 'evt_line_drift_guard',
      schedule_event_id: 'sched_line_drift_guard',
      competition_tier: 'WTA_500',
    };
    const stats = {
      evt_line_drift_guard: {
        player_a: { has_stats: true, features: { ranking: 5, recent_form: 0.7, surface_win_rate: 0.68, hold_pct: 0.72, break_pct: 0.4 } },
        player_b: { has_stats: true, features: { ranking: 35, recent_form: 0.43, surface_win_rate: 0.49, hold_pct: 0.59, break_pct: 0.27 } },
      },
    };
    const config = {
      MODEL_VERSION: 'test_model_v1', EDGE_THRESHOLD_MICRO: 0.001, EDGE_THRESHOLD_SMALL: 0.03, EDGE_THRESHOLD_MED: 0.05,
      EDGE_THRESHOLD_STRONG: 0.08, STAKE_UNITS_MICRO: 0.25, STAKE_UNITS_SMALL: 0.5, STAKE_UNITS_MED: 1,
      STAKE_UNITS_STRONG: 1.5, SIGNAL_COOLDOWN_MIN: 180, MINUTES_BEFORE_START_CUTOFF: 60, STALE_ODDS_WINDOW_MIN: 60,
      MAX_CURRENT_VS_OPEN_LINE_DELTA: 0.05, MAX_MINUTES_SINCE_OPEN_SNAPSHOT: 0, MIN_BOOK_COUNT: 0, MIN_LIQUIDITY: 0,
      NOTIFY_ENABLED: true, NOTIFY_TEST_MODE: false, DISCORD_WEBHOOK: 'https://hooks.slack.com/services/mock',
    };

    const result = stageGenerateSignals('run_line_drift_guard', config, [event], [match], stats);

    assertEquals_(1, result.rows.length);
    assertEquals_('line_drift_exceeded', result.rows[0].notification_outcome);
    assertEquals_('risk_guard_non_tradable', result.rows[0].signal_delivery_mode);
    assertEquals_(0, Number(result.rows[0].stake_units || 0));
    assertEquals_(1, Number(result.summary.reason_codes.line_drift_exceeded || 0));
    assertEquals_(true, !!(result.rows[0].notification_metadata && result.rows[0].notification_metadata.non_tradable));
  } finally {
    Date.now = originalDateNow;
    getSignalState_ = originalGetSignalState;
    setSignalState_ = originalSetSignalState;
    setStateValue_ = originalSetStateValue;
    appendLogRow_ = originalAppendLogRow;
    localAndUtcTimestamps_ = originalLocalAndUtcTimestamps;
  }
}

function testStageGenerateSignals_preActionGuardSuppressesOnEdgeDecayAndLowBooks_() {
  const originalDateNow = Date.now;
  const originalGetSignalState = getSignalState_;
  const originalSetSignalState = setSignalState_;
  const originalSetStateValue = setStateValue_;
  const originalAppendLogRow = appendLogRow_;
  const originalLocalAndUtcTimestamps = localAndUtcTimestamps_;

  const nowMs = Date.parse('2026-01-01T00:10:00.000Z');
  Date.now = function () { return nowMs; };
  getSignalState_ = function () { return { sent_hashes: {} }; };
  setSignalState_ = function () {};
  setStateValue_ = function () {};
  appendLogRow_ = function () {};
  localAndUtcTimestamps_ = function () {
    return { local: '2026-01-01T00:10:00-07:00', utc: '2026-01-01T07:10:00.000Z' };
  };

  try {
    const event = {
      event_id: 'evt_edge_decay_guard',
      market: 'h2h',
      outcome: 'player_a',
      bookmaker: 'book_x',
      bookmaker_keys_considered: ['book_x'],
      price: 150,
      open_price: 152,
      commence_time: new Date(nowMs + (5 * 60 * 60 * 1000)),
      odds_updated_time: new Date(nowMs),
      open_timestamp_epoch_ms: nowMs - (130 * 60 * 1000),
    };
    const match = {
      odds_event_id: 'evt_edge_decay_guard',
      schedule_event_id: 'sched_edge_decay_guard',
      competition_tier: 'WTA_500',
    };
    const stats = {
      evt_edge_decay_guard: {
        player_a: { has_stats: true, features: { ranking: 11, recent_form: 0.61, surface_win_rate: 0.6, hold_pct: 0.66, break_pct: 0.35 } },
        player_b: { has_stats: true, features: { ranking: 19, recent_form: 0.55, surface_win_rate: 0.54, hold_pct: 0.62, break_pct: 0.31 } },
      },
    };
    const config = {
      MODEL_VERSION: 'test_model_v1', EDGE_THRESHOLD_MICRO: 0.001, EDGE_THRESHOLD_SMALL: 0.03, EDGE_THRESHOLD_MED: 0.05,
      EDGE_THRESHOLD_STRONG: 0.08, STAKE_UNITS_MICRO: 0.25, STAKE_UNITS_SMALL: 0.5, STAKE_UNITS_MED: 1,
      STAKE_UNITS_STRONG: 1.5, SIGNAL_COOLDOWN_MIN: 180, MINUTES_BEFORE_START_CUTOFF: 60, STALE_ODDS_WINDOW_MIN: 60,
      MAX_CURRENT_VS_OPEN_LINE_DELTA: 0.25, MAX_MINUTES_SINCE_OPEN_SNAPSHOT: 60, MIN_BOOK_COUNT: 2, MIN_LIQUIDITY: 0,
      NOTIFY_ENABLED: true, NOTIFY_TEST_MODE: false, DISCORD_WEBHOOK: 'https://hooks.slack.com/services/mock',
    };

    const result = stageGenerateSignals('run_edge_decay_guard', config, [event], [match], stats);

    assertEquals_(1, result.rows.length);
    assertEquals_('edge_decay_exceeded', result.rows[0].notification_outcome);
    assertEquals_(1, Number(result.summary.reason_codes.edge_decay_exceeded || 0));
    assertEquals_(0, Number(result.sentCount || 0));
  } finally {
    Date.now = originalDateNow;
    getSignalState_ = originalGetSignalState;
    setSignalState_ = originalSetSignalState;
    setStateValue_ = originalSetStateValue;
    appendLogRow_ = originalAppendLogRow;
    localAndUtcTimestamps_ = originalLocalAndUtcTimestamps;
  }
}

function testStageFetchSchedule_updatesCreditBurnRateAndWarningCodes_() {
  const result = runStageFetchScheduleScenario_({
    stateJson: {
      LAST_SCHEDULE_API_CREDITS: {
        observed_at_utc: '2025-02-28T07:10:00.000Z',
        credit_snapshot: {
          remaining: 200,
        },
      },
      ODDS_API_BURN_RATE_STATE: {
        calls_per_day_rolling: 80,
      },
    },
    fetchResponses: [{
      events: [],
      reason_code: 'schedule_api_success',
      api_credit_usage: 1,
      api_call_count: 1,
      credit_headers: {
        requests_remaining: '100',
      },
    }],
  });

  assertEquals_(1, result.stage.summary.reason_codes.credit_burn_projected_exhaustion_lt_7d || 0);
  assertEquals_(1, result.stage.summary.reason_codes.credit_burn_projected_exhaustion_lt_3d || 0);

  const burnState = JSON.parse(result.stateWrites.ODDS_API_BURN_RATE_STATE || '{}');
  assertEquals_(true, !!burnState.warning_lt_7d);
  assertEquals_(true, !!burnState.warning_lt_3d);
  assertEquals_(100, Number(burnState.credits_remaining || 0));

  const lastCredits = JSON.parse(result.stateWrites.LAST_SCHEDULE_API_CREDITS || '{}');
  assertEquals_(true, !!lastCredits.burn_rate);
  assertEquals_(true, !!lastCredits.credit_snapshot);
}

function testMaybeNotifyCreditBurnRate_oncePerDay_() {
  const originalGetStateJson = getStateJson_;
  const originalSetStateValue = setStateValue_;
  const originalPostDiscordWebhook = postDiscordWebhook_;

  const state = {};
  let webhookCalls = 0;

  getStateJson_ = function (key) {
    return JSON.parse(state[key] || '{}');
  };
  setStateValue_ = function (key, value) {
    state[key] = value;
  };
  postDiscordWebhook_ = function () {
    webhookCalls += 1;
    return {
      outcome: 'sent',
      transport: 'discord_webhook',
      http_status: 204,
      test_mode: false,
    };
  };

  try {
    const config = {
      ODDS_BURN_RATE_NOTIFY_ENABLED: true,
      NOTIFY_ENABLED: true,
      DISCORD_WEBHOOK: 'https://discord.example/webhook',
      NOTIFY_TEST_MODE: false,
    };
    const burnRate = {
      warning_lt_7d: true,
      observed_at_utc: '2025-03-01T07:10:00.000Z',
      projected_days_remaining: 2.5,
      calls_per_day_rolling: 120,
      credits_remaining: 300,
    };

    const first = maybeNotifyCreditBurnRate_(config, 'run_1', burnRate);
    const second = maybeNotifyCreditBurnRate_(config, 'run_2', burnRate);

    assertEquals_(true, first.notify_attempted);
    assertEquals_('sent', first.outcome);
    assertEquals_(false, second.notify_attempted);
    assertEquals_('credit_burn_notify_already_sent_today', second.outcome);
    assertEquals_(1, webhookCalls);

    const notifyState = JSON.parse(state.ODDS_API_BURN_RATE_NOTIFY_STATE || '{}');
    assertEquals_('2025-03-01', String(notifyState.last_notify_day_utc || ''));
  } finally {
    getStateJson_ = originalGetStateJson;
    setStateValue_ = originalSetStateValue;
    postDiscordWebhook_ = originalPostDiscordWebhook;
  }
}

function testMaybeEmitRunRollup_emitsOnConfiguredCadenceWithIndependentPayload_() {
  const originalGetStateJson = getStateJson_;
  const originalSetStateValue = setStateValue_;
  const originalGetTopReasonCodes = getTopReasonCodes_;

  const state = {
    RUN_ROLLUP_STATE: JSON.stringify({
      run_count: 1,
      last_rollup_at_count: 0,
      last_rollup_snapshot: {
        fetched_odds: 2,
        fetched_schedule: 1,
        matched: 1,
        unmatched: 1,
        signals_found: 0,
        run_health_reason_code: 'old_reason',
      },
    }),
  };

  getStateJson_ = function (key) {
    return JSON.parse(state[key] || '{}');
  };
  setStateValue_ = function (key, value) {
    state[key] = value;
  };
  getTopReasonCodes_ = function (codes) {
    return Object.keys(codes || {}).map(function (key) {
      return { reason_code: key, count: Number(codes[key] || 0) };
    }).sort(function (a, b) { return b.count - a.count; });
  };

  try {
    const emitted = maybeEmitRunRollup_({ ROLLUP_EVERY_N_RUNS: 2 }, {
      fetched_odds: 3,
      fetched_schedule: 2,
      matched: 2,
      unmatched: 1,
      signals_found: 1,
      run_health_reason_code: 'new_reason',
      reason_codes: {
        no_player_match: 2,
        matched_count: 0,
      },
      stage_summaries: [
        { stage: 'stageFetchOdds', duration_ms: 100 },
        { stage: 'stageFetchOdds', duration_ms: 300 },
        { stage: 'stageFetchOdds', duration_ms: 400 },
      ],
      watchdog: {
        bootstrap_empty_cycles: 2,
        bootstrap_threshold: 3,
        productive_empty_cycles: 1,
        productive_threshold: 4,
        schedule_only_cycles: 0,
        schedule_only_threshold: 3,
      },
    });

    assertEquals_(true, emitted.emitted);
    assertEquals_(2, Number(emitted.run_count || 0));
    assertEquals_(2, Number(emitted.rollup.run_count || 0));
    assertEquals_(2, Number(emitted.rollup.top_reason_codes[0].count || 0));
    assertEquals_('no_player_match', emitted.rollup.top_reason_codes[0].reason_code);
    assertEquals_(100, Number(emitted.rollup.stage_duration_ms.stageFetchOdds.min || 0));
    assertEquals_(266.67, Number(emitted.rollup.stage_duration_ms.stageFetchOdds.avg || 0));
    assertEquals_(400, Number(emitted.rollup.stage_duration_ms.stageFetchOdds.p95 || 0));
    assertEquals_(1, Number(emitted.rollup.key_deltas_vs_previous_rollup.fetched_odds || 0));
    assertEquals_('old_reason→new_reason', emitted.rollup.key_deltas_vs_previous_rollup.run_health_reason_code);
    assertEquals_('2/3', emitted.rollup.watchdog_progression.bootstrap_empty_cycle.status);

    const standalone = JSON.parse(state.LAST_RUN_ROLLUP_JSON || '{}');
    assertEquals_('run_rollup_v1', standalone.rollup_schema);
    assertEquals_(2, Number(standalone.run_count || 0));
  } finally {
    getStateJson_ = originalGetStateJson;
    setStateValue_ = originalSetStateValue;
    getTopReasonCodes_ = originalGetTopReasonCodes;
  }
}

function testMaybeEmitRunRollup_tracksRunCountWhenCadenceNotReached_() {
  const originalGetStateJson = getStateJson_;
  const originalSetStateValue = setStateValue_;

  const state = {
    RUN_ROLLUP_STATE: JSON.stringify({ run_count: 4, last_rollup_at_count: 3 }),
  };

  getStateJson_ = function (key) {
    return JSON.parse(state[key] || '{}');
  };
  setStateValue_ = function (key, value) {
    state[key] = value;
  };

  try {
    const emitted = maybeEmitRunRollup_({ ROLLUP_EVERY_N_RUNS: 3 }, {
      fetched_odds: 0,
      reason_codes: {},
      stage_summaries: [],
      watchdog: {},
    });

    assertEquals_(false, emitted.emitted);
    assertEquals_(5, Number(emitted.run_count || 0));
    assertEquals_(1, Number(emitted.runs_until_next || 0));
    assertEquals_(undefined, state.LAST_RUN_ROLLUP_JSON);

    const persisted = JSON.parse(state.RUN_ROLLUP_STATE || '{}');
    assertEquals_(5, Number(persisted.run_count || 0));
    assertEquals_(3, Number(persisted.last_rollup_at_count || 0));
  } finally {
    getStateJson_ = originalGetStateJson;
    setStateValue_ = originalSetStateValue;
  }
}

function testAdaptRunLogRecordForLegacy_expandsAliasReasonMapsForLegacyRows_() {
  const adapted = adaptRunLogRecordForLegacy_({
    row_type: 'summary',
    run_id: 'run-1',
    stage: 'runEdgeBoard',
    message: JSON.stringify({ reason_codes: { OR_OUT_WIN: 2 } }),
    rejection_codes: JSON.stringify({ schema_id: REASON_CODE_ALIAS_SCHEMA_ID, reason_codes: { OPEN_LAG_HI: 1 } }),
    stage_summaries: JSON.stringify({
      schema_id: REASON_CODE_ALIAS_SCHEMA_ID,
      stage_summaries: [
        { stage: 'stageFetchOdds', reason_codes: { MM_DIAG_WR: 3 } },
      ],
    }),
  });

  const message = JSON.parse(adapted.message || '{}');
  const rejectionCodes = JSON.parse(adapted.rejection_codes || '{}');
  const stageSummaries = JSON.parse(adapted.stage_summaries || '[]');

  assertEquals_(2, Number((message.reason_codes || {}).odds_refresh_skipped_outside_window || 0));
  assertEquals_(1, Number(rejectionCodes.opening_lag_exceeded || 0));
  assertEquals_(3, Number((stageSummaries[0].reason_codes || {}).match_map_diagnostic_records_written || 0));
}

function testAdaptRunLogRecordForLegacy_reconstructsCompactV2Row_() {
  const adapted = adaptRunLogRecordForLegacy_({
    schema_version: 2,
    et: 'stageFetchOdds',
    rid: 'run-v2',
    st: 'stageFetchOdds',
    sa: '2026-03-12T12:00:00Z',
    ea: '2026-03-12T12:00:02Z',
    ss: 'success',
    rcd: 'stage_completed',
    ic: 10,
    oc: 5,
    pr: 'odds_api',
    acu: 1,
    rc: { OR_OUT_WIN: 2 },
    rm: { resolver: 'canonical' },
    msg: { context: 'compact' },
    rj: { OPEN_LAG_HI: 1 },
    ssu: [{ stage: 'stageFetchOdds', reason_codes: { MM_DIAG_WR: 2 } }],
  });

  const message = JSON.parse(adapted.message || '{}');
  const rejections = JSON.parse(adapted.rejection_codes || '{}');
  const stageSummaries = JSON.parse(adapted.stage_summaries || '[]');

  assertEquals_('stage', adapted.row_type);
  assertEquals_('run-v2', adapted.run_id);
  assertEquals_(2, Number((message.reason_codes || {}).odds_refresh_skipped_outside_window || 0));
  assertEquals_('odds_api', message.provider);
  assertEquals_(1, Number(rejections.opening_lag_exceeded || 0));
  assertEquals_(2, Number((stageSummaries[0].reason_codes || {}).match_map_diagnostic_records_written || 0));
}

function testStageFetchOdds_bypassStaleFallback_returnsApiFailureSource_() {
  const originalFetchOdds = fetchOddsWindowFromOddsApi_;
  const originalGetCachedPayload = getCachedPayload_;
  const originalGetCreditAwareRuntimeConfig = getCreditAwareRuntimeConfig_;
  const originalSetCachedPayload = setCachedPayload_;
  const originalSetStateValue = setStateValue_;
  const originalGetStateJson = getStateJson_;
  const originalUpdateCreditState = updateCreditStateFromHeaders_;
  const originalLocalAndUtcTimestamps = localAndUtcTimestamps_;
  const originalLogDiagnosticEvent = logDiagnosticEvent_;

  fetchOddsWindowFromOddsApi_ = function () {
    return {
      events: [],
      reason_code: 'odds_api_http_500',
      api_credit_usage: 1,
      api_call_count: 1,
      credit_headers: {},
    };
  };
  getCachedPayload_ = function () { return null; };
  getCreditAwareRuntimeConfig_ = function () { return { odds_window_cache_ttl_min: 10, odds_window_refresh_min: 1, mode: 'normal' }; };
  setCachedPayload_ = function () {};
  setStateValue_ = function () {};
  getStateJson_ = function (key) {
    if (key === 'ODDS_WINDOW_STALE_PAYLOAD') {
      return {
        cached_at_ms: Date.parse('2025-01-01T00:00:00.000Z'),
        events: [{
          event_id: 'evt_stale',
          commence_time: '2025-01-01T02:00:00.000Z',
        }],
      };
    }
    return null;
  };
  updateCreditStateFromHeaders_ = function () { return { header_present: false }; };
  localAndUtcTimestamps_ = function () { return { local: '2025-01-01T00:00:00-07:00', utc: '2025-01-01T07:00:00.000Z' }; };
  logDiagnosticEvent_ = function () {};

  try {
    const result = stageFetchOdds('run_odds_bypass_stale', {
      LOOKAHEAD_HOURS: 6,
      ODDS_WINDOW_FORCE_REFRESH: true,
    }, {
      startMs: Date.parse('2025-01-01T00:00:00.000Z'),
      endMs: Date.parse('2025-01-01T06:00:00.000Z'),
    }, {
      bypass_stale_fallback: true,
    });

    assertEquals_('fresh_api', result.selected_source);
    assertEquals_(1, result.summary.reason_codes.odds_api_failure_no_stale_fallback || 0);
    assertEquals_(1, result.summary.reason_codes.stale_fallback_bypassed || 0);
  } finally {
    fetchOddsWindowFromOddsApi_ = originalFetchOdds;
    getCachedPayload_ = originalGetCachedPayload;
    getCreditAwareRuntimeConfig_ = originalGetCreditAwareRuntimeConfig;
    setCachedPayload_ = originalSetCachedPayload;
    setStateValue_ = originalSetStateValue;
    getStateJson_ = originalGetStateJson;
    updateCreditStateFromHeaders_ = originalUpdateCreditState;
    localAndUtcTimestamps_ = originalLocalAndUtcTimestamps;
    logDiagnosticEvent_ = originalLogDiagnosticEvent;
  }
}

function testResolveProductiveOutputMitigationContext_activatesConfiguredFlags_() {
  const originalGetStateJson = getStateJson_;
  const originalAppendLogRow = appendLogRow_;
  const logs = [];

  getStateJson_ = function (key) {
    if (key === 'EMPTY_PRODUCTIVE_OUTPUT_STATE') {
      return { consecutive_count: 3 };
    }
    if (key === 'PRODUCTIVE_OUTPUT_MITIGATION_STATE') {
      return {
        force_fresh_odds_probe_pending: true,
        verbose_diagnostics_capture_pending: true,
      };
    }
    return {};
  };
  appendLogRow_ = function (row) { logs.push(row); };

  try {
    const result = resolveProductiveOutputMitigationContext_('run_mitigation', {
      EMPTY_PRODUCTIVE_OUTPUT_THRESHOLD: 3,
      PRODUCTIVE_OUTPUT_MITIGATION_ENABLED: true,
      PRODUCTIVE_OUTPUT_MITIGATION_OPENING_LAG_WIDEN_ENABLED: true,
      PRODUCTIVE_OUTPUT_MITIGATION_OPENING_LAG_EXTRA_MINUTES: 20,
      PRODUCTIVE_OUTPUT_MITIGATION_FORCE_FRESH_ODDS_PROBE_ENABLED: true,
      PRODUCTIVE_OUTPUT_MITIGATION_VERBOSE_DIAGNOSTICS_ENABLED: true,
    });

    assertEquals_(true, result.opening_lag_active);
    assertEquals_(true, result.force_fresh_odds_probe_active);
    assertEquals_(true, result.verbose_diagnostics_capture_active);
    assertEquals_(1, logs.length);
    assertEquals_('productive_output_mitigation', logs[0].stage);
    assertEquals_('productive_output_mitigation_activated', logs[0].reason_code);
  } finally {
    getStateJson_ = originalGetStateJson;
    appendLogRow_ = originalAppendLogRow;
  }
}
