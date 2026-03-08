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
    assertEquals_(1, warningPayload.failure_reasons.schedule_enrichment_no_schedule_events);
    assertEquals_(1, warningPayload.failure_reasons.no_player_match);
    assertEquals_('odds_1', warningPayload.sample_unmatched_events[0].odds_event_id);
    assertEquals_('no_player_match', warningPayload.sample_unmatched_events[0].rejection_code);
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
      signals_found: 0,
    }, {
      EMPTY_PRODUCTIVE_OUTPUT_THRESHOLD: 3,
    });

    assertEquals_(3, result.consecutive_count);
    assertEquals_(true, result.warning_needed);
    assertEquals_('productive_output_empty_streak_detected', result.reason_code);

    const stored = JSON.parse(writes.EMPTY_PRODUCTIVE_OUTPUT_STATE || '{}');
    assertEquals_(3, stored.consecutive_count);
    assertEquals_(4, stored.fetched_odds);
    assertEquals_(0, stored.signals_found);
  } finally {
    getStateJson_ = originalGetStateJson;
    setStateValue_ = originalSetStateValue;
    localAndUtcTimestamps_ = originalLocalAndUtcTimestamps;
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
    appendStageLog_ = function () {};
    logDiagnosticEvent_ = function () {};
    localAndUtcTimestamps_ = function () {
      return { local: '2025-03-01T00:00:00-07:00', utc: '2025-03-01T07:00:00.000Z' };
    };
    mergeReasonCounts_ = function (maps) {
      const out = {};
      (maps || []).forEach(function (map) {
        Object.keys(map || {}).forEach(function (key) {
          out[key] = Number(out[key] || 0) + Number(map[key] || 0);
        });
      });
      return out;
    };
    getTopReasonCodes_ = function () { return []; };
    updateBootstrapEmptyCycleState_ = function () {
      return { reason_code: '', warning_needed: false };
    };
    updateEmptyProductiveOutputState_ = function () {
      return { reason_code: '', warning_needed: false, consecutive_count: 0, threshold: 3 };
    };

    stageFetchOdds = function () {
      return {
        events: (opts.orchestrationScenario.oddsEvents || []).slice(),
        rows: (opts.orchestrationScenario.oddsRows || []).slice(),
        summary: { reason_codes: {} },
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

function testStageGenerateSignals_recordsMissingScheduleMatchReasonCode_() {
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
      event_id: 'evt_missing_match',
      market: 'h2h',
      outcome: 'player_a',
      bookmaker: 'book_x',
      price: 150,
      commence_time: new Date(nowMs + (5 * 60 * 60 * 1000)),
      odds_updated_time: new Date(nowMs),
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
      SIGNAL_DECISION_SAMPLE_LIMIT: 10,
    };

    const result = stageGenerateSignals('run_missing_match', config, [event], [], {});
    const decisionState = JSON.parse(stateWrites.LAST_SIGNAL_DECISIONS || '{}');

    assertEquals_(0, result.rows.length);
    assertEquals_(1, result.summary.reason_codes.missing_schedule_match || 0);
    assertEquals_(1, result.summary.input_count);
    assertEquals_(1, decisionState.processed_count || 0);
    assertEquals_(1, decisionState.input_count || 0);
    assertEquals_(1, (decisionState.reason_counts && decisionState.reason_counts.missing_schedule_match) || 0);
    assertEquals_(1, (decisionState.sampled_decisions || []).length);
    assertEquals_('missing_schedule_match', decisionState.sampled_decisions[0].decision_reason_code);
  } finally {
    Date.now = originalDateNow;
    getSignalState_ = originalGetSignalState;
    setSignalState_ = originalSetSignalState;
    setStateValue_ = originalSetStateValue;
    localAndUtcTimestamps_ = originalLocalAndUtcTimestamps;
  }
}

function testStageGenerateSignals_recordsMissingPlayerStatsReasonCode_() {
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
      event_id: 'evt_missing_stats',
      market: 'h2h',
      outcome: 'player_a',
      bookmaker: 'book_x',
      price: 150,
      commence_time: new Date(nowMs + (5 * 60 * 60 * 1000)),
      odds_updated_time: new Date(nowMs),
    };
    const match = {
      odds_event_id: 'evt_missing_stats',
      schedule_event_id: 'sched_missing_stats',
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
      SIGNAL_DECISION_SAMPLE_LIMIT: 10,
    };

    const result = stageGenerateSignals('run_missing_stats', config, [event], [match], {});
    const decisionState = JSON.parse(stateWrites.LAST_SIGNAL_DECISIONS || '{}');

    assertEquals_(0, result.rows.length);
    assertEquals_(1, result.summary.reason_codes.missing_player_stats || 0);
    assertEquals_(1, decisionState.processed_count || 0);
    assertEquals_(1, (decisionState.reason_counts && decisionState.reason_counts.missing_player_stats) || 0);
    assertEquals_('missing_player_stats', (decisionState.sampled_decisions[0] || {}).decision_reason_code || '');
  } finally {
    Date.now = originalDateNow;
    getSignalState_ = originalGetSignalState;
    setSignalState_ = originalSetSignalState;
    setStateValue_ = originalSetStateValue;
    localAndUtcTimestamps_ = originalLocalAndUtcTimestamps;
  }
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
    creditHeadersCaptured.push(Object.assign({}, headers || {}));
    return { header_present: Object.keys(headers || {}).length > 0 };
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
    return {
      events: events,
      reason_code: 'schedule_enrichment_test_passthrough',
      stats_reason_code: '',
      canonical_player_count: 0,
      stats_rows_applied: 0,
      h2h_rows_applied: 0,
      h2h_missing: 0,
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
    assertEquals_('player_stats_api_success_empty', meta.last_failure_reason);
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

function testFetchPlayerStatsFromLeadersSource_reasonCodes_() {
  const originalFetch = UrlFetchApp.fetch;

  try {
    UrlFetchApp.fetch = function () {
      throw new Error('network error');
    };

    const fetchFailed = fetchPlayerStatsFromLeadersSource_(['Player One']);
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

    const missingJs = fetchPlayerStatsFromLeadersSource_(['Player One']);
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

    const ok = fetchPlayerStatsFromLeadersSource_(['Player One']);
    assertEquals_(true, ok.ok);
    assertEquals_('ta_matchmx_ok', ok.reason_code);
    assertEquals_(14, ok.stats_by_player['player one'].ranking);
  } finally {
    UrlFetchApp.fetch = originalFetch;
  }
}
