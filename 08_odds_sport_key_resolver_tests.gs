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
      reason_code: 'schedule_active_keys_no_events',
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
      reason_code: 'schedule_active_keys_no_events',
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
      reason_code: 'schedule_active_keys_no_events',
      api_credit_usage: 1,
      api_call_count: 1,
      credit_headers: liveHeaders,
    }],
  });

  assertEquals_('fresh_api', result.stage.selected_source);
  assertEquals_(1, result.stage.summary.reason_codes.schedule_active_keys_no_events || 0);
  assertEquals_(1, result.fetchCalls.length);
  assertEquals_(1, result.lastMeta.live_fetch_happened ? 1 : 0);
  assertEquals_(1, result.lastMeta.stale_fallback_empty_forced_live ? 1 : 0);
  assertEquals_(1, result.creditHeadersCaptured.length);
  assertEquals_('489', String(result.creditHeadersCaptured[0].x_requests_remaining || ''));
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

  try {
    const stage = stageFetchSchedule('run_stage_fetch_schedule_test', {}, []);
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
