function testParseConfigRows_throwsOnSingleDuplicateKeyInErrorMode_() {
  const values = [
    ['key', 'value'],
    ['RUN_ENABLED', 'true'],
    ['LOOKAHEAD_HOURS', '36'],
    ['RUN_ENABLED', 'false'],
  ];

  assertThrows_(function () {
    parseConfigRows_(values, { mode: 'error', context: 'test' });
  }, '1 duplicate key(s)');
}

function testParseConfigRows_throwsOnMultipleDuplicateKeysInErrorMode_() {
  const values = [
    ['key', 'value'],
    ['RUN_ENABLED', 'true'],
    ['LOOKAHEAD_HOURS', '24'],
    ['RUN_ENABLED', 'false'],
    ['LOOKAHEAD_HOURS', '36'],
    ['ODDS_MARKETS', 'h2h'],
    ['ODDS_MARKETS', 'totals'],
  ];

  assertThrows_(function () {
    parseConfigRows_(values, { mode: 'error', context: 'test' });
  }, '3 duplicate key(s)');
}

function testParseConfigRows_noDuplicatesInErrorMode_returnsConfig_() {
  const values = [
    ['key', 'value'],
    ['RUN_ENABLED', 'true'],
    ['LOOKAHEAD_HOURS', '36'],
    ['ODDS_MARKETS', 'h2h'],
  ];

  const parsed = parseConfigRows_(values, { mode: 'error', context: 'test' });

  assertEquals_('true', parsed.config.RUN_ENABLED);
  assertEquals_('36', parsed.config.LOOKAHEAD_HOURS);
  assertEquals_('h2h', parsed.config.ODDS_MARKETS);
  assertArrayEquals_([], parsed.duplicate_keys);
}

function testParseConfigRows_warnLastWinsDeterministicPrecedence_() {
  const values = [
    ['key', 'value'],
    ['RUN_ENABLED', 'true'],
    ['LOOKAHEAD_HOURS', '24'],
    ['RUN_ENABLED', 'false'],
    ['LOOKAHEAD_HOURS', '36'],
  ];
  const logs = [];

  const parsed = parseConfigRows_(values, {
    mode: 'warn_last_wins',
    context: 'test',
    logger: function (msg) { logs.push(msg); },
  });

  assertEquals_('false', parsed.config.RUN_ENABLED);
  assertEquals_('36', parsed.config.LOOKAHEAD_HOURS);
  assertArrayEquals_(['LOOKAHEAD_HOURS', 'RUN_ENABLED'], parsed.duplicate_keys.sort());
  assertEquals_(2, logs.length);
}

function testFormatDuplicateConfigKeysError_includesRowsAndRepairHint_() {
  const message = formatDuplicateConfigKeysError_('getConfig_', {
    RUN_ENABLED: [4, 8],
  }, {
    RUN_ENABLED: 2,
  });

  assertTrue_(message.indexOf('getConfig_') >= 0, 'expected context in error');
  assertTrue_(message.indexOf('1 duplicate key(s)') >= 0, 'expected duplicate key count in error');
  assertTrue_(message.indexOf('first row: 2') >= 0, 'expected first row in error');
  assertTrue_(message.indexOf('4, 8') >= 0, 'expected duplicate rows in error');
  assertTrue_(message.indexOf('Why this fails') >= 0, 'expected why-this-fails help text in error');
  assertTrue_(message.indexOf('How to fix safely') >= 0, 'expected safe-fix help text in error');
  assertTrue_(message.indexOf('dedupeConfigSheet_()') >= 0, 'expected repair hint in error');
}

function assertThrows_(fn, expectedSubstring) {
  let threw = false;
  try {
    fn();
  } catch (err) {
    threw = true;
    if (expectedSubstring) {
      const msg = String((err && err.message) || err || '');
      assertTrue_(msg.indexOf(expectedSubstring) >= 0, 'expected error message to include: ' + expectedSubstring + '; actual: ' + msg);
    }
  }
  if (!threw) {
    throw new Error('Expected function to throw.');
  }
}

function testDedupeConfigSheet_lastWins_dedupesWatchedKeysAndLogsAudit_() {
  const sheet = createFakeConfigSheet_([
    ['key', 'value'],
    ['ODDS_SPORT_KEY', 'old_wta'],
    ['RUN_ENABLED', 'true'],
    ['PLAYER_STATS_API_BASE_URL', 'https://old.example'],
    ['ODDS_SPORT_KEY', 'new_wta'],
    ['PLAYER_STATS_API_BASE_URL', 'https://new.example'],
    ['PLAYER_STATS_API_KEY', 'key_old'],
    ['PLAYER_STATS_API_KEY', 'key_new'],
    ['PLAYER_STATS_SCRAPE_URLS', 'https://old.scrape'],
    ['PLAYER_STATS_SCRAPE_URLS', 'https://new.scrape'],
  ]);

  const originalAppend = appendLogRow_;
  const auditRows = [];
  appendLogRow_ = function (entry) { auditRows.push(entry); };

  try {
    const summary = dedupeConfigSheet_(sheet, {
      precedence: 'last_wins',
      preserve_row_order: true,
      include_missing_defaults: false,
      log_summary: false,
    });

    const output = sheet.getDataRange().getValues();
    const parsed = parseConfigRows_(output, { mode: 'error', context: 'testDedupeConfigSheet_lastWins' });

    assertArrayEquals_([], parsed.duplicate_keys);
    assertEquals_('new_wta', parsed.config.ODDS_SPORT_KEY);
    assertEquals_('https://new.example', parsed.config.PLAYER_STATS_API_BASE_URL);
    assertEquals_('key_new', parsed.config.PLAYER_STATS_API_KEY);
    assertEquals_('https://new.scrape', parsed.config.PLAYER_STATS_SCRAPE_URLS);
    assertEquals_(4, summary.removed_row_count);
    assertEquals_(true, summary.verified_unique_keys);
    assertEquals_(5, summary.watched_kept_row_by_key.ODDS_SPORT_KEY);
    assertEquals_(6, summary.watched_kept_row_by_key.PLAYER_STATS_API_BASE_URL);
    assertEquals_(8, summary.watched_kept_row_by_key.PLAYER_STATS_API_KEY);
    assertEquals_(10, summary.watched_kept_row_by_key.PLAYER_STATS_SCRAPE_URLS);
    assertEquals_(1, auditRows.length);
    assertTrue_(String(auditRows[0].message || '').indexOf('"watched_kept_row_by_key"') >= 0, 'expected watched key audit payload');
  } finally {
    appendLogRow_ = originalAppend;
  }
}

function createFakeConfigSheet_(initialValues) {
  let values = (initialValues || []).map(function (row) { return row.slice(); });
  let frozenRows = 0;
  return {
    getDataRange: function () {
      return {
        getValues: function () {
          return values.map(function (row) { return row.slice(); });
        },
      };
    },
    clearContents: function () {
      values = [];
    },
    getRange: function () {
      return {
        setValues: function (nextValues) {
          values = nextValues.map(function (row) { return row.slice(); });
        },
      };
    },
    getFrozenRows: function () { return frozenRows; },
    setFrozenRows: function (n) { frozenRows = n; },
  };
}

function testPreflightConfigUniqueness_detectsDuplicates_() {
  const originalSpreadsheetApp = SpreadsheetApp;
  SpreadsheetApp = {
    getActiveSpreadsheet: function () {
      return {
        getSheetByName: function () {
          return createFakeConfigSheet_([
            ['key', 'value'],
            ['RUN_ENABLED', 'true'],
            ['RUN_ENABLED', 'false'],
          ]);
        },
      };
    },
  };

  try {
    const preflight = preflightConfigUniqueness_('test');
    assertEquals_(false, preflight.ok);
    assertArrayEquals_(['RUN_ENABLED'], preflight.duplicate_keys);
    assertTrue_(String(preflight.user_message || '').indexOf('Repair Config') >= 0, 'expected repair action in user message');
  } finally {
    SpreadsheetApp = originalSpreadsheetApp;
  }
}

function testPreflightConfigUniqueness_passesWhenUnique_() {
  const originalSpreadsheetApp = SpreadsheetApp;
  SpreadsheetApp = {
    getActiveSpreadsheet: function () {
      return {
        getSheetByName: function () {
          return createFakeConfigSheet_([
            ['key', 'value'],
            ['RUN_ENABLED', 'true'],
            ['LOOKAHEAD_HOURS', '24'],
          ]);
        },
      };
    },
  };

  try {
    const preflight = preflightConfigUniqueness_('test');
    assertEquals_(true, preflight.ok);
    assertArrayEquals_([], preflight.duplicate_keys);
  } finally {
    SpreadsheetApp = originalSpreadsheetApp;
  }
}
