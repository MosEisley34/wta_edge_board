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
