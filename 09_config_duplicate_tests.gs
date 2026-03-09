function testParseConfigRows_throwsOnDuplicateKeysInErrorMode_() {
  const values = [
    ['key', 'value'],
    ['RUN_ENABLED', 'true'],
    ['LOOKAHEAD_HOURS', '36'],
    ['RUN_ENABLED', 'false'],
  ];

  assertThrows_(function () {
    parseConfigRows_(values, { mode: 'error', context: 'test' });
  }, 'Duplicate keys detected');
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
  });

  assertTrue_(message.indexOf('getConfig_') >= 0, 'expected context in error');
  assertTrue_(message.indexOf('RUN_ENABLED') >= 0, 'expected key in error');
  assertTrue_(message.indexOf('4, 8') >= 0, 'expected row numbers in error');
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
