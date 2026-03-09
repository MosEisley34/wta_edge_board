function testParseTaH2hPageHtml_expectedFormat_parsesRowsAndSchema_() {
  const fixture = [
    '<html><body>',
    '<div>Last Update: June 9, 2026</div>',
    '<table class="h2h">',
    '<tr><td><a href="/cgi-bin/player-classic.cgi?p1=Iga+Swiatek&p2=Aryna+Sabalenka">2-1</a></td></tr>',
    '<tr><td><a href="/cgi-bin/h2h.cgi?player1=Elena+Rybakina&player2=Coco+Gauff">3-2</a></td></tr>',
    '</table>',
    '</body></html>',
  ].join('');

  const parsed = parseTaH2hPageHtml_(fixture);

  assertTrue_(parsed.ok === true, 'expected parse success');
  assertEquals_('ta_h2h_matrix_table_v1', parsed.schema_version);
  assertEquals_(false, parsed.empty_table);
  assertEquals_(2, parsed.rows.length);
  assertEquals_('Iga Swiatek', parsed.rows[0].player_a);
  assertEquals_('Aryna Sabalenka', parsed.rows[0].player_b);
  assertEquals_(2, parsed.rows[0].wins_a);
  assertEquals_(1, parsed.rows[0].wins_b);
  assertTrue_(String(parsed.source_updated_date || '').length > 0, 'expected source updated date extraction');
}

function testParseTaH2hPageHtml_partialChangedFormat_usesFallbackSelectors_() {
  const fixture = [
    '<html><body>',
    '<section id="matrix">',
    '<a href="/cgi-bin/h2h.cgi?p1=Qinwen+Zheng&p2=Paula+Badosa">1:0</a>',
    '<a href="/cgi-bin/h2h.cgi?playera=Emma+Navarro&playerb=Daria+Kasatkina">0-2</a>',
    '</section>',
    '</body></html>',
  ].join('');

  const parsed = parseTaH2hPageHtml_(fixture);

  assertTrue_(parsed.ok === true, 'expected parse success for changed format');
  assertEquals_('anchor_fallback_v1', parsed.schema_version);
  assertEquals_(false, parsed.empty_table);
  assertEquals_(2, parsed.rows.length);
  assertEquals_('Qinwen Zheng', parsed.rows[0].player_a);
  assertEquals_('Paula Badosa', parsed.rows[0].player_b);
  assertEquals_(1, parsed.rows[0].wins_a);
  assertEquals_(0, parsed.rows[0].wins_b);
}

function testParseTaH2hPageHtml_emptyNoDataFormat_returnsEmptyTableWithoutFailure_() {
  const fixture = [
    '<html><body>',
    '<table class="h2h">',
    '<tr><th>Player</th><th>H2H</th></tr>',
    '<tr><td><a href="/cgi-bin/h2h.cgi?player1=Iga+Swiatek&player2=Aryna+Sabalenka">No matches</a></td></tr>',
    '</table>',
    '</body></html>',
  ].join('');

  const parsed = parseTaH2hPageHtml_(fixture);

  assertTrue_(parsed.ok === true, 'empty table should not fail parsing');
  assertEquals_(true, parsed.empty_table);
  assertEquals_(0, parsed.rows.length);
  assertEquals_('', parsed.diagnostics.parse_step_failed || '');
  assertTrue_(String(parsed.diagnostics.html_sha256 || '').length === 64, 'expected html hash in diagnostics');
}


function testFetchTaH2hDatasetFromSource_emptyTableReturnsDedicatedReason_() {
  const originalSleep = sleepTennisAbstractRequestGap_;
  const originalFetchWithRetry = playerStatsFetchWithRetry_;
  const originalLogDiagnostic = logTaH2hParseDiagnostic_;

  sleepTennisAbstractRequestGap_ = function () {};
  playerStatsFetchWithRetry_ = function () {
    return {
      ok: true,
      status_code: 200,
      api_call_count: 1,
      response: {
        getContentText: function () {
          return '<html><body><table><tr><td><a href="/cgi-bin/h2h.cgi?player1=A&player2=B">No matches</a></td></tr></table></body></html>';
        },
      },
    };
  };
  logTaH2hParseDiagnostic_ = function () {};

  try {
    const fetched = fetchTaH2hDatasetFromSource_({ PLAYER_STATS_TA_H2H_URL: 'https://example.test/h2h' });

    assertTrue_(fetched.ok === true, 'expected source fetch to succeed');
    assertEquals_('h2h_source_empty_table', fetched.reason_code);
    assertEquals_(0, (fetched.payload.rows || []).length);
  } finally {
    sleepTennisAbstractRequestGap_ = originalSleep;
    playerStatsFetchWithRetry_ = originalFetchWithRetry;
    logTaH2hParseDiagnostic_ = originalLogDiagnostic;
  }
}

function testGetTaH2hCoverageForCanonicalPair_partialCoverageReason_() {
  const originalGetTaH2hDataset = getTaH2hDataset_;
  getTaH2hDataset_ = function () {
    return {
      rows: [
        { player_a: 'Iga Swiatek', player_b: 'Aryna Sabalenka', wins_a: 2, wins_b: 1 },
        { player_a: 'Aryna Sabalenka', player_b: 'Coco Gauff', wins_a: 3, wins_b: 2 },
      ],
      by_pair: {
        'iga swiatek::aryna sabalenka': { player_a: 'Iga Swiatek', player_b: 'Aryna Sabalenka', wins_a: 2, wins_b: 1 },
        'aryna sabalenka::coco gauff': { player_a: 'Aryna Sabalenka', player_b: 'Coco Gauff', wins_a: 3, wins_b: 2 },
      },
    };
  };

  try {
    const coverage = getTaH2hCoverageForCanonicalPair_({}, 'Iga Swiatek', 'Coco Gauff');
    assertEquals_(null, coverage.row);
    assertEquals_('h2h_partial_coverage', coverage.reason_code);
  } finally {
    getTaH2hDataset_ = originalGetTaH2hDataset;
  }
}

function testGetTaH2hCoverageForCanonicalPair_playerNotInMatrixReason_() {
  const originalGetTaH2hDataset = getTaH2hDataset_;
  getTaH2hDataset_ = function () {
    return {
      rows: [
        { player_a: 'Iga Swiatek', player_b: 'Aryna Sabalenka', wins_a: 2, wins_b: 1 },
      ],
      by_pair: {
        'iga swiatek::aryna sabalenka': { player_a: 'Iga Swiatek', player_b: 'Aryna Sabalenka', wins_a: 2, wins_b: 1 },
      },
    };
  };

  try {
    const coverage = getTaH2hCoverageForCanonicalPair_({}, 'Iga Swiatek', 'Outside Player');
    assertEquals_(null, coverage.row);
    assertEquals_('h2h_player_not_in_matrix', coverage.reason_code);
  } finally {
    getTaH2hDataset_ = originalGetTaH2hDataset;
  }
}
