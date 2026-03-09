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

function testParseTaH2hPageHtml_currentMatrixStructure_parsesNonZeroPairs_() {
  const fixture = [
    '<html><body>',
    '<table class="h2h_matrix">',
    '<tr>',
    '<th>Player</th>',
    '<th><a href="/player/iga" title="Iga Swiatek">ISW</a></th>',
    '<th><a href="/player/aryna" title="Aryna Sabalenka">ASB</a></th>',
    '<th><a href="/player/coco" title="Coco Gauff">CGF</a></th>',
    '<th>vs1-5</th>',
    '<th>vs1-10</th>',
    '<th>vs1-15</th>',
    '</tr>',
    '<tr>',
    '<td><a href="/player/iga" title="Iga Swiatek">Iga Swiatek</a></td>',
    '<td>0-0</td><td>5-8</td><td>11-3</td><td>80%</td><td>71%</td><td>66%</td>',
    '</tr>',
    '<tr>',
    '<td><a href="/player/aryna" title="Aryna Sabalenka">Aryna Sabalenka</a></td>',
    '<td>8-5</td><td>0-0</td><td>6-4</td><td>77%</td><td>69%</td><td>63%</td>',
    '</tr>',
    '<tr>',
    '<td><a href="/player/coco" title="Coco Gauff">Coco Gauff</a></td>',
    '<td>3-11</td><td>4-6</td><td>0-0</td><td>75%</td><td>68%</td><td>61%</td>',
    '</tr>',
    '</table>',
    '</body></html>',
  ].join('');

  const parsed = parseTaH2hPageHtml_(fixture);

  assertTrue_(parsed.ok === true, 'expected parse success for current matrix structure');
  assertEquals_('ta_h2h_matrix_table_v1', parsed.schema_version);
  assertEquals_(false, parsed.empty_table);
  assertTrue_(parsed.rows.length > 0, 'expected non-zero parsed pair count');

  const byPair = {};
  for (let i = 0; i < parsed.rows.length; i += 1) {
    const row = parsed.rows[i];
    byPair[row.player_a + '::' + row.player_b] = row;
  }
  assertEquals_(5, byPair['iga swiatek::aryna sabalenka'].wins_a);
  assertEquals_(8, byPair['iga swiatek::aryna sabalenka'].wins_b);
  assertEquals_(11, byPair['iga swiatek::coco gauff'].wins_a);
  assertEquals_(3, byPair['iga swiatek::coco gauff'].wins_b);
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
    assertEquals_('source_coverage', coverage.reason_metadata.category);
    assertEquals_('top_15_matrix', coverage.reason_metadata.coverage_scope);
    assertEquals_(true, coverage.reason_metadata.expected_missing);
  } finally {
    getTaH2hDataset_ = originalGetTaH2hDataset;
  }
}


function testGetTaH2hCoverageForCanonicalPair_datasetUnavailableReturnsUpstreamFailureReason_() {
  const originalGetTaH2hDataset = getTaH2hDataset_;
  const originalGetStateJson = getStateJson_;
  getTaH2hDataset_ = function () { return null; };
  getStateJson_ = function (key) {
    if (key === 'PLAYER_STATS_H2H_LAST_FETCH_META') {
      return { last_failure_reason: 'ta_h2h_parse_failed' };
    }
    return {};
  };

  try {
    const coverage = getTaH2hCoverageForCanonicalPair_({}, 'Iga Swiatek', 'Coco Gauff');
    assertEquals_(null, coverage.row);
    assertEquals_('ta_h2h_parse_failed', coverage.reason_code);
    assertEquals_('pipeline_failure', coverage.reason_metadata.category);
    assertEquals_(false, coverage.reason_metadata.expected_missing);
  } finally {
    getTaH2hDataset_ = originalGetTaH2hDataset;
    getStateJson_ = originalGetStateJson;
  }
}

function testParseTaH2hPageHtml_matrixRows_useCanonicalSchemaTypes_() {
  const fixture = [
    '<html><body>',
    '<table class="h2h">',
    '<tr><td><a href="/cgi-bin/h2h.cgi?player1=Iga+Swiatek&player2=Aryna+Sabalenka">2-1</a></td></tr>',
    '</table>',
    '</body></html>',
  ].join('');

  const parsed = parseTaH2hPageHtml_(fixture);
  assertTrue_(parsed.ok === true, 'expected parse success');
  assertEquals_(1, parsed.rows.length);

  const row = parsed.rows[0] || {};
  assertEquals_('iga swiatek', row.player_a);
  assertEquals_('aryna sabalenka', row.player_b);
  assertEquals_('number', typeof row.wins_a);
  assertEquals_('number', typeof row.wins_b);
  assertEquals_('', row.source_updated_date);
}

function testFetchTaH2hDatasetFromSource_normalizesRowsAndPairKeysForJoinLookup_() {
  const originalSleep = sleepTennisAbstractRequestGap_;
  const originalFetchWithRetry = playerStatsFetchWithRetry_;

  sleepTennisAbstractRequestGap_ = function () {};
  playerStatsFetchWithRetry_ = function () {
    return {
      ok: true,
      status_code: 200,
      api_call_count: 1,
      response: {
        getContentText: function () {
          return '<html><body><table><tr><td><a href="/cgi-bin/h2h.cgi?player1=IGA+Swiatek&player2=Aryna+Sabalenka">2-1</a></td></tr></table></body></html>';
        },
      },
    };
  };

  try {
    const fetched = fetchTaH2hDatasetFromSource_({ PLAYER_STATS_TA_H2H_URL: 'https://example.test/h2h' });

    assertTrue_(fetched.ok === true, 'expected source fetch to succeed');
    assertEquals_(1, (fetched.payload.rows || []).length);
    assertEquals_('iga swiatek', fetched.payload.rows[0].player_a);
    assertEquals_('aryna sabalenka', fetched.payload.rows[0].player_b);
    assertTrue_(!!fetched.payload.by_pair['iga swiatek||aryna sabalenka'], 'expected canonical pair lookup key');
  } finally {
    sleepTennisAbstractRequestGap_ = originalSleep;
    playerStatsFetchWithRetry_ = originalFetchWithRetry;
  }
}

function testGetTaH2hCoverageForCanonicalPair_noMatchReturnsDebugSampleNearestKeys_() {
  const originalGetTaH2hDataset = getTaH2hDataset_;
  getTaH2hDataset_ = function () {
    return {
      rows: [
        { player_a: 'iga swiatek', player_b: 'aryna sabalenka', wins_a: 2, wins_b: 1 },
        { player_a: 'coco gauff', player_b: 'aryna sabalenka', wins_a: 3, wins_b: 2 },
      ],
      by_pair: {
        'iga swiatek||aryna sabalenka': { player_a: 'iga swiatek', player_b: 'aryna sabalenka', wins_a: 2, wins_b: 1 },
        'coco gauff||aryna sabalenka': { player_a: 'coco gauff', player_b: 'aryna sabalenka', wins_a: 3, wins_b: 2 },
      },
    };
  };

  try {
    const coverage = getTaH2hCoverageForCanonicalPair_({}, 'Iga Swiatek', 'Outside Player');
    assertEquals_(null, coverage.row);
    assertEquals_('h2h_player_not_in_matrix', coverage.reason_code);
    assertEquals_('iga swiatek||outside player', coverage.reason_metadata.debug_sample.requested_pair_keys[0]);
    assertEquals_('outside player||iga swiatek', coverage.reason_metadata.debug_sample.requested_pair_keys[1]);
    assertTrue_((coverage.reason_metadata.debug_sample.nearest_available_keys || []).length > 0, 'expected nearest key samples');
  } finally {
    getTaH2hDataset_ = originalGetTaH2hDataset;
  }
}
