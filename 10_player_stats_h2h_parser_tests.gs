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
    assertEquals_('iga swiatek||outside player', coverage.reason_metadata.debug_sample.schedule_key);
    assertTrue_((coverage.reason_metadata.debug_sample.nearest_candidate_keys || []).length > 0, 'expected nearest key samples');
    assertTrue_((coverage.reason_metadata.debug_sample.edit_distance_top_matches || []).length > 0, 'expected edit-distance matches');
  } finally {
    getTaH2hDataset_ = originalGetTaH2hDataset;
  }
}

function testParsePlayerStatsSourceConfigs_supportsNamedProviders_() {
  const configs = parsePlayerStatsSourceConfigs_({
    PLAYER_STATS_API_BASE_URL: 'Tennis Abstract=https://ta.example/api, WTA Stats Zone=https://wta.example/api, https://api.sofascore.com/tennis',
  });

  assertEquals_(3, configs.length);
  assertEquals_('tennis_abstract', configs[0].source_name);
  assertEquals_('https://ta.example/api', configs[0].base_url);
  assertEquals_('wta_stats_zone', configs[1].source_name);
  assertEquals_('sofascore', configs[2].source_name);
}

function testMergePlayerStatsMaps_appliesFeaturePrecedenceRules_() {
  const merged = mergePlayerStatsMaps_([
    {
      source_name: 'itf',
      stats_by_player: {
        'iga swiatek': { ranking: 3, recent_form: 0.4, hold_pct: null, break_pct: null, surface_win_rate: null },
      },
    },
    {
      source_name: 'wta_stats_zone',
      stats_by_player: {
        'iga swiatek': { ranking: 2, recent_form: 0.5, hold_pct: 0.6, break_pct: 0.41, surface_win_rate: 0.7 },
      },
    },
    {
      source_name: 'tennis_abstract',
      stats_by_player: {
        'iga swiatek': { ranking: 5, recent_form: 0.7, hold_pct: 0.8, break_pct: 0.5, surface_win_rate: 0.9 },
      },
    },
    {
      source_name: 'sofascore',
      stats_by_player: {
        'iga swiatek': { ranking: 7, recent_form: 0.95 },
      },
    },
  ], ['Iga Swiatek']);

  assertEquals_(2, merged['iga swiatek'].ranking);
  assertEquals_(0.9, merged['iga swiatek'].surface_win_rate);
  assertEquals_(0.8, merged['iga swiatek'].hold_pct);
  assertEquals_(0.5, merged['iga swiatek'].break_pct);
  assertEquals_(0.95, merged['iga swiatek'].recent_form);
  assertEquals_('scraped', merged['iga swiatek'].value_origin.hold_pct);
  assertEquals_(0, merged['iga swiatek'].placeholder_feature_count);
  assertTrue_(merged['iga swiatek'].trusted_feature_count >= 4);
}

function testMergePlayerStatsMaps_flagsPlaceholderZerosAsLowTrustAndPenalizesConfidence_() {
  const merged = mergePlayerStatsMaps_([
    {
      source_name: 'wta_stats_zone',
      stats_by_player: {
        'iga swiatek': { ranking: 0, recent_form: 0, hold_pct: 0, break_pct: 0 },
      },
    },
  ], ['Iga Swiatek']);

  assertEquals_(0, merged['iga swiatek'].ranking);
  assertEquals_('defaulted', merged['iga swiatek'].value_origin.ranking);
  assertEquals_(4, merged['iga swiatek'].placeholder_feature_count);
  assertEquals_(0, merged['iga swiatek'].trusted_feature_count);
  assertEquals_(0.08, merged['iga swiatek'].stats_confidence);
  assertEquals_('low_placeholder_only', merged['iga swiatek'].stats_confidence_band);
}

function testMergePlayerStatsMaps_treatsExplicitZeroOriginAsTrusted_() {
  const merged = mergePlayerStatsMaps_([
    {
      source_name: 'tennis_abstract',
      stats_by_player: {
        'iga swiatek': {
          ranking: 0,
          recent_form: 0,
          hold_pct: 0,
          break_pct: 0,
          value_origin: {
            ranking: 'scraped',
            recent_form: 'scraped',
            hold_pct: 'scraped',
            break_pct: 'scraped',
          },
        },
      },
    },
  ], ['Iga Swiatek']);

  assertEquals_(0, merged['iga swiatek'].placeholder_feature_count);
  assertEquals_(4, merged['iga swiatek'].trusted_feature_count);
  assertEquals_('high', merged['iga swiatek'].stats_confidence_band);
}

function testBuildPlayerStatsMergeDiagnostics_reportsCoverageAndContributions_() {
  const sourcePayloads = [
    {
      source_name: 'tennis_abstract',
      stats_by_player: {
        'iga swiatek': { hold_pct: 0.8, break_pct: 0.5 },
      },
    },
    {
      source_name: 'wta_stats_zone',
      stats_by_player: {
        'iga swiatek': { ranking: 1 },
      },
    },
  ];
  const merged = {
    'iga swiatek': { ranking: 1, hold_pct: 0.8, break_pct: 0.5, recent_form: null, surface_win_rate: null },
  };

  const diagnostics = buildPlayerStatsMergeDiagnostics_(sourcePayloads, merged, ['Iga Swiatek']);
  assertEquals_(1, diagnostics.per_source_players_parsed.tennis_abstract);
  assertEquals_(1, diagnostics.per_feature_non_null_contributions.ranking.wta_stats_zone);
  assertEquals_(1, diagnostics.per_feature_non_null_contributions.hold_pct.tennis_abstract);
  assertEquals_(1, diagnostics.final.players_with_non_null_stats);
  assertEquals_(0, diagnostics.final.placeholder_feature_count);
  assertEquals_(3, diagnostics.final.trusted_feature_count);
}


function testBuildPlayerStatsMergeDiagnostics_reportsEndpointLevelContributions_() {
  const sourcePayloads = [
    {
      source_name: 'sofascore',
      stats_by_player: {
        'iga swiatek': { ranking: 1, recent_form: 0.7, hold_pct: 0.65 },
      },
      endpoint_feature_sources_by_player: {
        'iga swiatek': {
          ranking: 'https://api.sofascore.com/api/v1/player/11',
          recent_form: 'https://api.sofascore.com/api/v1/player/11/events/last/0',
          hold_pct: 'https://api.sofascore.com/api/v1/player/11/statistics/overall',
        },
      },
    },
  ];
  const merged = {
    'iga swiatek': { ranking: 1, recent_form: 0.7, hold_pct: 0.65, break_pct: null, surface_win_rate: null },
  };

  const diagnostics = buildPlayerStatsMergeDiagnostics_(sourcePayloads, merged, ['Iga Swiatek']);
  assertEquals_(1, diagnostics.per_feature_endpoint_contributions.ranking.sofascore['https://api.sofascore.com/api/v1/player/11']);
  assertEquals_(1, diagnostics.per_feature_endpoint_contributions.recent_form.sofascore['https://api.sofascore.com/api/v1/player/11/events/last/0']);
}

function testSofascoreEndpointContractValidation_rejects404AndMissingKeys_() {
  assertTrue_(isSofascore404ErrorPayload_({ code: 404, message: 'Not Found' }) === true, '404 JSON error payload should fail');

  const missingKeys = validateSofascoreEndpointContract_({ events: [] }, {
    required_top_level_keys: ['player'],
    expected_payload_shape: { player: 'object' },
  });
  assertTrue_(missingKeys.ok === false, 'missing keys should fail contract');

  const shapeInvalid = validateSofascoreEndpointContract_({ player: [] }, {
    required_top_level_keys: ['player'],
    expected_payload_shape: { player: 'object' },
  });
  assertTrue_(shapeInvalid.ok === false, 'shape mismatch should fail contract');
}


function testSofascoreParticipantIndexAndFeatureExtraction_minimalCoverage_() {
  const indexed = indexSofascoreParticipants_({
    events: [
      {
        homeTeam: { id: 11, name: 'Iga Swiatek' },
        awayTeam: { id: 22, name: 'Aryna Sabalenka' },
      },
    ],
  }, {});

  assertEquals_(11, indexed['iga swiatek'].id);
  assertEquals_(22, indexed['aryna sabalenka'].id);

  const ranking = extractSofascoreRanking_({ player: { ranking: 2 } });
  assertEquals_(2, ranking);

  const form = extractSofascoreFormProxy_({
    events: [
      { homeTeam: { id: 11 }, awayTeam: { id: 22 }, winnerCode: 1 },
      { homeTeam: { id: 11 }, awayTeam: { id: 33 }, winnerCode: 2 },
      { homeTeam: { id: 44 }, awayTeam: { id: 11 }, winnerCode: 2 },
    ],
  }, 11);
  assertEquals_(0.667, form);
}

function testSofascoreParticipantIndex_supportsLiveDoublesTeamNameVariants_() {
  const liveFixture = {
    events: [
      {
        id: 101,
        status: { type: 'inprogress' },
        homeTeam: {
          id: 901,
          name: 'Coco Gauff / Jessica Pegula',
          shortName: 'Gauff/Pegula',
        },
        awayTeam: {
          id: 902,
          team: {
            name: 'Gabriela Dabrowski & Erin Routliffe',
            slug: 'gabriela-dabrowski-erin-routliffe',
          },
        },
      },
    ],
  };

  const indexed = indexSofascoreParticipants_(liveFixture, {});
  assertEquals_(901, indexed['coco gauff'].id);
  assertEquals_(901, indexed['jessica pegula'].id);
  assertEquals_(902, indexed['gabriela dabrowski'].id);
}

function testSofascoreParticipantIndex_supportsScheduledNestedNamesAndSlugFallbacks_() {
  const scheduledFixture = {
    events: [
      {
        id: 202,
        startTimestamp: 1770115200,
        homeTeam: {
          id: 1001,
          player: {
            name: 'Iga Swiatek',
            slug: 'iga-swiatek',
          },
        },
        awayTeam: {
          id: 1002,
          team: {
            slug: 'aryna-sabalenka',
          },
        },
      },
    ],
  };

  const indexed = indexSofascoreParticipants_(scheduledFixture, {});
  assertEquals_(1001, indexed['iga swiatek'].id);
  assertEquals_(1002, indexed['aryna sabalenka'].id);
}

function testFetchPlayerStatsFromSingleSource_sofascoreRoute_usesAdapter_() {
  const originalFetchSofascore = fetchPlayerStatsFromSofascore_;
  fetchPlayerStatsFromSofascore_ = function () {
    return {
      ok: true,
      reason_code: 'player_stats_sofascore_success',
      stats_by_player: {
        'Iga Swiatek': {
          ranking: 1,
          recent_form: 0.8,
          surface_win_rate: null,
          hold_pct: null,
          break_pct: null,
          source_used: 'sofascore_live',
          fallback_mode: 'limited_features',
          stats_confidence: 0.6,
        },
      },
      api_call_count: 3,
      source_name: 'sofascore',
    };
  };

  try {
    const result = fetchPlayerStatsFromSingleSource_({ source_name: 'sofascore', base_url: 'https://api.sofascore.com/api/v1' }, {}, ['Iga Swiatek'], new Date('2026-01-01T00:00:00Z'));
    assertEquals_(true, result.ok);
    assertEquals_('player_stats_sofascore_success', result.reason_code);
    assertEquals_(1, result.stats_by_player['Iga Swiatek'].ranking);
    assertEquals_('limited_features', result.stats_by_player['Iga Swiatek'].fallback_mode);
  } finally {
    fetchPlayerStatsFromSofascore_ = originalFetchSofascore;
  }
}

function testFetchPlayerStatsFromItfRankings_contractFailureUsesReasonCode_() {
  const originalFetch = UrlFetchApp.fetch;
  UrlFetchApp.fetch = function () {
    return {
      getResponseCode: function () { return 200; },
      getHeaders: function () { return { 'Content-Type': 'text/html; charset=utf-8' }; },
      getContentText: function () { return '<html>blocked</html>'; },
    };
  };

  try {
    const result = fetchPlayerStatsFromSingleSource_({ source_name: 'itf', base_url: 'https://itf.example/rankings' }, {}, ['Iga Swiatek'], new Date('2026-01-01T00:00:00Z'));
    assertEquals_(false, result.ok);
    assertEquals_('itf_contract_non_json', result.reason_code);
    assertEquals_(false, result.contract_check_passed);
    assertTrue_((result.missing_keys || []).indexOf('content-type:application/json') >= 0, 'expected missing content type contract key');
  } finally {
    UrlFetchApp.fetch = originalFetch;
  }
}


function testFetchPlayerStatsFromItfRankings_404FailureUsesReasonCode_() {
  const originalFetch = UrlFetchApp.fetch;
  UrlFetchApp.fetch = function () {
    return {
      getResponseCode: function () { return 404; },
      getHeaders: function () { return { 'Content-Type': 'application/json; charset=utf-8' }; },
      getContentText: function () { return JSON.stringify({ error: 'not found', code: 404 }); },
    };
  };

  try {
    const result = fetchPlayerStatsFromSingleSource_({ source_name: 'itf', base_url: 'https://itf.example/rankings' }, {}, ['Iga Swiatek'], new Date('2026-01-01T00:00:00Z'));
    assertEquals_(false, result.ok);
    assertEquals_('itf_contract_http_404', result.reason_code);
    assertEquals_(false, result.contract_check_passed);
    assertTrue_((result.missing_keys || []).indexOf('http_2xx') >= 0, 'expected missing http status contract key');
  } finally {
    UrlFetchApp.fetch = originalFetch;
  }
}

function testFetchPlayerStatsFromItfRankings_contractPassParsesRanking_() {
  const originalFetch = UrlFetchApp.fetch;
  UrlFetchApp.fetch = function () {
    return {
      getResponseCode: function () { return 200; },
      getHeaders: function () { return { 'Content-Type': 'application/json; charset=utf-8' }; },
      getContentText: function () {
        return JSON.stringify({
          data: {
            rankings: {
              rows: [
                { playerName: 'Iga Swiatek', rank: 1 },
              ],
            },
          },
        });
      },
    };
  };

  try {
    const result = fetchPlayerStatsFromSingleSource_({ source_name: 'itf', base_url: 'https://itf.example/rankings' }, {}, ['Iga Swiatek'], new Date('2026-01-01T00:00:00Z'));
    assertEquals_(true, result.ok);
    assertEquals_('player_stats_api_success', result.reason_code);
    assertEquals_(1, result.stats_by_player['iga swiatek'].ranking);
    assertEquals_(true, result.contract_check_passed);
    assertEquals_(0, (result.missing_keys || []).length);
  } finally {
    UrlFetchApp.fetch = originalFetch;
  }
}


function testCanonicalizePlayerName_aliasRules_coverInitialsDiacriticsHyphensAndPunctuation_() {
  assertEquals_('iga swiatek', canonicalizePlayerName_('I. Świątek', {}));
  assertEquals_('elena rybakina', canonicalizePlayerName_('E. Rybakina', {}));
  assertEquals_('marta kostyuk', canonicalizePlayerName_('M. Kostyuk', {}));
  assertEquals_('sonay kartal', canonicalizePlayerName_("Sonay-Kartal", {}));
}

function testCanonicalizePlayerName_aliasRules_supportNameOrderAndParticles_() {
  assertEquals_('iga swiatek', canonicalizePlayerName_('Swiatek, Iga', {}));
  assertEquals_('elena rybakina', canonicalizePlayerName_('Rybakina Elena', {}));
  assertEquals_('anna maria de la rosa', canonicalizePlayerName_('Anna Maria de la Rosa', {}));
}

function testCanonicalizeTaH2hPlayerName_stripsNumericPrefixesAndNormalizes_() {
  assertEquals_('marta kostyuk', canonicalizeTaH2hPlayerName_('1||Marta Kostyuk'));
  assertEquals_('elena rybakina', canonicalizeTaH2hPlayerName_('2 Elena Rybakina'));
  assertEquals_('aryna sabalenka', canonicalizeTaH2hPlayerName_('3  || Aryna Sabalenka'));
}

function testNormalizeTaH2hPairKey_handlesCurrentRealPairs_() {
  assertEquals_('marta kostyuk||elena rybakina', normalizeTaH2hPairKey_('1||Marta Kostyuk||2||Elena Rybakina'));
  assertEquals_('aryna sabalenka||naomi osaka', normalizeTaH2hPairKey_('Aryna Sabalenka||Naomi Osaka'));
}

function testBuildTaH2hLookupDebugSample_reportsPairKeyComparisonAfterNormalization_() {
  const debug = buildTaH2hLookupDebugSample_({
    by_pair: {
      '1||marta kostyuk||2||elena rybakina': { wins_a: 1, wins_b: 0 },
      'aryna sabalenka||naomi osaka': { wins_a: 2, wins_b: 1 },
    },
  }, 'marta kostyuk', 'elena rybakina');

  assertEquals_(2, debug.pair_key_comparison.dataset_key_counts.raw);
  assertEquals_(2, debug.pair_key_comparison.dataset_key_counts.normalized_unique);
  assertEquals_(true, debug.pair_key_comparison.schedule_matches_normalized_dataset.direct);
  assertEquals_(false, debug.pair_key_comparison.schedule_matches_normalized_dataset.reverse);
  assertEquals_('1||marta kostyuk||2||elena rybakina', debug.pair_key_comparison.normalized_key_drift_samples[0].raw);
  assertEquals_('marta kostyuk||elena rybakina', debug.pair_key_comparison.normalized_key_drift_samples[0].normalized);
}




function testIsSofascoreTennisPlayer_requiresTennisSlug_() {
  assertTrue_(isSofascoreTennisPlayer_({ player: { sport: { slug: 'tennis', id: 5, name: 'Tennis' } } }));
  assertFalse_(isSofascoreTennisPlayer_({ player: { sport: { slug: 'football', id: 5, name: 'Tennis' } } }));
}

const TEST_SOFASCORE_VERIFIED_TENNIS_PLAYER_ID = 9472;
const TEST_SOFASCORE_NON_TENNIS_PLAYER_ID = 7000;

function testFetchSofascorePlayerEnrichment_tennisPlayerRequestsRecentAndStats_() {
  const originalFetch = fetchSofascoreJson_;
  const calledUrls = [];
  fetchSofascoreJson_ = function (url) {
    calledUrls.push(url);
    if (url.indexOf('/player/' + String(TEST_SOFASCORE_VERIFIED_TENNIS_PLAYER_ID)) >= 0 && url.indexOf('/events/last/0') < 0 && url.indexOf('/statistics/') < 0) {
      return { ok: true, payload: { player: { id: TEST_SOFASCORE_VERIFIED_TENNIS_PLAYER_ID, sport: { slug: 'tennis', id: 5 } } }, api_call_count: 1 };
    }
    if (url.indexOf('/player/' + String(TEST_SOFASCORE_VERIFIED_TENNIS_PLAYER_ID) + '/events/last/0') >= 0) return { ok: true, payload: { events: [] }, api_call_count: 1 };
    if (url.indexOf('/player/' + String(TEST_SOFASCORE_VERIFIED_TENNIS_PLAYER_ID) + '/statistics/overall') >= 0) return { ok: true, payload: { statistics: {} }, api_call_count: 1 };
    if (url.indexOf('/player/' + String(TEST_SOFASCORE_VERIFIED_TENNIS_PLAYER_ID) + '/statistics/last/52') >= 0) return { ok: true, payload: { statistics: {} }, api_call_count: 1 };
    return { ok: false, payload: null, api_call_count: 1 };
  };

  try {
    const result = fetchSofascorePlayerEnrichment_(TEST_SOFASCORE_VERIFIED_TENNIS_PLAYER_ID, {});
    assertEquals_('sofascore_ok', result.reason_code);
    assertEquals_(4, calledUrls.length);
    assertTrue_(calledUrls.some(function (u) { return u.indexOf('/player/' + String(TEST_SOFASCORE_VERIFIED_TENNIS_PLAYER_ID) + '/events/last/0') >= 0; }), 'recent endpoint should be requested for tennis');
    assertTrue_(calledUrls.some(function (u) { return u.indexOf('/player/' + String(TEST_SOFASCORE_VERIFIED_TENNIS_PLAYER_ID) + '/statistics/overall') >= 0; }), 'overall statistics endpoint should be requested for tennis');
  } finally {
    fetchSofascoreJson_ = originalFetch;
  }
}

function testFetchSofascorePlayerEnrichment_nonTennisPlayerSkipsRecentAndStats_() {
  const originalFetch = fetchSofascoreJson_;
  const calledUrls = [];
  fetchSofascoreJson_ = function (url) {
    calledUrls.push(url);
    if (url.indexOf('/player/' + String(TEST_SOFASCORE_NON_TENNIS_PLAYER_ID)) >= 0 && url.indexOf('/events/last/0') < 0 && url.indexOf('/statistics/') < 0) {
      return { ok: true, payload: { player: { id: TEST_SOFASCORE_NON_TENNIS_PLAYER_ID, sport: { slug: 'football', id: 1 } } }, api_call_count: 1 };
    }
    return { ok: true, payload: {}, api_call_count: 1 };
  };

  try {
    const result = fetchSofascorePlayerEnrichment_(TEST_SOFASCORE_NON_TENNIS_PLAYER_ID, {});
    assertEquals_('source_entity_domain_mismatch', result.reason_code);
    assertEquals_('source_entity_domain_mismatch_non_tennis_sport_slug_football', result.domain_mismatch_reason);
    assertEquals_(1, calledUrls.length);
    assertTrue_(calledUrls[0].indexOf('/player/' + String(TEST_SOFASCORE_NON_TENNIS_PLAYER_ID)) >= 0, 'only player detail endpoint should be requested for non-tennis');
  } finally {
    fetchSofascoreJson_ = originalFetch;
  }
}


function testFetchPlayerStatsFromSofascore_domainMismatchSetsFailureAndNoDownstreamMetrics_() {
  const originalFetchJson = fetchSofascoreJson_;
  const originalEnrichment = fetchSofascorePlayerEnrichment_;

  fetchSofascoreJson_ = function () {
    return {
      ok: true,
      payload: {
        events: [
          {
            homeTeam: { id: TEST_SOFASCORE_NON_TENNIS_PLAYER_ID, name: 'Non Tennis Player' },
            awayTeam: { id: TEST_SOFASCORE_VERIFIED_TENNIS_PLAYER_ID, name: 'Iga Swiatek' },
          },
        ],
      },
      api_call_count: 1,
    };
  };

  fetchSofascorePlayerEnrichment_ = function (playerId) {
    if (Number(playerId) === TEST_SOFASCORE_NON_TENNIS_PLAYER_ID) {
      return {
        reason_code: 'source_entity_domain_mismatch',
        domain_mismatch_reason: 'source_entity_domain_mismatch_non_tennis_sport_slug_football',
        detail_payload: { player: { id: TEST_SOFASCORE_NON_TENNIS_PLAYER_ID, sport: { slug: 'football' } } },
        recent_payload: null,
        payloads_with_endpoints: [],
        detail_endpoint: 'https://api.sofascore.com/api/v1/player/' + String(TEST_SOFASCORE_NON_TENNIS_PLAYER_ID),
        recent_endpoint: null,
        attempted_endpoints: ['https://api.sofascore.com/api/v1/player/' + String(TEST_SOFASCORE_NON_TENNIS_PLAYER_ID)],
        api_call_count: 1,
      };
    }
    return {
      reason_code: 'sofascore_ok',
      detail_payload: { player: { ranking: 1, sport: { slug: 'tennis' } } },
      recent_payload: { events: [] },
      payloads_with_endpoints: [],
      detail_endpoint: 'https://api.sofascore.com/api/v1/player/' + String(TEST_SOFASCORE_VERIFIED_TENNIS_PLAYER_ID),
      recent_endpoint: 'https://api.sofascore.com/api/v1/player/' + String(TEST_SOFASCORE_VERIFIED_TENNIS_PLAYER_ID) + '/events/last/0',
      attempted_endpoints: [
        'https://api.sofascore.com/api/v1/player/' + String(TEST_SOFASCORE_VERIFIED_TENNIS_PLAYER_ID),
        'https://api.sofascore.com/api/v1/player/' + String(TEST_SOFASCORE_VERIFIED_TENNIS_PLAYER_ID) + '/events/last/0',
        'https://api.sofascore.com/api/v1/player/' + String(TEST_SOFASCORE_VERIFIED_TENNIS_PLAYER_ID) + '/statistics/overall',
      ],
      api_call_count: 3,
    };
  };

  try {
    const result = fetchPlayerStatsFromSofascore_({}, ['Non Tennis Player']);
    assertEquals_('source_entity_domain_mismatch', result.reason_code);
    assertEquals_('source_entity_domain_mismatch', result.stats_by_player['non tennis player'].fallback_mode);
    assertEquals_('source_entity_domain_mismatch_non_tennis_sport_slug_football', result.stats_by_player['non tennis player'].failure_reason);
    assertEquals_(null, result.endpoint_feature_sources_by_player['non tennis player'].recent_form);
    assertEquals_(null, result.endpoint_feature_sources_by_player['non tennis player'].hold_pct);
  } finally {
    fetchSofascoreJson_ = originalFetchJson;
    fetchSofascorePlayerEnrichment_ = originalEnrichment;
  }
}
