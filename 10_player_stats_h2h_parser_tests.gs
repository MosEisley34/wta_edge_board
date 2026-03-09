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
