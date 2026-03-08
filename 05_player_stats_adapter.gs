const MATCHMX_ROW_IDX = {
  DATE: 0,
  EVENT: 1,
  SURFACE: 2,
  PLAYER_NAME: 3,
  OPPONENT: 4,
  SCORE: 5,
  RANKING: 6,
  RECENT_FORM: 7,
  SURFACE_WIN_RATE: 8,
  HOLD_PCT: 9,
  BREAK_PCT: 10,
  BP_SAVED_PCT: 11,
  BP_CONV_PCT: 12,
  FIRST_SERVE_IN_PCT: 13,
  FIRST_SERVE_POINTS_WON_PCT: 14,
  SECOND_SERVE_POINTS_WON_PCT: 15,
  RETURN_POINTS_WON_PCT: 16,
  DOMINANCE_RATIO: 17,
  TOTAL_POINTS_WON_PCT: 18,
};

const PLAYER_STATS_H2H_CACHE_KEY = 'PLAYER_STATS_H2H_DATASET';

function fetchPlayerStatsBatch_(config, canonicalPlayers, asOfTime) {
  const players = dedupePlayerNames_(canonicalPlayers || []);
  const asOfDate = asOfTime instanceof Date ? asOfTime : new Date(asOfTime || Date.now());
  const nowMs = Date.now();
  const ttlMs = Math.max(1, Number(config.PLAYER_STATS_CACHE_TTL_MIN || 10)) * 60000;
  const refreshMinMs = Math.max(1, Number(config.PLAYER_STATS_REFRESH_MIN || 5)) * 60000;
  const forceRefresh = !!config.PLAYER_STATS_FORCE_REFRESH;
  const cacheKey = buildPlayerStatsCacheKey_(players, asOfDate);
  const cachedPayload = getCachedPlayerStatsPayload_(cacheKey);

  if (!forceRefresh && cachedPayload && Number.isFinite(cachedPayload.cached_at_ms) && (nowMs - cachedPayload.cached_at_ms <= ttlMs)) {
    persistPlayerStatsMeta_(cachedPayload, 'cached_fresh', nowMs, players.length, asOfDate);
    return {
      stats_by_player: cachedPayload.stats_by_player || {},
      reason_code: 'stats_cache_hit',
      source: 'cached_fresh',
      provider_available: true,
      api_credit_usage: 0,
      api_call_count: 0,
    };
  }

  if (!forceRefresh && cachedPayload && Number.isFinite(cachedPayload.cached_at_ms) && (nowMs - cachedPayload.cached_at_ms < refreshMinMs)) {
    persistPlayerStatsMeta_(cachedPayload, 'cached_stale_refresh_throttled', nowMs, players.length, asOfDate);
    return {
      stats_by_player: cachedPayload.stats_by_player || {},
      reason_code: 'stats_cache_stale_refresh_throttled',
      source: 'cached_stale_refresh_throttled',
      provider_available: true,
      api_credit_usage: 0,
      api_call_count: 0,
    };
  }

  const live = fetchPlayerStatsFromProvider_(config, players, asOfDate);
  if (live.ok) {
    const cachePayload = {
      cache_key: cacheKey,
      cached_at_ms: nowMs,
      as_of_time: asOfDate.toISOString(),
      player_count: players.length,
      stats_by_player: live.stats_by_player || {},
    };
    setCachedPlayerStatsPayload_(cacheKey, cachePayload);
    persistPlayerStatsMeta_(cachePayload, 'fresh_api', nowMs, players.length, asOfDate);

    return {
      stats_by_player: live.stats_by_player || {},
      reason_code: live.reason_code || 'player_stats_api_success',
      source: 'fresh_api',
      provider_available: true,
      api_credit_usage: Number(live.api_credit_usage || 0),
      api_call_count: Number(live.api_call_count || 0),
    };
  }

  const stalePayload = getStateJson_('PLAYER_STATS_STALE_PAYLOAD');
  if (stalePayload && stalePayload.stats_by_player) {
    persistPlayerStatsMeta_(stalePayload, 'cached_stale_fallback', nowMs, players.length, asOfDate);
    return {
      stats_by_player: stalePayload.stats_by_player || {},
      reason_code: 'stats_stale_fallback',
      source: 'cached_stale_fallback',
      provider_available: true,
      api_credit_usage: Number(live.api_credit_usage || 0),
      api_call_count: Number(live.api_call_count || 0),
    };
  }

  persistPlayerStatsMeta_({
    cache_key: cacheKey,
    cached_at_ms: nowMs,
    as_of_time: asOfDate.toISOString(),
    player_count: players.length,
    stats_by_player: {},
  }, 'provider_unavailable', nowMs, players.length, asOfDate);

  return {
    stats_by_player: {},
    reason_code: live.reason_code || 'player_stats_provider_unavailable',
    source: 'provider_unavailable',
    provider_available: false,
    api_credit_usage: Number(live.api_credit_usage || 0),
    api_call_count: Number(live.api_call_count || 0),
  };
}

function fetchPlayerStatsFromProvider_(config, canonicalPlayers, asOfTime) {
  const players = dedupePlayerNames_(canonicalPlayers || []);
  if (!players.length) {
    return {
      ok: true,
      reason_code: 'player_stats_no_players',
      stats_by_player: {},
      api_credit_usage: 0,
      api_call_count: 0,
    };
  }

  const sourceConfigs = parsePlayerStatsSourceConfigs_(config);

  const attempts = [];
  let totalApiCalls = 0;
  const normalizedMaps = [];

  sourceConfigs.forEach(function (sourceConfig) {
    const result = fetchPlayerStatsFromSingleSource_(sourceConfig, config, players, asOfTime);
    attempts.push(result.reason_code || 'player_stats_unknown_source_result');
    totalApiCalls += Number(result.api_call_count || 0);
    if (result.ok && result.stats_by_player) normalizedMaps.push(result.stats_by_player);
  });

  if (normalizedMaps.length) {
    const merged = mergePlayerStatsMaps_(normalizedMaps, players);
    return {
      ok: true,
      reason_code: normalizedMaps.length > 1 ? 'player_stats_multi_source_success' : 'player_stats_api_success',
      detail: attempts.join(','),
      stats_by_player: merged,
      api_credit_usage: 0,
      api_call_count: totalApiCalls,
    };
  }

  const leadersSource = fetchPlayerStatsFromLeadersSource_(players, config, asOfTime);
  if (leadersSource.ok) {
    return {
      ok: true,
      reason_code: leadersSource.reason_code || 'ta_matchmx_ok',
      detail: attempts.join(','),
      stats_by_player: leadersSource.stats_by_player || {},
      api_credit_usage: 0,
      api_call_count: totalApiCalls + Number(leadersSource.api_call_count || 0),
    };
  }

  const scraped = fetchPlayerStatsFromScrapeSources_(config, players);
  if (scraped.ok) {
    return {
      ok: true,
      reason_code: 'player_stats_scrape_success',
      detail: attempts.join(','),
      stats_by_player: scraped.stats_by_player || {},
      api_credit_usage: 0,
      api_call_count: totalApiCalls + Number(scraped.api_call_count || 0),
    };
  }

  return {
    ok: false,
    reason_code: leadersSource.reason_code || (attempts.length ? attempts[attempts.length - 1] : 'player_stats_provider_not_configured'),
    detail: attempts.join(','),
    api_credit_usage: 0,
    api_call_count: totalApiCalls + Number(leadersSource.api_call_count || 0) + Number(scraped.api_call_count || 0),
  };
}

function fetchPlayerStatsFromLeadersSource_(canonicalPlayers, config, asOfTime) {
  const leadersUrl = 'https://www.tennisabstract.com/cgi-bin/leaders_wta.cgi?players=top';
  let pageResponse;

  try {
    pageResponse = UrlFetchApp.fetch(leadersUrl, {
      method: 'get',
      muteHttpExceptions: true,
      headers: { Accept: 'text/html' },
    });
  } catch (e) {
    return { ok: false, reason_code: 'ta_leaders_page_fetch_failed', stats_by_player: {}, api_call_count: 1 };
  }

  if (Number(pageResponse.getResponseCode() || 0) < 200 || Number(pageResponse.getResponseCode() || 0) >= 300) {
    return { ok: false, reason_code: 'ta_leaders_page_fetch_failed', stats_by_player: {}, api_call_count: 1 };
  }

  const pageText = String(pageResponse.getContentText() || '');
  const jsUrl = extractLeadersJsUrl_(pageText, leadersUrl);
  if (!jsUrl) {
    return { ok: false, reason_code: 'ta_leaders_js_url_missing', stats_by_player: {}, api_call_count: 1 };
  }

  let jsResponse;
  try {
    jsResponse = UrlFetchApp.fetch(jsUrl, {
      method: 'get',
      muteHttpExceptions: true,
      headers: { Accept: 'application/javascript,text/plain' },
    });
  } catch (e) {
    return { ok: false, reason_code: 'ta_leaders_js_fetch_failed', stats_by_player: {}, api_call_count: 2 };
  }

  if (Number(jsResponse.getResponseCode() || 0) < 200 || Number(jsResponse.getResponseCode() || 0) >= 300) {
    return { ok: false, reason_code: 'ta_leaders_js_fetch_failed', stats_by_player: {}, api_call_count: 2 };
  }

  const jsPayload = String(jsResponse.getContentText() || '');
  const structuredRows = extractMatchMxRows_(jsPayload);
  if (!structuredRows.length) {
    return { ok: false, reason_code: 'ta_matchmx_parse_failed', stats_by_player: {}, api_call_count: 2 };
  }

  const statsByPlayer = normalizePlayerStatsResponse_(structuredRows, canonicalPlayers, {
    as_of_time: asOfTime,
    match_window_weeks: Number(config.PLAYER_STATS_MATCH_WINDOW_WEEKS || 52),
    recent_match_count: Number(config.PLAYER_STATS_RECENT_MATCH_COUNT || 0),
  });
  return {
    ok: Object.keys(statsByPlayer).length > 0,
    reason_code: Object.keys(statsByPlayer).length > 0 ? 'ta_matchmx_ok' : 'ta_matchmx_parse_failed',
    stats_by_player: statsByPlayer,
    api_call_count: 2,
  };
}

function extractLeadersJsUrl_(html, baseUrl) {
  const text = String(html || '');
  const match = text.match(/(?:https?:)?\/\/[^"'\s]*jsmatches\/[^"'\s]*leadersource[^"'\s]*wta\.js|\/?jsmatches\/[^"'\s]*leadersource[^"'\s]*wta\.js/i);
  if (!match || !match[0]) return '';

  const value = String(match[0]);
  if (/^https?:\/\//i.test(value)) return value;
  if (/^\/\//.test(value)) return 'https:' + value;

  const base = String(baseUrl || 'https://www.tennisabstract.com/').replace(/\/[^/]*$/, '/');
  const originMatch = String(baseUrl || '').match(/^https?:\/\/[^/]+/i);
  if (/^\//.test(value) && originMatch && originMatch[0]) return originMatch[0] + value;
  return base + value.replace(/^\/+/, '');
}

function extractMatchMxRows_(payload) {
  const text = String(payload || '');
  const rows = [];
  const rowRegex = /matchmx\s*(?:\[\s*\d+\s*\])?\s*=\s*\[([\s\S]*?)\]\s*;/g;
  let match;

  while ((match = rowRegex.exec(text)) !== null) {
    const tokens = parseJsArrayTokens_(match[1]);
    if (tokens.length < 6) continue;
    const structured = buildStructuredMatchMxRow_(tokens);
    if (!structured.player_name || !structured.score) continue;
    rows.push(structured);
  }

  return rows;
}

function parseJsArrayTokens_(arrayLiteralBody) {
  const body = String(arrayLiteralBody || '');
  const pattern = /"((?:\\.|[^"\\])*)"|'((?:\\.|[^'\\])*)'|([^,]+)/g;
  const tokens = [];
  let part;

  while ((part = pattern.exec(body)) !== null) {
    const raw = part[1] !== undefined ? part[1] : (part[2] !== undefined ? part[2] : part[3]);
    const normalized = String(raw || '').trim();
    if (normalized === 'null' || normalized === 'undefined') {
      tokens.push('');
      continue;
    }
    tokens.push(normalized.replace(/\\"/g, '"').replace(/\\'/g, "'"));
  }

  return tokens;
}

function buildStructuredMatchMxRow_(tokens) {
  const score = String(tokens[MATCHMX_ROW_IDX.SCORE] || '').trim();
  const hasWalkoverOrRet = /\b(?:ret|wo)\b/i.test(score);
  const numericStats = [];
  for (let i = MATCHMX_ROW_IDX.RANKING; i < tokens.length; i += 1) {
    const value = Number(tokens[i]);
    numericStats.push(Number.isFinite(value) ? value : null);
  }

  function take(index) {
    const value = Number(tokens[index]);
    return Number.isFinite(value) ? value : null;
  }

  return {
    date: String(tokens[MATCHMX_ROW_IDX.DATE] || ''),
    event: String(tokens[MATCHMX_ROW_IDX.EVENT] || ''),
    surface: String(tokens[MATCHMX_ROW_IDX.SURFACE] || ''),
    player_name: String(tokens[MATCHMX_ROW_IDX.PLAYER_NAME] || ''),
    opponent: String(tokens[MATCHMX_ROW_IDX.OPPONENT] || ''),
    score: score,
    ranking: take(MATCHMX_ROW_IDX.RANKING),
    recent_form: hasWalkoverOrRet ? null : take(MATCHMX_ROW_IDX.RECENT_FORM),
    surface_win_rate: hasWalkoverOrRet ? null : take(MATCHMX_ROW_IDX.SURFACE_WIN_RATE),
    hold_pct: hasWalkoverOrRet ? null : take(MATCHMX_ROW_IDX.HOLD_PCT),
    break_pct: hasWalkoverOrRet ? null : take(MATCHMX_ROW_IDX.BREAK_PCT),
    bp_saved_pct: hasWalkoverOrRet ? null : take(MATCHMX_ROW_IDX.BP_SAVED_PCT),
    bp_conv_pct: hasWalkoverOrRet ? null : take(MATCHMX_ROW_IDX.BP_CONV_PCT),
    first_serve_in_pct: hasWalkoverOrRet ? null : take(MATCHMX_ROW_IDX.FIRST_SERVE_IN_PCT),
    first_serve_points_won_pct: hasWalkoverOrRet ? null : take(MATCHMX_ROW_IDX.FIRST_SERVE_POINTS_WON_PCT),
    second_serve_points_won_pct: hasWalkoverOrRet ? null : take(MATCHMX_ROW_IDX.SECOND_SERVE_POINTS_WON_PCT),
    return_points_won_pct: hasWalkoverOrRet ? null : take(MATCHMX_ROW_IDX.RETURN_POINTS_WON_PCT),
    dr: hasWalkoverOrRet ? null : take(MATCHMX_ROW_IDX.DOMINANCE_RATIO),
    tpw_pct: hasWalkoverOrRet ? null : take(MATCHMX_ROW_IDX.TOTAL_POINTS_WON_PCT),
    numeric_stats: numericStats,
  };
}

function parsePlayerStatsSourceConfigs_(config) {
  const baseValue = String(config.PLAYER_STATS_API_BASE_URL || '').trim();
  if (!baseValue) return [];
  return baseValue.split(',')
    .map(function (value) { return String(value || '').trim().replace(/\/+$/, ''); })
    .filter(function (value) { return !!value; });
}

function fetchPlayerStatsFromSingleSource_(baseUrl, config, players, asOfTime) {
  const endpoint = String(baseUrl || '').replace(/\/$/, '') + '/players/stats/batch';
  const body = JSON.stringify({
    players: players,
    as_of_time: asOfTime.toISOString(),
  });

  const headers = {
    Accept: 'application/json',
    'Content-Type': 'application/json',
  };
  if (String(config.PLAYER_STATS_API_KEY || '')) {
    headers['X-API-Key'] = String(config.PLAYER_STATS_API_KEY);
  }

  let response;
  try {
    response = UrlFetchApp.fetch(endpoint, {
      method: 'post',
      payload: body,
      headers: headers,
      muteHttpExceptions: true,
    });
  } catch (e) {
    return {
      ok: false,
      reason_code: 'player_stats_transport_error',
      api_call_count: 1,
    };
  }

  const status = Number(response.getResponseCode() || 0);
  if (status < 200 || status >= 300) {
    return {
      ok: false,
      reason_code: 'player_stats_http_' + status,
      api_call_count: 1,
    };
  }

  let parsed;
  try {
    parsed = JSON.parse(response.getContentText() || '{}');
  } catch (e) {
    return {
      ok: false,
      reason_code: 'player_stats_parse_error',
      api_call_count: 1,
    };
  }

  return {
    ok: true,
    reason_code: 'player_stats_api_success',
    stats_by_player: normalizePlayerStatsResponse_(parsed, players),
    api_call_count: 1,
  };
}

function fetchPlayerStatsFromScrapeSources_(config, canonicalPlayers) {
  const templates = parseScrapeUrlTemplates_(config.PLAYER_STATS_SCRAPE_URLS);
  if (!templates.length || !canonicalPlayers.length) {
    return {
      ok: false,
      reason_code: 'player_stats_scrape_not_configured',
      stats_by_player: {},
      api_call_count: 0,
    };
  }

  const statsByPlayer = {};
  let apiCallCount = 0;

  canonicalPlayers.forEach(function (playerName) {
    const slug = buildPlayerSlug_(playerName);
    let playerStats = null;

    templates.forEach(function (template) {
      if (playerStats) return;
      const url = String(template || '').replace(/\{player\}/g, encodeURIComponent(slug));
      if (!url) return;

      apiCallCount += 1;
      try {
        const response = UrlFetchApp.fetch(url, {
          method: 'get',
          muteHttpExceptions: true,
          headers: { Accept: 'text/html,application/json' },
        });

        if (Number(response.getResponseCode() || 0) < 200 || Number(response.getResponseCode() || 0) >= 300) return;
        playerStats = extractStatsFromScrapeContent_(response.getContentText() || '', playerName);
      } catch (e) {
        playerStats = null;
      }
    });

    if (playerStats) statsByPlayer[playerName] = playerStats;
  });

  return {
    ok: Object.keys(statsByPlayer).length > 0,
    reason_code: Object.keys(statsByPlayer).length > 0 ? 'player_stats_scrape_success' : 'player_stats_scrape_empty',
    stats_by_player: statsByPlayer,
    api_call_count: apiCallCount,
  };
}

function parseScrapeUrlTemplates_(value) {
  return String(value || '').split(',')
    .map(function (part) { return String(part || '').trim(); })
    .filter(function (part) { return !!part; });
}

function buildPlayerSlug_(canonicalPlayerName) {
  return String(canonicalPlayerName || '').toLowerCase().trim().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '');
}

function extractStatsFromScrapeContent_(content, playerName) {
  const text = String(content || '');
  if (!text) return null;

  const statLineRegex = /(ranking|rank|recent_form|recent win rate|surface_win_rate|hold_pct|break_pct)\s*[:=]\s*([0-9]{1,3}(?:\.[0-9]+)?)/ig;
  const values = {};
  let match;
  while ((match = statLineRegex.exec(text)) !== null) {
    values[String(match[1] || '').toLowerCase().replace(/\s+/g, '_')] = Number(match[2]);
  }

  if (!Object.keys(values).length) return null;
  const normalized = normalizePlayerStatsResponse_([Object.assign({ player_name: playerName }, values)], [playerName]);
  return normalized[canonicalizePlayerName_(playerName, {})] || null;
}

function mergePlayerStatsMaps_(maps, canonicalPlayers) {
  const merged = {};
  const players = dedupePlayerNames_(canonicalPlayers || []);

  players.forEach(function (player) {
    let ranking = null;
    let recentForm = null;
    let surfaceWinRate = null;
    let holdPct = null;
    let breakPct = null;
    let bpSavedPct = null;
    let bpConvPct = null;
    let firstServeInPct = null;
    let firstServePointsWonPct = null;
    let secondServePointsWonPct = null;
    let returnPointsWonPct = null;
    let dr = null;
    let tpwPct = null;

    maps.forEach(function (statsMap) {
      const stats = statsMap && statsMap[player];
      if (!stats) return;
      if (ranking === null && stats.ranking !== null && stats.ranking !== undefined) ranking = stats.ranking;
      if (recentForm === null && stats.recent_form !== null && stats.recent_form !== undefined) recentForm = stats.recent_form;
      if (surfaceWinRate === null && stats.surface_win_rate !== null && stats.surface_win_rate !== undefined) surfaceWinRate = stats.surface_win_rate;
      if (holdPct === null && stats.hold_pct !== null && stats.hold_pct !== undefined) holdPct = stats.hold_pct;
      if (breakPct === null && stats.break_pct !== null && stats.break_pct !== undefined) breakPct = stats.break_pct;
      if (bpSavedPct === null && stats.bp_saved_pct !== null && stats.bp_saved_pct !== undefined) bpSavedPct = stats.bp_saved_pct;
      if (bpConvPct === null && stats.bp_conv_pct !== null && stats.bp_conv_pct !== undefined) bpConvPct = stats.bp_conv_pct;
      if (firstServeInPct === null && stats.first_serve_in_pct !== null && stats.first_serve_in_pct !== undefined) firstServeInPct = stats.first_serve_in_pct;
      if (firstServePointsWonPct === null && stats.first_serve_points_won_pct !== null && stats.first_serve_points_won_pct !== undefined) firstServePointsWonPct = stats.first_serve_points_won_pct;
      if (secondServePointsWonPct === null && stats.second_serve_points_won_pct !== null && stats.second_serve_points_won_pct !== undefined) secondServePointsWonPct = stats.second_serve_points_won_pct;
      if (returnPointsWonPct === null && stats.return_points_won_pct !== null && stats.return_points_won_pct !== undefined) returnPointsWonPct = stats.return_points_won_pct;
      if (dr === null && stats.dr !== null && stats.dr !== undefined) dr = stats.dr;
      if (tpwPct === null && stats.tpw_pct !== null && stats.tpw_pct !== undefined) tpwPct = stats.tpw_pct;
    });

    merged[player] = {
      ranking: ranking,
      recent_form: recentForm,
      surface_win_rate: surfaceWinRate,
      hold_pct: holdPct,
      break_pct: breakPct,
      bp_saved_pct: bpSavedPct,
      bp_conv_pct: bpConvPct,
      first_serve_in_pct: firstServeInPct,
      first_serve_points_won_pct: firstServePointsWonPct,
      second_serve_points_won_pct: secondServePointsWonPct,
      return_points_won_pct: returnPointsWonPct,
      dr: dr,
      tpw_pct: tpwPct,
    };
  });

  return merged;
}

function normalizePlayerStatsResponse_(providerPayload, canonicalPlayers, options) {
  const rows = extractPlayerStatsRows_(providerPayload);
  if (isMatchMxRows_(rows)) {
    return aggregateMatchMxRowsToStatsByPlayer_(rows, canonicalPlayers, options || {});
  }
  const normalized = {};

  rows.forEach(function (row) {
    const canonicalName = canonicalizePlayerName_(row.player_name || row.player || row.name || row.canonical_name || '', {});
    if (!canonicalName) return;
    normalized[canonicalName] = {
      ranking: parseIntegerMetric_(row.ranking, row.rank, row.world_ranking),
      recent_form: normalizeRateMetric_(row.recent_form, row.form, row.recent_win_rate),
      surface_win_rate: normalizeRateMetric_(row.surface_win_rate, row.surfaceWinRate, row.surface_rate),
      hold_pct: normalizeRateMetric_(row.hold_pct, row.hold_percentage, row.serve_hold_pct),
      break_pct: normalizeRateMetric_(row.break_pct, row.break_percentage, row.return_break_pct),
      bp_saved_pct: normalizeRateMetric_(row.bp_saved_pct, row.break_points_saved_pct),
      bp_conv_pct: normalizeRateMetric_(row.bp_conv_pct, row.break_points_converted_pct),
      first_serve_in_pct: normalizeRateMetric_(row.first_serve_in_pct, row.first_serve_pct),
      first_serve_points_won_pct: normalizeRateMetric_(row.first_serve_points_won_pct, row.first_serve_won_pct),
      second_serve_points_won_pct: normalizeRateMetric_(row.second_serve_points_won_pct, row.second_serve_won_pct),
      return_points_won_pct: normalizeRateMetric_(row.return_points_won_pct, row.return_won_pct),
      dr: normalizeFloatMetric_(row.dr, row.dominance_ratio),
      tpw_pct: normalizeRateMetric_(row.tpw_pct, row.total_points_won_pct),
    };
  });

  const map = {};
  (canonicalPlayers || []).forEach(function (name) {
    const canonical = canonicalizePlayerName_(name, {});
    if (!canonical) return;
    map[canonical] = normalized[canonical] || null;
  });

  return map;
}

function isMatchMxRows_(rows) {
  return !!(rows && rows.length && Object.prototype.hasOwnProperty.call(rows[0], 'numeric_stats'));
}

function aggregateMatchMxRowsToStatsByPlayer_(rows, canonicalPlayers, options) {
  const grouped = {};
  const asOfDate = options.as_of_time instanceof Date ? options.as_of_time : new Date(options.as_of_time || Date.now());
  const weeks = Math.max(0, Number(options.match_window_weeks || 52));
  const recentMatchCount = Math.max(0, Number(options.recent_match_count || 0));
  const windowStartMs = asOfDate.getTime() - (weeks * 7 * 24 * 60 * 60 * 1000);

  rows.forEach(function (row) {
    const canonicalName = canonicalizePlayerName_(row.player_name || row.player || row.name || '', {});
    if (!canonicalName) return;
    const matchDate = parseDateMs_(row.date);
    if (Number.isFinite(matchDate) && matchDate > asOfDate.getTime()) return;
    if (weeks > 0 && Number.isFinite(matchDate) && matchDate < windowStartMs) return;

    if (!grouped[canonicalName]) grouped[canonicalName] = [];
    grouped[canonicalName].push(row);
  });

  const stats = {};
  Object.keys(grouped).forEach(function (player) {
    const playerRows = grouped[player]
      .slice()
      .sort(function (a, b) { return parseDateMs_(b.date) - parseDateMs_(a.date); });
    const scopedRows = recentMatchCount > 0 ? playerRows.slice(0, recentMatchCount) : playerRows;

    stats[player] = {
      ranking: parseIntegerMetric_(scopedRows[0] && scopedRows[0].ranking),
      recent_form: normalizeRateMetric_(averageMetric_(scopedRows, 'recent_form')),
      surface_win_rate: normalizeRateMetric_(averageMetric_(scopedRows, 'surface_win_rate')),
      hold_pct: normalizeRateMetric_(averageMetric_(scopedRows, 'hold_pct')),
      break_pct: normalizeRateMetric_(averageMetric_(scopedRows, 'break_pct')),
      bp_saved_pct: normalizeRateMetric_(averageMetric_(scopedRows, 'bp_saved_pct')),
      bp_conv_pct: normalizeRateMetric_(averageMetric_(scopedRows, 'bp_conv_pct')),
      first_serve_in_pct: normalizeRateMetric_(averageMetric_(scopedRows, 'first_serve_in_pct')),
      first_serve_points_won_pct: normalizeRateMetric_(averageMetric_(scopedRows, 'first_serve_points_won_pct')),
      second_serve_points_won_pct: normalizeRateMetric_(averageMetric_(scopedRows, 'second_serve_points_won_pct')),
      return_points_won_pct: normalizeRateMetric_(averageMetric_(scopedRows, 'return_points_won_pct')),
      dr: normalizeFloatMetric_(averageMetric_(scopedRows, 'dr')),
      tpw_pct: normalizeRateMetric_(averageMetric_(scopedRows, 'tpw_pct')),
    };
  });

  const map = {};
  (canonicalPlayers || []).forEach(function (name) {
    const canonical = canonicalizePlayerName_(name, {});
    if (!canonical) return;
    map[canonical] = stats[canonical] || null;
  });
  return map;
}

function parseDateMs_(value) {
  const ms = Date.parse(String(value || ''));
  return Number.isFinite(ms) ? ms : NaN;
}

function averageMetric_(rows, key) {
  let sum = 0;
  let count = 0;
  (rows || []).forEach(function (row) {
    const value = Number(row && row[key]);
    if (!Number.isFinite(value)) return;
    sum += value;
    count += 1;
  });
  return count ? (sum / count) : null;
}

function normalizeFloatMetric_() {
  for (let i = 0; i < arguments.length; i += 1) {
    const value = Number(arguments[i]);
    if (Number.isFinite(value)) return roundNumber_(value, 3);
  }
  return null;
}

function extractPlayerStatsRows_(providerPayload) {
  if (Array.isArray(providerPayload)) return providerPayload;
  if (providerPayload && Array.isArray(providerPayload.data)) return providerPayload.data;
  if (providerPayload && Array.isArray(providerPayload.players)) return providerPayload.players;

  if (providerPayload && providerPayload.players && typeof providerPayload.players === 'object') {
    return Object.keys(providerPayload.players).map(function (name) {
      const row = providerPayload.players[name] || {};
      row.player_name = row.player_name || name;
      return row;
    });
  }

  return [];
}

function parseIntegerMetric_() {
  for (let i = 0; i < arguments.length; i += 1) {
    const value = Number(arguments[i]);
    if (Number.isFinite(value)) return Math.round(value);
  }
  return null;
}

function normalizeRateMetric_() {
  for (let i = 0; i < arguments.length; i += 1) {
    const value = Number(arguments[i]);
    if (!Number.isFinite(value)) continue;
    const normalized = value > 1 ? value / 100 : value;
    return roundNumber_(normalized, 3);
  }
  return null;
}

function dedupePlayerNames_(players) {
  const deduped = {};
  (players || []).forEach(function (name) {
    const canonical = canonicalizePlayerName_(name, {});
    if (!canonical) return;
    deduped[canonical] = true;
  });
  return Object.keys(deduped);
}

function buildPlayerStatsCacheKey_(players, asOfTime) {
  const normalizedPlayers = dedupePlayerNames_(players || []).sort();
  const asOfBucketMin = Math.floor((asOfTime.getTime()) / 60000);
  return ['PLAYER_STATS_PAYLOAD', asOfBucketMin, stringHashCode_(normalizedPlayers.join('|'))].join('|');
}

function getCachedPlayerStatsPayload_(cacheKey) {
  try {
    const raw = CacheService.getScriptCache().get(cacheKey);
    if (!raw) return null;
    return JSON.parse(raw);
  } catch (e) {
    return null;
  }
}

function setCachedPlayerStatsPayload_(cacheKey, payload) {
  CacheService.getScriptCache().put(cacheKey, JSON.stringify(payload || {}), 21600);
}

function persistPlayerStatsMeta_(payload, source, nowMs, playerCount, asOfTime) {
  const serializable = {
    cache_key: payload.cache_key || '',
    cached_at_ms: Number(payload.cached_at_ms || nowMs),
    source: source,
    as_of_time: payload.as_of_time || asOfTime.toISOString(),
    has_stats: Object.keys(payload.stats_by_player || {}).length > 0,
    player_count: Number(payload.player_count || playerCount || 0),
    stats_by_player: payload.stats_by_player || {},
  };

  setStateValue_('PLAYER_STATS_STALE_PAYLOAD', JSON.stringify(serializable));
  setStateValue_('PLAYER_STATS_LAST_FETCH_META', JSON.stringify({
    cache_key: serializable.cache_key,
    cached_at_ms: serializable.cached_at_ms,
    source: source,
    as_of_time: serializable.as_of_time,
    has_stats: serializable.has_stats,
    player_count: serializable.player_count,
  }));
}

function getTaH2hRowForCanonicalPair_(config, playerA, playerB) {
  const canonicalA = canonicalizePlayerName_(playerA || '', {});
  const canonicalB = canonicalizePlayerName_(playerB || '', {});
  if (!canonicalA || !canonicalB || canonicalA === canonicalB) return null;

  const dataset = getTaH2hDataset_(config || {});
  if (!dataset || !dataset.by_pair) return null;

  const directKey = buildTaH2hPairKey_(canonicalA, canonicalB);
  const direct = dataset.by_pair[directKey];
  if (direct) return direct;

  const reverseKey = buildTaH2hPairKey_(canonicalB, canonicalA);
  const reverse = dataset.by_pair[reverseKey];
  if (!reverse) return null;

  return {
    player_a: canonicalA,
    player_b: canonicalB,
    wins_a: Number(reverse.wins_b || 0),
    wins_b: Number(reverse.wins_a || 0),
    source_updated_date: reverse.source_updated_date || dataset.source_updated_date || '',
  };
}

function getTaH2hDataset_(config) {
  const runtimeConfig = config || {};
  const forceRefresh = toBoolean_(runtimeConfig.PLAYER_STATS_FORCE_REFRESH, false);
  const ttlMs = Math.max(1, Number(runtimeConfig.PLAYER_STATS_CACHE_TTL_MIN || 10)) * 60000;
  const nowMs = Date.now();

  if (!forceRefresh) {
    const cached = getCachedTaH2hDataset_();
    if (cached && Number.isFinite(cached.cached_at_ms) && nowMs - cached.cached_at_ms <= ttlMs) {
      return cached;
    }
  }

  const fresh = fetchTaH2hDatasetFromSource_(runtimeConfig);
  if (fresh.ok) {
    const payload = fresh.payload || { rows: [], by_pair: {} };
    payload.cached_at_ms = nowMs;
    setCachedTaH2hDataset_(payload);
    persistTaH2hDatasetState_(payload, 'fresh_h2h_page');
    return payload;
  }

  const stale = getStateJson_('PLAYER_STATS_H2H_STALE_PAYLOAD');
  if (stale && stale.by_pair) {
    return stale;
  }

  return null;
}

function fetchTaH2hDatasetFromSource_(config) {
  const url = String(config.PLAYER_STATS_TA_H2H_URL || 'https://tennisabstract.com/reports/h2hMatrixWta.html').trim();
  if (!url) return { ok: false, reason_code: 'ta_h2h_url_missing' };

  const headers = {
    Accept: 'text/html',
    'User-Agent': String(config.PLAYER_STATS_FETCH_USER_AGENT || 'Mozilla/5.0 (compatible; WTA-Edge-Board/1.0)'),
  };
  let response;
  try {
    response = UrlFetchApp.fetch(url, {
      method: 'get',
      headers: headers,
      muteHttpExceptions: true,
      followRedirects: true,
      validateHttpsCertificates: true,
    });
  } catch (e) {
    return { ok: false, reason_code: 'ta_h2h_fetch_failed', error: e && e.message };
  }

  const status = Number(response.getResponseCode() || 0);
  if (status < 200 || status >= 300) return { ok: false, reason_code: 'ta_h2h_http_' + status };

  const html = String(response.getContentText() || '');
  const rows = extractTaH2hMatrixRows_(html);
  if (!rows.length) return { ok: false, reason_code: 'ta_h2h_parse_failed' };

  const sourceUpdatedDate = extractTaH2hSourceUpdatedDate_(html);
  const byPair = {};
  rows.forEach(function (row) {
    row.source_updated_date = sourceUpdatedDate;
    byPair[buildTaH2hPairKey_(row.player_a, row.player_b)] = row;
  });

  return {
    ok: true,
    reason_code: 'ta_h2h_ok',
    payload: {
      source: url,
      source_updated_date: sourceUpdatedDate,
      fetched_at: new Date().toISOString(),
      rows: rows,
      by_pair: byPair,
    },
  };
}

function extractTaH2hMatrixRows_(html) {
  const text = String(html || '');
  const rows = [];
  const seen = {};
  const anchorRegex = /<a\b[^>]*href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi;
  let match;

  while ((match = anchorRegex.exec(text)) !== null) {
    const href = String(match[1] || '');
    const bodyText = stripHtmlTags_(match[2] || '');
    const score = extractH2hWinsFromText_(bodyText) || extractH2hWinsFromText_(href);
    if (!score) continue;

    const names = extractTaH2hPlayersFromHref_(href);
    if (!names) continue;

    const playerA = canonicalizePlayerName_(names.player_a, {});
    const playerB = canonicalizePlayerName_(names.player_b, {});
    if (!playerA || !playerB || playerA === playerB) continue;

    const key = buildTaH2hPairKey_(playerA, playerB);
    if (seen[key]) continue;
    seen[key] = true;

    rows.push({
      player_a: playerA,
      player_b: playerB,
      wins_a: score.wins_a,
      wins_b: score.wins_b,
      source_updated_date: '',
    });
  }

  return rows;
}

function extractTaH2hSourceUpdatedDate_(html) {
  const plain = stripHtmlTags_(html || '').replace(/\s+/g, ' ').trim();
  const match = plain.match(/last\s+update\s*:?\s*([^|\-]{4,40})/i);
  if (!match || !match[1]) return '';

  const value = String(match[1] || '').trim().replace(/[.;,]$/, '');
  const parsedMs = Date.parse(value);
  if (!Number.isFinite(parsedMs)) return value;
  return new Date(parsedMs).toISOString().slice(0, 10);
}

function extractTaH2hPlayersFromHref_(href) {
  const query = String(href || '').split('?')[1] || '';
  if (!query) return null;

  const params = {};
  query.split('&').forEach(function (entry) {
    if (!entry) return;
    const parts = entry.split('=');
    const key = decodeURIComponentSafe_(parts[0] || '').toLowerCase();
    const value = decodeURIComponentSafe_(parts.slice(1).join('=') || '');
    if (!key) return;
    params[key] = value;
  });

  const playerA = params.player1 || params.playera || params.p1 || params.a || params.winner || '';
  const playerB = params.player2 || params.playerb || params.p2 || params.b || params.loser || '';
  if (!playerA || !playerB) return null;

  return { player_a: playerA, player_b: playerB };
}

function extractH2hWinsFromText_(text) {
  const match = String(text || '').match(/(\d+)\s*[-–:]\s*(\d+)/);
  if (!match) return null;
  return {
    wins_a: Number(match[1]),
    wins_b: Number(match[2]),
  };
}

function stripHtmlTags_(html) {
  return String(html || '').replace(/<[^>]+>/g, ' ').replace(/&nbsp;/gi, ' ').replace(/&amp;/gi, '&');
}

function decodeURIComponentSafe_(value) {
  try {
    return decodeURIComponent(String(value || '').replace(/\+/g, ' '));
  } catch (e) {
    return String(value || '');
  }
}

function buildTaH2hPairKey_(playerA, playerB) {
  return [String(playerA || ''), String(playerB || '')].join('||');
}

function getCachedTaH2hDataset_() {
  try {
    const raw = CacheService.getScriptCache().get(PLAYER_STATS_H2H_CACHE_KEY);
    if (!raw) return null;
    return JSON.parse(raw);
  } catch (e) {
    return null;
  }
}

function setCachedTaH2hDataset_(payload) {
  CacheService.getScriptCache().put(PLAYER_STATS_H2H_CACHE_KEY, JSON.stringify(payload || {}), 21600);
}

function persistTaH2hDatasetState_(payload, source) {
  const rows = (payload && payload.rows) || [];
  const byPair = (payload && payload.by_pair) || {};
  const sourceUpdatedDate = (payload && payload.source_updated_date) || '';
  const serializableRows = rows.map(function (row) {
    return {
      player_a: row.player_a,
      player_b: row.player_b,
      wins_a: Number(row.wins_a || 0),
      wins_b: Number(row.wins_b || 0),
      source_updated_date: sourceUpdatedDate,
    };
  });

  const serializable = {
    source: (payload && payload.source) || '',
    source_updated_date: sourceUpdatedDate,
    fetched_at: (payload && payload.fetched_at) || new Date().toISOString(),
    cached_at_ms: Number((payload && payload.cached_at_ms) || Date.now()),
    row_count: serializableRows.length,
    rows: serializableRows,
    by_pair: byPair,
  };

  setStateValue_('PLAYER_STATS_H2H_STALE_PAYLOAD', JSON.stringify(serializable));
  setStateValue_('PLAYER_STATS_H2H_LAST_FETCH_META', JSON.stringify({
    source: serializable.source,
    source_updated_date: serializable.source_updated_date,
    fetched_at: serializable.fetched_at,
    cached_at_ms: serializable.cached_at_ms,
    source_type: source || '',
    row_count: serializable.row_count,
  }));
}
