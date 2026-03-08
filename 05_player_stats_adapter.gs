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
  if (!sourceConfigs.length) {
    return {
      ok: false,
      reason_code: 'player_stats_provider_not_configured',
      api_credit_usage: 0,
      api_call_count: 0,
    };
  }

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
    reason_code: attempts.length ? attempts[attempts.length - 1] : 'player_stats_provider_not_configured',
    detail: attempts.join(','),
    api_credit_usage: 0,
    api_call_count: totalApiCalls + Number(scraped.api_call_count || 0),
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

    maps.forEach(function (statsMap) {
      const stats = statsMap && statsMap[player];
      if (!stats) return;
      if (ranking === null && stats.ranking !== null && stats.ranking !== undefined) ranking = stats.ranking;
      if (recentForm === null && stats.recent_form !== null && stats.recent_form !== undefined) recentForm = stats.recent_form;
      if (surfaceWinRate === null && stats.surface_win_rate !== null && stats.surface_win_rate !== undefined) surfaceWinRate = stats.surface_win_rate;
      if (holdPct === null && stats.hold_pct !== null && stats.hold_pct !== undefined) holdPct = stats.hold_pct;
      if (breakPct === null && stats.break_pct !== null && stats.break_pct !== undefined) breakPct = stats.break_pct;
    });

    merged[player] = {
      ranking: ranking,
      recent_form: recentForm,
      surface_win_rate: surfaceWinRate,
      hold_pct: holdPct,
      break_pct: breakPct,
    };
  });

  return merged;
}

function normalizePlayerStatsResponse_(providerPayload, canonicalPlayers) {
  const rows = extractPlayerStatsRows_(providerPayload);
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
