function stageFetchOdds(runId, config) {
  const start = Date.now();
  const source = 'the_odds_api';
  const lookaheadMs = config.LOOKAHEAD_HOURS * 60 * 60 * 1000;
  const now = Date.now();
  const cacheTtlSec = Math.max(30, config.ODDS_CACHE_TTL_SEC);
  const cacheResult = getCachedPayload_('ODDS_WINDOW_PAYLOAD');
  let adapter;

  if (cacheResult && now - cacheResult.cached_at_ms <= cacheTtlSec * 1000) {
    adapter = {
      events: cacheResult.events,
      reason_code: 'odds_cache_hit',
      api_credit_usage: 0,
      api_call_count: 0,
      credit_headers: {},
    };
  } else {
    adapter = fetchOddsWindowFromOddsApi_(config, now, now + lookaheadMs);
    if (adapter.events && adapter.events.length) {
      setCachedPayload_('ODDS_WINDOW_PAYLOAD', adapter.events);
    } else if (adapter.reason_code !== 'odds_api_success') {
      const stale = getStateJson_('ODDS_WINDOW_STALE_PAYLOAD');
      if (stale && stale.events && stale.events.length) {
        adapter = {
          events: stale.events.map(deserializeOddsEvent_),
          reason_code: 'odds_stale_fallback',
          api_credit_usage: adapter.api_credit_usage,
          api_call_count: adapter.api_call_count,
          credit_headers: adapter.credit_headers,
        };
      }
    }
  }

  const raw = adapter.events || [];
  const filtered = raw.filter((event) => event.commence_time.getTime() >= now && event.commence_time.getTime() <= now + lookaheadMs);

  if (filtered.length) {
    setStateValue_('ODDS_WINDOW_STALE_PAYLOAD', JSON.stringify({
      stored_at: new Date().toISOString(),
      events: filtered.map(serializeOddsEvent_),
    }));
  }

  const rows = filtered.map((event) => ({
    key: [event.event_id, event.market, event.outcome].join('|'),
    event_id: event.event_id,
    bookmaker: event.bookmaker,
    bookmaker_keys_considered: (event.bookmaker_keys_considered || []).join(','),
    market: event.market,
    outcome: event.outcome,
    price: event.price,
    odds_timestamp: event.odds_updated_time.toISOString(),
    odds_updated_time: event.odds_updated_time.toISOString(),
    odds_updated_epoch_ms: event.odds_updated_time.getTime(),
    provider_odds_updated_time: event.provider_odds_updated_time ? event.provider_odds_updated_time.toISOString() : '',
    ingestion_timestamp: event.ingestion_timestamp.toISOString(),
    commence_time: event.commence_time.toISOString(),
    commence_epoch_ms: event.commence_time.getTime(),
    competition: event.competition,
    tournament: event.tournament || '',
    event_name: event.event_name || '',
    sport_title: event.sport_title || '',
    home_team: event.home_team || '',
    away_team: event.away_team || '',
    player_1: event.player_1,
    player_2: event.player_2,
    source,
    updated_at: new Date().toISOString(),
  }));

  const summary = buildStageSummary_(runId, 'stageFetchOdds', start, {
    input_count: raw.length,
    output_count: filtered.length,
    provider: source,
    api_credit_usage: adapter.api_credit_usage,
    reason_codes: {
      [adapter.reason_code]: 1,
      within_window: filtered.length,
      outside_window: raw.length - filtered.length,
      odds_rows_emitted: filtered.length,
      events_missing_h2h_outcomes: adapter.events_missing_h2h_outcomes || 0,
      bookmakers_without_h2h_market: adapter.bookmakers_without_h2h_market || 0,
    },
  });

  setStateValue_('LAST_ODDS_API_CREDITS', JSON.stringify({
    run_id: runId,
    observed_at: new Date().toISOString(),
    api_call_count: adapter.api_call_count || 0,
    credit_headers: adapter.credit_headers || {},
  }));

  return { events: filtered, rows, summary };
}

function stageFetchSchedule(runId, config, oddsEvents) {
  const start = Date.now();
  const source = 'the_odds_api_schedule';
  const window = deriveScheduleWindowFromOdds_(oddsEvents, config);
  const scheduleResp = window ? fetchScheduleFromOddsApi_(config, window) : {
    events: [],
    reason_code: 'schedule_window_empty',
    api_credit_usage: 0,
    api_call_count: 0,
    credit_headers: {},
  };
  const inWindow = scheduleResp.events || [];

  const reasonCounts = {};
  const canonicalExamples = [];
  const unresolvedCompetitions = [];
  const unresolvedCompetitionCounts = {};
  const rows = [];
  const allowedEvents = [];
  const tierResolverConfig = buildCompetitionTierResolverConfig_(config);

  inWindow.forEach((event) => {
    const resolved = resolveCompetitionTier_(event, tierResolverConfig);
    const canonical = resolved.canonical_tier;
    const decision = isAllowedTournament(canonical, config);
    reasonCounts[decision.reason_code] = (reasonCounts[decision.reason_code] || 0) + 1;
    canonicalExamples.push({
      raw_name: event.competition,
      canonical_tier: canonical,
      reason_code: decision.reason_code,
      matched_by: resolved.matched_by,
      matched_field: resolved.matched_field,
      resolver_fields: resolved.raw_fields,
    });

    if (canonical === 'UNKNOWN') {
      unresolvedCompetitions.push({
        event_id: event.event_id,
        competition: event.competition,
        source_fields: resolved.raw_fields,
      });

      const unresolvedKey = getUnresolvedCompetitionKey_(resolved.raw_fields);
      unresolvedCompetitionCounts[unresolvedKey] = (unresolvedCompetitionCounts[unresolvedKey] || 0) + 1;
    }

    rows.push({
      key: event.event_id,
      event_id: event.event_id,
      match_id: event.match_id,
      start_time: event.start_time.toISOString(),
      start_epoch_ms: event.start_time.getTime(),
      competition: event.competition,
      player_1: event.player_1,
      player_2: event.player_2,
      canonical_tier: canonical,
      is_allowed: decision.allowed,
      reason_code: decision.reason_code,
      source,
      updated_at: new Date().toISOString(),
    });

    if (decision.allowed) {
      allowedEvents.push({
        event_id: event.event_id,
        match_id: event.match_id,
        start_time: event.start_time,
        competition: event.competition,
        canonical_tier: canonical,
        player_1: event.player_1,
        player_2: event.player_2,
      });
    }
  });

  const summary = buildStageSummary_(runId, 'stageFetchSchedule', start, {
    input_count: inWindow.length,
    output_count: inWindow.length,
    provider: source,
    api_credit_usage: scheduleResp.api_credit_usage,
    reason_codes: reasonCounts,
  });

  summary.reason_codes[scheduleResp.reason_code] = (summary.reason_codes[scheduleResp.reason_code] || 0) + 1;

  setStateValue_('LAST_SCHEDULE_API_CREDITS', JSON.stringify({
    run_id: runId,
    observed_at: new Date().toISOString(),
    api_call_count: scheduleResp.api_call_count || 0,
    credit_headers: scheduleResp.credit_headers || {},
  }));

  return {
    events: allowedEvents,
    rows,
    summary,
    canonicalExamples,
    unresolvedCompetitions,
    unresolvedCompetitionCounts,
    topUnresolvedCompetitions: getTopCompetitionStrings_(unresolvedCompetitionCounts, 20),
    allowedCount: allowedEvents.length,
  };
}

function getUnresolvedCompetitionKey_(sourceFields) {
  for (let i = 0; i < sourceFields.length; i += 1) {
    const candidate = normalizeCompetitionValue_(sourceFields[i].value);
    if (candidate) return candidate;
  }
  return 'unknown_competition_value';
}

function getTopCompetitionStrings_(countMap, topN) {
  return Object.keys(countMap || {})
    .map((key) => ({ competition: key, count: countMap[key] }))
    .sort((a, b) => b.count - a.count)
    .slice(0, topN || 10);
}

function deriveScheduleWindowFromOdds_(oddsEvents, config) {
  if (!oddsEvents || !oddsEvents.length) return null;
  const commenceTimes = oddsEvents.map((e) => e.commence_time.getTime());
  const minMs = Math.min.apply(null, commenceTimes) - config.SCHEDULE_BUFFER_BEFORE_MIN * 60000;
  const maxMs = Math.max.apply(null, commenceTimes) + config.SCHEDULE_BUFFER_AFTER_MIN * 60000;
  return {
    startIso: new Date(minMs).toISOString(),
    endIso: new Date(maxMs).toISOString(),
  };
}

function fetchOddsWindowFromOddsApi_(config, startMs, endMs) {
  if (!config.ODDS_API_KEY) {
    return { events: [], reason_code: 'missing_api_key', api_credit_usage: 0, api_call_count: 0, credit_headers: {} };
  }
  const url = config.ODDS_API_BASE_URL + '/sports/' + encodeURIComponent(config.ODDS_SPORT_KEY) + '/odds'
    + '?apiKey=' + encodeURIComponent(config.ODDS_API_KEY)
    + '&regions=' + encodeURIComponent(config.ODDS_REGIONS)
    + '&markets=' + encodeURIComponent(config.ODDS_MARKETS)
    + '&oddsFormat=' + encodeURIComponent(config.ODDS_ODDS_FORMAT)
    + '&commenceTimeFrom=' + encodeURIComponent(new Date(startMs).toISOString())
    + '&commenceTimeTo=' + encodeURIComponent(new Date(endMs).toISOString());
  const fetched = callOddsApi_(url);
  if (!fetched.ok) return fetched;

  const events = [];
  let eventsMissingH2hOutcomes = 0;
  let bookmakersWithoutH2hMarket = 0;

  (fetched.payload || []).forEach((event) => {
    const bestByOutcome = {};
    const allBookmakers = event.bookmakers || [];

    allBookmakers.forEach((bookmaker) => {
      const h2hMarket = (bookmaker.markets || []).find((market) => market.key === 'h2h');
      if (!h2hMarket) {
        bookmakersWithoutH2hMarket += 1;
        return;
      }

      (h2hMarket.outcomes || []).forEach((outcome) => {
        const price = Number(outcome.price);
        if (!outcome.name || !Number.isFinite(price)) return;

        const side = String(outcome.name);
        const providerOddsUpdatedTime = outcome.last_update || h2hMarket.last_update || bookmaker.last_update || '';
        const parsedProviderOddsUpdatedTime = providerOddsUpdatedTime ? new Date(providerOddsUpdatedTime) : null;
        const providerOddsTimestamp = parsedProviderOddsUpdatedTime && !Number.isNaN(parsedProviderOddsUpdatedTime.getTime())
          ? parsedProviderOddsUpdatedTime
          : null;

        const candidate = {
          bookmaker: bookmaker.key || '',
          price,
          provider_odds_updated_time: providerOddsTimestamp,
        };

        if (!bestByOutcome[side]) {
          bestByOutcome[side] = {
            best: candidate,
            bookmakers: {},
          };
        }

        if (!bestByOutcome[side].bookmakers[candidate.bookmaker]) {
          bestByOutcome[side].bookmakers[candidate.bookmaker] = true;
        }

        if (candidate.price > bestByOutcome[side].best.price) {
          bestByOutcome[side].best = candidate;
        }
      });
    });

    const sides = Object.keys(bestByOutcome);
    if (!sides.length) {
      eventsMissingH2hOutcomes += 1;
      return;
    }

    sides.forEach((side) => {
      const best = bestByOutcome[side].best;
      const ingestionTimestamp = new Date();
      events.push({
        event_id: event.id,
        bookmaker: best.bookmaker,
        bookmaker_keys_considered: Object.keys(bestByOutcome[side].bookmakers),
        market: 'h2h',
        outcome: side,
        price: best.price,
        provider_odds_updated_time: best.provider_odds_updated_time,
        ingestion_timestamp: ingestionTimestamp,
        odds_updated_time: best.provider_odds_updated_time || ingestionTimestamp,
        commence_time: new Date(event.commence_time),
        competition: event.tournament || event.sport_title || '',
        tournament: event.tournament || '',
        event_name: event.name || '',
        sport_title: event.sport_title || '',
        home_team: event.home_team || '',
        away_team: event.away_team || '',
        player_1: event.home_team || '',
        player_2: event.away_team || '',
      });
    });
  });

  return {
    events,
    reason_code: 'odds_api_success',
    api_credit_usage: fetched.api_credit_usage,
    api_call_count: 1,
    credit_headers: fetched.credit_headers,
    events_missing_h2h_outcomes: eventsMissingH2hOutcomes,
    bookmakers_without_h2h_market: bookmakersWithoutH2hMarket,
  };
}

function fetchScheduleFromOddsApi_(config, window) {
  if (!config.ODDS_API_KEY) {
    return { events: [], reason_code: 'missing_api_key', api_credit_usage: 0, api_call_count: 0, credit_headers: {} };
  }
  const url = config.ODDS_API_BASE_URL + '/sports/' + encodeURIComponent(config.ODDS_SPORT_KEY) + '/events'
    + '?apiKey=' + encodeURIComponent(config.ODDS_API_KEY)
    + '&commenceTimeFrom=' + encodeURIComponent(window.startIso)
    + '&commenceTimeTo=' + encodeURIComponent(window.endIso);
  const fetched = callOddsApi_(url);
  if (!fetched.ok) return fetched;

  return {
    events: (fetched.payload || []).map((event) => ({
      event_id: event.id,
      match_id: event.id,
      start_time: new Date(event.commence_time),
      competition: event.tournament || event.sport_title || '',
      tournament: event.tournament || '',
      event_name: event.name || '',
      sport_title: event.sport_title || '',
      home_team: event.home_team || '',
      away_team: event.away_team || '',
      player_1: event.home_team || '',
      player_2: event.away_team || '',
    })),
    reason_code: 'schedule_api_success',
    api_credit_usage: fetched.api_credit_usage,
    api_call_count: 1,
    credit_headers: fetched.credit_headers,
  };
}

function callOddsApi_(url) {
  const resp = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
  const status = resp.getResponseCode();
  const headers = resp.getAllHeaders();
  const creditHeaders = {
    requests_used: Number(headers['x-requests-used'] || headers['X-Requests-Used'] || 0),
    requests_remaining: Number(headers['x-requests-remaining'] || headers['X-Requests-Remaining'] || 0),
    requests_last: Number(headers['x-requests-last'] || headers['X-Requests-Last'] || 0),
  };

  if (status < 200 || status >= 300) {
    return {
      ok: false,
      payload: [],
      reason_code: 'api_http_' + status,
      api_credit_usage: creditHeaders.requests_last || 0,
      api_call_count: 1,
      credit_headers: creditHeaders,
    };
  }

  return {
    ok: true,
    payload: JSON.parse(resp.getContentText() || '[]'),
    api_credit_usage: creditHeaders.requests_last || 0,
    api_call_count: 1,
    credit_headers: creditHeaders,
  };
}

function getCachedPayload_(key) {
  try {
    const raw = CacheService.getScriptCache().get(key);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    return {
      cached_at_ms: parsed.cached_at_ms,
      events: (parsed.events || []).map(deserializeOddsEvent_),
    };
  } catch (e) {
    return null;
  }
}

function setCachedPayload_(key, events) {
  CacheService.getScriptCache().put(key, JSON.stringify({
    cached_at_ms: Date.now(),
    events: events.map(serializeOddsEvent_),
  }), 21600);
}

function serializeOddsEvent_(event) {
  return {
    event_id: event.event_id,
    bookmaker: event.bookmaker,
    bookmaker_keys_considered: event.bookmaker_keys_considered,
    market: event.market,
    outcome: event.outcome,
    price: event.price,
    odds_timestamp: event.odds_updated_time.toISOString(),
    odds_updated_time: event.odds_updated_time.toISOString(),
    provider_odds_updated_time: event.provider_odds_updated_time ? event.provider_odds_updated_time.toISOString() : '',
    ingestion_timestamp: event.ingestion_timestamp.toISOString(),
    commence_time: event.commence_time.toISOString(),
    competition: event.competition,
    tournament: event.tournament || '',
    event_name: event.event_name || '',
    sport_title: event.sport_title || '',
    home_team: event.home_team || '',
    away_team: event.away_team || '',
    player_1: event.player_1,
    player_2: event.player_2,
  };
}

function deserializeOddsEvent_(event) {
  return {
    event_id: event.event_id,
    bookmaker: event.bookmaker,
    bookmaker_keys_considered: event.bookmaker_keys_considered || [],
    market: event.market,
    outcome: event.outcome,
    price: event.price,
    provider_odds_updated_time: event.provider_odds_updated_time ? new Date(event.provider_odds_updated_time) : null,
    ingestion_timestamp: new Date(event.ingestion_timestamp || event.odds_updated_time || event.odds_timestamp || event.commence_time),
    odds_updated_time: new Date(event.odds_updated_time || event.odds_timestamp || event.ingestion_timestamp || event.commence_time),
    odds_timestamp: new Date(event.odds_updated_time || event.odds_timestamp || event.ingestion_timestamp || event.commence_time),
    commence_time: new Date(event.commence_time),
    competition: event.competition,
    tournament: event.tournament || '',
    event_name: event.event_name || '',
    sport_title: event.sport_title || '',
    home_team: event.home_team || '',
    away_team: event.away_team || '',
    player_1: event.player_1,
    player_2: event.player_2,
  };
}
