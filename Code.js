const SHEETS = {
  CONFIG: 'Config',
  RUN_LOG: 'Run_Log',
  RAW_ODDS: 'Raw_Odds',
  RAW_SCHEDULE: 'Raw_Schedule',
  MATCH_MAP: 'Match_Map',
  STATE: 'State',
};

const DEFAULT_CONFIG = {
  RUN_ENABLED: 'true',
  LOOKAHEAD_HOURS: '36',
  ODDS_SPORT_KEY: 'tennis_wta',
  ODDS_MARKETS: 'h2h',
  ODDS_REGIONS: 'us',
  ODDS_ODDS_FORMAT: 'american',
  MATCH_TIME_TOLERANCE_MIN: '45',
  MATCH_FALLBACK_EXPANSION_MIN: '120',
  ALLOW_WTA_125: 'false',
  VERBOSE_LOGGING: 'true',
};

const PROPS = {
  TRIGGER_SPEC_V1: 'TRIGGER_SPEC_V1',
  LAST_RUN_TS: 'LAST_RUN_TS',
};

function installOrUpdateTriggers() {
  ensureTabsAndConfig_();

  const spec = {
    version: 1,
    functionName: 'runEdgeBoard',
    type: 'clock',
    everyMinutes: 15,
  };
  const signature = JSON.stringify(spec);
  const scriptProps = PropertiesService.getScriptProperties();
  const current = scriptProps.getProperty(PROPS.TRIGGER_SPEC_V1);

  if (current === signature) {
    logTriggerEvent_('trigger_noop', 'Trigger spec unchanged.');
    return;
  }

  ScriptApp.getProjectTriggers().forEach((trigger) => {
    if (trigger.getHandlerFunction() === 'runEdgeBoard') {
      ScriptApp.deleteTrigger(trigger);
    }
  });

  ScriptApp.newTrigger('runEdgeBoard').timeBased().everyMinutes(spec.everyMinutes).create();
  scriptProps.setProperty(PROPS.TRIGGER_SPEC_V1, signature);
  logTriggerEvent_('trigger_reinstalled', 'Trigger replaced with latest spec.');
}

function runEdgeBoard() {
  ensureTabsAndConfig_();

  const runId = buildRunId_();
  const startedAt = new Date();
  const scriptProps = PropertiesService.getScriptProperties();
  const lock = LockService.getScriptLock();
  const runEvents = [];

  if (!tryLock_(lock, 5000)) {
    appendRunLog_({
      run_id: runId,
      started_at: startedAt,
      ended_at: new Date(),
      status: 'skipped',
      lock_event: 'run_locked_skip',
      debounce_event: '',
      trigger_event: '',
      exception: '',
      stack: '',
      stage_summaries: '{}',
      rejection_codes: '{}',
      fetched_odds: 0,
      fetched_schedule: 0,
      allowed_tournaments: 0,
      matched: 0,
      unmatched: 0,
      cooldown_suppressed: 0,
      duplicate_suppressed: 0,
    });
    return;
  }

  let config;
  try {
    config = getConfig_();
    if (!config.RUN_ENABLED) {
      appendRunLog_({
        run_id: runId,
        started_at: startedAt,
        ended_at: new Date(),
        status: 'skipped',
        lock_event: '',
        debounce_event: 'run_disabled_skip',
        trigger_event: '',
        exception: '',
        stack: '',
        stage_summaries: '{}',
        rejection_codes: '{}',
        fetched_odds: 0,
        fetched_schedule: 0,
        allowed_tournaments: 0,
        matched: 0,
        unmatched: 0,
        cooldown_suppressed: 0,
        duplicate_suppressed: 0,
      });
      return;
    }

    const nowMs = Date.now();
    const lastRun = Number(scriptProps.getProperty(PROPS.LAST_RUN_TS) || 0);
    if (nowMs - lastRun < 2 * 60 * 1000) {
      appendRunLog_({
        run_id: runId,
        started_at: startedAt,
        ended_at: new Date(),
        status: 'skipped',
        lock_event: '',
        debounce_event: 'run_debounced_skip',
        trigger_event: '',
        exception: '',
        stack: '',
        stage_summaries: '{}',
        rejection_codes: '{}',
        fetched_odds: 0,
        fetched_schedule: 0,
        allowed_tournaments: 0,
        matched: 0,
        unmatched: 0,
        cooldown_suppressed: 0,
        duplicate_suppressed: 0,
      });
      return;
    }

    scriptProps.setProperty(PROPS.LAST_RUN_TS, String(nowMs));

    const stageSummaries = [];
    const canonicalExamples = [];
    const unmatchedSamples = [];

    const oddsStage = stageFetchOdds(runId, config);
    stageSummaries.push(oddsStage.summary);

    const scheduleStage = stageFetchSchedule(runId, config);
    stageSummaries.push(scheduleStage.summary);
    Array.prototype.push.apply(canonicalExamples, scheduleStage.canonicalExamples);

    const matchStage = stageMatchEvents(runId, config, oddsStage.events, scheduleStage.events);
    stageSummaries.push(matchStage.summary);
    Array.prototype.push.apply(unmatchedSamples, matchStage.unmatched.slice(0, 20));

    const persistStage = stagePersist(runId, config, {
      odds: oddsStage.rows,
      schedule: scheduleStage.rows,
      matchMap: matchStage.rows,
    });
    stageSummaries.push(persistStage.summary);

    const rejectionCodes = mergeReasonCounts_([
      scheduleStage.summary.reason_codes,
      matchStage.summary.reason_codes,
    ]);

    const verbosePayload = {
      run_id: runId,
      config_snapshot: config,
      stage_summaries: stageSummaries,
      canonicalization_examples: canonicalExamples,
      sample_unmatched_cases: unmatchedSamples,
      timing_breakdown_ms: stageSummaries.reduce((acc, s) => {
        acc[s.stage] = s.duration_ms;
        return acc;
      }, {}),
    };

    setStateValue_('LAST_RUN_VERBOSE_JSON', JSON.stringify(verbosePayload, null, 2));

    appendRunLog_({
      run_id: runId,
      started_at: startedAt,
      ended_at: new Date(),
      status: 'success',
      lock_event: '',
      debounce_event: '',
      trigger_event: '',
      exception: '',
      stack: '',
      stage_summaries: JSON.stringify(stageSummaries),
      rejection_codes: JSON.stringify(rejectionCodes),
      fetched_odds: oddsStage.events.length,
      fetched_schedule: scheduleStage.events.length,
      allowed_tournaments: scheduleStage.allowedCount,
      matched: matchStage.matchedCount,
      unmatched: matchStage.unmatchedCount,
      cooldown_suppressed: 0,
      duplicate_suppressed: 0,
    });
  } catch (error) {
    appendRunLog_({
      run_id: runId,
      started_at: startedAt,
      ended_at: new Date(),
      status: 'failed',
      lock_event: '',
      debounce_event: '',
      trigger_event: '',
      exception: String(error && error.message ? error.message : error),
      stack: String(error && error.stack ? error.stack : '').split('\n').slice(0, 4).join('\n'),
      stage_summaries: '{}',
      rejection_codes: '{}',
      fetched_odds: 0,
      fetched_schedule: 0,
      allowed_tournaments: 0,
      matched: 0,
      unmatched: 0,
      cooldown_suppressed: 0,
      duplicate_suppressed: 0,
    });
    throw error;
  } finally {
    lock.releaseLock();
  }
}

function stageFetchOdds(runId, config) {
  const start = Date.now();
  const source = 'scaffold_sample_odds_provider';
  const lookaheadMs = config.LOOKAHEAD_HOURS * 60 * 60 * 1000;
  const now = Date.now();

  const raw = buildSampleOdds_();
  const filtered = raw.filter((event) => {
    const t = event.commence_time.getTime();
    return t >= now && t <= now + lookaheadMs;
  });

  const rows = filtered.map((event) => ({
    key: [event.event_id, event.bookmaker, event.market, event.outcome].join('|'),
    event_id: event.event_id,
    bookmaker: event.bookmaker,
    market: event.market,
    outcome: event.outcome,
    price: event.price,
    commence_time: event.commence_time.toISOString(),
    competition: event.competition,
    home: event.home,
    away: event.away,
    source,
    updated_at: new Date().toISOString(),
  }));

  const summary = buildStageSummary_(runId, 'stageFetchOdds', start, {
    input_count: raw.length,
    output_count: filtered.length,
    provider: source,
    api_credit_usage: 0,
    reason_codes: {
      within_window: filtered.length,
      outside_window: raw.length - filtered.length,
    },
  });

  return { events: filtered, rows, summary };
}

function stageFetchSchedule(runId, config) {
  const start = Date.now();
  const source = 'scaffold_sample_schedule_provider';
  const now = Date.now();
  const lookaheadMs = config.LOOKAHEAD_HOURS * 60 * 60 * 1000;
  const bufferMs = config.MATCH_FALLBACK_EXPANSION_MIN * 60 * 1000;

  const raw = buildSampleSchedule_();
  const inWindow = raw.filter((event) => {
    const t = event.start_time.getTime();
    return t >= now && t <= now + lookaheadMs + bufferMs;
  });

  const reasonCounts = {};
  const canonicalExamples = [];
  const rows = [];
  const allowedEvents = [];

  inWindow.forEach((event) => {
    const canonical = canonicalizeCompetition(event.competition);
    const decision = isAllowedTournament(canonical, config);
    reasonCounts[decision.reason_code] = (reasonCounts[decision.reason_code] || 0) + 1;
    canonicalExamples.push({
      raw_name: event.competition,
      canonical_tier: canonical,
      reason_code: decision.reason_code,
    });

    rows.push({
      key: event.event_id,
      event_id: event.event_id,
      match_id: event.match_id,
      start_time: event.start_time.toISOString(),
      competition: event.competition,
      home: event.home,
      away: event.away,
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
        home: event.home,
        away: event.away,
      });
    }
  });

  const summary = buildStageSummary_(runId, 'stageFetchSchedule', start, {
    input_count: raw.length,
    output_count: inWindow.length,
    provider: source,
    api_credit_usage: 0,
    reason_codes: reasonCounts,
  });

  return {
    events: allowedEvents,
    rows,
    summary,
    canonicalExamples,
    allowedCount: allowedEvents.length,
  };
}

function stageMatchEvents(runId, config, oddsEvents, scheduleEvents) {
  const start = Date.now();
  const toleranceMin = config.MATCH_TIME_TOLERANCE_MIN;
  const fallbackMin = config.MATCH_FALLBACK_EXPANSION_MIN;
  const reasonCounts = {};
  const rows = [];
  const unmatched = [];
  let matchedCount = 0;

  oddsEvents.forEach((odds) => {
    const primary = findScheduleMatch_(odds, scheduleEvents, toleranceMin);
    const fallback = primary ? null : findScheduleMatch_(odds, scheduleEvents, toleranceMin + fallbackMin);

    if (primary || fallback) {
      const selected = primary || fallback;
      const matchType = primary ? 'primary_match' : 'fallback_match';
      rows.push({
        key: odds.event_id,
        odds_event_id: odds.event_id,
        schedule_event_id: selected.event_id,
        match_type: matchType,
        rejection_code: '',
        time_diff_min: selected.timeDiffMin,
        competition_tier: selected.canonical_tier,
        updated_at: new Date().toISOString(),
      });
      matchedCount += 1;
      reasonCounts[matchType] = (reasonCounts[matchType] || 0) + 1;
    } else {
      rows.push({
        key: odds.event_id,
        odds_event_id: odds.event_id,
        schedule_event_id: '',
        match_type: '',
        rejection_code: 'unmatched_no_schedule_match',
        time_diff_min: '',
        competition_tier: '',
        updated_at: new Date().toISOString(),
      });
      reasonCounts.unmatched_no_schedule_match = (reasonCounts.unmatched_no_schedule_match || 0) + 1;
      unmatched.push({
        odds_event_id: odds.event_id,
        competition: odds.competition,
        home: odds.home,
        away: odds.away,
        commence_time: odds.commence_time.toISOString(),
      });
    }
  });

  const summary = buildStageSummary_(runId, 'stageMatchEvents', start, {
    input_count: oddsEvents.length,
    output_count: rows.length,
    provider: 'internal_matcher',
    api_credit_usage: 0,
    reason_codes: reasonCounts,
  });

  return {
    rows,
    summary,
    matchedCount,
    unmatchedCount: oddsEvents.length - matchedCount,
    unmatched,
  };
}

function stagePersist(runId, config, payload) {
  const start = Date.now();

  upsertSheetRows_(SHEETS.RAW_ODDS, [
    'key', 'event_id', 'bookmaker', 'market', 'outcome', 'price', 'commence_time',
    'competition', 'home', 'away', 'source', 'updated_at',
  ], payload.odds);

  upsertSheetRows_(SHEETS.RAW_SCHEDULE, [
    'key', 'event_id', 'match_id', 'start_time', 'competition', 'home', 'away',
    'canonical_tier', 'is_allowed', 'reason_code', 'source', 'updated_at',
  ], payload.schedule);

  upsertSheetRows_(SHEETS.MATCH_MAP, [
    'key', 'odds_event_id', 'schedule_event_id', 'match_type',
    'rejection_code', 'time_diff_min', 'competition_tier', 'updated_at',
  ], payload.matchMap);

  const summary = buildStageSummary_(runId, 'stagePersist', start, {
    input_count: payload.odds.length + payload.schedule.length + payload.matchMap.length,
    output_count: payload.odds.length + payload.schedule.length + payload.matchMap.length,
    provider: 'google_sheets',
    api_credit_usage: 0,
    reason_codes: {
      raw_odds_upserts: payload.odds.length,
      raw_schedule_upserts: payload.schedule.length,
      match_map_upserts: payload.matchMap.length,
    },
  });

  return { summary };
}

function canonicalizeCompetition(name) {
  if (!name) return 'UNKNOWN';
  const norm = String(name).toLowerCase().replace(/\s+/g, ' ').trim();

  if (/(australian open|roland garros|french open|wimbledon|us open)/.test(norm)) return 'GRAND_SLAM';
  if (/wta\s*1000/.test(norm)) return 'WTA_1000';
  if (/wta\s*500/.test(norm)) return 'WTA_500';
  if (/wta\s*125/.test(norm)) return 'WTA_125';
  if (/wta/.test(norm)) return 'OTHER_WTA';
  return 'UNKNOWN';
}

function isAllowedTournament(canonical, config) {
  if (canonical === 'WTA_500') return { allowed: true, reason_code: 'allowed_wta500' };
  if (canonical === 'WTA_1000') return { allowed: true, reason_code: 'allowed_wta1000' };
  if (canonical === 'GRAND_SLAM') return { allowed: true, reason_code: 'allowed_grand_slam' };
  if (canonical === 'WTA_125') {
    return config.ALLOW_WTA_125
      ? { allowed: true, reason_code: 'allowed_wta125' }
      : { allowed: false, reason_code: 'rejected_wta125' };
  }
  if (canonical === 'OTHER_WTA') return { allowed: false, reason_code: 'rejected_other_tier' };
  return { allowed: false, reason_code: 'rejected_unknown_competition' };
}

function ensureTabsAndConfig_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  Object.keys(SHEETS).forEach((k) => ensureSheet_(ss, SHEETS[k]));

  ensureHeaders_(SHEETS.CONFIG, ['key', 'value']);
  ensureHeaders_(SHEETS.RUN_LOG, [
    'run_id', 'started_at', 'ended_at', 'status',
    'fetched_odds', 'fetched_schedule', 'allowed_tournaments', 'matched', 'unmatched',
    'rejection_codes', 'cooldown_suppressed', 'duplicate_suppressed',
    'lock_event', 'debounce_event', 'trigger_event', 'exception', 'stack', 'stage_summaries',
  ]);
  ensureHeaders_(SHEETS.STATE, ['key', 'value', 'updated_at']);

  ensureConfigDefaults_();
}

function ensureSheet_(ss, name) {
  let sh = ss.getSheetByName(name);
  if (!sh) sh = ss.insertSheet(name);
  return sh;
}

function ensureHeaders_(sheetName, headers) {
  const sh = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(sheetName);
  const firstRow = sh.getRange(1, 1, 1, headers.length).getValues()[0];
  const needsUpdate = headers.some((h, i) => firstRow[i] !== h);
  if (needsUpdate) {
    sh.getRange(1, 1, 1, headers.length).setValues([headers]);
  }
}

function ensureConfigDefaults_() {
  const sh = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(SHEETS.CONFIG);
  const values = sh.getDataRange().getValues();
  const existing = {};
  for (let i = 1; i < values.length; i += 1) {
    existing[values[i][0]] = values[i][1];
  }

  Object.keys(DEFAULT_CONFIG).forEach((key) => {
    if (existing[key] === undefined || existing[key] === '') {
      sh.appendRow([key, DEFAULT_CONFIG[key]]);
    }
  });
}

function getConfig_() {
  const sh = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(SHEETS.CONFIG);
  const values = sh.getDataRange().getValues();
  const config = {};

  for (let i = 1; i < values.length; i += 1) {
    const key = String(values[i][0] || '').trim();
    if (!key) continue;
    config[key] = values[i][1];
  }

  return {
    RUN_ENABLED: toBoolean_(config.RUN_ENABLED, true),
    LOOKAHEAD_HOURS: toNumber_(config.LOOKAHEAD_HOURS, 36),
    ODDS_SPORT_KEY: String(config.ODDS_SPORT_KEY || 'tennis_wta'),
    ODDS_MARKETS: String(config.ODDS_MARKETS || 'h2h'),
    ODDS_REGIONS: String(config.ODDS_REGIONS || 'us'),
    ODDS_ODDS_FORMAT: String(config.ODDS_ODDS_FORMAT || 'american'),
    MATCH_TIME_TOLERANCE_MIN: toNumber_(config.MATCH_TIME_TOLERANCE_MIN, 45),
    MATCH_FALLBACK_EXPANSION_MIN: toNumber_(config.MATCH_FALLBACK_EXPANSION_MIN, 120),
    ALLOW_WTA_125: toBoolean_(config.ALLOW_WTA_125, false),
    VERBOSE_LOGGING: toBoolean_(config.VERBOSE_LOGGING, true),
  };
}

function appendRunLog_(entry) {
  const sh = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(SHEETS.RUN_LOG);
  sh.appendRow([
    entry.run_id,
    toIso_(entry.started_at),
    toIso_(entry.ended_at),
    entry.status,
    entry.fetched_odds,
    entry.fetched_schedule,
    entry.allowed_tournaments,
    entry.matched,
    entry.unmatched,
    entry.rejection_codes,
    entry.cooldown_suppressed,
    entry.duplicate_suppressed,
    entry.lock_event,
    entry.debounce_event,
    entry.trigger_event,
    entry.exception,
    entry.stack,
    entry.stage_summaries,
  ]);
}

function logTriggerEvent_(triggerEvent, message) {
  appendRunLog_({
    run_id: buildRunId_(),
    started_at: new Date(),
    ended_at: new Date(),
    status: 'partial',
    fetched_odds: 0,
    fetched_schedule: 0,
    allowed_tournaments: 0,
    matched: 0,
    unmatched: 0,
    rejection_codes: '{}',
    cooldown_suppressed: 0,
    duplicate_suppressed: 0,
    lock_event: '',
    debounce_event: '',
    trigger_event: triggerEvent,
    exception: '',
    stack: '',
    stage_summaries: JSON.stringify([{ stage: 'installOrUpdateTriggers', message }]),
  });
}

function setStateValue_(key, value) {
  upsertSheetRows_(SHEETS.STATE, ['key', 'value', 'updated_at'], [{
    key,
    value,
    updated_at: new Date().toISOString(),
  }]);
}

function upsertSheetRows_(sheetName, headers, rows) {
  if (!rows || !rows.length) return;
  const sh = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(sheetName);
  ensureHeaders_(sheetName, headers);

  const lastRow = sh.getLastRow();
  const existing = lastRow > 1 ? sh.getRange(2, 1, lastRow - 1, headers.length).getValues() : [];
  const keyToRowIdx = {};

  existing.forEach((row, idx) => {
    keyToRowIdx[String(row[0])] = idx;
  });

  rows.forEach((obj) => {
    const newRow = headers.map((h) => obj[h]);
    const key = String(newRow[0]);
    if (Object.prototype.hasOwnProperty.call(keyToRowIdx, key)) {
      existing[keyToRowIdx[key]] = newRow;
    } else {
      keyToRowIdx[key] = existing.length;
      existing.push(newRow);
    }
  });

  if (existing.length) {
    sh.getRange(2, 1, existing.length, headers.length).setValues(existing);
  }
}

function buildStageSummary_(runId, stage, startMs, opts) {
  const endMs = Date.now();
  const summary = {
    run_id: runId,
    stage,
    started_at: new Date(startMs).toISOString(),
    ended_at: new Date(endMs).toISOString(),
    duration_ms: endMs - startMs,
    input_count: opts.input_count,
    output_count: opts.output_count,
    provider: opts.provider,
    api_credit_usage: opts.api_credit_usage,
    reason_codes: opts.reason_codes || {},
  };
  Logger.log(JSON.stringify(summary));
  return summary;
}

function findScheduleMatch_(odds, scheduleEvents, maxToleranceMin) {
  const oddsPlayers = normalizePlayers_(odds.home, odds.away);
  let best = null;
  scheduleEvents.forEach((sched) => {
    const schedPlayers = normalizePlayers_(sched.home, sched.away);
    if (oddsPlayers !== schedPlayers) return;
    const diffMin = Math.abs(odds.commence_time.getTime() - sched.start_time.getTime()) / 60000;
    if (diffMin <= maxToleranceMin && (!best || diffMin < best.timeDiffMin)) {
      best = {
        event_id: sched.event_id,
        canonical_tier: sched.canonical_tier,
        timeDiffMin: Math.round(diffMin),
      };
    }
  });
  return best;
}

function normalizePlayers_(a, b) {
  return [String(a || '').toLowerCase().trim(), String(b || '').toLowerCase().trim()].sort().join('|');
}

function mergeReasonCounts_(reasonMaps) {
  const merged = {};
  reasonMaps.forEach((map) => {
    Object.keys(map || {}).forEach((k) => {
      merged[k] = (merged[k] || 0) + map[k];
    });
  });
  return merged;
}

function buildSampleOdds_() {
  const now = Date.now();
  return [
    {
      event_id: 'odds_evt_1',
      bookmaker: 'sample_book',
      market: 'h2h',
      outcome: 'player_a',
      price: -130,
      commence_time: new Date(now + 2 * 60 * 60 * 1000),
      competition: 'WTA 500 Doha',
      home: 'Player A',
      away: 'Player B',
    },
    {
      event_id: 'odds_evt_2',
      bookmaker: 'sample_book',
      market: 'h2h',
      outcome: 'player_c',
      price: 115,
      commence_time: new Date(now + 3 * 60 * 60 * 1000),
      competition: 'WTA 125 Mumbai',
      home: 'Player C',
      away: 'Player D',
    },
  ];
}

function buildSampleSchedule_() {
  const now = Date.now();
  return [
    {
      event_id: 'sched_evt_1',
      match_id: 'match_1',
      start_time: new Date(now + 2 * 60 * 60 * 1000 + 8 * 60000),
      competition: 'WTA 500 Doha',
      home: 'Player A',
      away: 'Player B',
    },
    {
      event_id: 'sched_evt_2',
      match_id: 'match_2',
      start_time: new Date(now + 3 * 60 * 60 * 1000),
      competition: 'WTA 125 Mumbai',
      home: 'Player C',
      away: 'Player D',
    },
    {
      event_id: 'sched_evt_3',
      match_id: 'match_3',
      start_time: new Date(now + 5 * 60 * 60 * 1000),
      competition: 'ITF W100 Example',
      home: 'Player E',
      away: 'Player F',
    },
  ];
}

function buildRunId_() {
  return Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyyMMdd'T'HHmmss") + '_' + Utilities.getUuid().slice(0, 8);
}

function tryLock_(lock, timeoutMs) {
  try {
    lock.tryLock(timeoutMs);
    return lock.hasLock();
  } catch (e) {
    return false;
  }
}

function toBoolean_(value, fallback) {
  if (value === true || value === false) return value;
  if (value === undefined || value === null || value === '') return fallback;
  const lowered = String(value).toLowerCase().trim();
  return lowered === 'true' || lowered === '1' || lowered === 'yes';
}

function toNumber_(value, fallback) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function toIso_(value) {
  if (!value) return '';
  if (value instanceof Date) return value.toISOString();
  return String(value);
}
