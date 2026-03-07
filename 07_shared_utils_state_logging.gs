function stagePersist(runId, payload) {
  const start = Date.now();

  upsertSheetRows_(SHEETS.RAW_ODDS, [
    'key', 'event_id', 'bookmaker', 'bookmaker_keys_considered', 'market', 'outcome', 'price', 'odds_timestamp', 'odds_updated_time',
    'odds_updated_epoch_ms', 'provider_odds_updated_time', 'ingestion_timestamp', 'commence_time',
    'commence_epoch_ms', 'competition', 'player_1', 'player_2', 'source', 'updated_at',
  ], payload.odds);

  upsertSheetRows_(SHEETS.RAW_SCHEDULE, [
    'key', 'event_id', 'match_id', 'start_time', 'start_epoch_ms', 'competition', 'player_1', 'player_2',
    'canonical_tier', 'is_allowed', 'reason_code', 'source', 'updated_at',
  ], payload.schedule);

  upsertSheetRows_(SHEETS.RAW_PLAYER_STATS, [
    'key', 'event_id', 'player_canonical_name', 'source', 'feature_timestamp', 'feature_values', 'has_stats', 'updated_at',
  ], payload.playerStats);

  upsertSheetRows_(SHEETS.MATCH_MAP, [
    'key', 'odds_event_id', 'schedule_event_id', 'match_type',
    'rejection_code', 'time_diff_min', 'competition_tier', 'updated_at',
  ], payload.matchMap);

  upsertSheetRows_(SHEETS.SIGNALS, [
    'key', 'run_id', 'odds_event_id', 'schedule_event_id',
    'market', 'side', 'bookmaker', 'competition_tier', 'model_version',
    'model_probability', 'market_implied_probability', 'edge_value', 'edge_tier', 'stake_units',
    'signal_hash', 'notification_outcome', 'reason_code', 'created_at',
  ], payload.signals);

  const total = payload.odds.length + payload.schedule.length + payload.playerStats.length + payload.matchMap.length + payload.signals.length;
  const summary = buildStageSummary_(runId, 'stagePersist', start, {
    input_count: total,
    output_count: total,
    provider: 'google_sheets',
    api_credit_usage: 0,
    reason_codes: {
      raw_odds_upserts: payload.odds.length,
      raw_schedule_upserts: payload.schedule.length,
      raw_player_stats_upserts: payload.playerStats.length,
      match_map_upserts: payload.matchMap.length,
      signals_upserts: payload.signals.length,
    },
  });

  return { summary };
}

function appendStageLog_(runId, summary) {
  appendLogRow_({
    row_type: 'stage',
    run_id: runId,
    stage: summary.stage,
    started_at: summary.started_at,
    ended_at: summary.ended_at,
    status: 'success',
    reason_code: 'stage_completed',
    message: JSON.stringify({
      input_count: summary.input_count,
      output_count: summary.output_count,
      provider: summary.provider,
      reason_codes: summary.reason_codes,
    }),
  });
}

function appendLogRow_(entry) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sh = ss.getSheetByName(SHEETS.RUN_LOG);

  if (!sh) {
    sh = ensureSheet_(ss, SHEETS.RUN_LOG);
    ensureHeaders_(SHEETS.RUN_LOG, RUN_LOG_HEADERS);
  }

  sh.appendRow([
    entry.row_type || 'summary',
    entry.run_id || '',
    entry.stage || '',
    toIso_(entry.started_at),
    toIso_(entry.ended_at),
    entry.status || '',
    entry.reason_code || '',
    entry.message || '',
    entry.fetched_odds || 0,
    entry.fetched_schedule || 0,
    entry.allowed_tournaments || 0,
    entry.matched || 0,
    entry.unmatched || 0,
    entry.signals_found || 0,
    entry.rejection_codes || '{}',
    entry.cooldown_suppressed || 0,
    entry.duplicate_suppressed || 0,
    entry.lock_event || '',
    entry.debounce_event || '',
    entry.trigger_event || '',
    entry.exception || '',
    entry.stack || '',
    entry.stage_summaries || '[]',
  ]);
}

function setStateValue_(key, value) {
  upsertSheetRows_(SHEETS.STATE, ['key', 'value', 'updated_at'], [{
    key,
    value,
    updated_at: formatLocalIso_(new Date()),
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

function incrementDuplicatePreventedCount_() {
  const scriptProps = PropertiesService.getScriptProperties();
  const current = Number(scriptProps.getProperty(PROPS.DUPLICATE_PREVENTED_COUNT) || 0);
  const next = current + 1;
  scriptProps.setProperty(PROPS.DUPLICATE_PREVENTED_COUNT, String(next));
  return next;
}

function buildStageSummary_(runId, stage, startMs, opts) {
  const endMs = Date.now();
  const summary = {
    run_id: runId,
    stage,
    started_at: formatLocalIso_(new Date(startMs)),
    ended_at: formatLocalIso_(new Date(endMs)),
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

function stringHashCode_(value) {
  const text = String(value || '');
  let hash = 0;
  for (let i = 0; i < text.length; i += 1) {
    hash = ((hash << 5) - hash + text.charCodeAt(i)) | 0;
  }
  return Math.abs(hash);
}

function roundNumber_(value, decimals) {
  const factor = Math.pow(10, Number(decimals || 0));
  return Math.round(Number(value) * factor) / factor;
}

function getStateJson_(key) {
  const sh = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(SHEETS.STATE);
  const values = sh.getDataRange().getValues();
  for (let i = 1; i < values.length; i += 1) {
    if (String(values[i][0]) === key) {
      try {
        return JSON.parse(values[i][1]);
      } catch (e) {
        return null;
      }
    }
  }
  return null;
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



function getTopReasonCodes_(reasonMap, topN) {
  return Object.keys(reasonMap || {})
    .map((k) => ({ reason_code: k, count: reasonMap[k] }))
    .sort((a, b) => b.count - a.count)
    .slice(0, topN || 10);
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
  return formatLocalIso_(value);
}

function formatLocalIso_(value) {
  if (!value) return '';
  const date = value instanceof Date ? value : new Date(value);
  if (isNaN(date.getTime())) return '';
  return Utilities.formatDate(date, TIMESTAMP_TIMEZONE.ID, "yyyy-MM-dd'T'HH:mm:ss") + TIMESTAMP_TIMEZONE.OFFSET;
}

function localAndUtcTimestamps_(value) {
  if (!value) return { local: '', utc: '' };
  const date = value instanceof Date ? value : new Date(value);
  if (isNaN(date.getTime())) return { local: '', utc: '' };
  return {
    local: formatLocalIso_(date),
    utc: date.toISOString(),
  };
}
