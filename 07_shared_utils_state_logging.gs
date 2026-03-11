
const STATE_VALUE_SAFE_CHAR_THRESHOLD = 45000;
const STATE_VALUE_GUARD_REASON = 'state_value_summarized_size_guard';

const SECRET_REDACTION_MAP = {
  exact_keys: [
    'ODDS_API_KEY',
    'DISCORD_WEBHOOK',
    'WEBHOOK_URL',
    'API_KEY',
    'X_API_KEY',
    'AUTHORIZATION',
    'ACCESS_TOKEN',
    'TOKEN',
    'SECRET',
    'PASSWORD',
  ],
  key_name_pattern: /(api[_-]?key|secret|token|authorization|password|webhook)/i,
  query_param_pattern: /([?&](?:apiKey|api_key|token|access_token|key|secret|password|webhook|authorization)=)([^&#\s]*)/gi,
};


function appendRunStartConfigAuditLog_(runId, config, startedAt) {
  const cfg = config || {};
  appendLogRow_({
    row_type: 'ops',
    run_id: runId,
    stage: 'run_start_config_audit',
    started_at: startedAt || new Date(),
    status: 'success',
    reason_code: 'run_mode_gates',
    message: JSON.stringify({
      model_mode: String(cfg.MODEL_MODE || ''),
      disable_sofascore: !!cfg.DISABLE_SOFASCORE,
      require_opening_line_proximity: !!cfg.REQUIRE_OPENING_LINE_PROXIMITY,
      max_opening_lag_minutes: Math.max(0, Number(cfg.MAX_OPENING_LAG_MINUTES || 0)),
    }),
  });
}

function stagePersist(runId, payload) {
  const start = Date.now();

  upsertSheetRows_(SHEETS.RAW_ODDS, [
    'key', 'event_id', 'bookmaker', 'bookmaker_keys_considered', 'market', 'outcome', 'price', 'odds_timestamp', 'odds_updated_time',
    'odds_updated_epoch_ms', 'provider_odds_updated_time', 'open_timestamp', 'open_timestamp_epoch_ms', 'opening_lag_minutes', 'opening_lag_evaluated_at', 'is_actionable', 'reason_code', 'ingestion_timestamp', 'commence_time',
    'commence_epoch_ms', 'competition', 'player_1', 'player_2',
    'player_1_hold_pct', 'player_2_hold_pct', 'player_1_break_pct', 'player_2_break_pct',
    'player_1_form_score', 'player_2_form_score',
    'h2h_p1_wins', 'h2h_p2_wins', 'h2h_total_matches',
    'surface', 'stats_source', 'h2h_source', 'stats_as_of',
    'source', 'updated_at',
  ], payload.odds);

  upsertSheetRows_(SHEETS.RAW_SCHEDULE, [
    'key', 'event_id', 'match_id', 'start_time', 'start_epoch_ms', 'competition', 'player_1', 'player_2',
    'player_1_hold_pct', 'player_2_hold_pct', 'player_1_break_pct', 'player_2_break_pct',
    'player_1_form_score', 'player_2_form_score',
    'h2h_p1_wins', 'h2h_p2_wins', 'h2h_total_matches',
    'surface', 'stats_source', 'h2h_source', 'stats_as_of',
    'canonical_tier', 'resolved_source_field', 'resolved_source_value', 'is_allowed', 'reason_code', 'source', 'updated_at',
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

  const matchedMapUpserts = Number(payload.matchMapMatchedCount || 0);
  const rejectedMapUpserts = Number(payload.matchMapRejectedCount || 0);
  const diagnosticMatchMapUpserts = Number(payload.matchMapDiagnosticRecordsWritten || 0);
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
      match_map_upserts_matched: matchedMapUpserts,
      match_map_upserts_rejected: rejectedMapUpserts,
      match_map_diagnostic_records_written: diagnosticMatchMapUpserts,
      signals_upserts: payload.signals.length,
    },
  });

  return { summary };
}


function writeOpeningLagSkipState_(runId, payload) {
  const nowTs = localAndUtcTimestamps_(new Date());
  const statePayload = {
    run_id: runId,
    updated_at: nowTs.local,
    updated_at_utc: nowTs.utc,
    max_opening_lag_minutes: Number(payload.max_opening_lag_minutes || 0),
    require_opening_line_proximity: !!payload.require_opening_line_proximity,
    evaluated_count: Number(payload.evaluated_count || 0),
    actionable_count: Number(payload.actionable_count || 0),
    missing_open_timestamp: Number(payload.missing_open_timestamp || 0),
    opening_lag_exceeded: Number(payload.opening_lag_exceeded || 0),
  };

  setStateValue_('ODDS_OPENING_LAG_GATING_STATE', JSON.stringify(statePayload));

  appendLogRow_({
    row_type: 'ops',
    run_id: runId,
    stage: 'odds_opening_lag_gate',
    status: 'success',
    reason_code: statePayload.opening_lag_exceeded > 0 ? 'opening_lag_exceeded' : (statePayload.missing_open_timestamp > 0 ? 'missing_open_timestamp' : 'opening_lag_within_limit'),
    message: JSON.stringify(statePayload),
  });
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
    sanitizeForLog_(entry.message || ''),
    entry.fetched_odds || 0,
    entry.fetched_schedule || 0,
    entry.allowed_tournaments || 0,
    entry.matched || 0,
    entry.unmatched || 0,
    entry.signals_found || 0,
    sanitizeForLog_(entry.rejection_codes || '{}'),
    entry.cooldown_suppressed || 0,
    entry.duplicate_suppressed || 0,
    entry.lock_event || '',
    entry.debounce_event || '',
    entry.trigger_event || '',
    sanitizeForLog_(entry.exception || ''),
    sanitizeForLog_(entry.stack || ''),
    sanitizeForLog_(entry.stage_summaries || '[]'),
  ]);
}

function setStateValue_(key, value, opts) {
  const sanitizedValue = sanitizeForStateStorage_(value);
  const guarded = guardStateValueSize_(key, sanitizedValue, opts || {});
  upsertSheetRows_(SHEETS.STATE, ['key', 'value', 'updated_at'], [{
    key,
    value: guarded.value,
    updated_at: formatLocalIso_(new Date()),
  }]);
}

function guardStateValueSize_(key, serializedValue, opts) {
  const text = String(serializedValue === null || serializedValue === undefined ? '' : serializedValue);
  if (text.length <= STATE_VALUE_SAFE_CHAR_THRESHOLD) {
    return { value: serializedValue, summarized: false };
  }

  const sourceMeta = (opts && opts.source_meta) || {};
  const parsed = safeJsonParse_(text);
  const summary = {
    reason_code: STATE_VALUE_GUARD_REASON,
    key: String(key || ''),
    source: String(sourceMeta.source || (parsed && parsed.source) || ''),
    source_type: String(sourceMeta.source_type || (parsed && parsed.source_type) || ''),
    reference_key: String(sourceMeta.reference_key || (parsed && parsed.cache_key) || ''),
    original_chars: text.length,
    original_bytes: Utilities.newBlob(text).getBytes().length,
    hash_code: stringHashCode_(text),
    summarized_at: new Date().toISOString(),
    cached_at_ms: Number(sourceMeta.cached_at_ms || (parsed && parsed.cached_at_ms) || Date.now()),
    row_count: Number(sourceMeta.row_count || (parsed && parsed.row_count) || ((parsed && parsed.rows && parsed.rows.length) || 0)),
    player_count: Number(sourceMeta.player_count || (parsed && parsed.player_count) || 0),
    stats_count: Number(sourceMeta.stats_count || ((parsed && parsed.stats_by_player) ? Object.keys(parsed.stats_by_player).length : 0) || 0),
    storage_path: String(sourceMeta.storage_path || (parsed && parsed.storage_path) || ''),
  };

  Logger.log(JSON.stringify({
    event: 'state_value_size_guard_applied',
    reason_code: STATE_VALUE_GUARD_REASON,
    key: String(key || ''),
    original_chars: text.length,
    threshold_chars: STATE_VALUE_SAFE_CHAR_THRESHOLD,
    hash_code: summary.hash_code,
  }));

  return { value: JSON.stringify(summary), summarized: true, reason_code: STATE_VALUE_GUARD_REASON };
}

function safeJsonParse_(text) {
  try {
    return JSON.parse(String(text || ''));
  } catch (e) {
    return null;
  }
}

function sanitizeForStateStorage_(value) {
  if (value === null || value === undefined) return value;
  if (typeof value === 'string') return sanitizeStringForLog_(value);
  return JSON.stringify(sanitizeForLog_(value));
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

function updateBootstrapEmptyCycleState_(runId, oddsRowsEmitted, scheduleEventCount) {
  const previous = getStateJson_('BOOTSTRAP_EMPTY_CYCLE_STATE') || {};
  const config = getConfig_();
  const threshold = Math.max(1, Number(config.BOOTSTRAP_EMPTY_CYCLE_THRESHOLD || 3));
  const oddsCount = Number(oddsRowsEmitted || 0);
  const scheduleCount = Number(scheduleEventCount || 0);
  const now = new Date();
  const timestamps = localAndUtcTimestamps_(now);
  const hadNonEmptyOutput = oddsCount > 0 || scheduleCount > 0;

  const next = {
    run_id: runId,
    consecutive_empty_cycles: hadNonEmptyOutput ? 0 : (Number(previous.consecutive_empty_cycles || 0) + 1),
    diagnostics_counter: hadNonEmptyOutput
      ? Number(previous.diagnostics_counter || 0)
      : (Number(previous.diagnostics_counter || 0) + 1),
    threshold: threshold,
    mitigation_applied_for_cycle: hadNonEmptyOutput ? false : !!previous.mitigation_applied_for_cycle,
    last_non_empty_fetch_at: hadNonEmptyOutput ? timestamps.local : String(previous.last_non_empty_fetch_at || ''),
    last_non_empty_fetch_at_utc: hadNonEmptyOutput ? timestamps.utc : String(previous.last_non_empty_fetch_at_utc || ''),
    updated_at: timestamps.local,
    updated_at_utc: timestamps.utc,
  };

  setStateValue_('BOOTSTRAP_EMPTY_CYCLE_STATE', JSON.stringify(next));

  return {
    consecutive_empty_cycles: next.consecutive_empty_cycles,
    diagnostics_counter: next.diagnostics_counter,
    threshold: threshold,
    warning_needed: !hadNonEmptyOutput && next.consecutive_empty_cycles >= threshold,
    reason_code: (!hadNonEmptyOutput && next.consecutive_empty_cycles >= threshold)
      ? 'bootstrap_empty_cycle_detected'
      : '',
    last_non_empty_fetch_at: next.last_non_empty_fetch_at,
    last_non_empty_fetch_at_utc: next.last_non_empty_fetch_at_utc,
  };
}


function updateEmptyProductiveOutputState_(runId, metrics, config) {
  const previous = getStateJson_('EMPTY_PRODUCTIVE_OUTPUT_STATE') || {};
  const runtimeConfig = config || getConfig_();
  const threshold = Math.max(1, Number(runtimeConfig.EMPTY_PRODUCTIVE_OUTPUT_THRESHOLD || 3));
  const scheduleOnlyThreshold = Math.max(1, Number(runtimeConfig.SCHEDULE_ONLY_STREAK_NOTICE_THRESHOLD || 3));
  const fetchedOdds = Number(metrics && metrics.fetched_odds || 0);
  const fetchedSchedule = Number(metrics && metrics.fetched_schedule || 0);
  const signalsFound = Number(metrics && metrics.signals_found || 0);
  const isEmptyProductiveRun = fetchedOdds > 0 && signalsFound === 0;
  const isScheduleOnlyRun = fetchedSchedule > 0 && fetchedOdds === 0;
  const now = new Date();
  const timestamps = localAndUtcTimestamps_(now);

  const next = {
    run_id: runId,
    consecutive_count: isEmptyProductiveRun ? (Number(previous.consecutive_count || 0) + 1) : 0,
    schedule_only_consecutive_count: isScheduleOnlyRun ? (Number(previous.schedule_only_consecutive_count || 0) + 1) : 0,
    threshold: threshold,
    schedule_only_threshold: scheduleOnlyThreshold,
    fetched_odds: fetchedOdds,
    fetched_schedule: fetchedSchedule,
    signals_found: signalsFound,
    updated_at: timestamps.local,
    updated_at_utc: timestamps.utc,
  };

  setStateValue_('EMPTY_PRODUCTIVE_OUTPUT_STATE', JSON.stringify(next));

  return {
    consecutive_count: next.consecutive_count,
    threshold: threshold,
    warning_needed: isEmptyProductiveRun && next.consecutive_count >= threshold,
    reason_code: (isEmptyProductiveRun && next.consecutive_count >= threshold)
      ? 'productive_output_empty_streak_detected'
      : '',
    schedule_only_consecutive_count: next.schedule_only_consecutive_count,
    schedule_only_threshold: scheduleOnlyThreshold,
    schedule_only_notice_needed: isScheduleOnlyRun && next.schedule_only_consecutive_count >= scheduleOnlyThreshold,
    schedule_only_reason_code: (isScheduleOnlyRun && next.schedule_only_consecutive_count >= scheduleOnlyThreshold)
      ? 'schedule_only_streak_detected'
      : '',
    fetched_odds: fetchedOdds,
    fetched_schedule: fetchedSchedule,
    signals_found: signalsFound,
  };
}

function getLogVerbosityLevel_(config) {
  const runtimeConfig = config || {};
  if (Number.isFinite(Number(runtimeConfig.LOG_VERBOSITY_LEVEL))) {
    return Math.max(0, Math.min(3, Number(runtimeConfig.LOG_VERBOSITY_LEVEL)));
  }
  return runtimeConfig.VERBOSE_LOGGING ? 2 : 1;
}

function shouldLogVerbose_(config, minLevel) {
  return getLogVerbosityLevel_(config) >= Number(minLevel || 1);
}

function logDiagnosticEvent_(config, eventName, payload, minLevel) {
  if (!shouldLogVerbose_(config, minLevel || 1)) return;
  Logger.log(JSON.stringify(sanitizeForLog_({
    event: eventName,
    verbosity_level: getLogVerbosityLevel_(config),
    logged_at: formatLocalIso_(new Date()),
    payload: payload || {},
  })));
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
    reason_metadata: opts.reason_metadata || {},
  };
  logDiagnosticEvent_(opts.config || null, "stage_summary", summary, 1);
  return summary;
}

function sanitizeForLog_(value) {
  if (value === null || value === undefined) return value;

  if (typeof value === 'string') {
    return sanitizeStringForLog_(value);
  }

  if (value instanceof Date) {
    return value;
  }

  if (Array.isArray(value)) {
    return value.map((item) => sanitizeForLog_(item));
  }

  if (typeof value === 'object') {
    const sanitized = {};
    Object.keys(value).forEach((key) => {
      const keyLower = String(key || '').toLowerCase();
      if (SECRET_REDACTION_MAP.key_name_pattern.test(keyLower)) {
        sanitized[key] = maskSecretValue_(value[key]);
      } else {
        sanitized[key] = sanitizeForLog_(value[key]);
      }
    });
    return sanitized;
  }

  return value;
}

function sanitizeStringForLog_(text) {
  let sanitized = String(text || '');

  SECRET_REDACTION_MAP.exact_keys.forEach((keyName) => {
    const escaped = escapeRegex_(String(keyName || ''));
    const doubleQuotePattern = new RegExp('("' + escaped + '"\\s*:\\s*")([^"\\n\\r]*)(")', 'gi');
    const singleQuotePattern = new RegExp('(\'' + escaped + '\'\\s*:\\s*\')([^\'\\n\\r]*)(\')', 'gi');
    sanitized = sanitized.replace(doubleQuotePattern, function (_, prefix, secretValue, suffix) {
      return prefix + maskSecretValue_(secretValue) + suffix;
    });
    sanitized = sanitized.replace(singleQuotePattern, function (_, prefix, secretValue, suffix) {
      return prefix + maskSecretValue_(secretValue) + suffix;
    });
  });

  sanitized = sanitized.replace(SECRET_REDACTION_MAP.query_param_pattern, function (_, prefix, secretValue) {
    return prefix + maskSecretValue_(secretValue);
  });
  sanitized = sanitized.replace(/(\b(?:apiKey|api_key|token|access_token|key|secret|password|webhook|authorization)\s*[:=]\s*)([^\s"'&,]+)/gi, function (_, prefix, secretValue) {
    return prefix + maskSecretValue_(secretValue);
  });
  sanitized = sanitized.replace(/(x-api-key\s*[:=]\s*)([^\s"']+)/gi, function (_, prefix, secretValue) {
    return prefix + maskSecretValue_(secretValue);
  });
  sanitized = sanitized.replace(/(authorization\s*[:=]\s*bearer\s+)([^\s"']+)/gi, function (_, prefix, secretValue) {
    return prefix + maskSecretValue_(secretValue);
  });
  sanitized = sanitized.replace(/(hooks\.slack\.com\/services\/)([^\s"']+)/gi, function (_, prefix, secretValue) {
    return prefix + maskSecretValue_(secretValue);
  });
  sanitized = sanitized.replace(/(discord(?:app)?\.com\/api\/webhooks\/)([^\s"']+)/gi, function (_, prefix, secretValue) {
    return prefix + maskSecretValue_(secretValue);
  });

  return sanitized;
}

function maskSecretValue_(value) {
  const text = String(value === null || value === undefined ? '' : value);
  if (!text) return '';

  const prefixLen = Math.min(4, Math.max(1, Math.floor(text.length / 3)));
  const suffixLen = Math.min(3, Math.max(1, Math.floor(text.length / 3)));
  const prefix = text.slice(0, prefixLen);
  const suffix = text.slice(text.length - suffixLen);
  return prefix + '***' + suffix;
}

function escapeRegex_(text) {
  return String(text || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
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
      const value = Number((map || {})[k]);
      if (!Number.isFinite(value)) return;
      merged[k] = Number(merged[k] || 0) + value;
    });
  });
  return merged;
}

function normalizeUpstreamGateReason_(value) {
  const text = String(value === null || value === undefined ? '' : value).trim();
  return text || 'unspecified';
}

function mergeReasonMetadata_(reasonMaps) {
  const merged = {};
  const upstreamGateCandidates = [];

  (reasonMaps || []).forEach((map) => {
    Object.keys(map || {}).forEach((key) => {
      const rawValue = map[key];
      if (typeof rawValue === 'number' && Number.isFinite(rawValue)) return;
      const textValue = String(rawValue === null || rawValue === undefined ? '' : rawValue).trim();
      if (!textValue) return;

      if (key === 'upstream_gate_reason') {
        upstreamGateCandidates.push(normalizeUpstreamGateReason_(textValue));
      } else {
        merged[key] = textValue;
      }
    });
  });

  if (upstreamGateCandidates.length > 0) {
    const nonDefault = upstreamGateCandidates.filter((reason) => reason !== 'unspecified');
    merged.upstream_gate_reason = nonDefault.length > 0 ? nonDefault[0] : upstreamGateCandidates[0];
  }

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
