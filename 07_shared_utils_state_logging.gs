
const STATE_VALUE_SAFE_CHAR_THRESHOLD = 45000;
const STATE_VALUE_GUARD_REASON = 'state_value_summarized_size_guard';

let REASON_ALIAS_DICTIONARY_EMITTED_FOR_PROCESS = false;
let REASON_ALIAS_FALLBACK_WARNING_EMITTED = {};
let REASON_ALIAS_FALLBACK_WARNING_PENDING = {};
let REASON_ALIAS_FALLBACK_WARNING_AGGREGATE_CACHE = null;

const REASON_ALIAS_FALLBACK_WARNING_AGGREGATE_STATE_KEY = 'REASON_ALIAS_FALLBACK_WARNING_AGGREGATE_STATE';
const REASON_ALIAS_FALLBACK_WARNING_AGGREGATE_SCHEMA = 'reason_alias_fallback_warning_aggregate_v1';
const WATCHDOG_WARNING_AGGREGATE_STATE_KEY = 'WATCHDOG_WARNING_AGGREGATE_STATE';
const WATCHDOG_WARNING_AGGREGATE_SCHEMA = 'watchdog_warning_aggregate_v1';
let WATCHDOG_WARNING_AGGREGATE_CACHE = null;

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

const STAGE_LATENCY_EXPECTATIONS_MS = {
  idle: {
    stageFetchOdds: { min: 0, max: 9000 },
    stageFetchSchedule: { min: 0, max: 7000 },
    stageMatchEvents: { min: 0, max: 4400 },
    stageFetchPlayerStats: { min: 0, max: 12000 },
    stageGenerateSignals: { min: 0, max: 6000 },
    stagePersist: { min: 0, max: 6000 },
  },
  healthy: {
    stageFetchOdds: { min: 0, max: 3000 },
    stageFetchSchedule: { min: 0, max: 2500 },
    stageMatchEvents: { min: 0, max: 1500 },
    stageFetchPlayerStats: { min: 0, max: 4500 },
    stageGenerateSignals: { min: 0, max: 2000 },
    stagePersist: { min: 0, max: 2000 },
  },
  degraded: {
    stageFetchOdds: { min: 0, max: 4500 },
    stageFetchSchedule: { min: 0, max: 3500 },
    stageMatchEvents: { min: 0, max: 2200 },
    stageFetchPlayerStats: { min: 0, max: 6500 },
    stageGenerateSignals: { min: 0, max: 3000 },
    stagePersist: { min: 0, max: 3000 },
  },
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
      opening_lag_fallback_exemption_max_age_minutes: Math.max(0, Number(cfg.OPENING_LAG_FALLBACK_EXEMPTION_MAX_AGE_MINUTES || 0)),
      opening_lag_fallback_exemption_max_rows_per_run: Math.max(0, Number(cfg.OPENING_LAG_FALLBACK_EXEMPTION_MAX_ROWS_PER_RUN || 0)),
      opening_lag_fallback_key_match_window_minutes: Math.max(0, Number(cfg.OPENING_LAG_FALLBACK_KEY_MATCH_WINDOW_MINUTES || 0)),
      opening_lag_fallback_key_match_max_age_minutes: Math.max(0, Number(cfg.OPENING_LAG_FALLBACK_KEY_MATCH_MAX_AGE_MINUTES || 0)),
      stake_policy_mode: String(cfg.STAKE_POLICY_MODE || ''),
      account_currency: String(cfg.ACCOUNT_CURRENCY || ''),
      display_currency: String(cfg.DISPLAY_CURRENCY || ''),
      min_stake_per_currency_json: String(cfg.MIN_STAKE_PER_CURRENCY_JSON || ''),
    }),
  });
}

function stagePersist(runId, payload) {
  const start = Date.now();

  upsertSheetRows_(SHEETS.RAW_ODDS, [
    'key', 'event_id', 'bookmaker', 'bookmaker_keys_considered', 'market', 'outcome', 'price', 'odds_timestamp', 'odds_updated_time',
    'odds_updated_epoch_ms', 'provider_odds_updated_time', 'opening_price', 'evaluation_price', 'price_delta_bps', 'open_timestamp', 'open_timestamp_epoch_ms', 'opening_lag_minutes', 'opening_lag_evaluated_at', 'opening_lag_policy_tier', 'opening_lag_policy_tier_applied', 'opening_lag_fallback_exemption_max_age_minutes', 'opening_lag_fallback_minutes_to_start', 'opening_lag_fallback_age_bucket', 'decision_gate_status', 'is_actionable', 'fallback_exemption_reason_code', 'reason_code', 'ingestion_timestamp', 'commence_time',
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
    'rejection_code', 'time_diff_min', 'competition_tier',
    'odds_players_raw', 'odds_players_normalized',
    'candidate_players_raw', 'candidate_players_normalized',
    'similarity_scores', 'primary_time_delta_min', 'fallback_time_delta_min',
    'rejection_discriminator', 'updated_at',
  ], payload.matchMap);

  upsertSheetRows_(SHEETS.SIGNALS, [
    'key', 'run_id', 'odds_event_id', 'schedule_event_id',
    'market', 'side', 'bookmaker', 'competition_tier', 'model_version',
    'model_probability', 'market_implied_probability', 'edge_value', 'edge_tier', 'stake_units',
    'proposed_stake', 'recommended_stake', 'recommended_stake_currency',
    'min_stake_threshold', 'min_stake_applied', 'stake_policy_mode', 'stake_policy_decision_reason',
    'stake_mode_used', 'raw_risk_mxn', 'raw_target_win_mxn', 'final_risk_mxn', 'final_units',
    'stake_adjustment_reason', 'min_bet_mxn', 'bucket_step_mxn', 'unit_size_mxn',
    'opening_price', 'evaluation_price', 'price_delta_bps', 'opening_lag_minutes', 'decision_gate_status',
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
  const fallbackDiagnostics = payload.opening_lag_fallback_exemption_diagnostics || {};
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
    opening_lag_fallback_exempted: Number(payload.opening_lag_fallback_exempted || 0),
    opening_lag_fallback_exemption_denied_source: Number(payload.opening_lag_fallback_exemption_denied_source || 0),
    opening_lag_fallback_exemption_denied_age: Number(payload.opening_lag_fallback_exemption_denied_age || 0),
    opening_lag_fallback_exemption_denied_cap: Number(payload.opening_lag_fallback_exemption_denied_cap || 0),
    opening_lag_fallback_exemption_key_match_window_minutes: Number(payload.opening_lag_fallback_exemption_key_match_window_minutes || 0),
    opening_lag_fallback_exemption_key_match_max_age_minutes: Number(payload.opening_lag_fallback_exemption_key_match_max_age_minutes || 0),
    opening_lag_fallback_exemption_cap_mode: String(payload.opening_lag_fallback_exemption_cap_mode || 'unlimited_when_zero'),
    opening_lag_fallback_exemption_allowed_sources: Array.isArray(payload.opening_lag_fallback_exemption_allowed_sources)
      ? payload.opening_lag_fallback_exemption_allowed_sources.slice()
      : [],
    opening_lag_fallback_exemption_denied_sources: Array.isArray(payload.opening_lag_fallback_exemption_denied_sources)
      ? payload.opening_lag_fallback_exemption_denied_sources.slice()
      : [],
    opening_lag_fallback_exemption_config_validation: payload.opening_lag_fallback_exemption_config_validation || {},
    opening_lag_fallback_exemption_age_bucket_summary: payload.opening_lag_fallback_exemption_age_bucket_summary || {},
    opening_lag_fallback_exemption_diagnostics: fallbackDiagnostics,
    opening_lag_gate_diagnostics_summary: [
      'blocked_by_age=' + Number(fallbackDiagnostics.blocked_by_age || 0),
      'blocked_by_cap=' + Number(fallbackDiagnostics.blocked_by_cap || 0),
      'blocked_by_source=' + Number(fallbackDiagnostics.blocked_by_source || 0),
      'exempted=' + Number(fallbackDiagnostics.exempted || 0),
    ].join(';'),
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
  const summaryReasonCodes = cloneReasonCodeMap_((summary && summary.reason_codes) || {});
  appendLogRow_({
    row_type: 'stage',
    run_id: runId,
    stage: summary.stage,
    started_at: summary.started_at,
    ended_at: summary.ended_at,
    status: 'success',
    reason_code: 'stage_completed',
    message: JSON.stringify({
      reason_code_alias_schema_id: REASON_CODE_ALIAS_SCHEMA_ID,
      input_count: summary.input_count,
      output_count: summary.output_count,
      provider: summary.provider,
      reason_codes: cloneReasonCodeMap_(summaryReasonCodes),
    }),
    reason_codes: cloneReasonCodeMap_(summaryReasonCodes),
  });
}

function buildReasonCodeEnvelopeForLog_(reasonMap, schemaId, options) {
  const resolvedSchemaId = String(schemaId || REASON_CODE_ALIAS_SCHEMA_ID || '').trim();
  const compacted = compactReasonCodeMapForLog_(reasonMap || {}, resolvedSchemaId, options || {});
  const envelope = {
    schema_id: resolvedSchemaId,
    reason_codes: compacted.reason_codes,
  };
  if (Object.keys(compacted.fallback_aliases || {}).length > 0) {
    envelope.fallback_aliases = compacted.fallback_aliases;
  }
  return envelope;
}

function toReasonCodeMapForLog_(value, schemaId) {
  const parsed = parseLogJsonLike_(value, value);
  if (!parsed || typeof parsed !== 'object') return {};
  if (Object.prototype.hasOwnProperty.call(parsed, 'reason_codes')) {
    const envelopeSchemaId = String(parsed.schema_id || schemaId || REASON_CODE_ALIAS_SCHEMA_ID || '').trim();
    return expandReasonCodeMapForLegacy_(parsed.reason_codes || {}, envelopeSchemaId, parsed.fallback_aliases || {});
  }
  return expandReasonCodeMapForLegacy_(parsed, String(schemaId || REASON_CODE_ALIAS_SCHEMA_ID || '').trim());
}

function normalizeLogEntryForAppend_(entry) {
  const normalized = Object.assign({}, entry || {});
  const schemaId = String(REASON_CODE_ALIAS_SCHEMA_ID || '').trim();
  const allowCanonicalPassthrough = toBoolean_(
    normalized.allow_canonical_reason_code_passthrough,
    toBoolean_(DEFAULT_CONFIG.REASON_CODE_ALIAS_ALLOW_CANONICAL_PASSTHROUGH, false)
  );
  return serializeCompactReasonCodesForLogEntry_(normalized, schemaId, {
    allow_canonical_passthrough: allowCanonicalPassthrough,
  });
}

function serializeCompactReasonCodesForLogEntry_(normalizedEntry, schemaId, options) {
  const normalized = Object.assign({}, normalizedEntry || {});
  const compactOptions = Object.assign({}, options || {});
  const messagePayload = parseLogJsonLike_(normalized.message, null);
  const messageObject = messagePayload && typeof messagePayload === 'object' && !Array.isArray(messagePayload)
    ? Object.assign({}, messagePayload)
    : null;

  const rowReasonCodeMap = toReasonCodeMapForLog_(normalized.reason_codes, schemaId);
  const messageReasonCodeMap = messageObject && messageObject.reason_codes
    ? toReasonCodeMapForLog_(messageObject.reason_codes, String(messageObject.schema_id || schemaId))
    : {};
  const inferredReasonCode = String(normalized.reason_code || '').trim();
  const reasonCodeMap = resolveSinglePassReasonCodeMap_(rowReasonCodeMap, messageReasonCodeMap, inferredReasonCode);

  if (messageObject && Object.keys(reasonCodeMap).length > 0) {
    const compactEnvelope = buildReasonCodeEnvelopeForLog_(reasonCodeMap, schemaId, compactOptions);
    messageObject.schema_id = compactEnvelope.schema_id;
    messageObject.reason_codes = compactEnvelope.reason_codes;
    if (compactEnvelope.fallback_aliases) {
      messageObject.fallback_aliases = compactEnvelope.fallback_aliases;
    } else {
      delete messageObject.fallback_aliases;
    }
    normalized.message = JSON.stringify(messageObject);
  }

  const rejectionReasonCodes = toReasonCodeMapForLog_(normalized.rejection_codes, schemaId);
  normalized.rejection_codes = JSON.stringify(buildReasonCodeEnvelopeForLog_(rejectionReasonCodes, schemaId, compactOptions));

  const stageSummaryPayload = parseLogJsonLike_(normalized.stage_summaries, normalized.stage_summaries);
  if (Array.isArray(stageSummaryPayload)) {
    normalized.stage_summaries = JSON.stringify(compactStageSummariesForLog_(stageSummaryPayload, schemaId, compactOptions));
  } else if (stageSummaryPayload && typeof stageSummaryPayload === 'object' && stageSummaryPayload.stage_summaries) {
    normalized.stage_summaries = JSON.stringify(compactStageSummariesForLog_(stageSummaryPayload.stage_summaries || [], String(stageSummaryPayload.schema_id || schemaId), compactOptions, stageSummaryPayload));
  }

  const fallbackWarningAliases = {};
  const rejectionEnvelopeForWarning = parseLogJsonLike_(normalized.rejection_codes, null);
  if (rejectionEnvelopeForWarning && rejectionEnvelopeForWarning.fallback_aliases) {
    Object.assign(fallbackWarningAliases, rejectionEnvelopeForWarning.fallback_aliases || {});
  }
  if (messageObject && messageObject.fallback_aliases) {
    Object.assign(fallbackWarningAliases, messageObject.fallback_aliases || {});
  }
  const compactStageSummaryEnvelopeForWarning = parseLogJsonLike_(normalized.stage_summaries, null);
  const compactStageSummariesForWarning = compactStageSummaryEnvelopeForWarning
    && typeof compactStageSummaryEnvelopeForWarning === 'object'
    && Array.isArray(compactStageSummaryEnvelopeForWarning.stage_summaries)
      ? compactStageSummaryEnvelopeForWarning.stage_summaries
      : [];
  compactStageSummariesForWarning.forEach((summary) => {
    if (!summary || !summary.fallback_aliases) return;
    Object.assign(fallbackWarningAliases, summary.fallback_aliases || {});
  });
  if (Object.keys(fallbackWarningAliases).length > 0) {
    normalized.__reason_alias_fallback_warning = {
      schema_id: schemaId,
      aliases: Object.keys(fallbackWarningAliases).sort(),
      canonical_reasons: fallbackWarningAliases,
      allow_canonical_passthrough: !!compactOptions.allow_canonical_passthrough,
    };
  }

  return normalized;
}

function resolveSinglePassReasonCodeMap_(rowReasonCodeMap, messageReasonCodeMap, inferredReasonCode) {
  const rowMap = cloneReasonCodeMap_(rowReasonCodeMap || {});
  if (Object.keys(rowMap).length > 0) return rowMap;

  const messageMap = cloneReasonCodeMap_(messageReasonCodeMap || {});
  if (Object.keys(messageMap).length > 0) return messageMap;

  const inferred = String(inferredReasonCode || '').trim();
  if (!inferred) return {};
  const inferredMap = {};
  inferredMap[inferred] = 1;
  return inferredMap;
}

function appendLogRow_(entry) {
  const normalized = normalizeLogEntryForAppend_(entry || {});
  validateRunSummaryQualityContract_(normalized);
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sh = ss.getSheetByName(SHEETS.RUN_LOG);

  if (!sh) {
    sh = ensureSheet_(ss, SHEETS.RUN_LOG);
    ensureHeaders_(SHEETS.RUN_LOG, RUN_LOG_HEADERS);
  }

  sh.appendRow([
    normalized.row_type || 'summary',
    normalized.run_id || '',
    normalized.stage || '',
    toIso_(normalized.started_at),
    toIso_(normalized.ended_at),
    normalized.status || '',
    normalized.reason_code || '',
    sanitizeForLog_(normalized.message || ''),
    normalized.fetched_odds || 0,
    normalized.fetched_schedule || 0,
    normalized.allowed_tournaments || 0,
    normalized.matched || 0,
    normalized.unmatched || 0,
    normalized.signals_found || 0,
    normalized.stake_mode_used || '',
    normalized.raw_risk_mxn === null || normalized.raw_risk_mxn === undefined ? '' : normalized.raw_risk_mxn,
    normalized.raw_target_win_mxn === null || normalized.raw_target_win_mxn === undefined ? '' : normalized.raw_target_win_mxn,
    normalized.final_risk_mxn === null || normalized.final_risk_mxn === undefined ? '' : normalized.final_risk_mxn,
    normalized.final_units === null || normalized.final_units === undefined ? '' : normalized.final_units,
    normalized.stake_adjustment_reason || '',
    normalized.min_bet_mxn === null || normalized.min_bet_mxn === undefined ? '' : normalized.min_bet_mxn,
    normalized.bucket_step_mxn === null || normalized.bucket_step_mxn === undefined ? '' : normalized.bucket_step_mxn,
    normalized.unit_size_mxn === null || normalized.unit_size_mxn === undefined ? '' : normalized.unit_size_mxn,
    sanitizeForLog_(normalized.rejection_codes || '{}'),
    normalized.cooldown_suppressed || 0,
    normalized.duplicate_suppressed || 0,
    normalized.lock_event || '',
    normalized.debounce_event || '',
    normalized.trigger_event || '',
    sanitizeForLog_(normalized.exception || ''),
    sanitizeForLog_(normalized.stack || ''),
    sanitizeForLog_(normalized.stage_summaries || '[]'),
  ]);

  maybeEmitReasonAliasFallbackWarningOpsLog_(normalized);
  maybeEmitWatchdogWarningAggregateOpsLog_(normalized);
}

function validateRunSummaryQualityContract_(entry) {
  const rowType = String(entry && entry.row_type || '');
  const stage = String(entry && entry.stage || '');
  if (rowType !== 'summary' || stage !== 'runEdgeBoard') return;

  const signalSummary = parseLogJsonLike_(entry.signal_decision_summary, {});
  const qualityContract = signalSummary && typeof signalSummary === 'object' && signalSummary.quality_contract
    ? signalSummary.quality_contract
    : null;
  if (!qualityContract || typeof qualityContract !== 'object') {
    throw new Error('run_summary_quality_contract_missing: signal_decision_summary.quality_contract is required');
  }

  const featureCompleteness = Number(qualityContract.feature_completeness);
  if (!Number.isFinite(featureCompleteness) || featureCompleteness < 0 || featureCompleteness > 1) {
    throw new Error('run_summary_quality_contract_invalid_feature_completeness');
  }
  const featureReasonCode = String(qualityContract.feature_completeness_reason_code || '').trim();
  if (!featureReasonCode) {
    throw new Error('run_summary_quality_contract_missing_feature_reason_code');
  }

  const edgeVolatility = Number(qualityContract.edge_volatility);
  if (!Number.isFinite(edgeVolatility) || edgeVolatility < 0) {
    throw new Error('run_summary_quality_contract_invalid_edge_volatility');
  }
  const edgeReasonCode = String(qualityContract.edge_volatility_reason_code || '').trim();
  if (!edgeReasonCode) {
    throw new Error('run_summary_quality_contract_missing_edge_reason_code');
  }
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



function maybeEmitRunRollup_(config, payload) {
  const cfg = config || getConfig_();
  const cadence = Math.max(1, Number(cfg.ROLLUP_EVERY_N_RUNS || 10));
  const latencyEvaluation = resolveLatencyEvaluationContext_((payload && payload.run_health_reason_code) || '', (payload && payload.run_mode) || '');
  const prior = getStateJson_('RUN_ROLLUP_STATE') || {};
  const runCount = Number(prior.run_count || 0) + 1;
  const nextState = {
    run_count: runCount,
    last_rollup_at_count: Number(prior.last_rollup_at_count || 0),
    last_rollup_snapshot: prior.last_rollup_snapshot || null,
  };

  if (runCount % cadence !== 0) {
    setStateValue_('RUN_ROLLUP_STATE', JSON.stringify(nextState));
    return {
      emitted: false,
      run_count: runCount,
      cadence: cadence,
      runs_until_next: cadence - (runCount % cadence),
    };
  }

  const stageDurations = computeStageDurationRollup_((payload && payload.stage_summaries) || []);
  const stageLatencyContract = buildStageLatencyContract_(stageDurations, latencyEvaluation);
  const reasonCodes = compactReasonCodeMapForLog_((payload && payload.reason_codes) || {}, REASON_CODE_ALIAS_SCHEMA_ID).reason_codes;
  const topReasonCodesRaw = getTopReasonCodes_(reasonCodes, 5).filter(function (entry) {
    return Number(entry && entry.count || 0) > 0;
  });
  const topReasonCodeNormalization = normalizeReasonCodeEntriesForDisplay_(topReasonCodesRaw, REASON_CODE_ALIAS_SCHEMA_ID);
  const topReasonCodes = topReasonCodeNormalization.entries;

  const currentSnapshot = {
    fetched_odds: Number(payload && payload.fetched_odds || 0),
    fetched_schedule: Number(payload && payload.fetched_schedule || 0),
    matched: Number(payload && payload.matched || 0),
    unmatched: Number(payload && payload.unmatched || 0),
    signals_found: Number(payload && payload.signals_found || 0),
    run_health_reason_code: String(payload && payload.run_health_reason_code || ''),
    watchdog_bootstrap_empty_cycles: Number(payload && payload.watchdog && payload.watchdog.bootstrap_empty_cycles || 0),
    watchdog_productive_empty_cycles: Number(payload && payload.watchdog && payload.watchdog.productive_empty_cycles || 0),
    watchdog_schedule_only_cycles: Number(payload && payload.watchdog && payload.watchdog.schedule_only_cycles || 0),
  };

  const previousSnapshot = (prior && prior.last_rollup_snapshot) || null;
  const delta = computeRollupDelta_(currentSnapshot, previousSnapshot);
  const rollup = {
    rollup_schema: 'run_rollup_v2',
    run_count: runCount,
    rollup_every_n_runs: cadence,
    top_reason_codes: topReasonCodes,
    top_reason_codes_raw: topReasonCodesRaw,
    reason_code_display_normalization: topReasonCodeNormalization.metadata,
    stage_duration_ms: stageDurations,
    stage_latency_contract: stageLatencyContract,
    key_deltas_vs_previous_rollup: delta,
    watchdog_progression: {
      bootstrap_empty_cycle: formatWatchdogProgress_(payload && payload.watchdog && payload.watchdog.bootstrap_empty_cycles, payload && payload.watchdog && payload.watchdog.bootstrap_threshold),
      productive_output_empty_cycle: formatWatchdogProgress_(payload && payload.watchdog && payload.watchdog.productive_empty_cycles, payload && payload.watchdog && payload.watchdog.productive_threshold),
      schedule_only_cycle: formatWatchdogProgress_(payload && payload.watchdog && payload.watchdog.schedule_only_cycles, payload && payload.watchdog && payload.watchdog.schedule_only_threshold),
    },
  };

  nextState.last_rollup_at_count = runCount;
  nextState.last_rollup_snapshot = currentSnapshot;
  setStateValue_('RUN_ROLLUP_STATE', JSON.stringify(nextState));
  setStateValue_('LAST_RUN_ROLLUP_JSON', JSON.stringify(rollup, null, 2));
  setStateValue_('LAST_RUN_BASELINE_COMPARISON_JSON', JSON.stringify(buildRunBaselineComparisonArtifact_(rollup), null, 2));

  return {
    emitted: true,
    run_count: runCount,
    cadence: cadence,
    rollup: rollup,
  };
}

function computeRollupDelta_(current, previous) {
  const keys = ['fetched_odds', 'fetched_schedule', 'matched', 'unmatched', 'signals_found'];
  const delta = {};
  if (!previous) {
    keys.forEach(function (key) {
      delta[key] = Number(current && current[key] || 0);
    });
    delta.run_health_reason_code = String(current && current.run_health_reason_code || '');
    return delta;
  }
  keys.forEach(function (key) {
    delta[key] = Number(current && current[key] || 0) - Number(previous && previous[key] || 0);
  });
  const previousReason = String(previous && previous.run_health_reason_code || '');
  const currentReason = String(current && current.run_health_reason_code || '');
  delta.run_health_reason_code = previousReason === currentReason
    ? currentReason
    : (previousReason || '(none)') + '→' + (currentReason || '(none)');
  return delta;
}


function resolveRunHealthMode_(runHealthReasonCode) {
  const reason = String(runHealthReasonCode || '').trim();
  if (!reason) return 'healthy';
  const healthyReasonFamilies = {
    run_health_expected_temporary_no_odds: true,
    run_health_opening_lag_schedule_seed_no_odds: true,
  };
  return healthyReasonFamilies[reason] ? 'healthy' : 'degraded';
}

function resolveLatencyEvaluationContext_(runHealthReasonCode, runMode) {
  const reason = String(runHealthReasonCode || '').trim();
  const runHealthMode = resolveRunHealthMode_(reason);
  const normalizedRunMode = String(runMode || '').trim().toLowerCase() === 'outside_active_window'
    ? 'outside_active_window'
    : 'active_window';
  if (normalizedRunMode === 'outside_active_window' || reason === 'odds_refresh_skipped_outside_window') {
    return {
      contract_mode: runHealthMode,
      evaluation_mode: 'idle',
      anomaly_severity: 'informational',
      latency_expectation_profile: 'idle',
      run_mode_tag: 'outside_active_window',
      anomaly_context_tag: 'idle_context_informational',
    };
  }

  return {
    contract_mode: runHealthMode,
    evaluation_mode: 'active',
    anomaly_severity: runHealthMode === 'degraded' ? 'warning' : 'high',
    latency_expectation_profile: runHealthMode,
    run_mode_tag: normalizedRunMode,
    anomaly_context_tag: 'active_context_operational',
  };
}

function getStageLatencyExpectationsForMode_(mode) {
  const normalized = String(mode || 'healthy').toLowerCase();
  if (normalized === 'idle') return STAGE_LATENCY_EXPECTATIONS_MS.idle || {};
  if (normalized === 'degraded') return STAGE_LATENCY_EXPECTATIONS_MS.degraded || {};
  return STAGE_LATENCY_EXPECTATIONS_MS.healthy || {};
}

function shouldUseIdleRelaxedThresholdsForStage_(evaluationMode, stage) {
  const idleMode = String(evaluationMode || '').toLowerCase() === 'idle';
  if (!idleMode) return false;
  const stageName = String(stage || '').trim();
  return stageName === 'stageFetchSchedule' || stageName === 'stageGenerateSignals';
}

function resolveStageLatencyThreshold_(expectations, stage, evaluationMode) {
  const stageExpectations = expectations || {};
  const baseThreshold = stageExpectations[stage] || { min: 0, max: 0 };
  const threshold = {
    min: Number(baseThreshold.min || 0),
    max: Number(baseThreshold.max || 0),
  };
  if (shouldUseIdleRelaxedThresholdsForStage_(evaluationMode, stage)) {
    threshold.max = Math.round(threshold.max * 1.35);
  }
  return threshold;
}

function getLatencyAnomalyReasonCode_(stage, metric, contractMode, evaluationMode) {
  const stageSlug = String(stage || 'unknown_stage').replace(/[^A-Za-z0-9]+/g, '_').replace(/^_+|_+$/g, '').toLowerCase() || 'unknown_stage';
  const metricSlug = String(metric || 'duration').replace(/[^A-Za-z0-9]+/g, '_').replace(/^_+|_+$/g, '').toLowerCase() || 'duration';
  const modeSlug = String(contractMode || 'healthy').toLowerCase() === 'degraded' ? 'degraded' : 'healthy';
  const evaluationSlug = String(evaluationMode || 'active').toLowerCase() === 'idle' ? 'idle' : 'active';
  return 'latency_' + evaluationSlug + '_' + modeSlug + '_' + stageSlug + '_' + metricSlug + '_threshold_exceeded';
}

function buildStageLatencyContract_(stageDurations, evaluationContext) {
  const context = evaluationContext || {};
  const selectedMode = String(context.contract_mode || 'healthy').toLowerCase() === 'degraded' ? 'degraded' : 'healthy';
  const selectedEvaluationMode = String(context.evaluation_mode || 'active').toLowerCase() === 'idle' ? 'idle' : 'active';
  const anomalySeverity = String(context.anomaly_severity || '').trim() || (selectedEvaluationMode === 'idle' ? 'informational' : 'high');
  const expectationProfile = String(context.latency_expectation_profile || '').toLowerCase() || (selectedEvaluationMode === 'idle' ? 'idle' : selectedMode);
  const runModeTag = String(context.run_mode_tag || '').trim() || (selectedEvaluationMode === 'idle' ? 'outside_active_window' : 'active_window');
  const anomalyContextTag = String(context.anomaly_context_tag || '').trim() || (selectedEvaluationMode === 'idle' ? 'idle_context_informational' : 'active_context_operational');
  const expectations = getStageLatencyExpectationsForMode_(expectationProfile);
  const stageNames = Object.keys(expectations).concat(Object.keys(stageDurations || {})).filter(function (name, idx, arr) {
    return arr.indexOf(name) === idx;
  });

  const out = {
    schema: 'stage_latency_contract_v1',
    mode: selectedMode,
    evaluation_mode: selectedEvaluationMode,
    mode_context: selectedEvaluationMode,
    run_mode_tag: runModeTag,
    anomaly_context_tag: anomalyContextTag,
    anomaly_severity: anomalySeverity,
    latency_expectation_profile: expectationProfile,
    thresholds_ms: {},
    anomalies: {},
    anomaly_reason_codes: [],
  };

  const anomalyCodes = [];
  stageNames.forEach(function (stage) {
    const threshold = resolveStageLatencyThreshold_(expectations, stage, selectedEvaluationMode);
    const observed = (stageDurations && stageDurations[stage]) || null;
    out.thresholds_ms[stage] = {
      min: Number(threshold.min || 0),
      max: Number(threshold.max || 0),
    };
    if (!observed) {
      return;
    }

    const metrics = [
      { name: 'avg', value: Number(observed.avg || 0), compare: 'max' },
      { name: 'p95', value: Number(observed.p95 || 0), compare: 'max' },
    ];
    const stageAnomalies = [];
    metrics.forEach(function (metric) {
      if (!Number.isFinite(metric.value)) return;
      if (metric.compare === 'max' && metric.value > Number(threshold.max || 0)) {
        const reasonCode = getLatencyAnomalyReasonCode_(stage, metric.name, selectedMode, selectedEvaluationMode);
        stageAnomalies.push({
          metric: metric.name,
          observed_ms: metric.value,
          threshold_ms: Number(threshold.max || 0),
          severity: anomalySeverity,
          evaluation_mode: selectedEvaluationMode,
          mode_context: selectedEvaluationMode,
          run_mode_tag: runModeTag,
          anomaly_context_tag: anomalyContextTag,
          reason_code: reasonCode,
        });
        anomalyCodes.push(reasonCode);
      }
      if (metric.compare === 'min' && metric.value < Number(threshold.min || 0)) {
        const reasonCode = getLatencyAnomalyReasonCode_(stage, metric.name, selectedMode, selectedEvaluationMode);
        stageAnomalies.push({
          metric: metric.name,
          observed_ms: metric.value,
          threshold_ms: Number(threshold.min || 0),
          severity: anomalySeverity,
          evaluation_mode: selectedEvaluationMode,
          mode_context: selectedEvaluationMode,
          run_mode_tag: runModeTag,
          anomaly_context_tag: anomalyContextTag,
          reason_code: reasonCode,
        });
        anomalyCodes.push(reasonCode);
      }
    });

    if (stageAnomalies.length) {
      out.anomalies[stage] = {
        observed_ms: {
          min: Number(observed.min || 0),
          avg: Number(observed.avg || 0),
          p95: Number(observed.p95 || 0),
        },
        threshold_ms: {
          min: Number(threshold.min || 0),
          max: Number(threshold.max || 0),
        },
        reason_codes: stageAnomalies,
      };
    }
  });

  out.anomaly_reason_codes = anomalyCodes;
  return out;
}

function buildRunBaselineComparisonArtifact_(rollup) {
  const current = rollup || {};
  return {
    baseline_comparison_schema: 'run_baseline_comparison_v1',
    generated_at_utc: new Date().toISOString(),
    rollup_run_count: Number(current.run_count || 0),
    run_health_mode: (current.stage_latency_contract && current.stage_latency_contract.mode) || 'healthy',
    top_reason_codes: (current.top_reason_codes || []).slice(0, 5),
    top_reason_codes_raw: (current.top_reason_codes_raw || []).slice(0, 5),
    reason_code_display_normalization: current.reason_code_display_normalization || {
      normalization_applied: false,
      mapped_legacy_aliases: {},
      mapped_legacy_alias_count: 0,
    },
    stage_duration_ms: current.stage_duration_ms || {},
    stage_latency_contract: current.stage_latency_contract || {},
    key_deltas_vs_previous_rollup: current.key_deltas_vs_previous_rollup || {},
  };
}

function computeStageDurationRollup_(summaries) {
  const byStage = {};
  (summaries || []).forEach(function (summary) {
    if (!summary || !summary.stage) return;
    const duration = Number(summary.duration_ms || 0);
    if (!Number.isFinite(duration) || duration < 0) return;
    if (!byStage[summary.stage]) byStage[summary.stage] = [];
    byStage[summary.stage].push(duration);
  });

  const out = {};
  Object.keys(byStage).forEach(function (stage) {
    const values = byStage[stage].slice().sort(function (a, b) { return a - b; });
    const sum = values.reduce(function (acc, value) { return acc + value; }, 0);
    out[stage] = {
      min: values.length ? values[0] : 0,
      avg: values.length ? roundNumber_(sum / values.length, 2) : 0,
      p95: values.length ? percentileFromSorted_(values, 0.95) : 0,
    };
  });

  return out;
}

function percentileFromSorted_(values, p) {
  if (!values || !values.length) return 0;
  const rank = Math.ceil(Math.max(0, Math.min(1, Number(p || 0))) * values.length) - 1;
  const index = Math.max(0, Math.min(values.length - 1, rank));
  return Number(values[index] || 0);
}

function formatWatchdogProgress_(countValue, thresholdValue) {
  const count = Math.max(0, Number(countValue || 0));
  const threshold = Math.max(1, Number(thresholdValue || 1));
  return {
    count: count,
    threshold: threshold,
    status: String(count) + '/' + String(threshold),
  };
}

function maybeNotifyCreditBurnRate_(config, runId, burnRateSummary) {
  const summary = burnRateSummary || {};
  if (!summary.warning_lt_7d) {
    return {
      notify_attempted: false,
      outcome: 'credit_burn_warning_not_triggered',
      day_key_utc: String(summary.observed_at_utc || '').slice(0, 10),
    };
  }

  if (!config || !config.ODDS_BURN_RATE_NOTIFY_ENABLED) {
    return {
      notify_attempted: false,
      outcome: 'credit_burn_notify_disabled',
      day_key_utc: String(summary.observed_at_utc || '').slice(0, 10),
    };
  }

  const dayKeyUtc = String(summary.observed_at_utc || new Date().toISOString()).slice(0, 10);
  const notifyState = getStateJson_('ODDS_API_BURN_RATE_NOTIFY_STATE') || {};
  const previousDayKey = String(notifyState.last_notify_day_utc || '');
  if (previousDayKey && previousDayKey === dayKeyUtc) {
    return {
      notify_attempted: false,
      outcome: 'credit_burn_notify_already_sent_today',
      day_key_utc: dayKeyUtc,
    };
  }

  if (!config.NOTIFY_ENABLED || !config.DISCORD_WEBHOOK) {
    return {
      notify_attempted: false,
      outcome: !config.NOTIFY_ENABLED ? 'credit_burn_notify_disabled' : 'credit_burn_notify_missing_config',
      day_key_utc: dayKeyUtc,
    };
  }

  const projectedDays = Number(summary.projected_days_remaining);
  const rollingCallsPerDay = Number(summary.calls_per_day_rolling);
  const creditsRemaining = Number(summary.credits_remaining);
  const message = [
    '⚠️ **WTA Edge Odds API Burn Rate Warning**',
    '🆔 Run: `' + String(runId || '') + '`',
    '📉 Projected credits exhaustion: **' + (Number.isFinite(projectedDays) ? roundNumber_(projectedDays, 2) + ' days' : 'unknown') + '**',
    '🔥 Rolling burn rate: **' + (Number.isFinite(rollingCallsPerDay) ? roundNumber_(rollingCallsPerDay, 2) + ' calls/day' : 'unknown') + '**',
    '💳 Credits remaining: **' + (Number.isFinite(creditsRemaining) ? roundNumber_(creditsRemaining, 0) : 'unknown') + '**',
  ].join('\n');
  const notifyResult = postDiscordWebhook_(
    config.DISCORD_WEBHOOK,
    { content: message },
    !!config.NOTIFY_TEST_MODE,
    Number(config.NOTIFY_WEBHOOK_MAX_RETRIES || 0)
  );
  const outcome = String(notifyResult && notifyResult.outcome || 'notify_http_failed');
  if (outcome === 'sent') {
    setStateValue_('ODDS_API_BURN_RATE_NOTIFY_STATE', JSON.stringify({
      run_id: runId,
      last_notify_day_utc: dayKeyUtc,
      updated_at_utc: new Date().toISOString(),
    }));
  }

  return {
    notify_attempted: true,
    outcome: outcome,
    day_key_utc: dayKeyUtc,
    transport: notifyResult.transport,
    http_status: notifyResult.http_status,
    test_mode: !!notifyResult.test_mode,
  };
}

function getLogVerbosityLevel_(config) {
  const runtimeConfig = config || {};
  const logProfile = normalizeLogProfile_(runtimeConfig.LOG_PROFILE || DEFAULT_CONFIG.LOG_PROFILE);
  if (logProfile === 'compact') {
    const compactLevel = Number.isFinite(Number(runtimeConfig.LOG_VERBOSITY_LEVEL))
      ? Number(runtimeConfig.LOG_VERBOSITY_LEVEL)
      : 1;
    return Math.max(0, Math.min(1, compactLevel));
  }

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
    reason_codes: cloneReasonCodeMap_(opts.reason_codes || {}),
    reason_metadata: Object.assign({}, opts.reason_metadata || {}),
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
  if (!sh) return null;
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
    const safeMap = cloneReasonCodeMap_(map);
    Object.keys(safeMap).forEach((k) => {
      const value = Number(safeMap[k]);
      if (!Number.isFinite(value)) return;
      merged[k] = Number(merged[k] || 0) + value;
    });
  });
  return merged;
}

function cloneReasonCodeMap_(reasonMap) {
  const clone = {};
  Object.keys(reasonMap || {}).forEach((reasonCode) => {
    const value = Number((reasonMap || {})[reasonCode]);
    if (!Number.isFinite(value)) return;
    clone[reasonCode] = value;
  });
  return clone;
}

function areReasonCodeMapsEquivalent_(left, right) {
  const leftClone = cloneReasonCodeMap_(left);
  const rightClone = cloneReasonCodeMap_(right);
  const leftKeys = Object.keys(leftClone);
  const rightKeys = Object.keys(rightClone);
  if (leftKeys.length !== rightKeys.length) return false;
  for (let i = 0; i < leftKeys.length; i += 1) {
    const key = leftKeys[i];
    if (!Object.prototype.hasOwnProperty.call(rightClone, key)) return false;
    if (Number(leftClone[key]) !== Number(rightClone[key])) return false;
  }
  return true;
}


function assertDebugBoundedStageCounters_(config, checks) {
  const runtimeConfig = config || {};
  const debugMode = normalizeLogProfile_(runtimeConfig.LOG_PROFILE || DEFAULT_CONFIG.LOG_PROFILE) === 'verbose';
  if (!debugMode) return [];

  const violations = [];
  (checks || []).forEach(function (check) {
    const safeCheck = check || {};
    const stage = String(safeCheck.stage || 'unknown_stage');
    const mode = String(safeCheck.mode || 'odds_present');
    const maxName = String(safeCheck.max_name || 'max');
    const boundSource = String(safeCheck.bound_source || maxName || 'max');
    const max = Math.max(0, Number(safeCheck.max || 0));
    const counters = safeCheck.counters || {};
    Object.keys(counters).forEach(function (counterName) {
      const value = Number(counters[counterName] || 0);
      if (!Number.isFinite(value) || value <= max) return;
      violations.push({
        stage: stage,
        mode: mode,
        counter_name: counterName,
        counter_value: value,
        max_name: maxName,
        bound_source: boundSource,
        max_allowed: max,
      });
    });
  });

  return violations;
}

function getInvariantEnforcementLevel_(config) {
  const safeConfig = config || {};
  if (typeof normalizeInvariantEnforcementLevel_ === 'function') {
    return normalizeInvariantEnforcementLevel_(safeConfig.INVARIANT_ENFORCEMENT_LEVEL);
  }
  const level = String(safeConfig.INVARIANT_ENFORCEMENT_LEVEL || 'warn').toLowerCase();
  return level === 'strict' ? 'strict' : 'warn';
}

function enforceInvariant_(config, options) {
  const safeOptions = options || {};
  const violations = safeOptions.violations || [];
  if (!violations.length) {
    return { hard_failed: false, warning_emitted: false, enforcement_level: getInvariantEnforcementLevel_(config) };
  }

  const hardFail = !!safeOptions.hard_fail;
  const enforcementLevel = getInvariantEnforcementLevel_(config);
  const errorPayload = {
    invariant: String(safeOptions.invariant || 'invariant_violation'),
    context: String(safeOptions.context || ''),
    violations: violations,
    hard_fail: hardFail,
    enforcement_level: enforcementLevel,
  };

  if (hardFail || enforcementLevel === 'strict') {
    throw new Error(String(safeOptions.error_prefix || 'invariant_violation') + ':' + JSON.stringify(errorPayload));
  }

  if (typeof safeOptions.warn_logger === 'function') {
    safeOptions.warn_logger(errorPayload);
  }

  return {
    hard_failed: false,
    warning_emitted: true,
    enforcement_level: enforcementLevel,
    payload: errorPayload,
  };
}

function assertBoundedStageCounterInvariants_(config, checks, contextLabel) {
  const violations = assertDebugBoundedStageCounters_({ LOG_PROFILE: 'verbose' }, checks || []);
  if (violations.length === 0) return [];
  enforceInvariant_(config, {
    invariant: 'bounded_stage_counter_invariant_exceeded',
    context: contextLabel,
    violations: violations,
    hard_fail: false,
    error_prefix: 'bounded_stage_counter_invariant_exceeded',
  });
  return violations;
}


function maybeEmitReasonAliasDictionary_(config) {
  if (REASON_ALIAS_DICTIONARY_EMITTED_FOR_PROCESS) return;
  const dictionary = getReasonCodeAliasDictionary_();
  invertReasonCodeAliasDictionary_(dictionary);
  REASON_ALIAS_DICTIONARY_EMITTED_FOR_PROCESS = true;
  logDiagnosticEvent_(config || null, 'reason_code_alias_dictionary', {
    schema_id: REASON_CODE_ALIAS_SCHEMA_ID,
    aliases: dictionary,
    reverse_aliases: invertReasonCodeAliasDictionary_(dictionary),
  }, 1);
}

function getReasonCodeAliasDictionary_(schemaId) {
  const resolvedSchemaId = String(schemaId || REASON_CODE_ALIAS_SCHEMA_ID || '').trim();
  const dictionaries = REASON_CODE_ALIAS_DICTIONARIES || {};
  const dictionary = dictionaries[resolvedSchemaId] || {};
  return dictionary;
}

function invertReasonCodeAliasDictionary_(aliasDictionary) {
  const inverted = {};
  Object.keys(aliasDictionary || {}).forEach((reasonCode) => {
    const alias = String((aliasDictionary || {})[reasonCode] || '').trim();
    if (!alias) return;
    if (Object.prototype.hasOwnProperty.call(inverted, alias) && inverted[alias] !== reasonCode) {
      throw new Error('reason_alias_dictionary_collision_' + alias);
    }
    inverted[alias] = reasonCode;
  });
  return inverted;
}

function getReasonCodeAliasSchema_() {
  return {
    schema_id: REASON_CODE_ALIAS_SCHEMA_ID,
    aliases: getReasonCodeAliasDictionary_(),
  };
}

function reasonCodeToAlias_(reasonCode, schemaId, options) {
  const text = String(reasonCode || '').trim();
  if (!text) return { alias: '', canonical: '', was_fallback: false, was_passthrough: false };
  const dictionary = getReasonCodeAliasDictionary_(schemaId);
  if (Object.prototype.hasOwnProperty.call(dictionary, text)) {
    return { alias: String(dictionary[text]), canonical: text, was_fallback: false, was_passthrough: false };
  }
  const allowPassthrough = !!((options || {}).allow_canonical_passthrough);
  if (allowPassthrough) {
    return { alias: text, canonical: text, was_fallback: false, was_passthrough: true };
  }
  const fallbackAlias = buildReasonCodeFallbackAlias_(text);
  return { alias: fallbackAlias, canonical: text, was_fallback: true, was_passthrough: false };
}


function buildReasonCodeFallbackAlias_(reasonCode) {
  const hash = Number(stringHashCode_(String(reasonCode || '')) || 0);
  return 'UNK_' + hash.toString(36).toUpperCase();
}

function maybeEmitReasonAliasFallbackWarningOpsLog_(entry) {
  const rowType = String(entry && entry.row_type || '');
  const stage = String(entry && entry.stage || '');
  const runId = String(entry && entry.run_id || '');
  const isRunSummaryRow = rowType === 'summary' || stage === 'runEdgeBoard';
  const warning = entry && entry.__reason_alias_fallback_warning;

  if (warning && warning.aliases && warning.aliases.length) {
    const schemaId = String(warning.schema_id || REASON_CODE_ALIAS_SCHEMA_ID || '');
    const key = runId + '::' + schemaId;
    const pending = REASON_ALIAS_FALLBACK_WARNING_PENDING[key] || {
      schema_id: schemaId,
      fallback_aliases: {},
      allow_canonical_passthrough: false,
    };
    (warning.aliases || []).forEach((alias) => {
      const aliasText = String(alias || '').trim();
      if (!aliasText) return;
      pending.fallback_aliases[aliasText] = String((warning.canonical_reasons || {})[aliasText] || pending.fallback_aliases[aliasText] || '');
    });
    pending.allow_canonical_passthrough = pending.allow_canonical_passthrough || !!warning.allow_canonical_passthrough;
    REASON_ALIAS_FALLBACK_WARNING_PENDING[key] = pending;
  }

  if (!isRunSummaryRow) return;
  if (REASON_ALIAS_FALLBACK_WARNING_EMITTED[runId]) {
    maybeEmitReasonAliasFallbackAggregateSummary_(entry);
    return;
  }
  REASON_ALIAS_FALLBACK_WARNING_EMITTED[runId] = true;

  const aggregateState = getReasonAliasFallbackWarningAggregateState_();
  aggregateState.summary_run_count = Number(aggregateState.summary_run_count || 0) + 1;

  const summaryKeys = Object.keys(REASON_ALIAS_FALLBACK_WARNING_PENDING).filter((pendingKey) => {
    return pendingKey.indexOf(runId + '::') === 0;
  });
  const now = new Date();

  summaryKeys.forEach((pendingKey) => {
    const mergedWarning = REASON_ALIAS_FALLBACK_WARNING_PENDING[pendingKey] || {};
    delete REASON_ALIAS_FALLBACK_WARNING_PENDING[pendingKey];
    const fallbackAliases = Object.keys((mergedWarning && mergedWarning.fallback_aliases) || {}).filter((alias) => {
      return String(alias || '').indexOf('UNK_') === 0;
    }).sort();
    if (!fallbackAliases.length) return;
    const canonicalReasons = {};
    fallbackAliases.forEach((alias) => {
      canonicalReasons[alias] = String((mergedWarning.fallback_aliases || {})[alias] || '');
    });
    const aggregateEntry = registerReasonAliasFallbackWarningSet_(aggregateState, {
      schema_id: String(mergedWarning.schema_id || REASON_CODE_ALIAS_SCHEMA_ID || ''),
      fallback_aliases: fallbackAliases,
      canonical_reasons: canonicalReasons,
      allow_canonical_passthrough: !!mergedWarning.allow_canonical_passthrough,
      now: now,
    });
    if (aggregateEntry.emit_detailed_now) {
      appendReasonAliasFallbackWarningOpsRow_(entry, 'reason_code_alias_missing_fallback_emitted', {
        reason_code_alias_schema_id: aggregateEntry.schema_id,
        fallback_aliases: aggregateEntry.fallback_aliases,
        canonical_reasons: aggregateEntry.canonical_reasons,
        mode: aggregateEntry.mode,
        repeat_count: Number(aggregateEntry.repeat_count || 0),
        first_seen: aggregateEntry.first_seen,
        last_seen: aggregateEntry.last_seen,
      }, now);
    }
  });

  maybeEmitReasonAliasFallbackAggregateSummary_(entry, aggregateState);
  persistReasonAliasFallbackWarningAggregateState_(aggregateState);
}

function getReasonAliasFallbackWarningAggregateState_() {
  if (REASON_ALIAS_FALLBACK_WARNING_AGGREGATE_CACHE) return REASON_ALIAS_FALLBACK_WARNING_AGGREGATE_CACHE;
  const stored = getStateJson_(REASON_ALIAS_FALLBACK_WARNING_AGGREGATE_STATE_KEY) || {};
  const state = {
    schema: REASON_ALIAS_FALLBACK_WARNING_AGGREGATE_SCHEMA,
    summary_run_count: Number(stored.summary_run_count || 0),
    last_summary_emit_run_count: Number(stored.last_summary_emit_run_count || 0),
    sets: {},
  };
  const storedSets = stored.sets && typeof stored.sets === 'object' ? stored.sets : {};
  Object.keys(storedSets).forEach((setKey) => {
    const setEntry = storedSets[setKey] || {};
    state.sets[setKey] = {
      schema_id: String(setEntry.schema_id || ''),
      fallback_aliases: Array.isArray(setEntry.fallback_aliases) ? setEntry.fallback_aliases.slice().sort() : [],
      canonical_reasons: setEntry.canonical_reasons || {},
      mode: String(setEntry.mode || 'hash_fallback'),
      count: Number(setEntry.count || 0),
      first_seen: String(setEntry.first_seen || ''),
      last_seen: String(setEntry.last_seen || ''),
      last_detailed_emitted_at: String(setEntry.last_detailed_emitted_at || ''),
      last_detailed_emitted_at_ms: Number(setEntry.last_detailed_emitted_at_ms || 0),
      repeat_count_since_summary: Number(setEntry.repeat_count_since_summary || 0),
      repeat_count_since_detailed: Number(setEntry.repeat_count_since_detailed || 0),
    };
  });
  REASON_ALIAS_FALLBACK_WARNING_AGGREGATE_CACHE = state;
  return state;
}

function persistReasonAliasFallbackWarningAggregateState_(state) {
  const safeState = state || getReasonAliasFallbackWarningAggregateState_();
  REASON_ALIAS_FALLBACK_WARNING_AGGREGATE_CACHE = safeState;
  setStateValue_(REASON_ALIAS_FALLBACK_WARNING_AGGREGATE_STATE_KEY, JSON.stringify(safeState));
}

function buildReasonAliasFallbackAggregateSetKey_(schemaId, aliases) {
  return String(schemaId || '') + '::' + (aliases || []).join('|');
}

function registerReasonAliasFallbackWarningSet_(state, payload) {
  const safeState = state || getReasonAliasFallbackWarningAggregateState_();
  const schemaId = String(payload && payload.schema_id || REASON_CODE_ALIAS_SCHEMA_ID || '');
  const fallbackAliases = Array.isArray(payload && payload.fallback_aliases) ? payload.fallback_aliases.slice().sort() : [];
  const canonicalReasons = (payload && payload.canonical_reasons) || {};
  const mode = payload && payload.allow_canonical_passthrough ? 'canonical_passthrough' : 'hash_fallback';
  const now = (payload && payload.now) || new Date();
  const nowIso = formatLocalIso_(now);
  const setKey = buildReasonAliasFallbackAggregateSetKey_(schemaId, fallbackAliases);
  const existing = safeState.sets[setKey] || {
    schema_id: schemaId,
    fallback_aliases: fallbackAliases,
    canonical_reasons: {},
    mode: mode,
    count: 0,
    first_seen: nowIso,
    last_seen: nowIso,
    last_detailed_emitted_at: '',
    last_detailed_emitted_at_ms: 0,
    repeat_count_since_summary: 0,
    repeat_count_since_detailed: 0,
  };

  existing.count = Number(existing.count || 0) + 1;
  existing.last_seen = nowIso;
  existing.schema_id = schemaId;
  existing.fallback_aliases = fallbackAliases;
  existing.mode = mode;
  fallbackAliases.forEach((alias) => {
    existing.canonical_reasons[alias] = String(canonicalReasons[alias] || existing.canonical_reasons[alias] || '');
  });

  const isFirstObservation = Number(existing.count || 0) === 1;
  const emitDetailedNow = isFirstObservation;

  if (emitDetailedNow) {
    existing.last_detailed_emitted_at = nowIso;
    existing.last_detailed_emitted_at_ms = now.getTime();
    existing.repeat_count_since_detailed = 0;
  } else {
    existing.repeat_count_since_summary = Number(existing.repeat_count_since_summary || 0) + 1;
    existing.repeat_count_since_detailed = Number(existing.repeat_count_since_detailed || 0) + 1;
  }

  safeState.sets[setKey] = existing;
  return {
    emit_detailed_now: emitDetailedNow,
    schema_id: existing.schema_id,
    fallback_aliases: existing.fallback_aliases,
    canonical_reasons: existing.canonical_reasons,
    mode: existing.mode,
    repeat_count: Number(existing.repeat_count_since_detailed || 0),
    first_seen: existing.first_seen,
    last_seen: existing.last_seen,
  };
}

function maybeEmitReasonAliasFallbackAggregateSummary_(entry, state) {
  const safeState = state || getReasonAliasFallbackWarningAggregateState_();
  const cfg = getConfig_();
  const cadence = Math.max(1, Number(cfg.ROLLUP_EVERY_N_RUNS || 10));
  const runCount = Number(safeState.summary_run_count || 0);
  if (runCount <= 0 || runCount % cadence !== 0) return;

  const setKeys = Object.keys(safeState.sets || {}).sort();
  setKeys.forEach((setKey) => {
    const setEntry = safeState.sets[setKey] || {};
    const repeatCount = Number(setEntry.repeat_count_since_summary || 0);
    if (repeatCount <= 0) return;
    appendReasonAliasFallbackWarningOpsRow_(entry, 'reason_code_alias_missing_fallback_repeat_rollup', {
      reason_code_alias_schema_id: String(setEntry.schema_id || ''),
      fallback_aliases: Array.isArray(setEntry.fallback_aliases) ? setEntry.fallback_aliases.slice() : [],
      canonical_reasons: setEntry.canonical_reasons || {},
      mode: String(setEntry.mode || 'hash_fallback'),
      repeat_count: repeatCount,
      first_seen: String(setEntry.first_seen || ''),
      last_seen: String(setEntry.last_seen || ''),
    });
    setEntry.repeat_count_since_summary = 0;
  });
  safeState.last_summary_emit_run_count = runCount;
}

function appendReasonAliasFallbackWarningOpsRow_(entry, reasonCode, payload, now) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(SHEETS.RUN_LOG) || ensureSheet_(ss, SHEETS.RUN_LOG);
  const nowIso = formatLocalIso_(now || new Date());
  sh.appendRow([
    'ops',
    entry.run_id || '',
    'reason_alias_resolution',
    nowIso,
    nowIso,
    'warning',
    reasonCode,
    sanitizeForLog_(JSON.stringify(payload || {})),
    0,0,0,0,0,0,
    '{}',
    0,0,
    '', '', '', '', '',
    '[]',
  ]);
}


function maybeEmitWatchdogWarningAggregateOpsLog_(entry) {
  const normalized = entry || {};
  const rowType = String(normalized.row_type || '');
  const status = String(normalized.status || '').toLowerCase();
  const stage = String(normalized.stage || '');
  const reasonCode = String(normalized.reason_code || '').trim();
  const isWarningRow = rowType === 'ops' && status === 'warning' && stage.indexOf('watchdog') >= 0;
  const isRunSummaryRow = rowType === 'summary' || stage === 'runEdgeBoard';
  const aggregateState = getWatchdogWarningAggregateState_();

  if (isWarningRow && reasonCode) {
    const payload = parseLogJsonLike_(normalized.message, null);
    const summary = summarizeWatchdogWarningPayload_(payload);
    const aggregateEntry = registerWatchdogWarningAggregate_(aggregateState, {
      stage: stage,
      reason_code: reasonCode,
      payload: summary,
      now: new Date(),
    });
    if (aggregateEntry.emit_detailed_now) {
      appendWatchdogWarningOpsRow_(normalized, reasonCode, {
        warning_aggregate_schema_id: WATCHDOG_WARNING_AGGREGATE_SCHEMA,
        warning_summary_key: aggregateEntry.summary_key,
        repeat_count: Number(aggregateEntry.repeat_count || 0),
        first_seen: aggregateEntry.first_seen,
        last_seen: aggregateEntry.last_seen,
        warning_summary: summary,
      });
    }
  }

  if (isRunSummaryRow) {
    aggregateState.summary_run_count = Number(aggregateState.summary_run_count || 0) + 1;
    maybeEmitWatchdogWarningAggregateSummary_(normalized, aggregateState);
    persistWatchdogWarningAggregateState_(aggregateState);
  }
}

function summarizeWatchdogWarningPayload_(payload) {
  const parsed = payload && typeof payload === 'object' && !Array.isArray(payload)
    ? payload
    : {};
  const summary = {};
  if (Object.prototype.hasOwnProperty.call(parsed, 'consecutive_empty_cycles')) {
    summary.consecutive_empty_cycles = Number(parsed.consecutive_empty_cycles || 0);
  }
  if (Object.prototype.hasOwnProperty.call(parsed, 'consecutive_count')) {
    summary.consecutive_count = Number(parsed.consecutive_count || 0);
  }
  if (Object.prototype.hasOwnProperty.call(parsed, 'schedule_only_consecutive_count')) {
    summary.schedule_only_consecutive_count = Number(parsed.schedule_only_consecutive_count || 0);
  }
  if (Object.prototype.hasOwnProperty.call(parsed, 'threshold')) {
    summary.threshold = Number(parsed.threshold || 0);
  }
  if (Object.prototype.hasOwnProperty.call(parsed, 'schedule_only_threshold')) {
    summary.schedule_only_threshold = Number(parsed.schedule_only_threshold || 0);
  }
  if (Object.prototype.hasOwnProperty.call(parsed, 'reason_code')) {
    summary.reason_code = String(parsed.reason_code || '');
  }
  return summary;
}

function getWatchdogWarningAggregateState_() {
  if (WATCHDOG_WARNING_AGGREGATE_CACHE) return WATCHDOG_WARNING_AGGREGATE_CACHE;
  const stored = getStateJson_(WATCHDOG_WARNING_AGGREGATE_STATE_KEY) || {};
  const state = {
    schema: WATCHDOG_WARNING_AGGREGATE_SCHEMA,
    summary_run_count: Number(stored.summary_run_count || 0),
    last_summary_emit_run_count: Number(stored.last_summary_emit_run_count || 0),
    warnings: {},
  };
  const storedWarnings = stored.warnings && typeof stored.warnings === 'object' ? stored.warnings : {};
  Object.keys(storedWarnings).forEach((warningKey) => {
    const warningEntry = storedWarnings[warningKey] || {};
    state.warnings[warningKey] = {
      stage: String(warningEntry.stage || ''),
      reason_code: String(warningEntry.reason_code || ''),
      summary_key: String(warningEntry.summary_key || ''),
      payload_sample: warningEntry.payload_sample || {},
      count: Number(warningEntry.count || 0),
      first_seen: String(warningEntry.first_seen || ''),
      last_seen: String(warningEntry.last_seen || ''),
      last_detailed_emitted_at: String(warningEntry.last_detailed_emitted_at || ''),
      last_detailed_emitted_at_ms: Number(warningEntry.last_detailed_emitted_at_ms || 0),
      repeat_count_since_summary: Number(warningEntry.repeat_count_since_summary || 0),
      repeat_count_since_detailed: Number(warningEntry.repeat_count_since_detailed || 0),
    };
  });
  WATCHDOG_WARNING_AGGREGATE_CACHE = state;
  return state;
}

function persistWatchdogWarningAggregateState_(state) {
  const safeState = state || getWatchdogWarningAggregateState_();
  WATCHDOG_WARNING_AGGREGATE_CACHE = safeState;
  setStateValue_(WATCHDOG_WARNING_AGGREGATE_STATE_KEY, JSON.stringify(safeState));
}

function buildWatchdogWarningSummaryKey_(payload) {
  const keys = Object.keys(payload || {}).sort();
  if (!keys.length) return '{}';
  const normalized = {};
  keys.forEach((key) => {
    normalized[key] = payload[key];
  });
  return JSON.stringify(normalized);
}

function buildWatchdogWarningAggregateKey_(stage, reasonCode, summaryKey) {
  return [String(stage || ''), String(reasonCode || ''), String(summaryKey || '{}')].join('::');
}

function registerWatchdogWarningAggregate_(state, payload) {
  const safeState = state || getWatchdogWarningAggregateState_();
  const stage = String(payload && payload.stage || '');
  const reasonCode = String(payload && payload.reason_code || '');
  const warningPayload = (payload && payload.payload) || {};
  const now = (payload && payload.now) || new Date();
  const nowIso = formatLocalIso_(now);
  const summaryKey = buildWatchdogWarningSummaryKey_(warningPayload);
  const warningKey = buildWatchdogWarningAggregateKey_(stage, reasonCode, summaryKey);
  const existing = safeState.warnings[warningKey] || {
    stage: stage,
    reason_code: reasonCode,
    summary_key: summaryKey,
    payload_sample: warningPayload,
    count: 0,
    first_seen: nowIso,
    last_seen: nowIso,
    last_detailed_emitted_at: '',
    last_detailed_emitted_at_ms: 0,
    repeat_count_since_summary: 0,
    repeat_count_since_detailed: 0,
  };

  existing.stage = stage;
  existing.reason_code = reasonCode;
  existing.summary_key = summaryKey;
  existing.payload_sample = warningPayload;
  existing.count = Number(existing.count || 0) + 1;
  existing.last_seen = nowIso;

  const isFirstObservation = Number(existing.count || 0) === 1;
  const emitDetailedNow = isFirstObservation;

  if (emitDetailedNow) {
    existing.last_detailed_emitted_at = nowIso;
    existing.last_detailed_emitted_at_ms = now.getTime();
    existing.repeat_count_since_detailed = 0;
  } else {
    existing.repeat_count_since_summary = Number(existing.repeat_count_since_summary || 0) + 1;
    existing.repeat_count_since_detailed = Number(existing.repeat_count_since_detailed || 0) + 1;
  }

  safeState.warnings[warningKey] = existing;
  return {
    emit_detailed_now: emitDetailedNow,
    summary_key: existing.summary_key,
    repeat_count: Number(existing.repeat_count_since_detailed || 0),
    first_seen: String(existing.first_seen || ''),
    last_seen: String(existing.last_seen || ''),
  };
}

function maybeEmitWatchdogWarningAggregateSummary_(entry, state) {
  const safeState = state || getWatchdogWarningAggregateState_();
  const cfg = getConfig_();
  const cadence = Math.max(1, Number(cfg.ROLLUP_EVERY_N_RUNS || 10));
  const runCount = Number(safeState.summary_run_count || 0);
  if (runCount <= 0 || runCount % cadence !== 0) return;

  const warningKeys = Object.keys(safeState.warnings || {}).sort();
  warningKeys.forEach((warningKey) => {
    const warningEntry = safeState.warnings[warningKey] || {};
    const repeatCount = Number(warningEntry.repeat_count_since_summary || 0);
    if (repeatCount <= 0) return;

    appendWatchdogWarningOpsRow_(entry, 'watchdog_warning_repeat_rollup', {
      warning_aggregate_schema_id: WATCHDOG_WARNING_AGGREGATE_SCHEMA,
      stage: String(warningEntry.stage || ''),
      reason_code: String(warningEntry.reason_code || ''),
      warning_summary_key: String(warningEntry.summary_key || ''),
      repeat_count: repeatCount,
      first_seen: String(warningEntry.first_seen || ''),
      last_seen: String(warningEntry.last_seen || ''),
      warning_summary: warningEntry.payload_sample || {},
    });

    warningEntry.repeat_count_since_summary = 0;
  });

  safeState.last_summary_emit_run_count = runCount;
}

function appendWatchdogWarningOpsRow_(entry, reasonCode, payload, now) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(SHEETS.RUN_LOG) || ensureSheet_(ss, SHEETS.RUN_LOG);
  const nowIso = formatLocalIso_(now || new Date());
  sh.appendRow([
    'ops',
    entry.run_id || '',
    'watchdog_warning_aggregate',
    nowIso,
    nowIso,
    'warning',
    reasonCode,
    sanitizeForLog_(JSON.stringify(payload || {})),
    0,0,0,0,0,0,
    '{}',
    0,0,
    '', '', '', '', '',
    '[]',
  ]);
}

function reasonCodeAliasToCode_(reasonAlias, schemaId) {
  const text = String(reasonAlias || '').trim();
  if (!text) return '';
  const inverted = invertReasonCodeAliasDictionary_(getReasonCodeAliasDictionary_(schemaId));
  return String(inverted[text] || text);
}

function compactReasonCodeMapForLog_(reasonMap, schemaId, options) {
  const compacted = {};
  const fallbackAliases = {};
  Object.keys(reasonMap || {}).forEach((reasonCode) => {
    const value = Number((reasonMap || {})[reasonCode]);
    if (!Number.isFinite(value) || value === 0) return;
    const resolved = reasonCodeToAlias_(reasonCode, schemaId, options || {});
    if (!resolved.alias) return;
    compacted[resolved.alias] = value;
    if (resolved.was_fallback) {
      fallbackAliases[resolved.alias] = resolved.canonical;
    }
  });
  return {
    reason_codes: compacted,
    fallback_aliases: fallbackAliases,
  };
}

function compactStageSummariesForLog_(stageSummaries, schemaId, options, metadata) {
  const envelope = {
    schema_id: REASON_CODE_ALIAS_SCHEMA_ID,
    stage_summaries: (stageSummaries || []).map((summary) => {
      const cloned = Object.assign({}, summary || {});
      const compacted = compactReasonCodeMapForLog_((summary || {}).reason_codes || {}, schemaId, options || {});
      cloned.reason_codes = compacted.reason_codes;
      if (Object.keys(compacted.fallback_aliases || {}).length > 0) cloned.fallback_aliases = compacted.fallback_aliases;
      return cloned;
    }),
  };
  if (metadata && typeof metadata === 'object' && !Array.isArray(metadata)) {
    Object.keys(metadata).forEach((key) => {
      if (key === 'stage_summaries' || key === 'schema_id') return;
      envelope[key] = metadata[key];
    });
  }
  return envelope;
}

function buildRunExportParityMetadataFromStageSummaries_(runId, requiredStages, stageSummaries, options) {
  const runIdText = String(runId || '').trim();
  const required = Array.isArray(requiredStages) && requiredStages.length
    ? requiredStages.map((stage) => String(stage || '').trim()).filter((stage) => !!stage)
    : ['stageFetchPlayerStats'];
  const runIds = Array.isArray(options && options.latest_run_ids)
    ? (options.latest_run_ids || []).map((id) => String(id || '').trim()).filter((id) => !!id)
    : (runIdText ? [runIdText] : []);

  const status = {
    contract_name: 'run_log_export_parity_contract_v1',
    latest_run_ids: runIds,
    summary_presence_by_run_id: {},
    required_stage_summary_presence_by_run_id: {},
    pass: false,
    parity_status: 'failed',
    reason_code: 'export_parity_missing_run_id',
    checked_at: formatLocalIso_(new Date()),
    checked_at_utc: new Date().toISOString(),
  };

  runIds.forEach((id) => {
    status.summary_presence_by_run_id[id] = !!id;
    status.required_stage_summary_presence_by_run_id[id] = {};
    required.forEach((stage) => {
      status.required_stage_summary_presence_by_run_id[id][stage] = false;
    });
  });

  if (!runIds.length) return status;

  const normalizedStageSummaries = Array.isArray(stageSummaries)
    ? stageSummaries
    : ((stageSummaries && Array.isArray(stageSummaries.stage_summaries)) ? stageSummaries.stage_summaries : []);
  const observedStageSet = {};
  normalizedStageSummaries.forEach((summary) => {
    if (!summary || typeof summary !== 'object') return;
    const stageName = String(summary.stage || '').trim();
    if (stageName) observedStageSet[stageName] = true;
  });

  runIds.forEach((id) => {
    required.forEach((stage) => {
      status.required_stage_summary_presence_by_run_id[id][stage] = !!observedStageSet[stage];
    });
  });

  const missingSummary = Object.keys(status.summary_presence_by_run_id).some((id) => !status.summary_presence_by_run_id[id]);
  const missingStageSummary = Object.keys(status.required_stage_summary_presence_by_run_id).some((id) => {
    const requiredMap = status.required_stage_summary_presence_by_run_id[id] || {};
    return Object.keys(requiredMap).some((stage) => !requiredMap[stage]);
  });

  if (missingSummary) {
    status.reason_code = 'export_parity_missing_run_summary';
  } else if (missingStageSummary) {
    status.reason_code = 'export_parity_missing_stage_summary';
  } else {
    status.pass = true;
    status.parity_status = 'pass';
    status.reason_code = 'export_parity_contract_pass_precheck';
  }
  return status;
}

function buildRunExportParityContractState_(runId, requiredStages, options) {
  const runIdText = String(runId || '').trim();
  const required = Array.isArray(requiredStages) && requiredStages.length
    ? requiredStages.map((stage) => String(stage || '').trim()).filter((stage) => !!stage)
    : ['stageFetchPlayerStats'];
  const runIds = Array.isArray(options && options.latest_run_ids)
    ? (options.latest_run_ids || []).map((id) => String(id || '').trim()).filter((id) => !!id)
    : (runIdText ? [runIdText] : []);

  const status = {
    contract_key: 'LAST_EXPORT_PARITY_STATUS',
    contract_name: 'run_log_export_parity_contract_v1',
    latest_run_ids: runIds,
    summary_presence_by_run_id: {},
    required_stage_summary_presence_by_run_id: {},
    pass: false,
    parity_status: 'failed',
    reason_code: 'export_parity_missing_run_id',
    checked_at: formatLocalIso_(new Date()),
    checked_at_utc: new Date().toISOString(),
  };

  runIds.forEach((id) => {
    status.summary_presence_by_run_id[id] = false;
    status.required_stage_summary_presence_by_run_id[id] = {};
    required.forEach((stage) => {
      status.required_stage_summary_presence_by_run_id[id][stage] = false;
    });
  });

  if (!runIds.length) {
    setStateValue_('LAST_EXPORT_PARITY_STATUS', JSON.stringify(status));
    return status;
  }

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(SHEETS.RUN_LOG) || ensureSheet_(ss, SHEETS.RUN_LOG);
  const values = sh.getDataRange().getValues();
  if (!values || values.length <= 1) {
    status.reason_code = 'export_parity_run_log_empty';
    setStateValue_('LAST_EXPORT_PARITY_STATUS', JSON.stringify(status));
    return status;
  }

  const header = values[0];
  const idx = {
    row_type: header.indexOf('row_type'),
    run_id: header.indexOf('run_id'),
    stage: header.indexOf('stage'),
    stage_summaries: header.indexOf('stage_summaries'),
  };

  values.slice(1).forEach((row) => {
    const rowRunId = String(idx.run_id >= 0 ? row[idx.run_id] : '').trim();
    if (!rowRunId || !Object.prototype.hasOwnProperty.call(status.summary_presence_by_run_id, rowRunId)) return;

    const rowType = String(idx.row_type >= 0 ? row[idx.row_type] : '').trim();
    const stage = String(idx.stage >= 0 ? row[idx.stage] : '').trim();
    if (rowType !== 'summary' || stage !== 'runEdgeBoard') return;

    status.summary_presence_by_run_id[rowRunId] = true;
    const stagePayload = parseLogJsonLike_(idx.stage_summaries >= 0 ? row[idx.stage_summaries] : null, null);
    const stageSummaries = Array.isArray(stagePayload)
      ? stagePayload
      : ((stagePayload && Array.isArray(stagePayload.stage_summaries)) ? stagePayload.stage_summaries : []);

    const stageSet = {};
    stageSummaries.forEach((summary) => {
      if (!summary || typeof summary !== 'object') return;
      const stageName = String(summary.stage || '').trim();
      if (stageName) stageSet[stageName] = true;
    });

    required.forEach((requiredStage) => {
      status.required_stage_summary_presence_by_run_id[rowRunId][requiredStage] = !!stageSet[requiredStage];
    });
  });

  const missingSummary = Object.keys(status.summary_presence_by_run_id).some((id) => !status.summary_presence_by_run_id[id]);
  const missingStageSummary = Object.keys(status.required_stage_summary_presence_by_run_id).some((id) => {
    const requiredMap = status.required_stage_summary_presence_by_run_id[id] || {};
    return Object.keys(requiredMap).some((stage) => !requiredMap[stage]);
  });

  if (missingSummary) {
    status.reason_code = 'export_parity_missing_run_summary';
  } else if (missingStageSummary) {
    status.reason_code = 'export_parity_missing_stage_summary';
  } else {
    status.pass = true;
    status.parity_status = 'pass';
    status.reason_code = 'export_parity_contract_pass';
  }

  setStateValue_('LAST_EXPORT_PARITY_STATUS', JSON.stringify(status));
  return status;
}

function expandReasonCodeMapForLegacy_(reasonMap, schemaId, fallbackAliases) {
  const expanded = {};
  const fallbacks = fallbackAliases || {};
  Object.keys(reasonMap || {}).forEach((aliasOrCode) => {
    const aliasKey = String(aliasOrCode || '');
    const fallbackCode = String(fallbacks[aliasKey] || '').trim();
    const reasonCode = fallbackCode || reasonCodeAliasToCode_(aliasOrCode, schemaId);
    const value = Number((reasonMap || {})[aliasOrCode]);
    if (!reasonCode || !Number.isFinite(value)) return;
    expanded[reasonCode] = value;
  });
  return expanded;
}

function expandStageSummariesForLegacy_(stageSummaries, schemaId) {
  return (stageSummaries || []).map((summary) => {
    const cloned = Object.assign({}, summary || {});
    cloned.reason_codes = expandReasonCodeMapForLegacy_((summary || {}).reason_codes || {}, schemaId, (summary || {}).fallback_aliases || {});
    return cloned;
  });
}

function parseLogJsonLike_(value, fallback) {
  if (value === null || value === undefined || value === '') return fallback;
  if (typeof value === 'object') return value;
  if (typeof value !== 'string') return fallback;
  try {
    return JSON.parse(value);
  } catch (e) {
    return fallback;
  }
}

function adaptRunLogRecordForLegacy_(record) {
  const row = Object.assign({}, record || {});
  const schemaVersion = Number(row.schema_version || 0);

  if (schemaVersion !== 2) {
    const legacyMessage = parseLogJsonLike_(row.message, null);
    if (legacyMessage && typeof legacyMessage === 'object' && legacyMessage.reason_codes) {
      const messageSchemaId = String(legacyMessage.schema_id || REASON_CODE_ALIAS_SCHEMA_ID || '').trim();
      legacyMessage.reason_codes = expandReasonCodeMapForLegacy_(legacyMessage.reason_codes || {}, messageSchemaId, legacyMessage.fallback_aliases || {});
      row.message = JSON.stringify(legacyMessage);
    }
    const rejectionEnvelope = parseLogJsonLike_(row.rejection_codes, null);
    if (rejectionEnvelope && rejectionEnvelope.reason_codes) {
      const rejectionSchemaId = String(rejectionEnvelope.schema_id || REASON_CODE_ALIAS_SCHEMA_ID || '').trim();
      rejectionEnvelope.reason_codes = expandReasonCodeMapForLegacy_(rejectionEnvelope.reason_codes || {}, rejectionSchemaId, rejectionEnvelope.fallback_aliases || {});
      row.rejection_codes = JSON.stringify(rejectionEnvelope.reason_codes);
    }
    const stageSummaryEnvelope = parseLogJsonLike_(row.stage_summaries, null);
    if (stageSummaryEnvelope && stageSummaryEnvelope.stage_summaries) {
      const stageSummarySchemaId = String(stageSummaryEnvelope.schema_id || REASON_CODE_ALIAS_SCHEMA_ID || '').trim();
      const expandedSummaries = expandStageSummariesForLegacy_(stageSummaryEnvelope.stage_summaries || [], stageSummarySchemaId);
      row.stage_summaries = JSON.stringify(expandedSummaries);
    }
    return row;
  }

  const eventType = String(row.et || '');
  const stage = String(row.st || eventType || '');
  const rowType = eventType === 'summary'
    ? 'summary'
    : ((eventType === 'watchdog' || stage === 'run_start_config_audit' || stage.indexOf('watchdog') >= 0 || stage === 'run_lifecycle') ? 'ops' : 'stage');

  const legacyMessageObj = parseLogJsonLike_(row.msg, null);
  const compactSchemaId = String(row.ras || REASON_CODE_ALIAS_SCHEMA_ID || '').trim();
  const legacyMessage = legacyMessageObj && typeof legacyMessageObj === 'object'
    ? JSON.stringify((function () {
      const messagePayload = Object.assign({}, legacyMessageObj || {});
      if (messagePayload.reason_codes) {
        const messageSchemaId = String(messagePayload.schema_id || compactSchemaId || REASON_CODE_ALIAS_SCHEMA_ID || '').trim();
        messagePayload.reason_codes = expandReasonCodeMapForLegacy_(messagePayload.reason_codes || {}, messageSchemaId, messagePayload.fallback_aliases || ((row.rm || {}).fallback_aliases || {}));
      }
      return messagePayload;
    })())
    : String(row.msg || '');

  const expandedReasonCodes = expandReasonCodeMapForLegacy_(row.rc || {}, compactSchemaId, ((row.rm || {}).fallback_aliases || {}));
  const expandedRejections = expandReasonCodeMapForLegacy_(row.rj || {}, compactSchemaId, ((row.rm || {}).rejection_fallback_aliases || {}));
  const expandedStageSummaries = expandStageSummariesForLegacy_(row.ssu || [], compactSchemaId);

  if (legacyMessageObj && typeof legacyMessageObj === 'object' && Object.keys(expandedReasonCodes).length > 0) {
    const mergedMessage = Object.assign({}, legacyMessageObj, {
      reason_codes: Object.assign({}, expandedReasonCodes),
    });
    if (!Object.prototype.hasOwnProperty.call(mergedMessage, 'input_count')) mergedMessage.input_count = Number(row.ic || 0);
    if (!Object.prototype.hasOwnProperty.call(mergedMessage, 'output_count')) mergedMessage.output_count = Number(row.oc || 0);
    if (!Object.prototype.hasOwnProperty.call(mergedMessage, 'provider')) mergedMessage.provider = String(row.pr || '');
    if (!Object.prototype.hasOwnProperty.call(mergedMessage, 'api_credit_usage')) mergedMessage.api_credit_usage = Number(row.acu || 0);
    if (row.rm && typeof row.rm === 'object' && Object.keys(row.rm).length > 0) mergedMessage.reason_metadata = Object.assign({}, row.rm);
    return {
      row_type: rowType,
      run_id: String(row.rid || ''),
      stage: stage,
      started_at: String(row.sa || ''),
      ended_at: String(row.ea || ''),
      status: String(row.ss || ''),
      reason_code: String(row.rcd || ''),
      message: JSON.stringify(mergedMessage),
      fetched_odds: Number(row.fo || 0),
      fetched_schedule: Number(row.fs || 0),
      allowed_tournaments: Number(row.at || 0),
      matched: Number(row.mt || 0),
      unmatched: Number(row.um || 0),
      signals_found: Number(row.sg || 0),
      rejection_codes: JSON.stringify(expandedRejections),
      cooldown_suppressed: Number(row.cds || 0),
      duplicate_suppressed: Number(row.dds || 0),
      lock_event: String(row.lk || ''),
      debounce_event: String(row.db || ''),
      trigger_event: String(row.tr || ''),
      exception: String(row.ex || ''),
      stack: String(row.stk || ''),
      stage_summaries: JSON.stringify(expandedStageSummaries),
    };
  }

  return {
    row_type: rowType,
    run_id: String(row.rid || ''),
    stage: stage,
    started_at: String(row.sa || ''),
    ended_at: String(row.ea || ''),
    status: String(row.ss || ''),
    reason_code: String(row.rcd || ''),
    message: legacyMessage,
    fetched_odds: Number(row.fo || 0),
    fetched_schedule: Number(row.fs || 0),
    allowed_tournaments: Number(row.at || 0),
    matched: Number(row.mt || 0),
    unmatched: Number(row.um || 0),
    signals_found: Number(row.sg || 0),
    rejection_codes: JSON.stringify(expandedRejections),
    cooldown_suppressed: Number(row.cds || 0),
    duplicate_suppressed: Number(row.dds || 0),
    lock_event: String(row.lk || ''),
    debounce_event: String(row.db || ''),
    trigger_event: String(row.tr || ''),
    exception: String(row.ex || ''),
    stack: String(row.stk || ''),
    stage_summaries: JSON.stringify(expandedStageSummaries),
  };
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

function normalizeReasonCodeEntriesForDisplay_(entries, schemaId) {
  const normalizedEntries = [];
  const normalizationMap = getReasonCodeDisplayNormalizationMap_(schemaId);
  const appliedMappings = {};

  (entries || []).forEach(function (entry) {
    const safeEntry = entry || {};
    const rawReasonCode = String(safeEntry.reason_code || '').trim();
    const mapping = normalizationMap[rawReasonCode];
    if (!mapping || !mapping.alias) {
      normalizedEntries.push(Object.assign({}, safeEntry));
      return;
    }

    appliedMappings[rawReasonCode] = {
      canonical_alias: mapping.alias,
      canonical_reason: mapping.canonical_reason,
      source: mapping.source,
    };
    normalizedEntries.push(Object.assign({}, safeEntry, {
      reason_code: mapping.alias,
      legacy_reason_code: rawReasonCode,
    }));
  });

  return {
    entries: normalizedEntries,
    metadata: {
      normalization_applied: Object.keys(appliedMappings).length > 0,
      mapped_legacy_aliases: appliedMappings,
      mapped_legacy_alias_count: Object.keys(appliedMappings).length,
    },
  };
}

function getReasonCodeDisplayNormalizationMap_(schemaId) {
  const resolvedSchemaId = String(schemaId || REASON_CODE_ALIAS_SCHEMA_ID || '').trim();
  const aggregateState = getReasonAliasFallbackWarningAggregateState_();
  const sets = (aggregateState && aggregateState.sets) || {};
  const mapping = {};

  Object.keys(sets).forEach(function (setKey) {
    const entry = sets[setKey] || {};
    if (String(entry.schema_id || resolvedSchemaId) !== resolvedSchemaId) return;
    const canonicalReasons = entry.canonical_reasons || {};
    Object.keys(canonicalReasons).forEach(function (legacyAlias) {
      const aliasText = String(legacyAlias || '').trim();
      if (!aliasText || aliasText.indexOf('UNK_') !== 0) return;
      const canonicalReasonCode = String(canonicalReasons[legacyAlias] || '').trim();
      if (!canonicalReasonCode) return;
      const aliasResult = reasonCodeToAlias_(canonicalReasonCode, resolvedSchemaId, { allow_canonical_passthrough: true });
      const canonicalAlias = String(aliasResult.alias || '').trim();
      if (!canonicalAlias || canonicalAlias === aliasText || canonicalAlias.indexOf('UNK_') === 0) return;
      mapping[aliasText] = {
        alias: canonicalAlias,
        canonical_reason: canonicalReasonCode,
        source: 'reason_alias_fallback_warning_aggregate',
      };
    });
  });

  return mapping;
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
