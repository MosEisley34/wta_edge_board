function stageFetchPlayerStats(runId, config, oddsEvents, matchRows) {
  const start = Date.now();
  const reasonCounts = {
    stats_enriched: 0,
    stats_missing_player_a: 0,
    stats_missing_player_b: 0,
    stats_fallback_model_used: 0,
    skipped_no_matched_events: 0,
    skipped_no_player_keys: 0,
    provider_returned_empty: 0,
    provider_returned_null_features: 0,
  };

  const matchByOddsEventId = {};
  matchRows.forEach((row) => {
    matchByOddsEventId[row.odds_event_id] = row;
  });

  const rows = [];
  const byOddsEventId = {};
  const playersToFetch = [];
  const seedEvents = [];

  oddsEvents.forEach((event) => {
    const match = matchByOddsEventId[event.event_id];
    if (!match || !match.schedule_event_id) return;
    playersToFetch.push(canonicalizePlayerName_(event.player_1, {}));
    playersToFetch.push(canonicalizePlayerName_(event.player_2, {}));
  });

  if (!matchRows.length) {
    reasonCounts.skipped_no_matched_events += 1;
  }

  const asOfTime = oddsEvents.reduce(function (latest, event) {
    if (!(event && event.odds_updated_time instanceof Date)) return latest;
    return event.odds_updated_time.getTime() > latest.getTime() ? event.odds_updated_time : latest;
  }, new Date(0));
  const normalizedAsOfTime = asOfTime.getTime() > 0 ? asOfTime : new Date();

  if (!playersToFetch.length) {
    const schedulePayload = getStateJson_('SCHEDULE_WINDOW_STALE_PAYLOAD') || {};
    const scheduleEvents = Array.isArray(schedulePayload.events)
      ? schedulePayload.events.map(deserializeScheduleEvent_)
      : [];

    scheduleEvents.forEach(function (scheduleEvent) {
      const playerA = canonicalizePlayerName_(scheduleEvent.player_1, {});
      const playerB = canonicalizePlayerName_(scheduleEvent.player_2, {});
      if (!playerA || !playerB) return;
      playersToFetch.push(playerA);
      playersToFetch.push(playerB);
      seedEvents.push({
        event_id: scheduleEvent.event_id,
        player_1: playerA,
        player_2: playerB,
        odds_updated_time: normalizedAsOfTime,
        synthetic_schedule_seed: true,
      });
    });
  }

  if (!playersToFetch.length) {
    reasonCounts.skipped_no_player_keys += 1;
  }

  const statsBatch = fetchPlayerStatsBatch_(config, playersToFetch, normalizedAsOfTime);
  const statsByPlayer = statsBatch.stats_by_player || {};
  const providerUnavailable = statsBatch.provider_available === false;
  const providerNullFeatures = String(statsBatch.reason_code || '') === 'provider_returned_null_features';
  const providerReturnedEmpty = !providerUnavailable
    && playersToFetch.length > 0
    && Object.keys(statsByPlayer).length === 0;
  if (providerReturnedEmpty) reasonCounts.provider_returned_empty += 1;
  if (providerNullFeatures) reasonCounts.provider_returned_null_features += 1;
  const source = providerUnavailable ? 'derived_player_stats_v1_fallback' : 'player_stats_provider_v1';

  const statsInputEvents = seedEvents.length ? seedEvents : oddsEvents;
  statsInputEvents.forEach((event) => {
    const match = matchByOddsEventId[event.event_id];
    if (!event.synthetic_schedule_seed && (!match || !match.schedule_event_id)) return;

    const playerA = canonicalizePlayerName_(event.player_1, {});
    const playerB = canonicalizePlayerName_(event.player_2, {});
    const featureTimestamp = event.odds_updated_time.toISOString();

    const effectiveMatch = match || {
      competition_tier: 'UNKNOWN',
      schedule_event_id: event.event_id,
    };

    const playerAStats = resolvePlayerStatsPayload_(playerA, statsByPlayer, providerUnavailable, event, effectiveMatch, 'a', reasonCounts);
    const playerBStats = resolvePlayerStatsPayload_(playerB, statsByPlayer, providerUnavailable, event, effectiveMatch, 'b', reasonCounts);

    rows.push(buildRawPlayerStatsRow_(event.event_id, playerA, source, featureTimestamp, playerAStats));
    rows.push(buildRawPlayerStatsRow_(event.event_id, playerB, source, featureTimestamp, playerBStats));

    if (playerAStats.has_stats) reasonCounts.stats_enriched += 1;
    else reasonCounts.stats_missing_player_a += 1;

    if (playerBStats.has_stats) reasonCounts.stats_enriched += 1;
    else reasonCounts.stats_missing_player_b += 1;

    byOddsEventId[event.event_id] = {
      source,
      stats_provider_unavailable: providerUnavailable,
      stats_fallback_mode: providerNullFeatures ? 'null_features' : '',
      synthetic_schedule_seed: !!event.synthetic_schedule_seed,
      feature_timestamp: featureTimestamp,
      player_a: {
        canonical_name: playerA,
        features: playerAStats.features,
        has_stats: playerAStats.has_stats,
        stats_fallback_mode: playerAStats.stats_fallback_mode || '',
        provenance: playerAStats.provenance || source,
      },
      player_b: {
        canonical_name: playerB,
        features: playerBStats.features,
        has_stats: playerBStats.has_stats,
        stats_fallback_mode: playerBStats.stats_fallback_mode || '',
        provenance: playerBStats.provenance || source,
      },
    };
  });

  const summary = buildStageSummary_(runId, 'stageFetchPlayerStats', start, {
    input_count: statsInputEvents.length,
    output_count: rows.length,
    provider: source,
    api_credit_usage: Number(statsBatch.api_credit_usage || 0),
    reason_codes: reasonCounts,
  });

  return {
    rows,
    byOddsEventId,
    summary,
  };
}


function buildSkippedPlayerStatsStage_(runId, reasonCode) {
  const summary = buildStageSummary_(runId, 'stageFetchPlayerStats', Date.now(), {
    input_count: 0,
    output_count: 0,
    provider: 'stage_skipped',
    api_credit_usage: 0,
    reason_codes: {
      skipped_schedule_only_no_odds: reasonCode === 'skipped_schedule_only_no_odds' ? 1 : 0,
    },
  });

  summary.status = 'skipped';
  summary.reason_code = reasonCode || 'stage_skipped';
  summary.message = reasonCode === 'skipped_schedule_only_no_odds'
    ? 'Skipped player stats fetch because run is schedule-only with no odds candidates.'
    : 'Skipped player stats fetch.';

  return {
    rows: [],
    byOddsEventId: {},
    summary: summary,
  };
}

function resolvePlayerStatsPayload_(canonicalPlayerName, statsByPlayer, providerUnavailable, event, match, slot, reasonCounts) {
  const providerStats = statsByPlayer[canonicalPlayerName];

  if (providerStats) {
    const hasProviderStats = providerStats.ranking !== null
      || providerStats.recent_form !== null
      || providerStats.surface_win_rate !== null
      || providerStats.hold_pct !== null
      || providerStats.break_pct !== null;

    if (hasProviderStats) {
      return {
        has_stats: true,
        stats_fallback_mode: '',
        provenance: 'player_stats_provider_v1',
        features: {
          ranking: providerStats.ranking,
          recent_form: providerStats.recent_form,
          surface_win_rate: providerStats.surface_win_rate,
          hold_pct: providerStats.hold_pct,
          break_pct: providerStats.break_pct,
        },
      };
    }

    return {
      has_stats: false,
      stats_fallback_mode: 'null_features',
      provenance: 'player_stats_provider_v1',
      features: {
        ranking: null,
        recent_form: null,
        surface_win_rate: null,
        hold_pct: null,
        break_pct: null,
      },
    };
  }

  if (providerUnavailable) {
    reasonCounts.stats_fallback_model_used += 1;
    const pseudo = computePseudoPlayerStats_(canonicalPlayerName, event, match, slot);
    pseudo.stats_fallback_mode = 'provider_unavailable';
    pseudo.provenance = 'derived_player_stats_v1_fallback';
    return pseudo;
  }

  return {
    has_stats: false,
    stats_fallback_mode: 'missing_row',
    provenance: 'player_stats_provider_v1',
    features: {
      ranking: null,
      recent_form: null,
      surface_win_rate: null,
      hold_pct: null,
      break_pct: null,
    },
  };
}

function buildRawPlayerStatsRow_(eventId, canonicalPlayerName, source, featureTimestamp, payload) {
  return {
    key: [eventId, canonicalPlayerName, featureTimestamp].join('|'),
    event_id: eventId,
    player_canonical_name: canonicalPlayerName,
    source,
    feature_timestamp: featureTimestamp,
    feature_values: JSON.stringify(payload.features),
    has_stats: payload.has_stats,
    stats_fallback_mode: payload.stats_fallback_mode || '',
    provenance: payload.provenance || source,
    updated_at: formatLocalIso_(new Date()),
  };
}

function computePseudoPlayerStats_(canonicalPlayerName, event, match, slot) {
  const baseText = [canonicalPlayerName, event.event_id, match.competition_tier, slot].join('|');
  const seed = stringHashCode_(baseText);
  const available = (seed % 100) >= 12;

  if (!available) {
    return {
      has_stats: false,
      features: {
        ranking: null,
        recent_form: null,
        surface_win_rate: null,
        hold_pct: null,
        break_pct: null,
      },
    };
  }

  return {
    has_stats: true,
    features: {
      ranking: 1 + (seed % 220),
      recent_form: roundNumber_(0.35 + ((seed % 56) / 100), 3),
      surface_win_rate: roundNumber_(0.30 + (((seed >> 3) % 61) / 100), 3),
      hold_pct: roundNumber_(0.45 + (((seed >> 5) % 51) / 100), 3),
      break_pct: roundNumber_(0.20 + (((seed >> 7) % 36) / 100), 3),
    },
  };
}

function combinePlayerStatsFeatureBump_(statsBundle, reasonCodes) {
  if (!statsBundle) {
    reasonCodes.stats_fallback_model_used = (reasonCodes.stats_fallback_model_used || 0) + 1;
    return 0;
  }

  const playerA = statsBundle.player_a;
  const playerB = statsBundle.player_b;

  if (!playerA || !playerA.has_stats) reasonCodes.stats_missing_player_a = (reasonCodes.stats_missing_player_a || 0) + 1;
  if (!playerB || !playerB.has_stats) reasonCodes.stats_missing_player_b = (reasonCodes.stats_missing_player_b || 0) + 1;

  if (!playerA || !playerB || !playerA.has_stats || !playerB.has_stats) {
    if (statsBundle.stats_provider_unavailable === true) {
      reasonCodes.stats_fallback_model_used = (reasonCodes.stats_fallback_model_used || 0) + 1;
    }
    return 0;
  }

  const rankingDiff = (playerB.features.ranking - playerA.features.ranking) / 300;
  const recentFormDiff = (playerA.features.recent_form || 0) - (playerB.features.recent_form || 0);
  const surfaceDiff = (playerA.features.surface_win_rate || 0) - (playerB.features.surface_win_rate || 0);
  const serveReturnDiff = ((playerA.features.hold_pct || 0) - (playerB.features.hold_pct || 0))
    + ((playerA.features.break_pct || 0) - (playerB.features.break_pct || 0));

  return roundNumber_((rankingDiff * 0.25) + (recentFormDiff * 0.3) + (surfaceDiff * 0.25) + (serveReturnDiff * 0.2), 4);
}

function stageGenerateSignals(runId, config, oddsEvents, matchRows, playerStatsByOddsEventId, stageMeta) {
  const start = Date.now();
  const nowMs = Date.now();
  const upstreamGateReason = stageMeta && stageMeta.upstream_gate_reason ? String(stageMeta.upstream_gate_reason) : '';
  const rows = [];
  const sampledDecisions = [];
  const sampledDecisionLimit = Number(config.SIGNAL_DECISION_SAMPLE_LIMIT || 50);
  let processedCandidateCount = 0;
  const reasonCounts = {
    sent: 0,
    missing_match: 0,
    missing_stats: 0,
    null_features_fallback_scored: 0,
    invalid_features: 0,
    notify_disabled: 0,
    duplicate_suppressed: 0,
    cooldown_suppressed: 0,
    edge_below_threshold: 0,
    too_close_to_start_skip: 0,
    stale_odds_skip: 0,
    notify_http_failed: 0,
    notify_missing_config: 0,
  };
  const legacyReasonCodeMap = {
    missing_match: 'missing_schedule_match',
    missing_stats: 'missing_player_stats',
    invalid_features: 'insufficient_features',
  };
  const legacyReasonCounts = {};
  const signalState = getSignalState_();
  const seenHashesThisRun = {};
  const matchByOddsEventId = {};
  matchRows.forEach((row) => {
    matchByOddsEventId[row.odds_event_id] = row;
  });

  function captureDecision_(event, match, decisionReasonCode, detail) {
    processedCandidateCount += 1;
    reasonCounts[decisionReasonCode] = (reasonCounts[decisionReasonCode] || 0) + 1;
    const legacyDecisionReasonCode = legacyReasonCodeMap[decisionReasonCode] || null;
    if (legacyDecisionReasonCode) {
      legacyReasonCounts[legacyDecisionReasonCode] = (legacyReasonCounts[legacyDecisionReasonCode] || 0) + 1;
    }

    if (sampledDecisions.length < sampledDecisionLimit) {
      sampledDecisions.push({
        odds_event_id: event && event.event_id ? event.event_id : '',
        schedule_event_id: (match && match.schedule_event_id) || '',
        decision_reason_code: decisionReasonCode,
        market: event && event.market ? event.market : '',
        side: event && event.outcome ? event.outcome : '',
        bookmaker: event && event.bookmaker ? event.bookmaker : '',
        price: event && event.price,
        detail: detail || {},
      });
    }
  }

  oddsEvents.forEach((event) => {
    const match = matchByOddsEventId[event.event_id];
    if (!match || !match.schedule_event_id) {
      captureDecision_(event, match, 'missing_match', {
        scored: false,
      });
      return;
    }

    const statsBundle = (playerStatsByOddsEventId || {})[event.event_id] || null;
    const hasStatsBundleRows = !!(statsBundle && statsBundle.player_a && statsBundle.player_b);
    const hasPlayerStats = !!(hasStatsBundleRows
      && statsBundle.player_a.has_stats
      && statsBundle.player_b.has_stats);
    const nullFeaturesFallback = !!(hasStatsBundleRows
      && statsBundle.player_a.has_stats === false
      && statsBundle.player_b.has_stats === false
      && (statsBundle.player_a.stats_fallback_mode === 'null_features'
        || statsBundle.player_b.stats_fallback_mode === 'null_features'
        || statsBundle.stats_fallback_mode === 'null_features'));

    if (!hasPlayerStats && !nullFeaturesFallback) {
      captureDecision_(event, match, 'missing_stats', {
        scored: false,
      });
      return;
    }

    if (nullFeaturesFallback) reasonCounts.null_features_fallback_scored += 1;

    const modelVersion = statsBundle && statsBundle.player_a && statsBundle.player_b
      && statsBundle.player_a.has_stats && statsBundle.player_b.has_stats
      ? config.MODEL_VERSION
      : config.MODEL_VERSION + '_fallback';

    const impliedProbability = oddsPriceToImpliedProbability_(event.price);
    if (impliedProbability === null) {
      captureDecision_(event, match, 'invalid_features', {
        scored: false,
        field: 'implied_probability',
      });
      return;
    }

    const startCutoffMs = config.MINUTES_BEFORE_START_CUTOFF * 60000;
    if (event.commence_time.getTime() <= nowMs + startCutoffMs) {
      rows.push(buildSignalRow_(runId, config, event, match, {
        notification_outcome: 'too_close_to_start_skip',
        model_probability: estimateFairProbability_(impliedProbability, match.competition_tier, statsBundle, reasonCounts),
        market_implied_probability: impliedProbability,
        edge_value: 0,
        edge_tier: 'NONE',
        stake_units: 0,
        signal_hash: buildSignalHash_(event.event_id, event.market, event.outcome, modelVersion),
        model_version: modelVersion,
      }));
      captureDecision_(event, match, 'too_close_to_start_skip', {
        scored: true,
      });
      return;
    }

    const staleThresholdMs = config.STALE_ODDS_WINDOW_MIN * 60000;
    if (nowMs - event.odds_updated_time.getTime() > staleThresholdMs) {
      rows.push(buildSignalRow_(runId, config, event, match, {
        notification_outcome: 'stale_odds_skip',
        model_probability: estimateFairProbability_(impliedProbability, match.competition_tier, statsBundle, reasonCounts),
        market_implied_probability: impliedProbability,
        edge_value: 0,
        edge_tier: 'NONE',
        stake_units: 0,
        signal_hash: buildSignalHash_(event.event_id, event.market, event.outcome, modelVersion),
        model_version: modelVersion,
      }));
      captureDecision_(event, match, 'stale_odds_skip', {
        scored: true,
      });
      return;
    }

    const modelProbability = estimateFairProbability_(impliedProbability, match.competition_tier, statsBundle, reasonCounts);
    const edgeValue = roundNumber_(modelProbability - impliedProbability, 4);
    const edgeTierAndStake = classifyEdgeAndStake_(edgeValue, config);
    const signalHash = buildSignalHash_(event.event_id, event.market, event.outcome, modelVersion);

    if (edgeTierAndStake.edge_tier === 'NONE') {
      rows.push(buildSignalRow_(runId, config, event, match, {
        notification_outcome: 'edge_below_threshold',
        model_probability: modelProbability,
        market_implied_probability: impliedProbability,
        edge_value: edgeValue,
        edge_tier: edgeTierAndStake.edge_tier,
        stake_units: edgeTierAndStake.stake_units,
        signal_hash: signalHash,
        model_version: modelVersion,
      }));
      captureDecision_(event, match, 'edge_below_threshold', {
        scored: true,
      });
      return;
    }

    const notifyDecision = maybeNotifySignal_(signalState, seenHashesThisRun, signalHash, nowMs, config.SIGNAL_COOLDOWN_MIN);
    let notifyOutcome = notifyDecision.outcome;
    let notifyDiagnostics = null;

    if (notifyDecision.outcome === 'sent') {
      const sendResult = sendSignalNotification_(config, runId, signalHash, {
        side: event.outcome,
        market: event.market,
        bookmaker: event.bookmaker,
        competition_tier: match.competition_tier,
        edge_tier: edgeTierAndStake.edge_tier,
        edge_value: edgeValue,
        stake_units: edgeTierAndStake.stake_units,
        model_probability: modelProbability,
        market_implied_probability: impliedProbability,
        commence_time: event.commence_time,
        odds_event_id: event.event_id,
      });
      notifyOutcome = sendResult.outcome;
      const notifyLoggedAt = localAndUtcTimestamps_(new Date());
      notifyDiagnostics = {
        run_id: runId,
        signal_hash: signalHash,
        notification_outcome: notifyOutcome,
        logged_at: notifyLoggedAt.local,
        logged_at_utc: notifyLoggedAt.utc,
        timezone: TIMESTAMP_TIMEZONE.ID,
        timezone_offset: TIMESTAMP_TIMEZONE.OFFSET,
        http_status: sendResult.http_status,
        response_body_preview: sendResult.response_body_preview || '',
        test_mode: !!sendResult.test_mode,
        transport: sendResult.transport || 'discord_webhook',
      };

      if (notifyOutcome === 'sent') {
        signalState.sent_hashes[signalHash] = nowMs;
      }

      appendLogRow_({
        row_type: 'ops',
        run_id: runId,
        stage: 'signalNotifyDelivery',
        status: notifyOutcome === 'sent' ? 'success' : (notifyOutcome === 'notify_disabled' ? 'skipped' : 'failed'),
        reason_code: notifyOutcome,
        message: JSON.stringify(notifyDiagnostics),
      });
    }

    captureDecision_(event, match, notifyOutcome, {
      scored: true,
      model_probability: modelProbability,
      market_implied_probability: impliedProbability,
      edge_value: edgeValue,
      edge_tier: edgeTierAndStake.edge_tier,
      stake_units: edgeTierAndStake.stake_units,
    });

    rows.push(buildSignalRow_(runId, config, event, match, {
      notification_outcome: notifyOutcome,
      model_probability: modelProbability,
      market_implied_probability: impliedProbability,
      edge_value: edgeValue,
      edge_tier: edgeTierAndStake.edge_tier,
      stake_units: edgeTierAndStake.stake_units,
      signal_hash: signalHash,
      model_version: modelVersion,
      notification_metadata: notifyDiagnostics,
    }));
  });

  setSignalState_(signalState);
  const signalSnapshotsGeneratedAt = localAndUtcTimestamps_(new Date());
  setStateValue_('LAST_SIGNAL_SNAPSHOTS', JSON.stringify({
    run_id: runId,
    generated_at: signalSnapshotsGeneratedAt.local,
    generated_at_utc: signalSnapshotsGeneratedAt.utc,
    model_version: config.MODEL_VERSION,
    signals: rows.map((row) => ({
      event_id: row.odds_event_id,
      schedule_event_id: row.schedule_event_id,
      side: row.side,
      market: row.market,
      model_probability: row.model_probability,
      market_implied_probability: row.market_implied_probability,
      edge_value: row.edge_value,
      edge_tier: row.edge_tier,
      timestamp: row.created_at,
      timestamp_utc: row.created_at_utc || '',
      commence_time: row.commence_time,
      odds_updated_time: row.odds_updated_time,
      model_version: row.model_version,
      notification_outcome: row.notification_outcome,
      notification_metadata: row.notification_metadata || null,
    })),
  }));

  const deliveryDiagnostics = rows
    .filter((row) => row.notification_metadata)
    .map((row) => row.notification_metadata);
  const notifyDiagnosticsGeneratedAt = localAndUtcTimestamps_(new Date());
  setStateValue_('LAST_NOTIFY_DELIVERY_DIAGNOSTICS', JSON.stringify({
    run_id: runId,
    generated_at: notifyDiagnosticsGeneratedAt.local,
    generated_at_utc: notifyDiagnosticsGeneratedAt.utc,
    deliveries: deliveryDiagnostics,
  }));

  const signalDecisionsGeneratedAt = localAndUtcTimestamps_(new Date());
  const zeroInputExplanatory = oddsEvents.length === 0 ? {
    upstream_gate_reason: upstreamGateReason || 'unspecified',
    has_upstream_gate_reason: !!upstreamGateReason,
  } : null;
  const allDropReasons = Object.keys(reasonCounts)
    .filter(function (reasonCode) {
      return reasonCode !== 'sent'
        && reasonCode !== 'null_features_fallback_scored'
        && reasonCode !== 'stats_missing_player_a'
        && reasonCode !== 'stats_missing_player_b'
        && reasonCode !== 'stats_fallback_model_used';
    })
    .reduce(function (sum, reasonCode) {
      return sum + Number(reasonCounts[reasonCode] || 0);
    }, 0);

  setStateValue_('LAST_SIGNAL_DECISIONS', JSON.stringify({
    run_id: runId,
    generated_at: signalDecisionsGeneratedAt.local,
    generated_at_utc: signalDecisionsGeneratedAt.utc,
    reason_counts: reasonCounts,
    reason_counts_legacy: legacyReasonCounts,
    sampled_decisions: sampledDecisions,
    sampled_candidate_rows: sampledDecisions,
    input_count: oddsEvents.length,
    upstream_gate_reason: upstreamGateReason,
    explanatory_metadata: zeroInputExplanatory,
    processed_count: processedCandidateCount,
    all_drop_reasons: allDropReasons,
    sent_count: Number(reasonCounts.sent || 0),
    invariant: {
      sent_plus_drop_reasons_equals_input: Number(reasonCounts.sent || 0) + allDropReasons === oddsEvents.length,
      zero_input_has_explanatory_metadata: oddsEvents.length > 0 || !!zeroInputExplanatory,
    },
  }));

  if ((Number(reasonCounts.sent || 0) + allDropReasons) !== oddsEvents.length) {
    throw new Error('stageGenerateSignals invariant violated: sent + all_drop_reasons must equal input_count');
  }

  const summaryReasonCodes = Object.assign({}, reasonCounts);
  const summaryReasonMetadata = {};
  if (oddsEvents.length === 0) {
    summaryReasonMetadata.upstream_gate_reason = normalizeUpstreamGateReason_(upstreamGateReason);
  }

  const summary = buildStageSummary_(runId, 'stageGenerateSignals', start, {
    input_count: oddsEvents.length,
    output_count: rows.length,
    provider: 'internal_signal_builder',
    api_credit_usage: 0,
    reason_codes: summaryReasonCodes,
    reason_metadata: summaryReasonMetadata,
  });

  return {
    rows,
    summary,
    sentCount: reasonCounts.sent || 0,
    cooldownSuppressedCount: reasonCounts.cooldown_suppressed || 0,
    duplicateSuppressedCount: reasonCounts.duplicate_suppressed || 0,
  };
}

function buildSignalRow_(runId, config, event, match, detail) {
  return {
    key: detail.signal_hash,
    run_id: runId,
    odds_event_id: event.event_id,
    schedule_event_id: match.schedule_event_id,
    market: event.market,
    side: event.outcome,
    bookmaker: event.bookmaker,
    competition_tier: match.competition_tier,
    model_version: detail.model_version || config.MODEL_VERSION,
    model_probability: detail.model_probability,
    market_implied_probability: detail.market_implied_probability,
    edge_value: detail.edge_value,
    edge_tier: detail.edge_tier,
    stake_units: detail.stake_units,
    signal_hash: detail.signal_hash,
    notification_outcome: detail.notification_outcome,
    reason_code: detail.notification_outcome,
    commence_time: formatLocalIso_(event.commence_time),
    commence_time_utc: event.commence_time.toISOString(),
    odds_updated_time: formatLocalIso_(event.odds_updated_time),
    odds_updated_time_utc: event.odds_updated_time.toISOString(),
    created_at: formatLocalIso_(new Date()),
    created_at_utc: new Date().toISOString(),
    notification_metadata: detail.notification_metadata || null,
  };
}

function getSignalState_() {
  const existing = getStateJson_('SIGNAL_GUARD_STATE');
  if (!existing || typeof existing !== 'object') return { sent_hashes: {} };
  return {
    sent_hashes: existing.sent_hashes || {},
  };
}

function setSignalState_(state) {
  const signalStateUpdatedAt = localAndUtcTimestamps_(new Date());
  setStateValue_('SIGNAL_GUARD_STATE', JSON.stringify({
    updated_at: signalStateUpdatedAt.local,
    updated_at_utc: signalStateUpdatedAt.utc,
    sent_hashes: state.sent_hashes || {},
  }));
}

function maybeNotifySignal_(state, seenHashesThisRun, signalHash, nowMs, cooldownMin) {
  const lastSent = Number((state.sent_hashes || {})[signalHash] || 0);

  if (seenHashesThisRun[signalHash]) {
    return { outcome: 'duplicate_suppressed' };
  }

  seenHashesThisRun[signalHash] = true;

  if (lastSent > 0 && nowMs - lastSent < cooldownMin * 60000) {
    return { outcome: 'cooldown_suppressed' };
  }

  return { outcome: 'sent' };
}

function sendSignalNotification_(config, runId, signalHash, payload) {
  if (!config.NOTIFY_ENABLED) {
    return {
      outcome: 'notify_disabled',
      transport: 'discord_webhook',
      http_status: null,
      response_body_preview: 'notify_disabled',
      test_mode: !!config.NOTIFY_TEST_MODE,
    };
  }

  if (!config.DISCORD_WEBHOOK) {
    return {
      outcome: 'notify_missing_config',
      transport: 'discord_webhook',
      http_status: null,
      response_body_preview: '',
      test_mode: !!config.NOTIFY_TEST_MODE,
    };
  }

  const message = formatSignalNotificationMessage_(runId, signalHash, payload);
  return postDiscordWebhook_(config.DISCORD_WEBHOOK, { content: message }, !!config.NOTIFY_TEST_MODE);
}

function formatSignalNotificationMessage_(runId, signalHash, payload) {
  return [
    'WTA Edge Signal',
    'run_id=' + runId,
    'signal_hash=' + signalHash,
    'event_id=' + payload.odds_event_id,
    'market=' + payload.market,
    'side=' + payload.side,
    'bookmaker=' + payload.bookmaker,
    'tier=' + payload.competition_tier,
    'edge=' + payload.edge_value,
    'edge_tier=' + payload.edge_tier,
    'stake_units=' + payload.stake_units,
    'model_prob=' + payload.model_probability,
    'market_prob=' + payload.market_implied_probability,
    'commence=' + toIso_(payload.commence_time),
    'commence_utc=' + (payload.commence_time ? new Date(payload.commence_time).toISOString() : ''),
    'timezone=' + TIMESTAMP_TIMEZONE.ID,
  ].join(' | ');
}

function postDiscordWebhook_(webhookUrl, payload, testMode) {
  if (testMode) {
    return {
      outcome: 'sent',
      transport: 'discord_webhook',
      http_status: 200,
      response_body_preview: 'test_mode_no_post',
      test_mode: true,
    };
  }

  try {
    const response = UrlFetchApp.fetch(String(webhookUrl), {
      method: 'post',
      contentType: 'application/json',
      payload: JSON.stringify(payload || {}),
      muteHttpExceptions: true,
    });
    const code = Number(response.getResponseCode());
    const body = truncateForLog_(response.getContentText(), 300);

    return {
      outcome: code >= 200 && code < 300 ? 'sent' : 'notify_http_failed',
      transport: 'discord_webhook',
      http_status: code,
      response_body_preview: body,
      test_mode: false,
    };
  } catch (error) {
    return {
      outcome: 'notify_http_failed',
      transport: 'discord_webhook',
      http_status: null,
      response_body_preview: truncateForLog_(String(error && error.message ? error.message : error), 300),
      test_mode: false,
    };
  }
}

function truncateForLog_(value, maxLen) {
  const text = String(value || '');
  const limit = Number(maxLen || 300);
  if (text.length <= limit) return text;
  return text.slice(0, Math.max(0, limit - 3)) + '...';
}

function buildSignalHash_(eventId, market, side, modelVersion) {
  return [eventId || '', market || '', side || '', modelVersion || ''].join('|');
}

function oddsPriceToImpliedProbability_(price) {
  const p = Number(price);
  if (!Number.isFinite(p) || p === 0) return null;
  if (p > 0) return roundNumber_(100 / (p + 100), 4);
  return roundNumber_(Math.abs(p) / (Math.abs(p) + 100), 4);
}

function estimateFairProbability_(marketProbability, competitionTier, playerStatsBundle, reasonCodes) {
  const tierBump = {
    GRAND_SLAM: 0.012,
    WTA_1000: 0.01,
    WTA_500: 0.008,
    WTA_125: 0.005,
  };
  const underdogBump = marketProbability < 0.5 ? 0.01 : -0.005;
  const statsBump = combinePlayerStatsFeatureBump_(playerStatsBundle, reasonCodes || {});
  const fair = marketProbability + underdogBump + (tierBump[competitionTier] || 0.005) + statsBump;
  return roundNumber_(Math.max(0.02, Math.min(0.98, fair)), 4);
}

function classifyEdgeAndStake_(edgeValue, config) {
  if (edgeValue >= config.EDGE_THRESHOLD_STRONG) {
    return { edge_tier: 'STRONG', stake_units: config.STAKE_UNITS_STRONG };
  }
  if (edgeValue >= config.EDGE_THRESHOLD_MED) {
    return { edge_tier: 'MED', stake_units: config.STAKE_UNITS_MED };
  }
  if (edgeValue >= config.EDGE_THRESHOLD_SMALL) {
    return { edge_tier: 'SMALL', stake_units: config.STAKE_UNITS_SMALL };
  }
  if (edgeValue >= config.EDGE_THRESHOLD_MICRO) {
    return { edge_tier: 'MICRO', stake_units: config.STAKE_UNITS_MICRO };
  }
  return { edge_tier: 'NONE', stake_units: 0 };
}
