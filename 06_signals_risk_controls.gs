function stageFetchPlayerStats(runId, config, oddsEvents, matchRows) {
  const start = Date.now();
  const source = 'derived_player_stats_v1';
  const reasonCounts = {
    stats_enriched: 0,
    stats_missing_player_a: 0,
    stats_missing_player_b: 0,
  };

  const matchByOddsEventId = {};
  matchRows.forEach((row) => {
    matchByOddsEventId[row.odds_event_id] = row;
  });

  const rows = [];
  const byOddsEventId = {};

  oddsEvents.forEach((event) => {
    const match = matchByOddsEventId[event.event_id];
    if (!match || !match.schedule_event_id) return;

    const playerA = canonicalizePlayerName_(event.player_1, {});
    const playerB = canonicalizePlayerName_(event.player_2, {});
    const featureTimestamp = event.odds_updated_time.toISOString();

    const playerAStats = computePseudoPlayerStats_(playerA, event, match, 'a');
    const playerBStats = computePseudoPlayerStats_(playerB, event, match, 'b');

    rows.push(buildRawPlayerStatsRow_(event.event_id, playerA, source, featureTimestamp, playerAStats));
    rows.push(buildRawPlayerStatsRow_(event.event_id, playerB, source, featureTimestamp, playerBStats));

    if (playerAStats.has_stats) reasonCounts.stats_enriched += 1;
    else reasonCounts.stats_missing_player_a += 1;

    if (playerBStats.has_stats) reasonCounts.stats_enriched += 1;
    else reasonCounts.stats_missing_player_b += 1;

    byOddsEventId[event.event_id] = {
      source,
      feature_timestamp: featureTimestamp,
      player_a: {
        canonical_name: playerA,
        features: playerAStats.features,
        has_stats: playerAStats.has_stats,
      },
      player_b: {
        canonical_name: playerB,
        features: playerBStats.features,
        has_stats: playerBStats.has_stats,
      },
    };
  });

  const summary = buildStageSummary_(runId, 'stageFetchPlayerStats', start, {
    input_count: oddsEvents.length,
    output_count: rows.length,
    provider: source,
    api_credit_usage: 0,
    reason_codes: reasonCounts,
  });

  return {
    rows,
    byOddsEventId,
    summary,
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
    updated_at: new Date().toISOString(),
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
    reasonCodes.stats_fallback_model_used = (reasonCodes.stats_fallback_model_used || 0) + 1;
    return 0;
  }

  const rankingDiff = (playerB.features.ranking - playerA.features.ranking) / 300;
  const recentFormDiff = (playerA.features.recent_form || 0) - (playerB.features.recent_form || 0);
  const surfaceDiff = (playerA.features.surface_win_rate || 0) - (playerB.features.surface_win_rate || 0);
  const serveReturnDiff = ((playerA.features.hold_pct || 0) - (playerB.features.hold_pct || 0))
    + ((playerA.features.break_pct || 0) - (playerB.features.break_pct || 0));

  return roundNumber_((rankingDiff * 0.25) + (recentFormDiff * 0.3) + (surfaceDiff * 0.25) + (serveReturnDiff * 0.2), 4);
}

function stageGenerateSignals(runId, config, oddsEvents, matchRows, playerStatsByOddsEventId) {
  const start = Date.now();
  const nowMs = Date.now();
  const rows = [];
  const reasonCounts = {
    sent: 0,
    duplicate_suppressed: 0,
    cooldown_suppressed: 0,
    edge_below_threshold: 0,
    too_close_to_start_skip: 0,
    stale_odds_skip: 0,
    notify_http_failed: 0,
    notify_missing_config: 0,
  };
  const signalState = getSignalState_();
  const seenHashesThisRun = {};
  const matchByOddsEventId = {};
  matchRows.forEach((row) => {
    matchByOddsEventId[row.odds_event_id] = row;
  });

  oddsEvents.forEach((event) => {
    const match = matchByOddsEventId[event.event_id];
    if (!match || !match.schedule_event_id) return;

    const statsBundle = (playerStatsByOddsEventId || {})[event.event_id] || null;
    const modelVersion = statsBundle && statsBundle.player_a && statsBundle.player_b
      && statsBundle.player_a.has_stats && statsBundle.player_b.has_stats
      ? config.MODEL_VERSION
      : config.MODEL_VERSION + '_fallback';

    const impliedProbability = oddsPriceToImpliedProbability_(event.price);
    if (impliedProbability === null) return;

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
      reasonCounts.too_close_to_start_skip += 1;
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
      reasonCounts.stale_odds_skip += 1;
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
      reasonCounts.edge_below_threshold += 1;
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
      notifyDiagnostics = {
        run_id: runId,
        signal_hash: signalHash,
        notification_outcome: notifyOutcome,
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
        status: notifyOutcome === 'sent' ? 'success' : 'failed',
        reason_code: notifyOutcome,
        message: JSON.stringify(notifyDiagnostics),
      });
    }

    reasonCounts[notifyOutcome] = (reasonCounts[notifyOutcome] || 0) + 1;

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
  setStateValue_('LAST_SIGNAL_SNAPSHOTS', JSON.stringify({
    run_id: runId,
    generated_at: new Date().toISOString(),
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
  setStateValue_('LAST_NOTIFY_DELIVERY_DIAGNOSTICS', JSON.stringify({
    run_id: runId,
    generated_at: new Date().toISOString(),
    deliveries: deliveryDiagnostics,
  }));

  setStateValue_('LAST_SIGNAL_DECISIONS', JSON.stringify({
    run_id: runId,
    generated_at: new Date().toISOString(),
    reason_counts: reasonCounts,
  }));

  const summary = buildStageSummary_(runId, 'stageGenerateSignals', start, {
    input_count: matchRows.length,
    output_count: rows.length,
    provider: 'internal_signal_builder',
    api_credit_usage: 0,
    reason_codes: reasonCounts,
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
    commence_time: event.commence_time.toISOString(),
    odds_updated_time: event.odds_updated_time.toISOString(),
    created_at: new Date().toISOString(),
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
  setStateValue_('SIGNAL_GUARD_STATE', JSON.stringify({
    updated_at: new Date().toISOString(),
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
      outcome: 'sent',
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
