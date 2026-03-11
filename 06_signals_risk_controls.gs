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
    low_confidence_stats_scored: 0,
    full_confidence_stats_scored: 0,
    null_features_low_confidence_scored: 0,
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
  const statsCompleteness = summarizePlayerStatsCompleteness_(statsByPlayer);
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
      source_used: playerAStats.source_used || playerBStats.source_used || source,
      stats_provider_unavailable: providerUnavailable,
      stats_fallback_mode: providerNullFeatures ? 'null_features' : '',
      stats_confidence: resolveStatsBundleConfidence_(playerAStats, playerBStats, providerUnavailable, providerNullFeatures),
      synthetic_schedule_seed: !!event.synthetic_schedule_seed,
      feature_timestamp: featureTimestamp,
      player_a: {
        canonical_name: playerA,
        features: playerAStats.features,
        has_stats: playerAStats.has_stats,
        stats_confidence: resolvePlayerStatsConfidence_(playerAStats),
        stats_fallback_mode: playerAStats.stats_fallback_mode || '',
        provenance: playerAStats.provenance || source,
        source_used: playerAStats.source_used || source,
      },
      player_b: {
        canonical_name: playerB,
        features: playerBStats.features,
        has_stats: playerBStats.has_stats,
        stats_confidence: resolvePlayerStatsConfidence_(playerBStats),
        stats_fallback_mode: playerBStats.stats_fallback_mode || '',
        provenance: playerBStats.provenance || source,
        source_used: playerBStats.source_used || source,
      },
    };
  });

  const summary = buildStageSummary_(runId, 'stageFetchPlayerStats', start, {
    input_count: statsInputEvents.length,
    output_count: rows.length,
    provider: source,
    api_credit_usage: Number(statsBatch.api_credit_usage || 0),
    reason_codes: reasonCounts,
    reason_metadata: {
      players_with_non_null_stats: Number(statsCompleteness.players_with_non_null_stats || 0),
      players_with_null_only_stats: Number(statsCompleteness.players_with_null_only_stats || 0),
      player_stats_data_available: !!statsCompleteness.has_stats,
    },
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
        source_used: providerStats.source_used || 'player_stats_provider_v1',
        fallback_mode: providerStats.fallback_mode || '',
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
      source_used: providerStats.source_used || 'player_stats_provider_v1',
      fallback_mode: providerStats.fallback_mode || 'null_features',
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
    pseudo.source_used = 'derived_player_stats_v1_fallback';
    return pseudo;
  }

  return {
    has_stats: false,
    stats_fallback_mode: 'missing_row',
    provenance: 'player_stats_provider_v1',
    source_used: 'player_stats_provider_v1',
    fallback_mode: 'missing_row',
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
    stats_fallback_mode: payload.stats_fallback_mode || payload.fallback_mode || '',
    source_used: payload.source_used || source,
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


function resolveStatsCompletenessKeys_() {
  const coreKeys = Array.isArray(PLAYER_STATS_COMPLETENESS_KEYS) && PLAYER_STATS_COMPLETENESS_KEYS.length
    ? PLAYER_STATS_COMPLETENESS_KEYS.slice()
    : ['ranking', 'recent_form', 'surface_win_rate', 'hold_pct', 'break_pct'];
  const richKeys = [
    'recent_form_last_10',
    'surface_hold_pct',
    'surface_break_pct',
    'surface_recent_form',
    'first_serve_in_pct',
    'first_serve_points_won_pct',
    'second_serve_points_won_pct',
    'return_points_won_pct',
    'bp_saved_pct',
    'bp_conv_pct',
    'dr',
    'tpw_pct',
  ];
  return {
    core: coreKeys,
    rich: richKeys,
    all: coreKeys.concat(richKeys),
  };
}

function resolvePlayerStatsConfidence_(playerPayload) {
  const payload = playerPayload || {};
  const keys = resolveStatsCompletenessKeys_().all;
  const features = payload.features || {};
  const total = keys.length;
  if (!total) return 0;
  let nonNull = 0;
  for (let i = 0; i < keys.length; i += 1) {
    const value = features[keys[i]];
    if (value !== null && value !== undefined && value !== '') nonNull += 1;
  }

  let confidence = nonNull / total;
  const fallbackMode = String(payload.stats_fallback_mode || '').trim();
  if (!payload.has_stats) {
    confidence *= fallbackMode === 'null_features' ? 0.2 : 0;
  } else if (fallbackMode === 'null_features') {
    confidence *= 0.4;
  } else if (fallbackMode) {
    confidence *= 0.75;
  }

  return roundNumber_(Math.max(0, Math.min(1, confidence)), 4);
}

function resolveStatsBundleConfidence_(playerAStats, playerBStats, providerUnavailable, providerNullFeatures) {
  const playerAConfidence = resolvePlayerStatsConfidence_(playerAStats);
  const playerBConfidence = resolvePlayerStatsConfidence_(playerBStats);
  let confidence = (playerAConfidence + playerBConfidence) / 2;
  if (providerUnavailable) confidence *= 0.1;
  if (providerNullFeatures) confidence *= 0.5;
  return roundNumber_(Math.max(0, Math.min(1, confidence)), 4);
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

  const playerAFeatures = playerA.features || {};
  const playerBFeatures = playerB.features || {};

  const rankingDiff = ((playerBFeatures.ranking || 0) - (playerAFeatures.ranking || 0)) / 300;
  const recentFormDiff = (playerAFeatures.recent_form || 0) - (playerBFeatures.recent_form || 0);
  const recentFormLast10Diff = fallbackFeatureDiff_(playerAFeatures, playerBFeatures, 'recent_form_last_10', 'recent_form');
  const surfaceDiff = (playerAFeatures.surface_win_rate || 0) - (playerBFeatures.surface_win_rate || 0);
  const serveReturnDiff = ((playerAFeatures.hold_pct || 0) - (playerBFeatures.hold_pct || 0))
    + ((playerAFeatures.break_pct || 0) - (playerBFeatures.break_pct || 0));
  const surfaceServeReturnDiff = fallbackCompositeDiff_(
    playerAFeatures,
    playerBFeatures,
    ['surface_hold_pct', 'surface_break_pct'],
    ['hold_pct', 'break_pct']
  );
  const surfaceRecentFormDiff = fallbackFeatureDiff_(playerAFeatures, playerBFeatures, 'surface_recent_form', 'recent_form');
  const firstServeInDiff = (playerAFeatures.first_serve_in_pct || 0) - (playerBFeatures.first_serve_in_pct || 0);
  const firstServePointsWonDiff = (playerAFeatures.first_serve_points_won_pct || 0) - (playerBFeatures.first_serve_points_won_pct || 0);
  const secondServePointsWonDiff = (playerAFeatures.second_serve_points_won_pct || 0) - (playerBFeatures.second_serve_points_won_pct || 0);
  const returnPointsWonDiff = (playerAFeatures.return_points_won_pct || 0) - (playerBFeatures.return_points_won_pct || 0);
  const bpSavedDiff = (playerAFeatures.bp_saved_pct || 0) - (playerBFeatures.bp_saved_pct || 0);
  const bpConvDiff = (playerAFeatures.bp_conv_pct || 0) - (playerBFeatures.bp_conv_pct || 0);
  const dominanceRatioDiff = (playerAFeatures.dr || 0) - (playerBFeatures.dr || 0);
  const totalPointsWonDiff = (playerAFeatures.tpw_pct || 0) - (playerBFeatures.tpw_pct || 0);

  const rawBump = roundNumber_(
    (rankingDiff * 0.2)
    + (recentFormDiff * 0.17)
    + (recentFormLast10Diff * 0.03)
    + (surfaceDiff * 0.14)
    + (serveReturnDiff * 0.13)
    + (surfaceServeReturnDiff * 0.02)
    + (surfaceRecentFormDiff * 0.01)
    + (firstServeInDiff * 0.07)
    + (firstServePointsWonDiff * 0.07)
    + (secondServePointsWonDiff * 0.06)
    + (returnPointsWonDiff * 0.04)
    + (bpSavedDiff * 0.03)
    + (bpConvDiff * 0.02)
    + (dominanceRatioDiff * 0.005)
    + (totalPointsWonDiff * 0.005),
    4
  );

  const confidence = Number(statsBundle.stats_confidence);
  const resolvedConfidence = Number.isFinite(confidence)
    ? Math.max(0, Math.min(1, confidence))
    : resolveStatsBundleConfidence_(
      playerA,
      playerB,
      statsBundle.stats_provider_unavailable === true,
      statsBundle.stats_fallback_mode === 'null_features'
    );

  if (resolvedConfidence < 0.999) {
    reasonCodes.low_confidence_stats_scored = (reasonCodes.low_confidence_stats_scored || 0) + 1;
    if (resolvedConfidence <= 0.05) {
      reasonCodes.null_features_low_confidence_scored = (reasonCodes.null_features_low_confidence_scored || 0) + 1;
    }
  } else {
    reasonCodes.full_confidence_stats_scored = (reasonCodes.full_confidence_stats_scored || 0) + 1;
  }

  return roundNumber_(rawBump * resolvedConfidence, 4);
}


function fallbackFeatureDiff_(playerAFeatures, playerBFeatures, preferredKey, fallbackKey) {
  const playerAValue = resolveFeatureWithFallback_(playerAFeatures, preferredKey, fallbackKey);
  const playerBValue = resolveFeatureWithFallback_(playerBFeatures, preferredKey, fallbackKey);
  return playerAValue - playerBValue;
}

function fallbackCompositeDiff_(playerAFeatures, playerBFeatures, preferredKeys, fallbackKeys) {
  let diff = 0;
  for (let i = 0; i < preferredKeys.length; i += 1) {
    diff += fallbackFeatureDiff_(playerAFeatures, playerBFeatures, preferredKeys[i], fallbackKeys[i]);
  }
  return diff;
}

function resolveFeatureWithFallback_(features, preferredKey, fallbackKey) {
  const preferred = Number(features && features[preferredKey]);
  if (Number.isFinite(preferred)) return preferred;
  const fallback = Number(features && features[fallbackKey]);
  return Number.isFinite(fallback) ? fallback : 0;
}

function stageGenerateSignals(runId, config, oddsEvents, matchRows, playerStatsByOddsEventId, stageMeta) {
  const start = Date.now();
  const nowMs = Date.now();
  const upstreamGateReason = stageMeta && stageMeta.upstream_gate_reason ? String(stageMeta.upstream_gate_reason) : '';
  const fallbackOnlyMode = upstreamGateReason === 'stats_zero_coverage';
  const rows = [];
  const sampledDecisions = [];
  let lastH2hDecision = null;
  const h2hDecisionCounts = { h2h_applied: 0, h2h_low_sample: 0, h2h_unavailable: 0 };
  const sampledDecisionLimit = Number(config.SIGNAL_DECISION_SAMPLE_LIMIT || 50);
  let processedCandidateCount = 0;
  const reasonCounts = {
    sent: 0,
    missing_match: 0,
    missing_stats: 0,
    null_features_fallback_scored: 0,
    low_confidence_stats_scored: 0,
    full_confidence_stats_scored: 0,
    null_features_low_confidence_scored: 0,
    invalid_features: 0,
    notify_disabled: 0,
    duplicate_suppressed: 0,
    cooldown_suppressed: 0,
    edge_below_threshold: 0,
    too_close_to_start_skip: 0,
    stale_odds_skip: 0,
    line_drift_exceeded: 0,
    edge_decay_exceeded: 0,
    liquidity_too_low: 0,
    notify_http_failed: 0,
    notify_missing_config: 0,
    fallback_only: 0,
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
      const detailWithH2h = Object.assign({}, detail || {});
      if (lastH2hDecision) {
        detailWithH2h.h2h_decision = lastH2hDecision;
      }
      sampledDecisions.push({
        odds_event_id: event && event.event_id ? event.event_id : '',
        schedule_event_id: (match && match.schedule_event_id) || '',
        decision_reason_code: decisionReasonCode,
        market: event && event.market ? event.market : '',
        side: event && event.outcome ? event.outcome : '',
        bookmaker: event && event.bookmaker ? event.bookmaker : '',
        price: event && event.price,
        detail: detailWithH2h,
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
    const enrichedStatsBundle = attachH2hStatsContext_(statsBundle, event);
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

    const statsConfidence = Number(statsBundle && statsBundle.stats_confidence);
    const resolvedStatsConfidence = Number.isFinite(statsConfidence)
      ? statsConfidence
      : resolveStatsBundleConfidence_(
        statsBundle && statsBundle.player_a,
        statsBundle && statsBundle.player_b,
        !!(statsBundle && statsBundle.stats_provider_unavailable),
        String((statsBundle && statsBundle.stats_fallback_mode) || '') === 'null_features'
      );

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
      const modelProbabilityTooClose = estimateFairProbability_(impliedProbability, match.competition_tier, enrichedStatsBundle, reasonCounts, config);
      lastH2hDecision = resolveH2hProbabilityBump_(enrichedStatsBundle, config || {});
      recordH2hDecisionCount_(h2hDecisionCounts, lastH2hDecision);
      rows.push(buildSignalRow_(runId, config, event, match, {
        notification_outcome: 'too_close_to_start_skip',
        model_probability: modelProbabilityTooClose,
        market_implied_probability: impliedProbability,
        edge_value: 0,
        edge_tier: 'NONE',
        stake_units: 0,
        signal_hash: buildSignalHash_(event.event_id, event.market, event.outcome, modelVersion),
        model_version: modelVersion,
        stats_confidence: resolvedStatsConfidence,
      }));
      captureDecision_(event, match, 'too_close_to_start_skip', {
        scored: true,
        stats_confidence: resolvedStatsConfidence,
      });
      return;
    }

    const staleThresholdMs = config.STALE_ODDS_WINDOW_MIN * 60000;
    if (nowMs - event.odds_updated_time.getTime() > staleThresholdMs) {
      const modelProbabilityStale = estimateFairProbability_(impliedProbability, match.competition_tier, enrichedStatsBundle, reasonCounts, config);
      lastH2hDecision = resolveH2hProbabilityBump_(enrichedStatsBundle, config || {});
      recordH2hDecisionCount_(h2hDecisionCounts, lastH2hDecision);
      rows.push(buildSignalRow_(runId, config, event, match, {
        notification_outcome: 'stale_odds_skip',
        model_probability: modelProbabilityStale,
        market_implied_probability: impliedProbability,
        edge_value: 0,
        edge_tier: 'NONE',
        stake_units: 0,
        signal_hash: buildSignalHash_(event.event_id, event.market, event.outcome, modelVersion),
        model_version: modelVersion,
        stats_confidence: resolvedStatsConfidence,
      }));
      captureDecision_(event, match, 'stale_odds_skip', {
        scored: true,
        stats_confidence: resolvedStatsConfidence,
      });
      return;
    }

    const modelProbability = estimateFairProbability_(impliedProbability, match.competition_tier, enrichedStatsBundle, reasonCounts, config);
    lastH2hDecision = resolveH2hProbabilityBump_(enrichedStatsBundle, config || {});
    recordH2hDecisionCount_(h2hDecisionCounts, lastH2hDecision);
    const edgeValue = roundNumber_(modelProbability - impliedProbability, 4);
    const edgeTierAndStake = classifyEdgeAndStake_(edgeValue, config);
    const signalHash = buildSignalHash_(event.event_id, event.market, event.outcome, modelVersion);
    const preActionGate = evaluatePreActionRiskGuard_(event, config, nowMs);

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
        stats_confidence: resolvedStatsConfidence,
      }));
      captureDecision_(event, match, 'edge_below_threshold', {
        scored: true,
        stats_confidence: resolvedStatsConfidence,
      });
      return;
    }

    if (!preActionGate.is_tradable) {
      captureDecision_(event, match, preActionGate.reason_code, {
        scored: true,
        model_probability: modelProbability,
        market_implied_probability: impliedProbability,
        edge_value: edgeValue,
        edge_tier: edgeTierAndStake.edge_tier,
        stake_units: edgeTierAndStake.stake_units,
        stats_confidence: resolvedStatsConfidence,
        pre_action_guard: preActionGate,
      });

      rows.push(buildSignalRow_(runId, config, event, match, {
        notification_outcome: preActionGate.reason_code,
        model_probability: modelProbability,
        market_implied_probability: impliedProbability,
        edge_value: edgeValue,
        edge_tier: 'NONE',
        stake_units: 0,
        signal_hash: signalHash,
        model_version: modelVersion,
        stats_confidence: resolvedStatsConfidence,
        signal_delivery_mode: 'risk_guard_non_tradable',
        notification_metadata: {
          non_tradable: true,
          pre_action_guard: preActionGate,
        },
      }));
      return;
    }

    const notifyDecision = maybeNotifySignal_(signalState, seenHashesThisRun, signalHash, nowMs, config.SIGNAL_COOLDOWN_MIN);
    let notifyOutcome = notifyDecision.outcome;
    let notifyDiagnostics = null;

    if (notifyDecision.outcome === 'sent' && fallbackOnlyMode) {
      notifyOutcome = 'fallback_only';
    } else if (notifyDecision.outcome === 'sent') {
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
        rationale_context: {
          edge_value: edgeValue,
          model_probability: modelProbability,
          market_implied_probability: impliedProbability,
          stats_bundle: enrichedStatsBundle,
          stats_confidence: resolvedStatsConfidence,
          h2h_decision: lastH2hDecision,
        },
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
      stats_confidence: resolvedStatsConfidence,
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
      stats_confidence: resolvedStatsConfidence,
      signal_delivery_mode: fallbackOnlyMode ? 'fallback_only' : 'normal',
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
      stats_confidence: row.stats_confidence,
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
  const observedStatsConfidence = rows
    .map(function (row) { return Number(row.stats_confidence); })
    .filter(function (value) { return Number.isFinite(value); });

  const allDropReasons = Object.keys(reasonCounts)
    .filter(function (reasonCode) {
      return reasonCode !== 'sent'
        && reasonCode !== 'null_features_fallback_scored'
        && reasonCode !== 'stats_missing_player_a'
        && reasonCode !== 'stats_missing_player_b'
        && reasonCode !== 'stats_fallback_model_used'
        && reasonCode !== 'low_confidence_stats_scored'
        && reasonCode !== 'full_confidence_stats_scored'
        && reasonCode !== 'null_features_low_confidence_scored';
    })
    .reduce(function (sum, reasonCode) {
      return sum + Number(reasonCounts[reasonCode] || 0);
    }, 0);

  setStateValue_('LAST_SIGNAL_DECISIONS', JSON.stringify({
    run_id: runId,
    generated_at: signalDecisionsGeneratedAt.local,
    generated_at_utc: signalDecisionsGeneratedAt.utc,
    reason_counts: reasonCounts,
    h2h_decision_counts: h2hDecisionCounts,
    reason_counts_legacy: legacyReasonCounts,
    sampled_decisions: sampledDecisions,
    sampled_candidate_rows: sampledDecisions,
    input_count: oddsEvents.length,
    upstream_gate_reason: upstreamGateReason,
    explanatory_metadata: zeroInputExplanatory,
    processed_count: processedCandidateCount,
    all_drop_reasons: allDropReasons,
    stats_confidence_summary: {
      observed_count: observedStatsConfidence.length,
      average: observedStatsConfidence.length
        ? roundNumber_(observedStatsConfidence.reduce(function (sum, value) { return sum + value; }, 0) / observedStatsConfidence.length, 4)
        : null,
      minimum: observedStatsConfidence.length ? Math.min.apply(null, observedStatsConfidence) : null,
      maximum: observedStatsConfidence.length ? Math.max.apply(null, observedStatsConfidence) : null,
    },
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

function evaluatePreActionRiskGuard_(event, config, nowMs) {
  const thresholds = {
    max_current_vs_open_line_delta: Number(config.MAX_CURRENT_VS_OPEN_LINE_DELTA || 0),
    max_minutes_since_open_snapshot: Number(config.MAX_MINUTES_SINCE_OPEN_SNAPSHOT || 0),
    min_book_count: Number(config.MIN_BOOK_COUNT || 0),
    min_liquidity: Number(config.MIN_LIQUIDITY || 0),
  };

  const currentVsOpenLineDelta = resolveCurrentVsOpenLineDelta_(event);
  const minutesSinceOpenSnapshot = resolveMinutesSinceOpenSnapshot_(event, nowMs);
  const bookCount = resolveBookCount_(event);
  const liquidity = resolveLiquidity_(event);

  const checks = [
    {
      reason_code: 'line_drift_exceeded',
      threshold_key: 'max_current_vs_open_line_delta',
      observed: currentVsOpenLineDelta,
      threshold: thresholds.max_current_vs_open_line_delta,
      compare: function (observed, threshold) { return observed > threshold; },
    },
    {
      reason_code: 'edge_decay_exceeded',
      threshold_key: 'max_minutes_since_open_snapshot',
      observed: minutesSinceOpenSnapshot,
      threshold: thresholds.max_minutes_since_open_snapshot,
      compare: function (observed, threshold) { return observed > threshold; },
    },
    {
      reason_code: 'liquidity_too_low',
      threshold_key: 'min_book_count',
      observed: bookCount,
      threshold: thresholds.min_book_count,
      compare: function (observed, threshold) { return observed < threshold; },
    },
    {
      reason_code: 'liquidity_too_low',
      threshold_key: 'min_liquidity',
      observed: liquidity,
      threshold: thresholds.min_liquidity,
      compare: function (observed, threshold) { return observed < threshold; },
    },
  ];

  for (let i = 0; i < checks.length; i += 1) {
    const check = checks[i];
    const thresholdEnabled = Number.isFinite(check.threshold) && check.threshold > 0;
    const observedAvailable = Number.isFinite(check.observed);
    if (!thresholdEnabled || !observedAvailable) continue;
    if (check.compare(check.observed, check.threshold)) {
      return {
        is_tradable: false,
        reason_code: check.reason_code,
        threshold_key: check.threshold_key,
        threshold: check.threshold,
        observed: check.observed,
        metrics: {
          current_vs_open_line_delta: Number.isFinite(currentVsOpenLineDelta) ? roundNumber_(currentVsOpenLineDelta, 4) : null,
          minutes_since_open_snapshot: Number.isFinite(minutesSinceOpenSnapshot) ? roundNumber_(minutesSinceOpenSnapshot, 2) : null,
          book_count: Number.isFinite(bookCount) ? bookCount : null,
          liquidity: Number.isFinite(liquidity) ? liquidity : null,
        },
      };
    }
  }

  return {
    is_tradable: true,
    reason_code: '',
  };
}

function resolveCurrentVsOpenLineDelta_(event) {
  const directDelta = Number(event && event.current_vs_open_line_delta);
  if (Number.isFinite(directDelta)) return Math.abs(directDelta);

  const openPriceCandidates = [
    Number(event && event.open_price),
    Number(event && event.opening_price),
    Number(event && event.open_odds_price),
  ];
  const openPrice = openPriceCandidates.find(function (value) { return Number.isFinite(value); });
  const currentPrice = Number(event && event.price);
  if (!Number.isFinite(openPrice) || !Number.isFinite(currentPrice)) return NaN;

  return Math.abs(oddsPriceToImpliedProbability_(openPrice) - oddsPriceToImpliedProbability_(currentPrice));
}

function resolveMinutesSinceOpenSnapshot_(event, nowMs) {
  const directMinutes = Number(event && event.minutes_since_open_snapshot);
  if (Number.isFinite(directMinutes)) return directMinutes;

  const openSnapshotMs = Number(event && event.open_timestamp_epoch_ms);
  if (Number.isFinite(openSnapshotMs) && openSnapshotMs > 0) {
    return (nowMs - openSnapshotMs) / 60000;
  }

  if (event && event.open_timestamp instanceof Date) {
    return (nowMs - event.open_timestamp.getTime()) / 60000;
  }

  return NaN;
}

function resolveBookCount_(event) {
  const explicitBookCount = Number(event && event.book_count);
  if (Number.isFinite(explicitBookCount)) return explicitBookCount;

  const bookmakerCount = Number(event && event.bookmaker_count);
  if (Number.isFinite(bookmakerCount)) return bookmakerCount;

  const bookmakerKeys = event && event.bookmaker_keys_considered;
  return Array.isArray(bookmakerKeys) ? bookmakerKeys.length : NaN;
}

function resolveLiquidity_(event) {
  const candidates = [
    Number(event && event.liquidity),
    Number(event && event.market_liquidity),
    Number(event && event.available_liquidity),
  ];
  const value = candidates.find(function (candidate) { return Number.isFinite(candidate); });
  return Number.isFinite(value) ? value : NaN;
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
    stats_confidence: Number.isFinite(Number(detail.stats_confidence)) ? Number(detail.stats_confidence) : null,
    signal_hash: detail.signal_hash,
    notification_outcome: detail.notification_outcome,
    signal_delivery_mode: detail.signal_delivery_mode || 'normal',
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
  const commenceLocal = toIso_(payload.commence_time);
  const commenceUtc = payload.commence_time ? new Date(payload.commence_time).toISOString() : '';
  const rationaleParagraph = buildSignalRationaleParagraph_(payload.rationale_context || payload);
  return [
    '🎾 **WTA Edge Signal**',
    '📌 **' + payload.side + '** (' + payload.market + ') @ **' + payload.bookmaker + '**',
    '📊 Edge: **' + payload.edge_value + '** (' + payload.edge_tier + ') | Stake: **' + payload.stake_units + 'u**',
    '🤖 Model: **' + payload.model_probability + '** vs Market: **' + payload.market_implied_probability + '**',
    '🏟️ Tier: **' + payload.competition_tier + '**',
    '🕒 Start: **' + commenceLocal + '** (' + TIMESTAMP_TIMEZONE.ID + ')',
    '🌐 UTC: **' + commenceUtc + '**',
    '🆔 Run: `' + runId + '` | Event: `' + payload.odds_event_id + '`',
    '🧬 Signal: `' + signalHash + '`',
    '',
    '**Why this edge**',
    rationaleParagraph,
  ].join('\n');
}

function buildSignalRationaleParagraph_(context) {
  const base = context || {};
  const edgeValue = toFiniteNumberOrNull_(base.edge_value);
  const modelProbability = toFiniteNumberOrNull_(base.model_probability);
  const marketProbability = toFiniteNumberOrNull_(base.market_implied_probability);
  const statsConfidence = toFiniteNumberOrNull_(base.stats_confidence);
  const statsBundle = base.stats_bundle || {};
  const h2hDecision = base.h2h_decision || {};

  const hasModelAndMarket = Number.isFinite(modelProbability) && Number.isFinite(marketProbability);
  const edgePoints = Number.isFinite(edgeValue)
    ? roundNumber_(edgeValue * 100, 2)
    : (hasModelAndMarket ? roundNumber_((modelProbability - marketProbability) * 100, 2) : null);

  const sentences = [];
  if (hasModelAndMarket && Number.isFinite(edgePoints)) {
    sentences.push(
      'Model win probability is ' + roundNumber_(modelProbability * 100, 1) + '% versus market ' + roundNumber_(marketProbability * 100, 1)
      + '%, creating a ' + (edgePoints >= 0 ? '+' : '') + edgePoints + 'pp gap.'
    );
  } else {
    sentences.push('Model signals an edge over current market pricing after baseline tier and underdog adjustments.');
  }

  const contributorNotes = resolveTopPositiveFeatureContributors_(statsBundle, 2);
  if (contributorNotes.length) {
    sentences.push('Top stat drivers were ' + contributorNotes.join(' and ') + '.');
  } else {
    sentences.push('Player feature coverage is limited, so this leans more on market baseline inputs than granular stat edges.');
  }

  if (h2hDecision && h2hDecision.applied && Number(h2hDecision.bump) !== 0) {
    const h2hBumpPoints = roundNumber_(Number(h2hDecision.bump) * 100, 2);
    const sampleSize = Number(h2hDecision.sample_size || 0);
    sentences.push('Head-to-head added ' + (h2hBumpPoints >= 0 ? '+' : '') + h2hBumpPoints + 'pp from a ' + sampleSize + '-match sample.');
  }

  const qualifier = resolveStatsConfidenceQualifier_(statsConfidence);
  if (qualifier.label) {
    sentences.push('Stats confidence is ' + qualifier.label + ' (' + qualifier.value + '), so weighting is ' + qualifier.weighting + '.');
  } else {
    sentences.push('Stats confidence is unavailable, so this explanation defaults to conservative weighting language.');
  }

  const bounded = sentences.join(' ');
  return truncateForLog_(bounded, 680);
}


function toFiniteNumberOrNull_(value) {
  if (value === null || value === undefined || value === '') return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function resolveStatsConfidenceQualifier_(confidence) {
  if (!Number.isFinite(confidence)) {
    return { label: '', value: '', weighting: '' };
  }
  const bounded = Math.max(0, Math.min(1, confidence));
  if (bounded >= 0.8) {
    return { label: 'high', value: roundNumber_(bounded, 2), weighting: 'fully engaged' };
  }
  if (bounded >= 0.45) {
    return { label: 'moderate', value: roundNumber_(bounded, 2), weighting: 'partially scaled' };
  }
  return { label: 'limited', value: roundNumber_(bounded, 2), weighting: 'heavily scaled down' };
}

function resolveTopPositiveFeatureContributors_(statsBundle, maxContributors) {
  if (!statsBundle || !statsBundle.player_a || !statsBundle.player_b) return [];
  const playerA = statsBundle.player_a;
  const playerB = statsBundle.player_b;
  if (!playerA.has_stats || !playerB.has_stats) return [];

  const playerAFeatures = playerA.features || {};
  const playerBFeatures = playerB.features || {};
  const confidence = Number.isFinite(Number(statsBundle.stats_confidence))
    ? Math.max(0, Math.min(1, Number(statsBundle.stats_confidence)))
    : 1;

  const contributors = [
    {
      label: 'stronger ranking profile',
      contribution: (((playerBFeatures.ranking || 0) - (playerAFeatures.ranking || 0)) / 300) * 0.2 * confidence,
    },
    {
      label: 'better recent form',
      contribution: ((playerAFeatures.recent_form || 0) - (playerBFeatures.recent_form || 0)) * 0.17 * confidence,
    },
    {
      label: 'surface win-rate edge',
      contribution: ((playerAFeatures.surface_win_rate || 0) - (playerBFeatures.surface_win_rate || 0)) * 0.14 * confidence,
    },
    {
      label: 'serve/return pressure edge',
      contribution: ((((playerAFeatures.hold_pct || 0) - (playerBFeatures.hold_pct || 0))
        + ((playerAFeatures.break_pct || 0) - (playerBFeatures.break_pct || 0))) * 0.13) * confidence,
    },
    {
      label: 'first-serve quality edge',
      contribution: ((playerAFeatures.first_serve_points_won_pct || 0) - (playerBFeatures.first_serve_points_won_pct || 0)) * 0.07 * confidence,
    },
    {
      label: 'second-serve resilience edge',
      contribution: ((playerAFeatures.second_serve_points_won_pct || 0) - (playerBFeatures.second_serve_points_won_pct || 0)) * 0.06 * confidence,
    },
  ];

  return contributors
    .filter(function (item) { return item.contribution > 0.0001; })
    .sort(function (left, right) { return right.contribution - left.contribution; })
    .slice(0, Math.max(1, Number(maxContributors || 2)))
    .map(function (item) {
      return item.label + ' (+' + roundNumber_(item.contribution * 100, 2) + 'pp)';
    });
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



function recordH2hDecisionCount_(counts, decision) {
  const status = decision && decision.status ? String(decision.status) : 'h2h_unavailable';
  if (status === 'h2h_applied' || status === 'h2h_low_sample' || status === 'h2h_unavailable') {
    counts[status] = (counts[status] || 0) + 1;
    return;
  }
  counts.h2h_unavailable = (counts.h2h_unavailable || 0) + 1;
}

function attachH2hStatsContext_(statsBundle, event) {
  const base = statsBundle && typeof statsBundle === 'object' ? statsBundle : {};
  return Object.assign({}, base, {
    h2h: {
      p1_wins: pickNumericEventValue_(event, ['h2h_p1_wins']),
      p2_wins: pickNumericEventValue_(event, ['h2h_p2_wins']),
      total_matches: pickNumericEventValue_(event, ['h2h_total_matches']),
      source: String(((event || {}).h2h_source || (base.h2h || {}).source || '')).trim(),
      mode_reason_code: String((base.h2h_mode_reason_code || '')).trim(),
    },
  });
}

function pickNumericEventValue_(event, keys) {
  const source = event || {};
  const keyList = keys || [];
  for (let i = 0; i < keyList.length; i += 1) {
    const value = source[keyList[i]];
    if (value === '' || value === null || value === undefined) continue;
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function resolveH2hProbabilityBump_(playerStatsBundle, config) {
  const details = {
    applied: false,
    status: 'h2h_unavailable',
    reason_code: 'h2h_missing',
    bump: 0,
    sample_size: 0,
    source: '',
  };

  if (!config || config.H2H_BUMP_ENABLED === false) {
    details.status = 'h2h_disabled';
    details.reason_code = 'h2h_bump_disabled';
    return details;
  }

  const h2h = playerStatsBundle && playerStatsBundle.h2h ? playerStatsBundle.h2h : null;
  if (!h2h) {
    return details;
  }

  const modeReasonCode = String(h2h.mode_reason_code || '').trim();
  if (modeReasonCode === 'h2h_source_empty_table' || modeReasonCode === 'ta_h2h_empty_table') {
    details.status = 'h2h_unavailable';
    details.reason_code = 'h2h_source_empty_table';
    return details;
  }

  const p1Wins = Number(h2h.p1_wins);
  const p2Wins = Number(h2h.p2_wins);
  let totalMatches = Number(h2h.total_matches);
  if (!Number.isFinite(totalMatches) && Number.isFinite(p1Wins) && Number.isFinite(p2Wins)) {
    totalMatches = p1Wins + p2Wins;
  }

  if (!Number.isFinite(p1Wins) || !Number.isFinite(p2Wins) || !Number.isFinite(totalMatches) || totalMatches <= 0) {
    details.reason_code = 'h2h_missing';
    return details;
  }

  const minMatches = Math.max(1, Number(config.H2H_MIN_MATCHES || 0));
  details.sample_size = totalMatches;
  details.source = String(h2h.source || '');
  if (totalMatches < minMatches) {
    details.status = 'h2h_low_sample';
    details.reason_code = 'h2h_low_sample';
    return details;
  }

  const rawDelta = (p1Wins - p2Wins) / totalMatches;
  const maxAbs = Math.max(0, Number(config.H2H_MAX_ABS_BUMP || 0));
  const bounded = Math.max(-maxAbs, Math.min(maxAbs, rawDelta));

  details.applied = true;
  details.status = 'h2h_applied';
  details.reason_code = 'h2h_applied';
  details.bump = roundNumber_(bounded, 4);
  return details;
}

function oddsPriceToImpliedProbability_(price) {
  const p = Number(price);
  if (!Number.isFinite(p) || p === 0) return null;
  if (p > 0) return roundNumber_(100 / (p + 100), 4);
  return roundNumber_(Math.abs(p) / (Math.abs(p) + 100), 4);
}

function estimateFairProbability_(marketProbability, competitionTier, playerStatsBundle, reasonCodes, config) {
  const tierBump = {
    GRAND_SLAM: 0.012,
    WTA_1000: 0.01,
    WTA_500: 0.008,
    WTA_125: 0.005,
  };
  const underdogBump = marketProbability < 0.5 ? 0.01 : -0.005;
  const statsBump = combinePlayerStatsFeatureBump_(playerStatsBundle, reasonCodes || {});
  const h2hDecision = resolveH2hProbabilityBump_(playerStatsBundle, config || {});
  const fair = marketProbability
    + underdogBump
    + (tierBump[competitionTier] || 0.005)
    + statsBump
    + h2hDecision.bump;
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
