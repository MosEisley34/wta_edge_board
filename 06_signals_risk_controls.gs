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
    stats_out_of_cohort: 0,
    stats_rank_unknown: 0,
    stats_top100_filter_excluded: 0,
    stats_top100_fallback_applied: 0,
    stats_zero_coverage: 0,
  };

  const matchByOddsEventId = {};
  matchRows.forEach((row) => {
    matchByOddsEventId[row.odds_event_id] = row;
  });

  const rows = [];
  const byOddsEventId = {};
  const playersToFetch = [];
  const seedEvents = [];
  const playerSlotSourceLabels = {
    a: [],
    b: [],
  };

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
  const requestedUniquePlayers = [];
  const requestedPlayerSet = {};
  playersToFetch.forEach(function (playerName) {
    const canonicalName = String(playerName || '').trim();
    if (!canonicalName || requestedPlayerSet[canonicalName]) return;
    requestedPlayerSet[canonicalName] = true;
    requestedUniquePlayers.push(canonicalName);
  });
  const resolvedPlayerSet = {};
  Object.keys(statsByPlayer).forEach(function (playerName) {
    const canonicalName = String(playerName || '').trim();
    if (!canonicalName || !requestedPlayerSet[canonicalName]) return;
    resolvedPlayerSet[canonicalName] = true;
  });
  const unresolvedPlayers = requestedUniquePlayers.filter(function (playerName) {
    return !resolvedPlayerSet[playerName];
  });
  const requestedPlayerCount = requestedUniquePlayers.length;
  const resolvedPlayerCount = Object.keys(resolvedPlayerSet).length;
  const unresolvedPlayerCount = unresolvedPlayers.length;
  const overlapRatio = requestedPlayerCount > 0 ? resolvedPlayerCount / requestedPlayerCount : 0;
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

    const playerAStats = resolvePlayerStatsPayload_(playerA, statsByPlayer, providerUnavailable, event, effectiveMatch, 'a', reasonCounts, config);
    const playerBStats = resolvePlayerStatsPayload_(playerB, statsByPlayer, providerUnavailable, event, effectiveMatch, 'b', reasonCounts, config);
    playerSlotSourceLabels.a.push(String(playerAStats.source_used || source));
    playerSlotSourceLabels.b.push(String(playerBStats.source_used || source));

    rows.push(buildRawPlayerStatsRow_(event.event_id, playerA, source, featureTimestamp, playerAStats));
    rows.push(buildRawPlayerStatsRow_(event.event_id, playerB, source, featureTimestamp, playerBStats));

    const zeroCoverage = Number(statsCompleteness.players_with_non_null_stats || 0) === 0;
    const playerATerminalReasonCode = resolvePlayerTerminalReasonCode_('a', playerAStats, {
      zero_coverage: zeroCoverage,
      player_source: String(playerAStats.source_used || ''),
    });
    const playerBTerminalReasonCode = resolvePlayerTerminalReasonCode_('b', playerBStats, {
      zero_coverage: zeroCoverage,
      player_source: String(playerBStats.source_used || ''),
    });
    reasonCounts[playerATerminalReasonCode] = Number(reasonCounts[playerATerminalReasonCode] || 0) + 1;
    reasonCounts[playerBTerminalReasonCode] = Number(reasonCounts[playerBTerminalReasonCode] || 0) + 1;

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
        usable_stats: playerAStats.usable_stats !== undefined ? !!playerAStats.usable_stats : !!playerAStats.has_stats,
        stats_confidence: resolvePlayerStatsConfidence_(playerAStats),
        stats_fallback_mode: playerAStats.stats_fallback_mode || '',
        provenance: playerAStats.provenance || source,
        source_used: playerAStats.source_used || source,
        cohort: playerAStats.cohort || '',
        cohort_reason_code: playerAStats.cohort_reason_code || '',
        allow_out_of_cohort_fallback: playerAStats.allow_out_of_cohort_fallback !== false,
      },
      player_b: {
        canonical_name: playerB,
        features: playerBStats.features,
        has_stats: playerBStats.has_stats,
        usable_stats: playerBStats.usable_stats !== undefined ? !!playerBStats.usable_stats : !!playerBStats.has_stats,
        stats_confidence: resolvePlayerStatsConfidence_(playerBStats),
        stats_fallback_mode: playerBStats.stats_fallback_mode || '',
        provenance: playerBStats.provenance || source,
        source_used: playerBStats.source_used || source,
        cohort: playerBStats.cohort || '',
        cohort_reason_code: playerBStats.cohort_reason_code || '',
        allow_out_of_cohort_fallback: playerBStats.allow_out_of_cohort_fallback !== false,
      },
    };
  });

  const resolvedWithUsableStatsCount = requestedUniquePlayers.reduce(function (count, playerName) {
    if (!resolvedPlayerSet[playerName]) return count;
    const stats = statsByPlayer[playerName];
    const usability = evaluateProviderStatsUsability_(stats, config);
    return count + (usability.usable ? 1 : 0);
  }, 0);
  const outOfCohortCount = requestedUniquePlayers.reduce(function (count, playerName) {
    if (!resolvedPlayerSet[playerName]) return count;
    const stats = statsByPlayer[playerName] || {};
    return count + (String(stats.cohort || '') === 'out_of_cohort' ? 1 : 0);
  }, 0);
  const unknownRankCount = requestedUniquePlayers.reduce(function (count, playerName) {
    if (!resolvedPlayerSet[playerName]) return count;
    const stats = statsByPlayer[playerName] || {};
    return count + (String(stats.cohort || '') === 'unknown_rank' ? 1 : 0);
  }, 0);
  const statsSelectionMetadata = statsBatch.selection_metadata && typeof statsBatch.selection_metadata === 'object'
    ? statsBatch.selection_metadata
    : {};
  const fallbackDiagnostics = statsSelectionMetadata.fallback_diagnostics && typeof statsSelectionMetadata.fallback_diagnostics === 'object'
    ? statsSelectionMetadata.fallback_diagnostics
    : {};
  const cohortMode = String(config && config.PLAYER_STATS_COHORT_MODE || '').toLowerCase();
  const top100FilterExcludedCount = cohortMode === 'top100'
    ? Number(fallbackDiagnostics.skipped_by_mode_gate_count || 0)
    : 0;
  const top100FallbackAppliedCount = cohortMode === 'top100'
    ? Object.keys(fallbackDiagnostics.fallback_source_by_player || {}).length
    : 0;
  const playerResolutionSourceByPlayer = buildPlayerResolutionSourceMap_(requestedUniquePlayers, statsByPlayer, statsSelectionMetadata, providerUnavailable);
  const resolutionSourceSummary = summarizePlayerResolutionSourceMap_(playerResolutionSourceByPlayer);
  reasonCounts.stats_out_of_cohort = outOfCohortCount;
  reasonCounts.stats_rank_unknown = unknownRankCount;
  reasonCounts.stats_top100_filter_excluded = top100FilterExcludedCount;

  const summary = buildStageSummary_(runId, 'stageFetchPlayerStats', start, {
    input_count: statsInputEvents.length,
    output_count: rows.length,
    provider: source,
    api_credit_usage: Number(statsBatch.api_credit_usage || 0),
    reason_codes: reasonCounts,
    reason_metadata: {
      coverage: buildPlayerStatsCoverageMetadata_(requestedPlayerCount, resolvedPlayerCount, unresolvedPlayerCount),
      players_with_non_null_stats: Number(statsCompleteness.players_with_non_null_stats || 0),
      players_with_null_only_stats: Number(statsCompleteness.players_with_null_only_stats || 0),
      player_stats_data_available: resolvedWithUsableStatsCount > 0,
      requested_player_count: requestedPlayerCount,
      resolved_player_count: resolvedPlayerCount,
      resolved_name_count: resolvedPlayerCount,
      resolved_with_usable_stats_count: resolvedWithUsableStatsCount,
      unresolved_player_count: unresolvedPlayerCount,
      out_of_cohort_count: outOfCohortCount,
      rank_unknown_count: unknownRankCount,
      top100_filter_excluded_count: top100FilterExcludedCount,
      top100_fallback_applied_count: top100FallbackAppliedCount,
      overlap_ratio: roundNumber_(overlapRatio, 3),
      top_unresolved_player_samples: unresolvedPlayers.slice(0, 20),
      players_total: requestedPlayerCount,
      players_found_ta: resolutionSourceSummary.players_found_ta,
      players_fallback_provider: resolutionSourceSummary.players_fallback_provider,
      players_fallback_model: resolutionSourceSummary.players_fallback_model,
      players_unresolved: resolutionSourceSummary.players_unresolved,
      player_a_source: aggregatePlayerSlotSourceLabel_(playerSlotSourceLabels.a),
      player_b_source: aggregatePlayerSlotSourceLabel_(playerSlotSourceLabels.b),
      player_resolution_source_by_player: playerResolutionSourceByPlayer,
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
    reason_metadata: {
      coverage: buildPlayerStatsCoverageMetadata_(0, 0, 0),
      players_with_non_null_stats: 0,
      players_with_null_only_stats: 0,
      player_stats_data_available: false,
      requested_player_count: 0,
      resolved_player_count: 0,
      resolved_name_count: 0,
      resolved_with_usable_stats_count: 0,
      unresolved_player_count: 0,
      out_of_cohort_count: 0,
      rank_unknown_count: 0,
      top100_filter_excluded_count: 0,
      top100_fallback_applied_count: 0,
      overlap_ratio: 0,
      top_unresolved_player_samples: [],
      players_total: 0,
      players_found_ta: 0,
      players_fallback_provider: 0,
      players_fallback_model: 0,
      players_unresolved: 0,
      player_a_source: 'none',
      player_b_source: 'none',
      player_resolution_source_by_player: {},
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

function buildPlayerStatsCoverageMetadata_(requestedPlayerCount, resolvedPlayerCount, unresolvedPlayerCount) {
  const requested = Math.max(0, Number(requestedPlayerCount || 0));
  const resolved = Math.max(0, Number(resolvedPlayerCount || 0));
  const unresolved = Math.max(0, Number(unresolvedPlayerCount || 0));
  return {
    requested: requested,
    resolved: resolved,
    unresolved: unresolved,
    resolved_rate: requested > 0 ? roundNumber_(resolved / requested, 4) : 0,
  };
}

function resolvePlayerStatsPayload_(canonicalPlayerName, statsByPlayer, providerUnavailable, event, match, slot, reasonCounts, config) {
  const providerStats = statsByPlayer[canonicalPlayerName];

  if (providerStats) {
    const statsUsability = evaluateProviderStatsUsability_(providerStats, config);
    const hasProviderStats = statsUsability.has_data;

    if (hasProviderStats) {
      return {
        has_stats: statsUsability.usable,
        usable_stats: statsUsability.usable,
        stats_fallback_mode: '',
        provenance: 'player_stats_provider_v1',
        source_used: providerStats.source_used || 'player_stats_provider_v1',
        fallback_mode: providerStats.fallback_mode || '',
        cohort: providerStats.cohort || '',
        cohort_reason_code: providerStats.cohort_reason_code || '',
        allow_out_of_cohort_fallback: providerStats.allow_out_of_cohort_fallback !== undefined
          ? !!providerStats.allow_out_of_cohort_fallback
          : true,
        features: {
          ranking: providerStats.ranking,
          recent_form: providerStats.recent_form,
          surface_win_rate: providerStats.surface_win_rate,
          hold_pct: providerStats.hold_pct,
          break_pct: providerStats.break_pct,
        },
      };
    }
  }

  const pseudo = computePseudoPlayerStats_(canonicalPlayerName, event, match, slot);
  pseudo.stats_fallback_mode = providerUnavailable
    ? 'provider_unavailable'
    : (providerStats ? 'null_features' : 'missing_row');
  pseudo.provenance = 'derived_player_stats_v1_fallback';
  pseudo.source_used = 'derived_player_stats_v1_fallback';
  pseudo.fallback_mode = pseudo.stats_fallback_mode;
  return pseudo;
}

function resolvePlayerTerminalReasonCode_(slot, playerPayload, context) {
  const payload = playerPayload || {};
  const terminalContext = context || {};
  const sourceUsed = String(terminalContext.player_source || payload.source_used || '').toLowerCase();
  const zeroCoverage = terminalContext.zero_coverage === true;

  if (payload.has_stats) {
    if (sourceUsed.indexOf('derived_player_stats_v1_fallback') >= 0 || String(payload.stats_fallback_mode || '') === 'provider_unavailable') {
      return 'stats_fallback_model_used';
    }
    if (isAlternatePlayerStatsProviderSource_(sourceUsed)) {
      return 'stats_top100_fallback_applied';
    }
    return 'stats_enriched';
  }

  if (zeroCoverage) return 'stats_zero_coverage';
  return slot === 'a' ? 'stats_missing_player_a' : 'stats_missing_player_b';
}

function isAlternatePlayerStatsProviderSource_(sourceLabel) {
  const normalized = String(sourceLabel || '').toLowerCase();
  if (!normalized) return false;
  if (normalized.indexOf('tennis_abstract') >= 0 || normalized.indexOf('ta_') === 0) return false;
  if (normalized.indexOf('player_stats_provider_v1') >= 0) return false;
  if (normalized.indexOf('derived_player_stats_v1_fallback') >= 0) return false;
  return normalized === 'sofascore' || normalized === 'scrape' || normalized === 'itf' || normalized === 'wta_stats_zone' || normalized === 'tennis_explorer';
}

function buildPlayerResolutionSourceMap_(requestedPlayers, statsByPlayer, statsSelectionMetadata, providerUnavailable) {
  const sourcesFromSelection = statsSelectionMetadata && typeof statsSelectionMetadata === 'object'
    ? (statsSelectionMetadata.player_source_by_player || {})
    : {};
  const map = {};
  (requestedPlayers || []).forEach(function (playerName) {
    const key = String(playerName || '').trim();
    if (!key) return;
    const providerStats = (statsByPlayer || {})[key] || {};
    const preferredSource = String(sourcesFromSelection[key] || providerStats.source_used || '').trim();
    if (preferredSource) {
      map[key] = preferredSource;
    } else {
      map[key] = providerUnavailable ? 'derived_player_stats_v1_fallback' : 'unresolved';
    }
  });
  return map;
}

function summarizePlayerResolutionSourceMap_(sourceByPlayer) {
  const summary = {
    players_found_ta: 0,
    players_fallback_provider: 0,
    players_fallback_model: 0,
    players_unresolved: 0,
  };
  Object.keys(sourceByPlayer || {}).forEach(function (playerName) {
    const sourceLabel = String(sourceByPlayer[playerName] || '').toLowerCase().trim();
    if (!sourceLabel) return;
    if (sourceLabel.indexOf('tennis_abstract') >= 0 || sourceLabel.indexOf('ta_') === 0) {
      summary.players_found_ta += 1;
      return;
    }
    if (isAlternatePlayerStatsProviderSource_(sourceLabel)) {
      summary.players_fallback_provider += 1;
      return;
    }
    if (sourceLabel.indexOf('derived_player_stats_v1_fallback') >= 0) {
      summary.players_fallback_model += 1;
      return;
    }
    if (sourceLabel === 'unresolved') {
      summary.players_unresolved += 1;
    }
  });
  return summary;
}

function aggregatePlayerSlotSourceLabel_(labels) {
  const normalized = (labels || [])
    .map(function (label) { return String(label || '').trim(); })
    .filter(function (label) { return !!label; });
  if (!normalized.length) return 'none';
  const unique = {};
  normalized.forEach(function (label) { unique[label] = true; });
  const keys = Object.keys(unique);
  if (keys.length === 1) return keys[0];
  return 'mixed';
}

function evaluateProviderStatsUsability_(providerStats, config) {
  const stats = providerStats || {};
  const hasProviderStats = stats.ranking !== null
    || stats.recent_form !== null
    || stats.surface_win_rate !== null
    || stats.hold_pct !== null
    || stats.break_pct !== null;
  if (!hasProviderStats) return { has_data: false, usable: false };

  const trustedFeatureCount = Number(stats.trusted_feature_count || 0);
  const placeholderFeatureCount = Number(stats.placeholder_feature_count || 0);
  const hasOnlyPlaceholderStats = placeholderFeatureCount > 0 && trustedFeatureCount === 0;
  const numericValues = [stats.ranking, stats.recent_form, stats.surface_win_rate, stats.hold_pct, stats.break_pct]
    .filter(function (value) { return value !== null && value !== undefined && value !== ''; })
    .map(function (value) { return Number(value); })
    .filter(function (value) { return Number.isFinite(value); });
  const hasZeroOnlyStats = numericValues.length > 0 && numericValues.every(function (value) { return value === 0; });
  const allowPlaceholderFallback = !!(config && config.PLAYER_STATS_ALLOW_PLACEHOLDER_FALLBACK);
  const allowZeroOnlyFallback = !!(config && config.PLAYER_STATS_ALLOW_ZERO_ONLY_FALLBACK);
  const outOfCohortBlocked = String(stats.cohort || '') === 'out_of_cohort'
    && stats.allow_out_of_cohort_fallback === false;

  const usable = !outOfCohortBlocked
    && (!hasOnlyPlaceholderStats || allowPlaceholderFallback)
    && (!hasZeroOnlyStats || allowZeroOnlyFallback);
  return {
    has_data: true,
    usable: usable,
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
      usable_stats: false,
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
    usable_stats: true,
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

function resolveStatsCoverageProfile_(statsBundle) {
  const playerA = statsBundle && statsBundle.player_a ? statsBundle.player_a : null;
  const playerB = statsBundle && statsBundle.player_b ? statsBundle.player_b : null;
  const usableA = !!(playerA && (playerA.usable_stats !== undefined ? playerA.usable_stats : playerA.has_stats));
  const usableB = !!(playerB && (playerB.usable_stats !== undefined ? playerB.usable_stats : playerB.has_stats));
  if (usableA && usableB) {
    return { score: 1, tier: 'full', reason_code: 'coverage_full' };
  }
  if (usableA || usableB) {
    return { score: 0.6, tier: 'medium', reason_code: 'coverage_scaled_medium' };
  }
  return { score: 0.25, tier: 'low', reason_code: 'coverage_scaled_low' };
}

function withCoverageContext_(detail, coverageProfile) {
  const base = detail && typeof detail === 'object' ? Object.assign({}, detail) : {};
  const coverage = coverageProfile && typeof coverageProfile === 'object'
    ? coverageProfile
    : { score: 0.25, tier: 'low', reason_code: 'coverage_scaled_low' };
  base.stats_coverage_score = Number(coverage.score);
  base.stats_coverage_tier = String(coverage.tier || '');
  base.stats_coverage_reason_code = String(coverage.reason_code || '');
  return base;
}

function resolveStatsBundleCohortPolicyOutcome_(statsBundle, upstreamOutcome) {
  const defaultOutcome = String(upstreamOutcome || '');
  if (!statsBundle || !statsBundle.player_a || !statsBundle.player_b) return defaultOutcome || 'unknown';
  const playerA = statsBundle.player_a || {};
  const playerB = statsBundle.player_b || {};
  const blockedA = String(playerA.cohort || '') === 'out_of_cohort' && playerA.allow_out_of_cohort_fallback === false;
  const blockedB = String(playerB.cohort || '') === 'out_of_cohort' && playerB.allow_out_of_cohort_fallback === false;
  if (blockedA || blockedB) return 'blocked_out_of_cohort';
  const usableA = playerA.usable_stats !== undefined ? !!playerA.usable_stats : !!playerA.has_stats;
  const usableB = playerB.usable_stats !== undefined ? !!playerB.usable_stats : !!playerB.has_stats;
  if (usableA && usableB) return 'eligible';
  return defaultOutcome || 'unknown';
}

function combinePlayerStatsFeatureBump_(statsBundle, reasonCodes) {
  if (!statsBundle) {
    reasonCodes.stats_fallback_model_used = (reasonCodes.stats_fallback_model_used || 0) + 1;
    return 0;
  }

  const playerA = statsBundle.player_a;
  const playerB = statsBundle.player_b;
  const coverageProfile = resolveStatsCoverageProfile_(statsBundle);
  const coverageScore = Math.max(0, Math.min(1, Number(coverageProfile.score)));
  if (coverageProfile.reason_code === 'coverage_scaled_low') {
    reasonCodes.coverage_scaled_low = (reasonCodes.coverage_scaled_low || 0) + 1;
  } else if (coverageProfile.reason_code === 'coverage_scaled_medium') {
    reasonCodes.coverage_scaled_medium = (reasonCodes.coverage_scaled_medium || 0) + 1;
  }

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

  return roundNumber_(rawBump * resolvedConfidence * coverageScore, 4);
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

function resolveStartCutoffMinutes_(event, statsConfidence, config) {
  const baseCutoffMinutes = Math.max(0, Number(config && config.MINUTES_BEFORE_START_CUTOFF || 0));
  const eventMarket = String(event && event.market || '').toLowerCase();
  const h2hRelaxedCutoffMinutes = Math.max(0, Number(config && config.MINUTES_BEFORE_START_CUTOFF_H2H || baseCutoffMinutes));
  const minimumStatsConfidenceForRelaxation = Number(config && config.MIN_START_CUTOFF_RELAXED_STATS_CONFIDENCE || 0);
  const resolvedStatsConfidence = Number(statsConfidence);
  const confidenceEligible = Number.isFinite(resolvedStatsConfidence) && resolvedStatsConfidence >= minimumStatsConfidenceForRelaxation;
  const h2hEligible = eventMarket === 'h2h' && confidenceEligible;
  return {
    base_cutoff_minutes: baseCutoffMinutes,
    applied_cutoff_minutes: h2hEligible ? Math.min(baseCutoffMinutes, h2hRelaxedCutoffMinutes) : baseCutoffMinutes,
    h2h_relaxed_cutoff_minutes: h2hRelaxedCutoffMinutes,
    minimum_stats_confidence_for_relaxation: minimumStatsConfidenceForRelaxation,
    relaxation_applied: h2hEligible && h2hRelaxedCutoffMinutes < baseCutoffMinutes,
  };
}

function compareSignalCandidatePriority_(candidate, currentBest) {
  const candidateRank = [
    Number(candidate && candidate.edge_value || 0),
    Number(candidate && candidate.model_probability || 0),
    String((candidate && candidate.event && candidate.event.outcome) || ''),
    String((candidate && candidate.event && candidate.event.bookmaker) || ''),
    String((candidate && candidate.signal_hash) || ''),
  ];
  const bestRank = [
    Number(currentBest && currentBest.edge_value || 0),
    Number(currentBest && currentBest.model_probability || 0),
    String((currentBest && currentBest.event && currentBest.event.outcome) || ''),
    String((currentBest && currentBest.event && currentBest.event.bookmaker) || ''),
    String((currentBest && currentBest.signal_hash) || ''),
  ];
  if (candidateRank[0] > bestRank[0]) return 1;
  if (candidateRank[0] < bestRank[0]) return -1;
  if (candidateRank[1] > bestRank[1]) return 1;
  if (candidateRank[1] < bestRank[1]) return -1;
  if (candidateRank[2] < bestRank[2]) return 1;
  if (candidateRank[2] > bestRank[2]) return -1;
  if (candidateRank[3] < bestRank[3]) return 1;
  if (candidateRank[3] > bestRank[3]) return -1;
  if (candidateRank[4] < bestRank[4]) return 1;
  if (candidateRank[4] > bestRank[4]) return -1;
  return 0;
}

function stageGenerateSignals(runId, config, oddsEvents, matchRows, playerStatsByOddsEventId, stageMeta) {
  const start = Date.now();
  const nowMs = Date.now();
  const upstreamGateReason = stageMeta && stageMeta.upstream_gate_reason ? String(stageMeta.upstream_gate_reason) : '';
  const upstreamGateInputs = stageMeta && stageMeta.upstream_gate_inputs
    ? Object.assign({}, stageMeta.upstream_gate_inputs)
    : {};
  const normalizedUpstreamGateReason = normalizeUpstreamGateReason_(upstreamGateReason);
  const fallbackOnlyMode = upstreamGateReason === 'stats_zero_coverage';
  const upstreamResolvedUsableStatsCount = Number(upstreamGateInputs.resolved_with_usable_stats_count || 0);
  const upstreamCohortPolicyOutcome = String(upstreamGateInputs.cohort_policy_outcome || '');
  const rows = [];
  const sampledDecisions = [];
  let lastH2hDecision = null;
  const h2hDecisionCounts = { h2h_applied: 0, h2h_low_sample: 0, h2h_unavailable: 0 };
  const sampledDecisionLimit = Number(config.SIGNAL_DECISION_SAMPLE_LIMIT || 50);
  const staleSuppressionDiagnosticLimit = Math.max(0, Number(config.STALE_ODDS_SUPPRESSION_DIAGNOSTIC_LIMIT || 10));
  const staleSuppressionDiagnostics = [];
  const suppressionPrecheckSkipScoring = toBoolean_(
    config.SIGNAL_SUPPRESSION_PRECHECK_SKIP_SCORING,
    toBoolean_(DEFAULT_CONFIG.SIGNAL_SUPPRESSION_PRECHECK_SKIP_SCORING, true)
  );
  let processedCandidateCount = 0;
  let scoredCandidateCount = 0;
  const pendingNotificationCandidates = [];
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
    opposite_side_conflict_suppressed: 0,
    same_side_conflict_suppressed: 0,
    edge_below_threshold: 0,
    too_close_to_start_skip: 0,
    stale_odds_skip: 0,
    line_drift_exceeded: 0,
    edge_decay_exceeded: 0,
    liquidity_too_low: 0,
    notify_http_failed: 0,
    notify_missing_config: 0,
    fallback_only: 0,
    coverage_scaled_low: 0,
    coverage_scaled_medium: 0,
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
    if (detail && detail.scored) scoredCandidateCount += 1;
    reasonCounts[decisionReasonCode] = (reasonCounts[decisionReasonCode] || 0) + 1;
    const legacyDecisionReasonCode = legacyReasonCodeMap[decisionReasonCode] || null;
    if (legacyDecisionReasonCode) {
      legacyReasonCounts[legacyDecisionReasonCode] = (legacyReasonCounts[legacyDecisionReasonCode] || 0) + 1;
    }

    if (sampledDecisions.length < sampledDecisionLimit) {
      const detailWithH2h = Object.assign({
        competition_tier: String(match && match.competition_tier || ''),
      }, detail || {});
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
        opening_price: resolveOpeningPrice_(event),
        evaluation_price: resolveEvaluationPrice_(event),
        price_delta_bps: resolvePriceDeltaBps_(event),
        opening_lag_minutes: resolveOpeningLagMinutes_(event, nowMs),
        decision_gate_status: resolveDecisionGateStatus_({ notification_outcome: decisionReasonCode }),
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
    const coverageProfile = resolveStatsCoverageProfile_(enrichedStatsBundle);
    const hasStatsBundleRows = !!(statsBundle && statsBundle.player_a && statsBundle.player_b);
    const hasUsableStats = !!(hasStatsBundleRows
      && (statsBundle.player_a.usable_stats !== undefined ? statsBundle.player_a.usable_stats : statsBundle.player_a.has_stats)
      && (statsBundle.player_b.usable_stats !== undefined ? statsBundle.player_b.usable_stats : statsBundle.player_b.has_stats));
    const cohortPolicyOutcome = resolveStatsBundleCohortPolicyOutcome_(statsBundle, upstreamCohortPolicyOutcome);
    const cohortBlocked = cohortPolicyOutcome === 'blocked_out_of_cohort';
    const upstreamZeroCoverageBlocked = upstreamResolvedUsableStatsCount === 0 && upstreamCohortPolicyOutcome === 'blocked_out_of_cohort';
    const nullFeaturesFallback = !!(hasStatsBundleRows
      && statsBundle.player_a.has_stats === false
      && statsBundle.player_b.has_stats === false
      && (statsBundle.player_a.stats_fallback_mode === 'null_features'
        || statsBundle.player_b.stats_fallback_mode === 'null_features'
        || statsBundle.stats_fallback_mode === 'null_features'));

    if ((cohortBlocked || upstreamZeroCoverageBlocked) && !nullFeaturesFallback) {
      captureDecision_(event, match, 'missing_stats', withCoverageContext_({
        scored: false,
        cohort_policy_outcome: cohortPolicyOutcome,
        resolved_with_usable_stats_count: upstreamResolvedUsableStatsCount,
      }, coverageProfile));
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
      && (statsBundle.player_a.usable_stats !== undefined ? statsBundle.player_a.usable_stats : statsBundle.player_a.has_stats)
      && (statsBundle.player_b.usable_stats !== undefined ? statsBundle.player_b.usable_stats : statsBundle.player_b.has_stats)
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

    const startCutoffPolicy = resolveStartCutoffMinutes_(event, resolvedStatsConfidence, config || {});
    const startCutoffMs = Number(startCutoffPolicy.applied_cutoff_minutes || 0) * 60000;
    const commenceMs = event.commence_time.getTime();
    const minutesToStart = roundNumber_((commenceMs - nowMs) / 60000, 2);
    if (commenceMs <= nowMs + startCutoffMs) {
      let modelProbabilityTooClose = null;
      if (!suppressionPrecheckSkipScoring) {
        modelProbabilityTooClose = estimateFairProbability_(impliedProbability, match.competition_tier, enrichedStatsBundle, reasonCounts, config);
        lastH2hDecision = resolveH2hProbabilityBump_(enrichedStatsBundle, config || {});
        recordH2hDecisionCount_(h2hDecisionCounts, lastH2hDecision);
      }
      rows.push(buildSignalRow_(runId, config, event, match, withCoverageContext_({
        notification_outcome: 'too_close_to_start_skip',
        model_probability: modelProbabilityTooClose,
        market_implied_probability: impliedProbability,
        edge_value: 0,
        edge_tier: 'NONE',
        stake_units: 0,
        signal_hash: buildSignalHash_(event.event_id, event.market, event.outcome, modelVersion),
        model_version: modelVersion,
        stats_confidence: resolvedStatsConfidence,
      }, coverageProfile)));
      captureDecision_(event, match, 'too_close_to_start_skip', withCoverageContext_({
        scored: !suppressionPrecheckSkipScoring,
        stats_confidence: resolvedStatsConfidence,
        suppression_precheck_skip_scoring: suppressionPrecheckSkipScoring,
        timestamp_field: 'commence_time',
        reference_timestamp_field: 'now',
        commence_time_utc: event.commence_time.toISOString(),
        reference_timestamp_utc: new Date(nowMs).toISOString(),
        minutes_to_start: minutesToStart,
        threshold_minutes: Number(startCutoffPolicy.applied_cutoff_minutes || 0),
        threshold_minutes_base: Number(startCutoffPolicy.base_cutoff_minutes || 0),
        threshold_minutes_h2h_relaxed: Number(startCutoffPolicy.h2h_relaxed_cutoff_minutes || 0),
        start_cutoff_relaxation_applied: !!startCutoffPolicy.relaxation_applied,
      }, coverageProfile));
      return;
    }

    const staleThresholdMs = config.STALE_ODDS_WINDOW_MIN * 60000;
    const oddsUpdatedMs = event.odds_updated_time.getTime();
    const staleAgeMs = nowMs - oddsUpdatedMs;
    if (staleAgeMs > staleThresholdMs) {
      const staleAgeMinutes = roundNumber_(staleAgeMs / 60000, 2);
      if (staleSuppressionDiagnostics.length < staleSuppressionDiagnosticLimit) {
        staleSuppressionDiagnostics.push({
          odds_event_id: event && event.event_id ? String(event.event_id) : '',
          timestamp_field: 'odds_updated_time',
          odds_timestamp_local: formatLocalIso_(event.odds_updated_time),
          odds_timestamp_utc: event.odds_updated_time.toISOString(),
          reference_timestamp_field: 'now',
          reference_timestamp_local: formatLocalIso_(new Date(nowMs)),
          reference_timestamp_utc: new Date(nowMs).toISOString(),
          computed_age_minutes: staleAgeMinutes,
          computed_age_ms: staleAgeMs,
          threshold_minutes: Number(config.STALE_ODDS_WINDOW_MIN || 0),
          threshold_ms: staleThresholdMs,
        });
      }
      let modelProbabilityStale = null;
      if (!suppressionPrecheckSkipScoring) {
        modelProbabilityStale = estimateFairProbability_(impliedProbability, match.competition_tier, enrichedStatsBundle, reasonCounts, config);
        lastH2hDecision = resolveH2hProbabilityBump_(enrichedStatsBundle, config || {});
        recordH2hDecisionCount_(h2hDecisionCounts, lastH2hDecision);
      }
      rows.push(buildSignalRow_(runId, config, event, match, withCoverageContext_({
        notification_outcome: 'stale_odds_skip',
        model_probability: modelProbabilityStale,
        market_implied_probability: impliedProbability,
        edge_value: 0,
        edge_tier: 'NONE',
        stake_units: 0,
        signal_hash: buildSignalHash_(event.event_id, event.market, event.outcome, modelVersion),
        model_version: modelVersion,
        stats_confidence: resolvedStatsConfidence,
      }, coverageProfile)));
      captureDecision_(event, match, 'stale_odds_skip', withCoverageContext_({
        scored: !suppressionPrecheckSkipScoring,
        stats_confidence: resolvedStatsConfidence,
        suppression_precheck_skip_scoring: suppressionPrecheckSkipScoring,
        timestamp_field: 'odds_updated_time',
        reference_timestamp_field: 'now',
        odds_updated_time_utc: event.odds_updated_time.toISOString(),
        reference_timestamp_utc: new Date(nowMs).toISOString(),
        stale_age_minutes: staleAgeMinutes,
        threshold_minutes: Number(config.STALE_ODDS_WINDOW_MIN || 0),
      }, coverageProfile));
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
      rows.push(buildSignalRow_(runId, config, event, match, withCoverageContext_({
        notification_outcome: 'edge_below_threshold',
        model_probability: modelProbability,
        market_implied_probability: impliedProbability,
        edge_value: edgeValue,
        edge_tier: edgeTierAndStake.edge_tier,
        stake_units: edgeTierAndStake.stake_units,
        signal_hash: signalHash,
        model_version: modelVersion,
        stats_confidence: resolvedStatsConfidence,
      }, coverageProfile)));
      captureDecision_(event, match, 'edge_below_threshold', withCoverageContext_({
        scored: true,
        stats_confidence: resolvedStatsConfidence,
      }, coverageProfile));
      return;
    }

    if (!preActionGate.is_tradable) {
      captureDecision_(event, match, preActionGate.reason_code, withCoverageContext_({
        scored: true,
        model_probability: modelProbability,
        market_implied_probability: impliedProbability,
        edge_value: edgeValue,
        edge_tier: edgeTierAndStake.edge_tier,
        stake_units: edgeTierAndStake.stake_units,
        stats_confidence: resolvedStatsConfidence,
        pre_action_guard: preActionGate,
      }, coverageProfile));

      rows.push(buildSignalRow_(runId, config, event, match, withCoverageContext_({
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
      }, coverageProfile)));
      return;
    }

    const stakePolicyDecision = evaluateSignalStakePolicy_(edgeTierAndStake.stake_units, event.price, config);
    if (stakePolicyDecision.reason_code === 'stake_policy_config_error') {
      captureDecision_(event, match, stakePolicyDecision.reason_code, withCoverageContext_({
        scored: true,
        model_probability: modelProbability,
        market_implied_probability: impliedProbability,
        edge_value: edgeValue,
        edge_tier: edgeTierAndStake.edge_tier,
        stake_units: edgeTierAndStake.stake_units,
        stats_confidence: resolvedStatsConfidence,
        stake_policy_decision: stakePolicyDecision,
      }, coverageProfile));
      rows.push(buildSignalRow_(runId, config, event, match, withCoverageContext_({
        notification_outcome: stakePolicyDecision.reason_code,
        model_probability: modelProbability,
        market_implied_probability: impliedProbability,
        edge_value: edgeValue,
        edge_tier: edgeTierAndStake.edge_tier,
        stake_units: edgeTierAndStake.stake_units,
        signal_hash: signalHash,
        model_version: modelVersion,
        stats_confidence: resolvedStatsConfidence,
        stake_policy_decision: stakePolicyDecision,
        signal_delivery_mode: 'stake_policy_error',
      }, coverageProfile)));
      return;
    }
    if (stakePolicyDecision.decision === 'suppressed') {
      captureDecision_(event, match, stakePolicyDecision.reason_code, withCoverageContext_({
        scored: true,
        model_probability: modelProbability,
        market_implied_probability: impliedProbability,
        edge_value: edgeValue,
        edge_tier: edgeTierAndStake.edge_tier,
        stake_units: edgeTierAndStake.stake_units,
        stats_confidence: resolvedStatsConfidence,
        stake_policy_decision: stakePolicyDecision,
      }, coverageProfile));
      rows.push(buildSignalRow_(runId, config, event, match, withCoverageContext_({
        notification_outcome: stakePolicyDecision.reason_code,
        model_probability: modelProbability,
        market_implied_probability: impliedProbability,
        edge_value: edgeValue,
        edge_tier: 'NONE',
        stake_units: 0,
        signal_hash: signalHash,
        model_version: modelVersion,
        stats_confidence: resolvedStatsConfidence,
        stake_policy_decision: stakePolicyDecision,
        signal_delivery_mode: 'stake_policy_suppressed',
      }, coverageProfile)));
      return;
    }

    pendingNotificationCandidates.push({
      event: event,
      match: match,
      signal_hash: signalHash,
      model_version: modelVersion,
      model_probability: modelProbability,
      market_implied_probability: impliedProbability,
      edge_value: edgeValue,
      edge_tier: edgeTierAndStake.edge_tier,
      stake_units: edgeTierAndStake.stake_units,
      stake_policy_decision: stakePolicyDecision,
      stats_confidence: resolvedStatsConfidence,
      stats_coverage_score: Number(coverageProfile.score),
      stats_coverage_tier: String(coverageProfile.tier || ''),
      stats_coverage_reason_code: String(coverageProfile.reason_code || ''),
      h2h_decision: lastH2hDecision,
      enriched_stats_bundle: enrichedStatsBundle,
    });
    return;
  });

  const preferredH2hCandidateByGroup = {};
  const preferredH2hCandidateByGroupAndSide = {};
  pendingNotificationCandidates.forEach(function (candidate, index) {
    const event = candidate.event || {};
    if (String(event.market || '').toLowerCase() !== 'h2h') return;
    const groupKey = [runId, String(event.event_id || ''), 'h2h'].join('|');
    const sideKey = String(event.outcome || '');
    const groupSideKey = [groupKey, sideKey].join('|');
    const currentBestSideIndex = preferredH2hCandidateByGroupAndSide[groupSideKey];
    if (currentBestSideIndex === undefined) {
      preferredH2hCandidateByGroupAndSide[groupSideKey] = index;
    } else {
      const currentBestSideCandidate = pendingNotificationCandidates[currentBestSideIndex];
      if (compareSignalCandidatePriority_(candidate, currentBestSideCandidate) > 0) {
        preferredH2hCandidateByGroupAndSide[groupSideKey] = index;
      }
    }
    const currentBestIndex = preferredH2hCandidateByGroup[groupKey];
    if (currentBestIndex === undefined) {
      preferredH2hCandidateByGroup[groupKey] = index;
      return;
    }
    const currentBest = pendingNotificationCandidates[currentBestIndex];
    if (compareSignalCandidatePriority_(candidate, currentBest) > 0) {
      preferredH2hCandidateByGroup[groupKey] = index;
    }
  });

  pendingNotificationCandidates.forEach(function (candidate, index) {
    const event = candidate.event;
    const match = candidate.match;
    const isH2h = String(event && event.market || '').toLowerCase() === 'h2h';
    const groupKey = [runId, String(event && event.event_id || ''), 'h2h'].join('|');
    const sideKey = String(event && event.outcome || '');
    const preferredIndex = preferredH2hCandidateByGroup[groupKey];
    const preferredBySideIndex = preferredH2hCandidateByGroupAndSide[[groupKey, sideKey].join('|')];
    const winningCandidate = pendingNotificationCandidates[preferredIndex] || {};
    const winningSide = String((winningCandidate.event && winningCandidate.event.outcome) || '');
    const suppressedByOppositeSideConflict = isH2h
      && preferredIndex !== undefined
      && preferredIndex !== index
      && sideKey !== winningSide;
    const suppressedBySameSideConflict = isH2h
      && preferredBySideIndex !== undefined
      && preferredBySideIndex !== index
      && sideKey === winningSide;

    if (suppressedByOppositeSideConflict) {
      captureDecision_(event, match, 'opposite_side_conflict_suppressed', withCoverageContext_({
        scored: true,
        model_probability: candidate.model_probability,
        market_implied_probability: candidate.market_implied_probability,
        edge_value: candidate.edge_value,
        edge_tier: candidate.edge_tier,
        stake_units: candidate.stake_units,
        stats_confidence: candidate.stats_confidence,
        conflict_loser_side: sideKey,
        conflict_winner_side: String((winningCandidate.event && winningCandidate.event.outcome) || ''),
        conflict_winner_edge_value: Number(winningCandidate.edge_value || 0),
        conflict_resolution: 'strongest_side_preserved',
      }, candidate));
      rows.push(buildSignalRow_(runId, config, event, match, withCoverageContext_({
        notification_outcome: 'opposite_side_conflict_suppressed',
        model_probability: candidate.model_probability,
        market_implied_probability: candidate.market_implied_probability,
        edge_value: candidate.edge_value,
        edge_tier: 'NONE',
        stake_units: 0,
        signal_hash: candidate.signal_hash,
        model_version: candidate.model_version,
        stats_confidence: candidate.stats_confidence,
        signal_delivery_mode: 'h2h_conflict_suppressed',
        stake_policy_decision: candidate.stake_policy_decision,
      }, candidate)));
      return;
    }
    if (suppressedBySameSideConflict) {
      const winningSideCandidate = pendingNotificationCandidates[preferredBySideIndex] || {};
      captureDecision_(event, match, 'same_side_conflict_suppressed', withCoverageContext_({
        scored: true,
        model_probability: candidate.model_probability,
        market_implied_probability: candidate.market_implied_probability,
        edge_value: candidate.edge_value,
        edge_tier: candidate.edge_tier,
        stake_units: candidate.stake_units,
        stats_confidence: candidate.stats_confidence,
        conflict_loser_side: sideKey,
        conflict_winner_side: String((winningSideCandidate.event && winningSideCandidate.event.outcome) || ''),
        conflict_winner_edge_value: Number(winningSideCandidate.edge_value || 0),
        conflict_resolution: 'same_side_best_price_preserved',
      }, candidate));
      rows.push(buildSignalRow_(runId, config, event, match, withCoverageContext_({
        notification_outcome: 'same_side_conflict_suppressed',
        model_probability: candidate.model_probability,
        market_implied_probability: candidate.market_implied_probability,
        edge_value: candidate.edge_value,
        edge_tier: 'NONE',
        stake_units: 0,
        signal_hash: candidate.signal_hash,
        model_version: candidate.model_version,
        stats_confidence: candidate.stats_confidence,
        signal_delivery_mode: 'h2h_same_side_conflict_suppressed',
        stake_policy_decision: candidate.stake_policy_decision,
      }, candidate)));
      return;
    }

    const notifyDecision = maybeNotifySignal_(signalState, seenHashesThisRun, candidate.signal_hash, nowMs, config.SIGNAL_COOLDOWN_MIN);
    let notifyOutcome = notifyDecision.outcome;
    let notifyDiagnostics = null;

    if (notifyDecision.outcome === 'sent' && fallbackOnlyMode) {
      notifyOutcome = 'fallback_only';
    } else if (notifyDecision.outcome === 'sent') {
      const sendResult = sendSignalNotification_(config, runId, candidate.signal_hash, {
        side: event.outcome,
        market: event.market,
        bookmaker: event.bookmaker,
        competition_tier: match.competition_tier,
        edge_tier: candidate.edge_tier,
        edge_value: candidate.edge_value,
        stake_units: candidate.stake_units,
        recommended_stake: candidate.stake_policy_decision.final_stake,
        recommended_stake_currency: candidate.stake_policy_decision.account_currency,
        model_probability: candidate.model_probability,
        market_implied_probability: candidate.market_implied_probability,
        commence_time: event.commence_time,
        odds_event_id: event.event_id,
        rationale_context: {
          edge_value: candidate.edge_value,
          model_probability: candidate.model_probability,
          market_implied_probability: candidate.market_implied_probability,
          stats_bundle: candidate.enriched_stats_bundle,
          stats_confidence: candidate.stats_confidence,
          h2h_decision: candidate.h2h_decision,
        },
      });
      notifyOutcome = sendResult.outcome;
      const notifyLoggedAt = localAndUtcTimestamps_(new Date());
      notifyDiagnostics = {
        run_id: runId,
        signal_hash: candidate.signal_hash,
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
        signalState.sent_hashes[candidate.signal_hash] = nowMs;
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

    captureDecision_(event, match, notifyOutcome, withCoverageContext_({
      scored: true,
      model_probability: candidate.model_probability,
      market_implied_probability: candidate.market_implied_probability,
      edge_value: candidate.edge_value,
      edge_tier: candidate.edge_tier,
      stake_units: candidate.stake_units,
      stats_confidence: candidate.stats_confidence,
      stake_policy_decision: candidate.stake_policy_decision,
    }, candidate));

    rows.push(buildSignalRow_(runId, config, event, match, withCoverageContext_({
      notification_outcome: notifyOutcome,
      model_probability: candidate.model_probability,
      market_implied_probability: candidate.market_implied_probability,
      edge_value: candidate.edge_value,
      edge_tier: candidate.edge_tier,
      stake_units: candidate.stake_units,
      signal_hash: candidate.signal_hash,
      model_version: candidate.model_version,
      notification_metadata: notifyDiagnostics,
      stats_confidence: candidate.stats_confidence,
      signal_delivery_mode: fallbackOnlyMode ? 'fallback_only' : 'normal',
      stake_policy_decision: candidate.stake_policy_decision,
    }, candidate)));
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
      opening_price: row.opening_price,
      evaluation_price: row.evaluation_price,
      price_delta_bps: row.price_delta_bps,
      opening_lag_minutes: row.opening_lag_minutes,
      decision_gate_status: row.decision_gate_status,
      timestamp: row.created_at,
      timestamp_utc: row.created_at_utc || '',
      commence_time: row.commence_time,
      odds_updated_time: row.odds_updated_time,
      model_version: row.model_version,
      notification_outcome: row.notification_outcome,
      notification_metadata: row.notification_metadata || null,
      stats_confidence: row.stats_confidence,
      stats_coverage_score: row.stats_coverage_score,
      stats_coverage_tier: row.stats_coverage_tier,
      stats_coverage_reason_code: row.stats_coverage_reason_code,
      recommended_stake: row.recommended_stake,
      recommended_stake_currency: row.recommended_stake_currency,
      proposed_stake: row.proposed_stake,
      min_stake_threshold: row.min_stake_threshold,
      stake_policy_mode: row.stake_policy_mode,
      stake_policy_decision_reason: row.stake_policy_decision_reason,
      stake_mode_used: row.stake_mode_used,
      raw_risk_mxn: row.raw_risk_mxn,
      raw_target_win_mxn: row.raw_target_win_mxn,
      final_risk_mxn: row.final_risk_mxn,
      final_units: row.final_units,
      stake_adjustment_reason: row.stake_adjustment_reason,
      min_bet_mxn: row.min_bet_mxn,
      bucket_step_mxn: row.bucket_step_mxn,
      unit_size_mxn: row.unit_size_mxn,
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
    upstream_gate_reason: normalizedUpstreamGateReason,
    has_upstream_gate_reason: normalizedUpstreamGateReason !== 'unspecified',
    upstream_gate_inputs: upstreamGateInputs,
  } : null;
  const observedStatsConfidence = rows
    .map(function (row) { return Number(row.stats_confidence); })
    .filter(function (value) { return Number.isFinite(value); });
  const observedCoverageScores = rows
    .map(function (row) { return Number(row.stats_coverage_score); })
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
        && reasonCode !== 'null_features_low_confidence_scored'
        && reasonCode !== 'coverage_scaled_low'
        && reasonCode !== 'coverage_scaled_medium';
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
    upstream_gate_reason: normalizedUpstreamGateReason,
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
    stats_coverage_summary: {
      observed_count: observedCoverageScores.length,
      average: observedCoverageScores.length
        ? roundNumber_(observedCoverageScores.reduce(function (sum, value) { return sum + value; }, 0) / observedCoverageScores.length, 4)
        : null,
      minimum: observedCoverageScores.length ? Math.min.apply(null, observedCoverageScores) : null,
      maximum: observedCoverageScores.length ? Math.max.apply(null, observedCoverageScores) : null,
    },
    sent_count: Number(reasonCounts.sent || 0),
    invariant: {
      sent_plus_drop_reasons_equals_input: Number(reasonCounts.sent || 0) + allDropReasons === oddsEvents.length,
      zero_input_has_explanatory_metadata: oddsEvents.length > 0 || !!zeroInputExplanatory,
    },
  }));

  if ((Number(reasonCounts.sent || 0) + allDropReasons) !== oddsEvents.length) {
    const invariantViolations = [{
      sent_count: Number(reasonCounts.sent || 0),
      all_drop_reasons: allDropReasons,
      input_count: oddsEvents.length,
      processed_count: processedCandidateCount,
    }];
    const invariantEnforcement = enforceInvariant_(config, {
      invariant: 'stage_generate_signals_sent_plus_drop_reasons_equals_input',
      context: 'stageGenerateSignals',
      violations: invariantViolations,
      hard_fail: true,
      error_prefix: 'stageGenerateSignals invariant violated',
    });
    if (invariantEnforcement.warning_emitted) {
      appendLogRow_({
        row_type: 'ops',
        run_id: runId,
        stage: 'stageGenerateSignals',
        status: 'warning',
        reason_code: 'stage_generate_signals_invariant_violation',
        message: JSON.stringify(invariantEnforcement.payload || {}),
      });
    }
  }

  const previousSignalDecisionSummary = getStateJson_('LAST_SIGNAL_DECISION_SUMMARY') || {};
  const signalDecisionSummary = buildSignalDecisionRunSummary_({
    run_id: runId,
    input_count: oddsEvents.length,
    processed_count: processedCandidateCount,
    scored_count: scoredCandidateCount,
    sent_count: Number(reasonCounts.sent || 0),
    reason_counts: reasonCounts,
    sampled_decisions: sampledDecisions,
    signal_rows: rows,
    stale_suppression_diagnostics: staleSuppressionDiagnostics,
    previous_summary: previousSignalDecisionSummary,
    config: config || {},
  });
  const suppressionTrendSummary = buildSignalSuppressionTrendSummary_({
    run_id: runId,
    input_count: oddsEvents.length,
    reason_counts: reasonCounts,
    sampled_decisions: sampledDecisions,
    config: config || {},
  });
  signalDecisionSummary.suppression_trends = suppressionTrendSummary;
  setStateValue_('LAST_SIGNAL_DECISION_SUMMARY', JSON.stringify(signalDecisionSummary));

  const summaryReasonCodes = Object.assign({}, reasonCounts);
  const signalQualityMetrics = {
    feature_completeness: null,
    feature_completeness_reason_code: 'deferred_to_run_quality_contract',
    matched_events: Number(
      matchRows.filter(function (row) {
        return !!(row && row.schedule_event_id);
      }).length
    ),
    matched_events_reason_code: 'stage_match_events_rows_with_schedule_event_id',
    scored_signals: Number(scoredCandidateCount || 0),
    scored_signals_reason_code: 'stage_generate_signals_scored_candidate_count',
  };
  const summaryReasonMetadata = {
    signal_decision_summary: JSON.stringify(signalDecisionSummary),
    signal_quality_metrics: JSON.stringify(signalQualityMetrics),
    stale_suppression_diagnostics: JSON.stringify(staleSuppressionDiagnostics),
    coverage_scaling: JSON.stringify({
      coverage_scaled_low: Number(reasonCounts.coverage_scaled_low || 0),
      coverage_scaled_medium: Number(reasonCounts.coverage_scaled_medium || 0),
      average_coverage_score: observedCoverageScores.length
        ? roundNumber_(observedCoverageScores.reduce(function (sum, value) { return sum + value; }, 0) / observedCoverageScores.length, 4)
        : null,
    }),
  };
  if (oddsEvents.length === 0) {
    summaryReasonMetadata.upstream_gate_reason = normalizedUpstreamGateReason;
    summaryReasonMetadata.upstream_gate_inputs = JSON.stringify(upstreamGateInputs);
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
    scoredCount: scoredCandidateCount,
    signalDecisionSummary: signalDecisionSummary,
    signalQualityMetrics: signalQualityMetrics,
  };
}

function buildSignalDecisionRunSummary_(payload) {
  const safe = payload || {};
  const reasonCounts = Object.assign({}, safe.reason_counts || {});
  const sampledDecisions = Array.isArray(safe.sampled_decisions) ? safe.sampled_decisions : [];
  const signalRows = Array.isArray(safe.signal_rows) ? safe.signal_rows : [];
  const staleSuppressionDiagnostics = Array.isArray(safe.stale_suppression_diagnostics)
    ? safe.stale_suppression_diagnostics
    : [];
  const previousSummary = safe.previous_summary && typeof safe.previous_summary === 'object'
    ? safe.previous_summary
    : {};
  const config = safe.config && typeof safe.config === 'object' ? safe.config : {};
  const suppressionReasonGroups = {
    cooldown: ['cooldown_suppressed'],
    conflict: ['opposite_side_conflict_suppressed', 'same_side_conflict_suppressed'],
    edge: ['edge_below_threshold'],
    stale: ['stale_odds_skip'],
    timing: ['too_close_to_start_skip'],
    risk_guard: ['line_drift_exceeded', 'edge_decay_exceeded', 'liquidity_too_low'],
    config: ['notify_disabled', 'notify_missing_config'],
  };

  const suppressionSummary = Object.keys(suppressionReasonGroups).reduce(function (acc, groupName) {
    const reasons = suppressionReasonGroups[groupName];
    const byReason = {};
    let total = 0;
    reasons.forEach(function (reasonCode) {
      const count = Number(reasonCounts[reasonCode] || 0);
      byReason[reasonCode] = count;
      total += count;
    });
    acc[groupName] = {
      total: total,
      by_reason: byReason,
    };
    return acc;
  }, {});

  const topSuppressionReasons = Object.keys(reasonCounts)
    .map(function (reasonCode) {
      return { reason_code: reasonCode, count: Number(reasonCounts[reasonCode] || 0) };
    })
    .filter(function (entry) {
      return entry.count > 0 && /(?:_suppressed|_skip$|edge_below_threshold|notify_missing_config|notify_disabled|line_drift_exceeded|edge_decay_exceeded|liquidity_too_low)/.test(entry.reason_code);
    })
    .sort(function (a, b) { return b.count - a.count; })
    .slice(0, 3);

  const sampledSuppressionExamples = topSuppressionReasons.map(function (entry) {
    const examples = sampledDecisions
      .filter(function (decision) { return String(decision && decision.decision_reason_code || '') === entry.reason_code; })
      .slice(0, 2)
      .map(function (decision) {
        const safeDecision = decision || {};
        return {
          odds_event_id: safeDecision.odds_event_id || '',
          side: safeDecision.side || '',
          market: safeDecision.market || '',
          edge_value: Number(safeDecision.detail && safeDecision.detail.edge_value),
          minutes_to_start: Number(safeDecision.detail && safeDecision.detail.minutes_to_start),
        };
      });
    return {
      reason_code: entry.reason_code,
      count: entry.count,
      examples: examples,
    };
  });

  const edgeValues = signalRows
    .map(function (row) { return Number(row && row.edge_value); })
    .filter(function (value) { return Number.isFinite(value); })
    .sort(function (a, b) { return a - b; });
  const edgeCount = edgeValues.length;
  const edgeP50 = percentileFromSortedValues_(edgeValues, 0.5);
  const edgeP90 = percentileFromSortedValues_(edgeValues, 0.9);
  const edgeP95 = percentileFromSortedValues_(edgeValues, 0.95);
  const edgeMean = edgeCount > 0
    ? roundNumber_(edgeValues.reduce(function (sum, value) { return sum + value; }, 0) / edgeCount, 4)
    : null;
  const edgeDistribution = {
    count: edgeCount,
    min: edgeCount > 0 ? roundNumber_(edgeValues[0], 4) : null,
    p50: edgeP50,
    p90: edgeP90,
    p95: edgeP95,
    max: edgeCount > 0 ? roundNumber_(edgeValues[edgeCount - 1], 4) : null,
    mean: edgeMean,
  };

  const previousEdgeQuality = previousSummary && previousSummary.edge_quality ? previousSummary.edge_quality : {};
  const previousEdgeDistribution = previousEdgeQuality && previousEdgeQuality.edge_distribution
    ? previousEdgeQuality.edge_distribution
    : {};
  const previousP50 = Number(previousEdgeDistribution.p50);
  const previousP95 = Number(previousEdgeDistribution.p95);
  const previousMean = Number(previousEdgeDistribution.mean);
  const volatility = {
    baseline_run_id: String(previousSummary.run_id || ''),
    delta_p50: Number.isFinite(previousP50) && Number.isFinite(edgeP50)
      ? roundNumber_(edgeP50 - previousP50, 4)
      : null,
    delta_p95: Number.isFinite(previousP95) && Number.isFinite(edgeP95)
      ? roundNumber_(edgeP95 - previousP95, 4)
      : null,
    delta_mean: Number.isFinite(previousMean) && Number.isFinite(edgeMean)
      ? roundNumber_(edgeMean - previousMean, 4)
      : null,
    abs_delta_p95: Number.isFinite(previousP95) && Number.isFinite(edgeP95)
      ? roundNumber_(Math.abs(edgeP95 - previousP95), 4)
      : null,
    abs_delta_mean: Number.isFinite(previousMean) && Number.isFinite(edgeMean)
      ? roundNumber_(Math.abs(edgeMean - previousMean), 4)
      : null,
  };

  const decisionGateReasonCounts = Object.keys(reasonCounts)
    .filter(function (reasonCode) {
      return Number(reasonCounts[reasonCode] || 0) > 0
        && reasonCode !== 'sent'
        && reasonCode !== 'full_confidence_stats_scored'
        && reasonCode !== 'low_confidence_stats_scored'
        && reasonCode !== 'null_features_low_confidence_scored'
        && reasonCode !== 'coverage_scaled_low'
        && reasonCode !== 'coverage_scaled_medium'
        && reasonCode !== 'null_features_fallback_scored';
    })
    .sort(function (a, b) { return Number(reasonCounts[b] || 0) - Number(reasonCounts[a] || 0); })
    .reduce(function (acc, reasonCode) {
      acc[reasonCode] = Number(reasonCounts[reasonCode] || 0);
      return acc;
    }, {});

  const volatilityThreshold = Number(config.EDGE_VOLATILITY_ALERT_THRESHOLD || 0.03);
  const instabilityDetected = Number.isFinite(volatility.abs_delta_p95)
    && volatility.abs_delta_p95 > volatilityThreshold;

  const sentCount = Number(safe.sent_count || 0);
  const scoredCount = Number(safe.scored_count || 0);
  const stakePolicySummary = summarizeSignalRowsStakePolicy_(signalRows, config);
  const suppressionPolicy = {
    stale_odds_skip: {
      timestamp_field: 'odds_updated_time',
      reference_timestamp_field: 'now',
      freshness_threshold_minutes: Number(config.STALE_ODDS_WINDOW_MIN || 0),
      timezone_normalization: 'Date epoch milliseconds (UTC absolute comparison); diagnostics include local+UTC strings',
    },
    too_close_to_start_skip: {
      timestamp_field: 'commence_time',
      reference_timestamp_field: 'now_plus_cutoff',
      freshness_threshold_minutes: Number(config.MINUTES_BEFORE_START_CUTOFF || 0),
      h2h_relaxed_threshold_minutes: Number(config.MINUTES_BEFORE_START_CUTOFF_H2H || config.MINUTES_BEFORE_START_CUTOFF || 0),
      minimum_stats_confidence_for_relaxation: Number(config.MIN_START_CUTOFF_RELAXED_STATS_CONFIDENCE || 0),
      timezone_normalization: 'Date epoch milliseconds (UTC absolute comparison)',
    },
  };
  const suppressionFamilyDiagnostics = buildSuppressionFamilyDiagnostics_(reasonCounts, sampledDecisions, config);
  const governanceWarnings = [];
  if (suppressionFamilyDiagnostics.concentration_warning && suppressionFamilyDiagnostics.concentration_warning.warning_needed) {
    governanceWarnings.push({
      warning_code: 'suppression_family_concentration_high',
      severity: 'warning',
      detail: suppressionFamilyDiagnostics.concentration_warning,
    });
  }

  return {
    run_id: String(safe.run_id || ''),
    input_count: Number(safe.input_count || 0),
    processed_count: Number(safe.processed_count || 0),
    scored_count: scoredCount,
    sent_count: sentCount,
    suppression_counts: suppressionSummary,
    suppression_family_diagnostics: suppressionFamilyDiagnostics,
    suppression_policy: suppressionPolicy,
    stale_suppression_diagnostics: staleSuppressionDiagnostics,
    sampled_top_suppressions: sampledSuppressionExamples,
    stake_policy_summary: stakePolicySummary,
    edge_quality: {
      edge_distribution: edgeDistribution,
      edge_volatility_vs_previous_run: volatility,
      decision_gate_reason_counts: decisionGateReasonCounts,
      instability_detected: instabilityDetected,
      instability_threshold: volatilityThreshold,
    },
    alignment_checks: {
      sent_matches_reason_counts: sentCount === Number(reasonCounts.sent || 0),
      cooldown_matches_reason_counts: Number((suppressionSummary.cooldown && suppressionSummary.cooldown.by_reason && suppressionSummary.cooldown.by_reason.cooldown_suppressed) || 0) === Number(reasonCounts.cooldown_suppressed || 0),
      conflict_matches_reason_counts: Number((suppressionSummary.conflict && suppressionSummary.conflict.total) || 0) === Number(reasonCounts.opposite_side_conflict_suppressed || 0) + Number(reasonCounts.same_side_conflict_suppressed || 0),
      edge_matches_reason_counts: Number((suppressionSummary.edge && suppressionSummary.edge.by_reason && suppressionSummary.edge.by_reason.edge_below_threshold) || 0) === Number(reasonCounts.edge_below_threshold || 0),
      stale_matches_reason_counts: Number((suppressionSummary.stale && suppressionSummary.stale.by_reason && suppressionSummary.stale.by_reason.stale_odds_skip) || 0) === Number(reasonCounts.stale_odds_skip || 0),
      timing_matches_reason_counts: Number((suppressionSummary.timing && suppressionSummary.timing.by_reason && suppressionSummary.timing.by_reason.too_close_to_start_skip) || 0) === Number(reasonCounts.too_close_to_start_skip || 0),
      config_matches_reason_counts: Number((suppressionSummary.config && suppressionSummary.config.total) || 0) === Number(reasonCounts.notify_disabled || 0) + Number(reasonCounts.notify_missing_config || 0),
      scored_not_less_than_sent: scoredCount >= sentCount,
    },
    governance_warnings: governanceWarnings,
  };
}

function buildSignalSuppressionTrendSummary_(payload) {
  const safe = payload || {};
  const runId = String(safe.run_id || '');
  const inputCount = Number(safe.input_count || 0);
  const reasonCounts = Object.assign({}, safe.reason_counts || {});
  const sampledDecisions = Array.isArray(safe.sampled_decisions) ? safe.sampled_decisions : [];
  const config = safe.config && typeof safe.config === 'object' ? safe.config : {};
  const windowSize = Math.max(1, Number(config.SUPPRESSION_ANALYTICS_RUN_WINDOW || 20));
  const history = getStateJson_('LAST_SIGNAL_SUPPRESSION_HISTORY');
  const existingHistory = Array.isArray(history) ? history : [];
  const nextEntry = {
    run_id: runId,
    input_count: inputCount,
    reason_counts: reasonCounts,
    sampled_decisions: sampledDecisions,
  };
  const nextHistory = existingHistory
    .concat([nextEntry])
    .slice(-windowSize);
  setStateValue_('LAST_SIGNAL_SUPPRESSION_HISTORY', JSON.stringify(nextHistory));

  const totals = nextHistory.reduce(function (acc, entry) {
    acc.input_count += Number(entry && entry.input_count || 0);
    const entryReasonCounts = (entry && entry.reason_counts) || {};
    Object.keys(entryReasonCounts).forEach(function (reasonCode) {
      acc.reason_counts[reasonCode] = Number(acc.reason_counts[reasonCode] || 0) + Number(entryReasonCounts[reasonCode] || 0);
    });
    return acc;
  }, { input_count: 0, reason_counts: {} });

  const suppressionRatesByReason = Object.keys(totals.reason_counts || {})
    .filter(function (reasonCode) {
      return /(?:_suppressed|_skip$|edge_below_threshold|line_drift_exceeded|edge_decay_exceeded|liquidity_too_low)/.test(reasonCode);
    })
    .sort()
    .reduce(function (acc, reasonCode) {
      const count = Number(totals.reason_counts[reasonCode] || 0);
      acc[reasonCode] = {
        count: count,
        rate_pct_of_inputs: totals.input_count > 0 ? roundNumber_((count / totals.input_count) * 100, 2) : 0,
      };
      return acc;
    }, {});

  const allSampledSuppressed = nextHistory.reduce(function (acc, entry) {
    const sampled = Array.isArray(entry && entry.sampled_decisions) ? entry.sampled_decisions : [];
    sampled.forEach(function (decision) {
      const reasonCode = String(decision && decision.decision_reason_code || '');
      if (!/(?:_suppressed|_skip$)/.test(reasonCode)) return;
      acc.push(decision);
    });
    return acc;
  }, []);

  const recurringTournaments = aggregateTopKeyedCounts_(allSampledSuppressed, function (decision) {
    return String(decision && decision.detail && decision.detail.competition_tier || 'unknown_tier');
  }, 3);
  const recurringStartTimeWindows = aggregateTopKeyedCounts_(allSampledSuppressed.filter(function (decision) {
    return String(decision && decision.decision_reason_code || '') === 'too_close_to_start_skip';
  }), function (decision) {
    const minutes = Number(decision && decision.detail && decision.detail.minutes_to_start);
    if (!Number.isFinite(minutes)) return 'unknown_window';
    if (minutes <= 5) return '<=5m';
    if (minutes <= 15) return '5m_to_15m';
    if (minutes <= 30) return '15m_to_30m';
    if (minutes <= 60) return '30m_to_60m';
    return '>60m';
  }, 5);
  const recurringSideConflicts = aggregateTopKeyedCounts_(allSampledSuppressed.filter(function (decision) {
    const reasonCode = String(decision && decision.decision_reason_code || '');
    return reasonCode === 'opposite_side_conflict_suppressed' || reasonCode === 'same_side_conflict_suppressed';
  }), function (decision) {
    const loserSide = String(decision && decision.detail && decision.detail.conflict_loser_side || '');
    const winnerSide = String(decision && decision.detail && decision.detail.conflict_winner_side || '');
    return [loserSide || 'unknown_loser', winnerSide || 'unknown_winner'].join(' -> ');
  }, 5);
  const familyDiagnostics = buildSuppressionFamilyDiagnostics_(totals.reason_counts, allSampledSuppressed, config);
  const governanceRollup = {
    suppression_family_concentration_warning: familyDiagnostics.concentration_warning,
  };

  return {
    run_window_size: windowSize,
    runs_analyzed: nextHistory.length,
    total_inputs_analyzed: totals.input_count,
    suppression_rates_by_reason: suppressionRatesByReason,
    suppression_family_diagnostics: familyDiagnostics,
    governance_rollup: governanceRollup,
    recurring_edge_cases: {
      tournaments: recurringTournaments,
      start_time_windows: recurringStartTimeWindows,
      side_conflicts: recurringSideConflicts,
    },
  };
}

function buildSuppressionFamilyDiagnostics_(reasonCounts, sampledDecisions, config) {
  const safeReasonCounts = Object.assign({}, reasonCounts || {});
  const safeSampledDecisions = Array.isArray(sampledDecisions) ? sampledDecisions : [];
  const safeConfig = config && typeof config === 'object' ? config : {};
  const concentrationThresholdPct = Number(safeConfig.SUPPRESSION_CONCENTRATION_THRESHOLD_PCT || 60);
  const exampleCap = Math.max(1, Number(safeConfig.SUPPRESSION_FAMILY_EVENT_SAMPLE_LIMIT || 3));
  const familyReasonGroups = {
    timing: ['too_close_to_start_skip'],
    stale: ['stale_odds_skip'],
    edge: ['edge_below_threshold'],
    cooldown: ['cooldown_suppressed'],
  };
  const familyNames = Object.keys(familyReasonGroups);
  const familyTotals = familyNames.reduce(function (acc, familyName) {
    const reasons = familyReasonGroups[familyName];
    acc[familyName] = reasons.reduce(function (sum, reasonCode) {
      return sum + Number(safeReasonCounts[reasonCode] || 0);
    }, 0);
    return acc;
  }, {});
  const totalSuppressions = familyNames.reduce(function (sum, familyName) {
    return sum + Number(familyTotals[familyName] || 0);
  }, 0);

  const byFamily = familyNames.reduce(function (acc, familyName) {
    const reasons = familyReasonGroups[familyName];
    const sortedReasons = reasons
      .map(function (reasonCode) {
        return { reason_code: reasonCode, count: Number(safeReasonCounts[reasonCode] || 0) };
      })
      .sort(function (a, b) {
        if (b.count !== a.count) return b.count - a.count;
        return a.reason_code < b.reason_code ? -1 : (a.reason_code > b.reason_code ? 1 : 0);
      });
    const topReason = sortedReasons.length && sortedReasons[0].count > 0
      ? sortedReasons[0]
      : { reason_code: '', count: 0 };
    const exampleEventIds = [];
    safeSampledDecisions.forEach(function (decision) {
      if (exampleEventIds.length >= exampleCap) return;
      const reasonCode = String(decision && decision.decision_reason_code || '');
      if (reasons.indexOf(reasonCode) === -1) return;
      const eventId = String(decision && decision.odds_event_id || '').trim();
      if (!eventId || exampleEventIds.indexOf(eventId) >= 0) return;
      exampleEventIds.push(eventId);
    });
    const familyTotal = Number(familyTotals[familyName] || 0);
    acc[familyName] = {
      total: familyTotal,
      share_pct: totalSuppressions > 0 ? roundNumber_((familyTotal / totalSuppressions) * 100, 2) : 0,
      top_reason: topReason,
      example_event_ids: exampleEventIds,
    };
    return acc;
  }, {});

  const rankedFamilies = familyNames
    .map(function (familyName) {
      const familyEntry = byFamily[familyName] || {};
      return {
        family: familyName,
        total: Number(familyEntry.total || 0),
        share_pct: Number(familyEntry.share_pct || 0),
      };
    })
    .sort(function (a, b) {
      if (b.total !== a.total) return b.total - a.total;
      return a.family < b.family ? -1 : (a.family > b.family ? 1 : 0);
    });
  const dominantFamily = rankedFamilies[0] || { family: '', total: 0, share_pct: 0 };
  const warningNeeded = dominantFamily.total > 0
    && Number.isFinite(concentrationThresholdPct)
    && dominantFamily.share_pct > concentrationThresholdPct;
  const concentrationWarning = {
    warning_needed: warningNeeded,
    threshold_pct: concentrationThresholdPct,
    dominant_family: dominantFamily.family,
    dominant_share_pct: dominantFamily.share_pct,
    dominant_total: dominantFamily.total,
    total_suppressions: totalSuppressions,
    top_reason: dominantFamily.family && byFamily[dominantFamily.family]
      ? byFamily[dominantFamily.family].top_reason
      : { reason_code: '', count: 0 },
    message: warningNeeded
      ? 'single_suppression_family_exceeds_concentration_threshold'
      : '',
  };

  return {
    total_suppressions: totalSuppressions,
    concentration_threshold_pct: concentrationThresholdPct,
    by_family: byFamily,
    concentration_warning: concentrationWarning,
  };
}

function aggregateTopKeyedCounts_(rows, keyResolver, limit) {
  const counts = {};
  (rows || []).forEach(function (row) {
    const key = String(keyResolver(row) || '').trim();
    if (!key) return;
    counts[key] = Number(counts[key] || 0) + 1;
  });
  return Object.keys(counts)
    .map(function (key) { return { key: key, count: counts[key] }; })
    .sort(function (a, b) {
      if (b.count !== a.count) return b.count - a.count;
      return a.key < b.key ? -1 : (a.key > b.key ? 1 : 0);
    })
    .slice(0, Math.max(1, Number(limit || 3)));
}

function percentileFromSortedValues_(values, quantile) {
  if (!Array.isArray(values) || values.length === 0) return null;
  const q = Number(quantile);
  if (!Number.isFinite(q)) return null;
  if (q <= 0) return roundNumber_(Number(values[0]), 4);
  if (q >= 1) return roundNumber_(Number(values[values.length - 1]), 4);
  const position = (values.length - 1) * q;
  const lower = Math.floor(position);
  const upper = Math.ceil(position);
  if (lower === upper) return roundNumber_(Number(values[lower]), 4);
  const lowerValue = Number(values[lower]);
  const upperValue = Number(values[upper]);
  if (!Number.isFinite(lowerValue) || !Number.isFinite(upperValue)) return null;
  const interpolated = lowerValue + (upperValue - lowerValue) * (position - lower);
  return roundNumber_(interpolated, 4);
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
  const stakePolicyDecision = detail.stake_policy_decision || evaluateSignalStakePolicy_(detail.stake_units, event && event.price, config || {});
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
    proposed_stake: stakePolicyDecision.proposed_stake,
    recommended_stake: stakePolicyDecision.final_stake,
    recommended_stake_currency: stakePolicyDecision.account_currency,
    min_stake_threshold: stakePolicyDecision.minimum_stake,
    min_stake_applied: stakePolicyDecision.decision === 'adjusted',
    stake_policy_mode: stakePolicyDecision.policy_mode,
    stake_policy_decision_reason: stakePolicyDecision.reason_code,
    stake_mode_used: stakePolicyDecision.stake_mode_used || '',
    raw_risk_mxn: stakePolicyDecision.raw_risk_mxn,
    raw_target_win_mxn: stakePolicyDecision.raw_target_win_mxn,
    final_risk_mxn: stakePolicyDecision.final_risk_mxn,
    final_units: stakePolicyDecision.final_units,
    stake_adjustment_reason: stakePolicyDecision.stake_adjustment_reason || stakePolicyDecision.reason_code || '',
    min_bet_mxn: stakePolicyDecision.min_bet_mxn,
    bucket_step_mxn: stakePolicyDecision.bucket_step_mxn,
    unit_size_mxn: stakePolicyDecision.unit_size_mxn,
    opening_price: resolveOpeningPrice_(event),
    evaluation_price: resolveEvaluationPrice_(event),
    price_delta_bps: resolvePriceDeltaBps_(event),
    opening_lag_minutes: resolveOpeningLagMinutes_(event, Date.now()),
    decision_gate_status: resolveDecisionGateStatus_(detail),
    stats_confidence: Number.isFinite(Number(detail.stats_confidence)) ? Number(detail.stats_confidence) : null,
    stats_coverage_score: Number.isFinite(Number(detail.stats_coverage_score)) ? Number(detail.stats_coverage_score) : null,
    stats_coverage_tier: String(detail.stats_coverage_tier || ''),
    stats_coverage_reason_code: String(detail.stats_coverage_reason_code || ''),
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

function evaluateSignalStakePolicy_(stakeUnits, oddsOrConfig, maybeConfig) {
  const hasLegacySignature = arguments.length < 3;
  const cfg = hasLegacySignature ? (oddsOrConfig || {}) : (maybeConfig || {});
  const policyMode = normalizeStakePolicyMode_(cfg.STAKE_POLICY_MODE);
  const accountCurrency = String(cfg.ACCOUNT_CURRENCY || 'MXN').toUpperCase();
  const minimums = parseMinStakePerCurrency_(cfg.MIN_STAKE_PER_CURRENCY_JSON);
  const configuredMaxStake = resolveMaxStakeForCurrency_(cfg, accountCurrency);
  const unitSizeMxn = 100;
  const bucketStepMxn = 20;
  const oddsPrice = hasLegacySignature ? NaN : Number(oddsOrConfig);
  const stakeModeUsed = Number.isFinite(oddsPrice) && oddsPrice < 0 ? 'to_win' : 'to_risk';
  if (!Object.prototype.hasOwnProperty.call(minimums, accountCurrency)) {
    return {
      decision: 'config_error',
      reason_code: 'stake_policy_config_error',
      reason_codes: ['stake_policy_config_error'],
      policy_mode: policyMode,
      account_currency: accountCurrency,
      proposed_stake: null,
      final_stake: null,
      raw_risk_mxn: null,
      raw_target_win_mxn: null,
      final_risk_mxn: null,
      final_units: null,
      minimum_stake: null,
      min_bet_mxn: null,
      bucket_step_mxn: bucketStepMxn,
      unit_size_mxn: unitSizeMxn,
      stake_mode_used: stakeModeUsed,
      stake_adjustment_reason: 'stake_policy_config_error',
    };
  }

  const minimumStake = Number(minimums[accountCurrency]);
  const suggestedUnits = roundHalfUp_(Number(stakeUnits), 4);
  if (!Number.isFinite(suggestedUnits)) {
    return {
      decision: 'missing_stake',
      reason_code: 'stake_missing_unscored',
      reason_codes: ['stake_missing_unscored'],
      policy_mode: policyMode,
      account_currency: accountCurrency,
      proposed_stake: null,
      final_stake: null,
      raw_risk_mxn: null,
      raw_target_win_mxn: null,
      final_risk_mxn: null,
      final_units: null,
      minimum_stake: minimumStake,
      min_bet_mxn: minimumStake,
      bucket_step_mxn: bucketStepMxn,
      unit_size_mxn: unitSizeMxn,
      stake_mode_used: stakeModeUsed,
      stake_adjustment_reason: 'stake_missing_unscored',
    };
  }
  const suggestedMxn = roundHalfUp_(suggestedUnits * unitSizeMxn, 2);
  const initialRiskMxn = convertSuggestedStakeToRiskMxn_(suggestedMxn, oddsPrice);
  const rawTargetWinMxn = stakeModeUsed === 'to_win' ? suggestedMxn : null;
  if (!Number.isFinite(initialRiskMxn)) {
    return {
      decision: 'missing_stake',
      reason_code: 'stake_missing_unscored',
      reason_codes: ['stake_missing_unscored'],
      policy_mode: policyMode,
      account_currency: accountCurrency,
      proposed_stake: suggestedMxn,
      final_stake: null,
      raw_risk_mxn: null,
      raw_target_win_mxn: rawTargetWinMxn,
      final_risk_mxn: null,
      final_units: null,
      minimum_stake: minimumStake,
      min_bet_mxn: minimumStake,
      bucket_step_mxn: bucketStepMxn,
      unit_size_mxn: unitSizeMxn,
      stake_mode_used: stakeModeUsed,
      stake_adjustment_reason: 'stake_missing_unscored',
    };
  }
  const adjusted = applyRiskStakeAdjustments_(initialRiskMxn, {
    minimum_stake: minimumStake,
    bucket_step: bucketStepMxn,
    maximum_stake: configuredMaxStake,
    american_odds: oddsPrice,
  });
  const finalRiskMxn = roundHalfUp_(adjusted.final_risk_mxn, 2);
  const finalUnits = roundHalfUp_(finalRiskMxn / unitSizeMxn, 4);
  const reasonCodes = adjusted.reason_codes.length ? adjusted.reason_codes.slice() : ['stake_policy_pass'];
  const decision = adjusted.reason_codes.length ? 'adjusted' : 'passed';
  const primaryReason = reasonCodes[0];
  return {
    decision: decision,
    reason_code: primaryReason,
    reason_codes: reasonCodes,
    policy_mode: policyMode,
    account_currency: accountCurrency,
    proposed_stake: suggestedMxn,
    final_stake: finalRiskMxn,
    raw_risk_mxn: roundHalfUp_(initialRiskMxn, 2),
    raw_target_win_mxn: rawTargetWinMxn,
    final_risk_mxn: finalRiskMxn,
    final_units: finalUnits,
    minimum_stake: minimumStake,
    min_bet_mxn: minimumStake,
    bucket_step_mxn: bucketStepMxn,
    unit_size_mxn: unitSizeMxn,
    maximum_stake: configuredMaxStake,
    stake_mode: adjusted.stake_mode,
    stake_mode_used: adjusted.stake_mode,
    stake_adjustment_reason: primaryReason,
    suggested_units: suggestedUnits,
  };
}

function convertSuggestedStakeToRiskMxn_(suggestedStakeMxn, americanOdds) {
  const suggested = Number(suggestedStakeMxn);
  if (!Number.isFinite(suggested)) return NaN;
  const odds = Number(americanOdds);
  if (Number.isFinite(odds) && odds < 0) return roundHalfUp_((suggested * Math.abs(odds)) / 100, 2);
  return roundHalfUp_(suggested, 2);
}

function applyRiskStakeAdjustments_(riskMxn, options) {
  const cfg = options || {};
  let finalRisk = Number(riskMxn);
  const minStake = Number(cfg.minimum_stake);
  const bucketStep = Number(cfg.bucket_step);
  const maxStake = Number(cfg.maximum_stake);
  const reasonCodes = [];

  if (Number.isFinite(minStake) && finalRisk < minStake) {
    finalRisk = minStake;
    reasonCodes.push('stake_raised_to_min');
  }
  if (Number.isFinite(bucketStep) && bucketStep > 0) {
    const rounded = roundHalfUp_(Math.round(finalRisk / bucketStep) * bucketStep, 2);
    if (rounded !== finalRisk) {
      reasonCodes.push(rounded > finalRisk ? 'stake_bucket_rounded_up' : 'stake_bucket_rounded_down');
      finalRisk = rounded;
    }
  }
  if (Number.isFinite(maxStake) && finalRisk > maxStake) {
    finalRisk = maxStake;
    reasonCodes.push('stake_capped_to_max');
  }
  return {
    final_risk_mxn: finalRisk,
    reason_codes: reasonCodes,
    stake_mode: Number.isFinite(Number(cfg.american_odds)) && Number(cfg.american_odds) < 0 ? 'to_win' : 'to_risk',
  };
}

function resolveMaxStakeForCurrency_(cfg, accountCurrency) {
  if (Number.isFinite(Number(cfg && cfg.MAX_BET_MXN))) return roundHalfUp_(Number(cfg.MAX_BET_MXN), 2);
  const maxPerCurrency = parseMinStakePerCurrency_(cfg && cfg.MAX_STAKE_PER_CURRENCY_JSON);
  if (Object.prototype.hasOwnProperty.call(maxPerCurrency, accountCurrency)) {
    return roundHalfUp_(Number(maxPerCurrency[accountCurrency]), 2);
  }
  return null;
}

function normalizeStakePolicyMode_(value) {
  const normalized = String(value || 'strict_suppress_below_min').toLowerCase();
  if (normalized === 'round_up_to_min') return normalized;
  return 'strict_suppress_below_min';
}

function parseMinStakePerCurrency_(raw) {
  let parsed = raw;
  if (typeof parsed === 'string') {
    try {
      parsed = JSON.parse(parsed);
    } catch (err) {
      parsed = {};
    }
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return {};
  return Object.keys(parsed).reduce(function (acc, key) {
    const numeric = Number(parsed[key]);
    if (!Number.isFinite(numeric) || numeric < 0) return acc;
    acc[String(key || '').toUpperCase()] = roundHalfUp_(numeric, 2);
    return acc;
  }, {});
}

function roundHalfUp_(value, decimals) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return NaN;
  const places = Math.max(0, Number(decimals || 0));
  const scale = Math.pow(10, places);
  if (numeric >= 0) return Math.floor(numeric * scale + 0.5) / scale;
  return Math.ceil(numeric * scale - 0.5) / scale;
}

function summarizeSignalRowsStakePolicy_(signalRows, config) {
  const rows = Array.isArray(signalRows) ? signalRows : [];
  const reasonCounts = {};
  const summary = {
    policy_mode: normalizeStakePolicyMode_(config && config.STAKE_POLICY_MODE),
    account_currency: String(config && config.ACCOUNT_CURRENCY || 'MXN').toUpperCase(),
    signal_rows_evaluated: 0,
    suppressed_count: 0,
    adjusted_count: 0,
    passed_count: 0,
    missing_stake_count: 0,
    config_error_count: 0,
    reason_counts: reasonCounts,
    stake_mode_used: '',
    raw_risk_mxn: null,
    raw_target_win_mxn: null,
    final_risk_mxn: null,
    final_units: null,
    stake_adjustment_reason: '',
    min_bet_mxn: null,
    bucket_step_mxn: null,
    unit_size_mxn: null,
  };
  const representativeRow = rows.find(function (row) {
    return row && String(row.stake_policy_decision_reason || '').trim() !== '';
  }) || null;
  rows.forEach(function (row) {
    const reasonCode = String(row && row.stake_policy_decision_reason || '');
    if (!reasonCode) return;
    summary.signal_rows_evaluated += 1;
    reasonCounts[reasonCode] = Number(reasonCounts[reasonCode] || 0) + 1;
    if (reasonCode === 'stake_below_min_suppressed') summary.suppressed_count += 1;
    if (reasonCode === 'stake_rounded_to_min'
      || reasonCode === 'stake_raised_to_min'
      || reasonCode === 'stake_bucket_rounded_down'
      || reasonCode === 'stake_bucket_rounded_up'
      || reasonCode === 'stake_capped_to_max') summary.adjusted_count += 1;
    if (reasonCode === 'stake_policy_pass') summary.passed_count += 1;
    if (reasonCode === 'stake_missing_unscored') summary.missing_stake_count += 1;
    if (reasonCode === 'stake_policy_config_error') summary.config_error_count += 1;
  });
  if (representativeRow) {
    summary.stake_mode_used = String(representativeRow.stake_mode_used || '');
    summary.raw_risk_mxn = Number.isFinite(Number(representativeRow.raw_risk_mxn)) ? Number(representativeRow.raw_risk_mxn) : null;
    summary.raw_target_win_mxn = Number.isFinite(Number(representativeRow.raw_target_win_mxn)) ? Number(representativeRow.raw_target_win_mxn) : null;
    summary.final_risk_mxn = Number.isFinite(Number(representativeRow.final_risk_mxn)) ? Number(representativeRow.final_risk_mxn) : null;
    summary.final_units = Number.isFinite(Number(representativeRow.final_units)) ? Number(representativeRow.final_units) : null;
    summary.stake_adjustment_reason = String(
      representativeRow.stake_adjustment_reason
      || representativeRow.stake_policy_decision_reason
      || ''
    );
    summary.min_bet_mxn = Number.isFinite(Number(representativeRow.min_bet_mxn)) ? Number(representativeRow.min_bet_mxn) : null;
    summary.bucket_step_mxn = Number.isFinite(Number(representativeRow.bucket_step_mxn)) ? Number(representativeRow.bucket_step_mxn) : null;
    summary.unit_size_mxn = Number.isFinite(Number(representativeRow.unit_size_mxn)) ? Number(representativeRow.unit_size_mxn) : null;
  }
  return summary;
}


function resolveOpeningPrice_(event) {
  const candidates = [
    Number(event && event.opening_price),
    Number(event && event.open_price),
    Number(event && event.open_odds_price),
  ];
  const value = candidates.find(function (candidate) { return Number.isFinite(candidate); });
  return Number.isFinite(value) ? value : null;
}

function resolveEvaluationPrice_(event) {
  const candidates = [
    Number(event && event.evaluation_price),
    Number(event && event.price),
  ];
  const value = candidates.find(function (candidate) { return Number.isFinite(candidate); });
  return Number.isFinite(value) ? value : null;
}

function resolvePriceDeltaBps_(event) {
  const direct = Number(event && event.price_delta_bps);
  if (Number.isFinite(direct)) return direct;

  const openingPrice = resolveOpeningPrice_(event);
  const evaluationPrice = resolveEvaluationPrice_(event);
  const openingImplied = oddsPriceToImpliedProbability_(openingPrice);
  const evaluationImplied = oddsPriceToImpliedProbability_(evaluationPrice);
  if (!Number.isFinite(openingImplied) || !Number.isFinite(evaluationImplied)) return null;
  return roundNumber_((evaluationImplied - openingImplied) * 10000, 2);
}

function resolveOpeningLagMinutes_(event, nowMs) {
  const explicit = Number(event && event.opening_lag_minutes);
  if (Number.isFinite(explicit)) return explicit;
  const referenceMs = Number.isFinite(Number(nowMs)) ? Number(nowMs) : Date.now();
  return resolveMinutesSinceOpenSnapshot_(event, referenceMs);
}

function resolveDecisionGateStatus_(detail) {
  const explicit = String(detail && detail.decision_gate_status || '');
  if (explicit) return explicit;
  const outcome = String(detail && detail.notification_outcome || '');
  return outcome || 'unknown';
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
  return postDiscordWebhook_(
    config.DISCORD_WEBHOOK,
    { content: message },
    !!config.NOTIFY_TEST_MODE,
    Number(config.NOTIFY_WEBHOOK_MAX_RETRIES || 0)
  );
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


function postDiscordWebhook_(webhookUrl, payload, testMode, maxRetries) {
  if (testMode) {
    return {
      outcome: 'sent',
      transport: 'discord_webhook',
      http_status: 200,
      response_body_preview: 'test_mode_no_post',
      test_mode: true,
      retry_count: 0,
      attempt_count: 1,
    };
  }

  const retryBudget = Math.max(0, Number(maxRetries || 0));
  let attemptCount = 0;
  let lastFailure = null;

  while (attemptCount <= retryBudget) {
    attemptCount += 1;
    try {
      const response = UrlFetchApp.fetch(String(webhookUrl), {
        method: 'post',
        contentType: 'application/json',
        payload: JSON.stringify(payload || {}),
        muteHttpExceptions: true,
      });
      const code = Number(response.getResponseCode());
      const body = truncateForLog_(response.getContentText(), 300);
      if (code >= 200 && code < 300) {
        return {
          outcome: 'sent',
          transport: 'discord_webhook',
          http_status: code,
          response_body_preview: body,
          test_mode: false,
          retry_count: attemptCount - 1,
          attempt_count: attemptCount,
        };
      }

      lastFailure = {
        outcome: 'notify_http_failed',
        transport: 'discord_webhook',
        http_status: code,
        response_body_preview: body,
        test_mode: false,
        retry_count: attemptCount - 1,
        attempt_count: attemptCount,
      };
      if (code < 500 && code !== 429) break;
    } catch (error) {
      lastFailure = {
        outcome: 'notify_http_failed',
        transport: 'discord_webhook',
        http_status: null,
        response_body_preview: truncateForLog_(String(error && error.message ? error.message : error), 300),
        test_mode: false,
        retry_count: attemptCount - 1,
        attempt_count: attemptCount,
      };
    }
  }

  return lastFailure || {
    outcome: 'notify_http_failed',
    transport: 'discord_webhook',
    http_status: null,
    response_body_preview: 'notify_http_failed',
    test_mode: false,
    retry_count: Math.max(0, attemptCount - 1),
    attempt_count: Math.max(1, attemptCount),
  };
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
