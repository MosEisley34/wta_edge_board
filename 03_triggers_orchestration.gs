function installOrUpdateTriggers() {
  ensureTabsAndConfig_();
  const config = getConfig_();

  const spec = {
    version: 1,
    functionName: 'runEdgeBoard',
    type: 'clock',
    everyMinutes: Math.max(1, Number(config.PIPELINE_TRIGGER_EVERY_MIN || 15)),
  };

  const signature = JSON.stringify(spec);
  const scriptProps = PropertiesService.getScriptProperties();
  const existingSignature = scriptProps.getProperty(PROPS.PIPELINE_TRIGGER_SIGNATURE);
  const existingPipelineTriggers = ScriptApp.getProjectTriggers().filter((t) => t.getHandlerFunction() === spec.functionName);
  const runId = buildRunId_();

  if (existingSignature === signature && existingPipelineTriggers.length > 0) {
    appendLogRow_({
      row_type: 'ops',
      run_id: runId,
      stage: 'installOrUpdateTriggers',
      status: 'success',
      reason_code: 'trigger_noop',
      message: 'Trigger signature unchanged and trigger already exists.',
      trigger_event: 'trigger_noop',
    });
    verifyTriggerInstallation_(spec, signature, runId, 'trigger_noop');
    return;
  }

  existingPipelineTriggers.forEach((trigger) => ScriptApp.deleteTrigger(trigger));
  ScriptApp.newTrigger(spec.functionName).timeBased().everyMinutes(spec.everyMinutes).create();
  scriptProps.setProperty(PROPS.PIPELINE_TRIGGER_SIGNATURE, signature);

  const currentTriggerCount = ScriptApp.getProjectTriggers().filter((t) => t.getHandlerFunction() === spec.functionName).length;

  appendLogRow_({
    row_type: 'ops',
    run_id: runId,
    stage: 'installOrUpdateTriggers',
    status: 'success',
    reason_code: 'trigger_reinstalled',
    message: JSON.stringify({
      action: 'install_or_refresh',
      trigger_count: currentTriggerCount,
      schedule_minutes: spec.everyMinutes,
    }),
    trigger_event: 'trigger_reinstalled',
  });

  verifyTriggerInstallation_(spec, signature, runId, 'trigger_reinstalled');
}

function installAndVerifyRunEdgeBoardTrigger() {
  installOrUpdateTriggers();
  return diagnosticsTriggerInstallHealth();
}

function diagnosticsTriggerInstallHealth() {
  const config = getConfig_();
  const spec = {
    functionName: 'runEdgeBoard',
    everyMinutes: Math.max(1, Number(config.PIPELINE_TRIGGER_EVERY_MIN || 15)),
  };
  const signature = JSON.stringify({
    version: 1,
    functionName: spec.functionName,
    type: 'clock',
    everyMinutes: spec.everyMinutes,
  });
  return verifyTriggerInstallation_(spec, signature, buildRunId_(), 'trigger_health_check');
}

function verifyTriggerInstallation_(spec, signature, runId, installAction) {
  const scriptProps = PropertiesService.getScriptProperties();
  const matchingTriggers = ScriptApp.getProjectTriggers().filter((t) => t.getHandlerFunction() === spec.functionName);
  const signatureStored = scriptProps.getProperty(PROPS.PIPELINE_TRIGGER_SIGNATURE) === signature;
  const triggerCount = matchingTriggers.length;
  const lockState = checkScriptLockClear_();
  const debounceState = checkDebounceState_();
  const nextRunEstimateIso = estimateNextRunIso_(spec.everyMinutes);

  const verificationPassed = triggerCount === 1 && signatureStored;

  appendLogRow_({
    row_type: 'ops',
    run_id: runId,
    stage: 'verifyTriggerInstallation',
    status: verificationPassed ? 'success' : 'error',
    reason_code: verificationPassed ? 'trigger_install_verified' : 'trigger_install_invalid',
    message: JSON.stringify({
      install_action: installAction || 'unknown',
      trigger_count: triggerCount,
      signature_stored: signatureStored,
      expected_exactly_one_trigger: true,
    }),
    trigger_event: verificationPassed ? 'trigger_install_verified' : 'trigger_install_invalid',
  });

  appendLogRow_({
    row_type: 'ops',
    run_id: runId,
    stage: 'postInstallTriggerHealth',
    status: 'ok',
    reason_code: 'trigger_post_install_health',
    message: JSON.stringify({
      trigger_count: triggerCount,
      next_run_estimate: nextRunEstimateIso,
      debounce_clear: debounceState.debounce_clear,
      lock_clear: lockState.lock_clear,
      lock_check_error: lockState.lock_check_error,
    }),
  });

  return {
    run_id: runId,
    trigger_count: triggerCount,
    signature_stored: signatureStored,
    next_run_estimate: nextRunEstimateIso,
    debounce_clear: debounceState.debounce_clear,
    debounce_wait_ms_remaining: debounceState.debounce_wait_ms_remaining,
    lock_clear: lockState.lock_clear,
    lock_check_error: lockState.lock_check_error,
    verification_passed: verificationPassed,
  };
}

function checkDebounceState_() {
  const scriptProps = PropertiesService.getScriptProperties();
  const config = getConfig_();
  const nowMs = Date.now();
  const lastRunTs = Number(scriptProps.getProperty(PROPS.LAST_PIPELINE_RUN_TS) || 0);
  const debounceMs = Number(config.DUPLICATE_DEBOUNCE_MS || 0);
  const elapsed = nowMs - lastRunTs;
  const remaining = lastRunTs > 0 ? Math.max(0, debounceMs - elapsed) : 0;

  return {
    debounce_clear: remaining === 0,
    debounce_wait_ms_remaining: remaining,
  };
}

function checkScriptLockClear_() {
  const lock = LockService.getScriptLock();
  let hasLock = false;
  try {
    hasLock = lock.tryLock(1);
    return {
      lock_clear: hasLock,
      lock_check_error: '',
    };
  } catch (e) {
    return {
      lock_clear: false,
      lock_check_error: String(e && e.message ? e.message : e),
    };
  } finally {
    if (hasLock) {
      lock.releaseLock();
    }
  }
}

function estimateNextRunIso_(everyMinutes) {
  const minutes = Number(everyMinutes || 0);
  if (!minutes || minutes <= 0) return '';
  return formatLocalIso_(new Date(Date.now() + minutes * 60 * 1000));
}

function removePipelineTriggers() {
  ScriptApp.getProjectTriggers().forEach((trigger) => {
    if (trigger.getHandlerFunction() === 'runEdgeBoard') ScriptApp.deleteTrigger(trigger);
  });

  appendLogRow_({
    row_type: 'ops',
    run_id: buildRunId_(),
    stage: 'removePipelineTriggers',
    status: 'success',
    reason_code: 'trigger_removed',
    message: 'Removed runEdgeBoard triggers.',
    trigger_event: 'trigger_removed',
  });
}

function runEdgeBoard() {
  ensureTabsAndConfig_();

  const runId = buildRunId_();
  const startedAt = new Date();
  const scriptProps = PropertiesService.getScriptProperties();
  const lock = LockService.getScriptLock();
  let lifecycleContext = null;

  if (!tryLock_(lock, 5000)) {
    const prevented = incrementDuplicatePreventedCount_();
    appendLogRow_({
      row_type: 'summary',
      run_id: runId,
      stage: 'runEdgeBoard',
      started_at: startedAt,
      ended_at: new Date(),
      status: 'skipped',
      reason_code: 'run_locked_skip',
      message: 'Skipped due to script lock contention.',
      lock_event: 'run_locked_skip',
      duplicate_suppressed: prevented,
    });
    return;
  }

  try {
    const preflight = preflightConfigUniqueness_('runEdgeBoard preflight');
    if (!preflight.ok) {
      appendLogRow_({
        row_type: 'ops',
        run_id: runId,
        stage: 'config_uniqueness_preflight',
        status: 'failed',
        reason_code: preflight.reason_code,
        message: JSON.stringify({
          context: 'runEdgeBoard',
          duplicate_keys: preflight.duplicate_keys,
        }),
      });
      appendLogRow_({
        row_type: 'summary',
        run_id: runId,
        stage: 'runEdgeBoard',
        started_at: startedAt,
        ended_at: new Date(),
        status: 'skipped',
        reason_code: preflight.reason_code,
        message: preflight.message,
      });
      return;
    }

    const config = getConfig_();
    const maxRuntimeMs = resolvePipelineMaxRuntimeMs_(config);
    if (!config.RUN_ENABLED) {
      appendLogRow_({
        row_type: 'summary',
        run_id: runId,
        stage: 'runEdgeBoard',
        started_at: startedAt,
        ended_at: new Date(),
        status: 'skipped',
        reason_code: 'run_disabled_skip',
        message: 'RUN_ENABLED is false.',
      });
      return;
    }

    appendRunStartConfigAuditLog_(runId, config, startedAt);

    const nowMs = Date.now();
    const debounceMs = config.DUPLICATE_DEBOUNCE_MS;
    const lastRunTs = Number(scriptProps.getProperty(PROPS.LAST_PIPELINE_RUN_TS) || 0);
    if (nowMs - lastRunTs < debounceMs) {
      const prevented = incrementDuplicatePreventedCount_();
      appendLogRow_({
        row_type: 'summary',
        run_id: runId,
        stage: 'runEdgeBoard',
        started_at: startedAt,
        ended_at: new Date(),
        status: 'skipped',
        reason_code: 'run_debounced_skip',
        message: 'Skipped by debounce window.',
        debounce_event: 'run_debounced_skip',
        duplicate_suppressed: prevented,
      });
      return;
    }

    const oddsWindowDecision = resolveOddsWindowForPipeline_(config, nowMs);
    const decidedAt = localAndUtcTimestamps_(new Date());
    setStateValue_('ODDS_REFRESH_MODE_META', JSON.stringify({
      run_id: runId,
      decided_at: decidedAt.local,
      decided_at_utc: decidedAt.utc,
      current_refresh_mode: oddsWindowDecision.current_refresh_mode || '',
      decision_reason_code: oddsWindowDecision.decision_reason_code || '',
      bootstrap_mode: !!oddsWindowDecision.bootstrap_mode,
      transitioned_from_bootstrap_to_active_window: !!oddsWindowDecision.transitioned_from_bootstrap_to_active_window,
    }));
    appendLogRow_({
      row_type: 'ops',
      run_id: runId,
      stage: 'resolveOddsWindowForPipeline',
      status: oddsWindowDecision.should_fetch_odds ? 'success' : 'skipped',
      reason_code: oddsWindowDecision.decision_reason_code,
      message: oddsWindowDecision.decision_message,
    });

    appendLogRow_({
      row_type: 'ops',
      run_id: runId,
      stage: 'odds_bootstrap_metadata',
      status: 'success',
      reason_code: oddsWindowDecision.decision_reason_code === 'odds_refresh_bootstrap_blocked_by_credit_limit'
        ? 'odds_refresh_bootstrap_blocked_by_credit_limit'
        : (oddsWindowDecision.bootstrap_mode ? 'odds_refresh_bootstrap_fetch' : 'odds_refresh_bootstrap_inactive'),
      message: JSON.stringify({
        bootstrap_mode: !!oddsWindowDecision.bootstrap_mode,
        bootstrap_window_hours: Number(oddsWindowDecision.bootstrap_window_hours || 0),
        bootstrap_cached_payload_has_events: !!oddsWindowDecision.bootstrap_cached_payload_has_events,
        bootstrap_cached_payload_source: oddsWindowDecision.bootstrap_cached_payload_source || '',
        bootstrap_empty_cycle_mitigation_active: !!oddsWindowDecision.bootstrap_empty_cycle_mitigation_active,
        bootstrap_empty_cycle_count: Number(oddsWindowDecision.bootstrap_empty_cycle_count || 0),
        bootstrap_empty_cycle_threshold: Number(oddsWindowDecision.bootstrap_empty_cycle_threshold || 0),
        current_refresh_mode: oddsWindowDecision.current_refresh_mode || '',
        previous_refresh_mode: oddsWindowDecision.previous_refresh_mode || '',
        transitioned_from_bootstrap_to_active_window: !!oddsWindowDecision.transitioned_from_bootstrap_to_active_window,
        transition_state: oddsWindowDecision.transitioned_from_bootstrap_to_active_window
          ? 'bootstrap_to_active_window'
          : (oddsWindowDecision.bootstrap_mode ? 'bootstrap_active' : 'active_window_or_skipped'),
      }),
    });

    const runKey = buildRunIdempotencyKey_(config, oddsWindowDecision, nowMs);
    lifecycleContext = acquireRunLifecycleLease_(scriptProps, runId, runKey, startedAt, maxRuntimeMs);
    if (!lifecycleContext.acquired) {
      const prevented = incrementDuplicatePreventedCount_();
      appendLogRow_({
        row_type: 'summary',
        run_id: runId,
        stage: 'runEdgeBoard',
        started_at: startedAt,
        ended_at: new Date(),
        status: 'skipped',
        reason_code: 'run_idempotency_overlap_skip',
        message: 'Skipped due to active run lease for same idempotency key.',
        duplicate_suppressed: prevented,
      });
      return;
    }
    appendRunLifecycleStatus_(runId, lifecycleContext.wasResumed ? 'resumed' : 'started', {
      run_key: runKey,
      run_key_hash: lifecycleContext.runKeyHash,
      lease_status: lifecycleContext.watchdogRecovered ? 'watchdog_recovered' : 'active',
    });
    if (lifecycleContext.watchdogRecovered) {
      appendRunLifecycleStatus_(runId, 'watchdog_recovered', {
        run_key: runKey,
        run_key_hash: lifecycleContext.runKeyHash,
        recovered_from_run_id: lifecycleContext.recoveredFromRunId,
      });
    }

    const initialOddsStage = runCheckpointedStage_(lifecycleContext, 'odds_fetch', function () {
      assertPipelineRuntimeBudget_(lifecycleContext, runId);
      return oddsWindowDecision.should_fetch_odds
        ? stageFetchOdds(runId, config, oddsWindowDecision.odds_fetch_window)
        : buildSkippedOddsStage_(runId, oddsWindowDecision.decision_reason_code, oddsWindowDecision.decision_message);
    });
    const oddsStage = runCheckpointedStage_(lifecycleContext, 'odds_gate', function () {
      assertPipelineRuntimeBudget_(lifecycleContext, runId);
      return applyOpeningLagActionabilityGate_(runId, config, initialOddsStage);
    });
    appendStageLog_(runId, oddsStage.summary);

    const selectedOddsSource = (oddsWindowDecision.selected_source === 'fallback_static_window' || oddsWindowDecision.selected_source === 'bootstrap_static_window')
      ? oddsWindowDecision.selected_source
      : (oddsStage.selected_source || 'cached_stale_fallback');
    appendLogRow_({
      row_type: 'ops',
      run_id: runId,
      stage: 'odds_source_selection',
      status: 'success',
      reason_code: selectedOddsSource,
      message: 'Odds source selected for this run: ' + selectedOddsSource,
    });

    const scheduleStage = runCheckpointedStage_(lifecycleContext, 'schedule', function () {
      assertPipelineRuntimeBudget_(lifecycleContext, runId);
      return stageFetchSchedule(runId, config, oddsStage.events, {
        bootstrap_empty_cycle_mitigation_active: !!oddsWindowDecision.bootstrap_empty_cycle_mitigation_active,
      });
    });
    appendStageLog_(runId, scheduleStage.summary);

    const matchStage = runCheckpointedStage_(lifecycleContext, 'match', function () {
      assertPipelineRuntimeBudget_(lifecycleContext, runId);
      return stageMatchEvents(runId, config, oddsStage.events, scheduleStage.events);
    });
    appendStageLog_(runId, matchStage.summary);

    const playerStatsSkipReason = derivePlayerStatsSkipReason_(oddsStage, matchStage);
    const playerStatsStage = runCheckpointedStage_(lifecycleContext, 'player_stats', function () {
      assertPipelineRuntimeBudget_(lifecycleContext, runId);
      return playerStatsSkipReason
        ? buildSkippedPlayerStatsStage_(runId, playerStatsSkipReason)
        : stageFetchPlayerStats(runId, config, oddsStage.events, matchStage.rows);
    });
    appendStageLog_(runId, playerStatsStage.summary);

    const signalUpstreamGateReason = deriveSignalUpstreamGateReason_(oddsStage, matchStage, playerStatsStage);
    const signalStage = runCheckpointedStage_(lifecycleContext, 'signals', function () {
      assertPipelineRuntimeBudget_(lifecycleContext, runId);
      return stageGenerateSignals(
        runId,
        config,
        oddsStage.events,
        matchStage.rows,
        playerStatsStage.byOddsEventId,
        {
          upstream_gate_reason: signalUpstreamGateReason,
        }
      );
    });
    appendStageLog_(runId, signalStage.summary);

    const persistStage = runCheckpointedStage_(lifecycleContext, 'persist', function () {
      assertPipelineRuntimeBudget_(lifecycleContext, runId);
      return stagePersist(runId, {
        odds: oddsStage.rows,
        schedule: scheduleStage.rows,
        playerStats: playerStatsStage.rows,
        matchMap: matchStage.rows,
        matchMapMatchedCount: matchStage.matchedCount,
        matchMapRejectedCount: matchStage.rejectedCount,
        matchMapDiagnosticRecordsWritten: matchStage.diagnosticRecordsWritten,
        signals: signalStage.rows,
      });
    });
    appendStageLog_(runId, persistStage.summary);

    logDiagnosticEvent_(config, 'pipeline_stage_counts', {
      run_id: runId,
      fetched_odds: oddsStage.events.length,
      fetched_schedule: scheduleStage.events.length,
      matched: matchStage.matchedCount,
      unmatched: matchStage.unmatchedCount,
      rejected: matchStage.rejectedCount,
      player_stats_rows: playerStatsStage.rows.length,
      signals_found: signalStage.rows.length,
      signals_sent: signalStage.sentCount,
      cooldown_suppressed: signalStage.cooldownSuppressedCount,
      duplicate_suppressed: signalStage.duplicateSuppressedCount,
      persist_total_rows: oddsStage.rows.length + scheduleStage.rows.length + playerStatsStage.rows.length + matchStage.rows.length + signalStage.rows.length,
    }, 2);

    logDiagnosticEvent_(config, 'pipeline_sampling', {
      run_id: runId,
      sample_odds_event_ids: oddsStage.events.slice(0, 10).map((event) => event.event_id),
      sample_unmatched_cases: matchStage.unmatched.slice(0, 5),
      sample_top_unresolved_competitions: scheduleStage.topUnresolvedCompetitions.slice(0, 5),
      sample_signals: signalStage.rows.slice(0, 5).map((row) => ({
        odds_event_id: row.odds_event_id,
        market: row.market,
        side: row.side,
        edge_value: row.edge_value,
        reason_code: row.reason_code,
      })),
    }, 3);

    const fetchedOddsCount = Number(oddsStage.events.length || 0);
    const matchedCount = Number(matchStage.matchedCount || 0);
    const signalsFoundCount = Number(signalStage.rows.length || 0);
    const playersWithNonNullStats = Number(
      playerStatsStage
      && playerStatsStage.summary
      && playerStatsStage.summary.reason_metadata
      && playerStatsStage.summary.reason_metadata.players_with_non_null_stats
      || 0
    );
    const statsZeroCoverage = matchedCount > 0 && playersWithNonNullStats === 0;
    if (statsZeroCoverage) {
      playerStatsStage.summary.reason_codes.stats_zero_coverage =
        (playerStatsStage.summary.reason_codes.stats_zero_coverage || 0) + 1;
    }
    const runHealthDiagnostics = evaluateRunHealthDiagnostics_({
      fetched_odds: fetchedOddsCount,
      fetched_schedule: scheduleStage.events.length,
      matched: matchedCount,
      signals_found: signalsFoundCount,
      players_with_non_null_stats: playersWithNonNullStats,
      sample_unmatched_cases: matchStage.unmatched,
      odds_reason_codes: oddsStage.summary.reason_codes,
      schedule_reason_codes: scheduleStage.summary.reason_codes,
      match_reason_codes: matchStage.summary.reason_codes,
    });

    if (runHealthDiagnostics.warning_payload && runHealthDiagnostics.should_emit_warning) {
      appendLogRow_({
        row_type: 'ops',
        run_id: runId,
        stage: 'run_health_guard',
        status: 'warning',
        reason_code: runHealthDiagnostics.warning_payload.reason_code,
        message: JSON.stringify(runHealthDiagnostics.warning_payload),
      });
      logDiagnosticEvent_(config, 'run_health_guard_warning', runHealthDiagnostics.warning_payload, 1);
    }

    const reasonCodeMaps = [
      oddsStage.summary.reason_codes,
      scheduleStage.summary.reason_codes,
      matchStage.summary.reason_codes,
      playerStatsStage.summary.reason_codes,
      signalStage.summary.reason_codes,
      persistStage.summary.reason_codes,
    ];
    const reasonMetadataMaps = [
      oddsStage.summary.reason_metadata,
      scheduleStage.summary.reason_metadata,
      matchStage.summary.reason_metadata,
      playerStatsStage.summary.reason_metadata,
      signalStage.summary.reason_metadata,
      persistStage.summary.reason_metadata,
    ];

    const combinedReasonCodes = mergeReasonCounts_(reasonCodeMaps);
    const combinedReasonMetadata = mergeReasonMetadata_(reasonMetadataMaps);

    const emptyCycleState = updateBootstrapEmptyCycleState_(runId, oddsStage.rows.length, scheduleStage.events.length);
    if (emptyCycleState.reason_code) {
      combinedReasonCodes[emptyCycleState.reason_code] = (combinedReasonCodes[emptyCycleState.reason_code] || 0) + 1;
    }
    if (runHealthDiagnostics.degraded_reason_code) {
      combinedReasonCodes[runHealthDiagnostics.degraded_reason_code] = (combinedReasonCodes[runHealthDiagnostics.degraded_reason_code] || 0) + 1;
    }
    if (emptyCycleState.warning_needed) {
      appendLogRow_({
        row_type: 'ops',
        run_id: runId,
        stage: 'bootstrap_empty_cycle_watchdog',
        status: 'warning',
        reason_code: 'bootstrap_empty_cycle_detected',
        message: JSON.stringify({
          consecutive_empty_cycles: emptyCycleState.consecutive_empty_cycles,
          threshold: emptyCycleState.threshold,
          diagnostics_counter: emptyCycleState.diagnostics_counter,
          last_non_empty_fetch_at: emptyCycleState.last_non_empty_fetch_at || '',
          last_non_empty_fetch_at_utc: emptyCycleState.last_non_empty_fetch_at_utc || '',
        }),
      });
    }

    const productiveOutputState = updateEmptyProductiveOutputState_(runId, {
      fetched_odds: fetchedOddsCount,
      fetched_schedule: scheduleStage.events.length,
      signals_found: signalsFoundCount,
    }, config);
    if (productiveOutputState.reason_code) {
      combinedReasonCodes[productiveOutputState.reason_code] = (combinedReasonCodes[productiveOutputState.reason_code] || 0) + 1;
    }
    if (productiveOutputState.warning_needed) {
      const productiveWarningPayload = {
        reason_code: productiveOutputState.reason_code || 'productive_output_empty_streak_detected',
        streak_count: productiveOutputState.consecutive_count,
        threshold: productiveOutputState.threshold,
        fetched_odds: fetchedOddsCount,
        signals_found: signalsFoundCount,
        run_id: runId,
      };
      appendLogRow_({
        row_type: 'ops',
        run_id: runId,
        stage: 'productive_output_watchdog',
        status: 'warning',
        reason_code: productiveWarningPayload.reason_code,
        message: JSON.stringify(productiveWarningPayload),
      });
      logDiagnosticEvent_(config, 'productive_output_watchdog_warning', productiveWarningPayload, 1);
    }

    if (productiveOutputState.schedule_only_notice_needed) {
      const expectedIdleOutsideOddsWindow = runHealthDiagnostics.status === 'idle_outside_odds_window'
        || runHealthDiagnostics.reason_code === 'odds_refresh_skipped_outside_window';
      const scheduleOnlyPayload = {
        reason_code: productiveOutputState.schedule_only_reason_code || 'schedule_only_streak_detected',
        streak_count: productiveOutputState.schedule_only_consecutive_count,
        threshold: productiveOutputState.schedule_only_threshold,
        fetched_schedule: scheduleStage.events.length,
        fetched_odds: fetchedOddsCount,
        notice_severity: 'low',
        expected_idle: expectedIdleOutsideOddsWindow,
        odds_window_context: expectedIdleOutsideOddsWindow ? 'outside_window' : 'within_window_or_unknown',
        message: expectedIdleOutsideOddsWindow
          ? 'Schedule-only streak reached notice threshold while odds refresh is outside window; this run is expected to be idle and should not be treated as a pipeline failure.'
          : 'Schedule-only streak reached notice threshold.',
        run_id: runId,
      };
      appendLogRow_({
        row_type: 'ops',
        run_id: runId,
        stage: 'schedule_only_watchdog',
        status: 'notice',
        reason_code: scheduleOnlyPayload.reason_code,
        message: JSON.stringify(scheduleOnlyPayload),
      });
      logDiagnosticEvent_(config, 'schedule_only_watchdog_notice', scheduleOnlyPayload, 1);
    }

    const runStartedAt = localAndUtcTimestamps_(startedAt);
    const runEndedAt = localAndUtcTimestamps_(new Date());
    const verbosePayload = {
      run_id: runId,
      timezone: TIMESTAMP_TIMEZONE.ID,
      timezone_offset: TIMESTAMP_TIMEZONE.OFFSET,
      started_at: runStartedAt.local,
      started_at_utc: runStartedAt.utc,
      ended_at: runEndedAt.local,
      ended_at_utc: runEndedAt.utc,
      config_snapshot: config,
      odds_refresh_decision: {
        decision_reason_code: oddsWindowDecision.decision_reason_code,
        should_fetch_odds: oddsWindowDecision.should_fetch_odds,
        first_eligible_match_start: oddsWindowDecision.first_eligible_start_ms ? formatLocalIso_(new Date(oddsWindowDecision.first_eligible_start_ms)) : '',
        first_eligible_match_start_utc: oddsWindowDecision.first_eligible_start_ms ? new Date(oddsWindowDecision.first_eligible_start_ms).toISOString() : '',
        last_eligible_match_start: oddsWindowDecision.last_eligible_start_ms ? formatLocalIso_(new Date(oddsWindowDecision.last_eligible_start_ms)) : '',
        last_eligible_match_start_utc: oddsWindowDecision.last_eligible_start_ms ? new Date(oddsWindowDecision.last_eligible_start_ms).toISOString() : '',
        refresh_window_start: oddsWindowDecision.refresh_window_start_ms ? formatLocalIso_(new Date(oddsWindowDecision.refresh_window_start_ms)) : '',
        refresh_window_start_utc: oddsWindowDecision.refresh_window_start_ms ? new Date(oddsWindowDecision.refresh_window_start_ms).toISOString() : '',
        refresh_window_end: oddsWindowDecision.refresh_window_end_ms ? formatLocalIso_(new Date(oddsWindowDecision.refresh_window_end_ms)) : '',
        refresh_window_end_utc: oddsWindowDecision.refresh_window_end_ms ? new Date(oddsWindowDecision.refresh_window_end_ms).toISOString() : '',
        eligible_match_count: oddsWindowDecision.eligible_match_count || 0,
        bootstrap_mode: !!oddsWindowDecision.bootstrap_mode,
        bootstrap_window_hours: Number(oddsWindowDecision.bootstrap_window_hours || 0),
        bootstrap_cached_payload_has_events: !!oddsWindowDecision.bootstrap_cached_payload_has_events,
        bootstrap_cached_payload_source: oddsWindowDecision.bootstrap_cached_payload_source || '',
        current_refresh_mode: oddsWindowDecision.current_refresh_mode || '',
        previous_refresh_mode: oddsWindowDecision.previous_refresh_mode || '',
        transitioned_from_bootstrap_to_active_window: !!oddsWindowDecision.transitioned_from_bootstrap_to_active_window,
        transition_state: oddsWindowDecision.transitioned_from_bootstrap_to_active_window
          ? 'bootstrap_to_active_window'
          : (oddsWindowDecision.bootstrap_mode ? 'bootstrap_active' : 'active_window_or_skipped'),
      },
      stage_summaries: [
        oddsStage.summary,
        scheduleStage.summary,
        matchStage.summary,
        playerStatsStage.summary,
        signalStage.summary,
        persistStage.summary,
      ],
      canonicalization_examples: {
        competition: scheduleStage.canonicalExamples.slice(0, 25),
        players: matchStage.canonicalizationExamples.slice(0, 25),
      },
      unresolved_competitions: scheduleStage.unresolvedCompetitions.slice(0, 50),
      top_unresolved_competitions: scheduleStage.topUnresolvedCompetitions,
      sample_unmatched_cases: matchStage.unmatched.slice(0, 20),
      top_rejection_reasons: getTopReasonCodes_(combinedReasonCodes, 10),
      reason_codes: combinedReasonCodes,
      reason_metadata: combinedReasonMetadata,
      upstream_gate_reason: combinedReasonMetadata.upstream_gate_reason || '',
      run_health: {
        status: runHealthDiagnostics.status,
        reason_code: runHealthDiagnostics.reason_code,
        diagnostics: runHealthDiagnostics.warning_payload,
      },
      productive_output_watchdog: productiveOutputState,
    };

    setStateValue_('LAST_RUN_VERBOSE_JSON', JSON.stringify(verbosePayload, null, 2));
    const competitionDiagnosticsGeneratedAt = localAndUtcTimestamps_(new Date());
    setStateValue_('LAST_RUN_COMPETITION_DIAGNOSTICS_JSON', JSON.stringify({
      run_id: runId,
      generated_at: competitionDiagnosticsGeneratedAt.local,
      generated_at_utc: competitionDiagnosticsGeneratedAt.utc,
      source_fields_priority: (scheduleStage.canonicalExamples[0] && scheduleStage.canonicalExamples[0].resolver_fields || []).map((f) => f.field),
      top_unresolved_competitions: scheduleStage.topUnresolvedCompetitions,
      unresolved_competition_counts: scheduleStage.unresolvedCompetitionCounts,
    }, null, 2));

    // Policy: only successful orchestration updates debounce; crashed runs should retry immediately.
    scriptProps.setProperty(PROPS.LAST_PIPELINE_RUN_TS, String(nowMs));

    appendLogRow_({
      row_type: 'summary',
      run_id: runId,
      stage: 'runEdgeBoard',
      started_at: startedAt,
      ended_at: new Date(),
      status: runHealthDiagnostics.summary_status,
      reason_code: runHealthDiagnostics.summary_reason_code,
      message: runHealthDiagnostics.summary_message,
      fetched_odds: fetchedOddsCount,
      fetched_schedule: scheduleStage.events.length,
      allowed_tournaments: scheduleStage.allowedCount,
      matched: matchedCount,
      unmatched: matchStage.unmatchedCount,
      rejected: matchStage.rejectedCount,
      signals_found: signalStage.sentCount,
      rejection_codes: JSON.stringify(combinedReasonCodes),
      stage_summaries: JSON.stringify(verbosePayload.stage_summaries),
      cooldown_suppressed: signalStage.cooldownSuppressedCount,
      duplicate_suppressed: signalStage.duplicateSuppressedCount,
    });
    markRunLifecycleCompleted_(lifecycleContext, runId);
  } catch (error) {
    const errorMessage = String(error && error.message ? error.message : error);
    appendRunLifecycleStatus_(runId, 'aborted', {
      reason_code: error && error.reason_code ? error.reason_code : 'run_exception',
      message: errorMessage,
    });
    appendLogRow_({
      row_type: 'summary',
      run_id: runId,
      stage: 'runEdgeBoard',
      started_at: startedAt,
      ended_at: new Date(),
      status: 'failed',
      reason_code: 'run_exception',
      message: errorMessage,
      exception: errorMessage,
      stack: String(error && error.stack ? error.stack : ''),
    });
    throw error;
  } finally {
    releaseRunLifecycleLease_(scriptProps, lifecycleContext, runId);
    lock.releaseLock();
  }
}

function resolvePipelineMaxRuntimeMs_(config) {
  const configured = Number(config && config.PIPELINE_MAX_RUNTIME_MS || 0);
  if (configured > 0) return configured;
  return 330000;
}

function buildRunIdempotencyKey_(config, oddsWindowDecision, nowMs) {
  const anchorDate = new Date(Number(nowMs || Date.now()));
  const dateWindow = [
    formatLocalIso_(anchorDate).slice(0, 10),
    oddsWindowDecision && oddsWindowDecision.refresh_window_start_ms ? String(oddsWindowDecision.refresh_window_start_ms) : '',
    oddsWindowDecision && oddsWindowDecision.refresh_window_end_ms ? String(oddsWindowDecision.refresh_window_end_ms) : '',
  ].join('|');
  const tournamentWindow = [
    'wta125:' + (config && config.ALLOW_WTA_125 ? '1' : '0'),
    'wta250:' + (config && config.ALLOW_WTA_250 ? '1' : '0'),
    'lookahead:' + String(config && config.LOOKAHEAD_HOURS || ''),
  ].join('|');
  const mode = [
    String(config && config.MODEL_MODE || ''),
    String(oddsWindowDecision && oddsWindowDecision.current_refresh_mode || ''),
    String(oddsWindowDecision && oddsWindowDecision.decision_reason_code || ''),
  ].join('|');
  return [dateWindow, tournamentWindow, mode].join('::');
}

function acquireRunLifecycleLease_(scriptProps, runId, runKey, startedAt, maxRuntimeMs) {
  const runKeyHash = String(stringHashCode_(runKey));
  const leasePropKey = 'RUN_ACTIVE_LEASE_' + runKeyHash;
  const checkpointStateKey = 'RUN_CHECKPOINT_' + runKeyHash;
  const nowMs = Date.now();
  const existing = safeJsonParse_(scriptProps.getProperty(leasePropKey) || '{}') || {};
  const leaseAgeMs = Number(existing.heartbeat_ms || existing.started_at_ms || 0) > 0
    ? (nowMs - Number(existing.heartbeat_ms || existing.started_at_ms || 0))
    : Number.MAX_SAFE_INTEGER;
  const isStale = !!(existing.run_id && leaseAgeMs > Math.max(30000, Number(maxRuntimeMs || 0)));
  if (existing.run_id && !isStale) {
    return { acquired: false, runKeyHash: runKeyHash };
  }
  const checkpoint = getStateJson_(checkpointStateKey) || {};
  const wasResumed = checkpoint && checkpoint.last_stage && checkpoint.status !== 'completed';
  const leasePayload = {
    run_id: runId,
    run_key_hash: runKeyHash,
    started_at_ms: startedAt.getTime(),
    heartbeat_ms: nowMs,
    max_runtime_ms: Number(maxRuntimeMs || 0),
    status: 'active',
  };
  scriptProps.setProperty(leasePropKey, JSON.stringify(leasePayload));
  return {
    acquired: true,
    runId: runId,
    wasResumed: !!wasResumed,
    checkpoint: checkpoint,
    checkpointStateKey: checkpointStateKey,
    leasePropKey: leasePropKey,
    runKeyHash: runKeyHash,
    maxRuntimeMs: Number(maxRuntimeMs || 0),
    startedAtMs: startedAt.getTime(),
    watchdogRecovered: isStale,
    recoveredFromRunId: String(existing.run_id || ''),
  };
}

function runCheckpointedStage_(lifecycleContext, stageName, computeFn) {
  if (!lifecycleContext) return computeFn();
  const checkpoint = lifecycleContext.checkpoint || {};
  const stageOutputs = checkpoint.stage_outputs || {};
  const stageOrder = {
    odds_fetch: 1,
    odds_gate: 2,
    schedule: 3,
    match: 4,
    player_stats: 5,
    signals: 6,
    persist: 7,
  };
  const checkpointStageOrder = Number(stageOrder[checkpoint.last_stage] || 0);
  const currentStageOrder = Number(stageOrder[stageName] || 0);
  if (checkpointStageOrder >= currentStageOrder && stageOutputs[stageName]) {
    refreshRunHeartbeat_(lifecycleContext, stageName);
    return hydrateCheckpointedStageOutput_(stageName, stageOutputs[stageName]);
  }
  const output = computeFn();
  const nextCheckpoint = {
    run_id: lifecycleContext.runId,
    run_key_hash: lifecycleContext.runKeyHash,
    last_stage: stageName,
    updated_at: formatLocalIso_(new Date()),
    stage_outputs: Object.assign({}, stageOutputs, { [stageName]: output }),
  };
  lifecycleContext.checkpoint = nextCheckpoint;
  setStateValue_(lifecycleContext.checkpointStateKey, JSON.stringify(nextCheckpoint));
  refreshRunHeartbeat_(lifecycleContext, stageName);
  return output;
}

function hydrateCheckpointedStageOutput_(stageName, output) {
  if (!output) return output;
  if (stageName !== 'odds_fetch' && stageName !== 'odds_gate') return output;
  const hydrated = JSON.parse(JSON.stringify(output));
  const events = Array.isArray(hydrated.events) ? hydrated.events : [];
  events.forEach(function (event) {
    if (!event || typeof event !== 'object') return;
    if (event.provider_odds_updated_time && typeof event.provider_odds_updated_time === 'string') {
      event.provider_odds_updated_time = new Date(event.provider_odds_updated_time);
    }
    if (event.commence_time && typeof event.commence_time === 'string') {
      event.commence_time = new Date(event.commence_time);
    }
    if (event.open_timestamp && typeof event.open_timestamp === 'string') {
      event.open_timestamp = new Date(event.open_timestamp);
    }
    if (event.opening_lag_evaluated_at && typeof event.opening_lag_evaluated_at === 'string') {
      event.opening_lag_evaluated_at = new Date(event.opening_lag_evaluated_at);
    }
  });
  hydrated.events = events;
  return hydrated;
}

function refreshRunHeartbeat_(lifecycleContext, stageName) {
  if (!lifecycleContext) return;
  const payload = {
    run_id: lifecycleContext.runId,
    run_key_hash: lifecycleContext.runKeyHash,
    started_at_ms: lifecycleContext.startedAtMs,
    heartbeat_ms: Date.now(),
    max_runtime_ms: lifecycleContext.maxRuntimeMs,
    stage: stageName || '',
    status: 'active',
  };
  PropertiesService.getScriptProperties().setProperty(lifecycleContext.leasePropKey, JSON.stringify(payload));
}

function assertPipelineRuntimeBudget_(lifecycleContext, runId) {
  if (!lifecycleContext) return;
  const elapsed = Date.now() - Number(lifecycleContext.startedAtMs || 0);
  if (elapsed <= Number(lifecycleContext.maxRuntimeMs || 0)) return;
  const error = new Error('Pipeline max runtime exceeded; aborting run.');
  error.reason_code = 'run_max_runtime_exceeded';
  appendRunLifecycleStatus_(runId, 'aborted', {
    reason_code: error.reason_code,
    elapsed_ms: elapsed,
    max_runtime_ms: lifecycleContext.maxRuntimeMs,
  });
  throw error;
}

function markRunLifecycleCompleted_(lifecycleContext, runId) {
  if (!lifecycleContext) return;
  appendRunLifecycleStatus_(runId, 'completed', {
    run_key_hash: lifecycleContext.runKeyHash,
  });
  setStateValue_(lifecycleContext.checkpointStateKey, JSON.stringify({
    run_id: runId,
    run_key_hash: lifecycleContext.runKeyHash,
    completed_at: formatLocalIso_(new Date()),
    status: 'completed',
  }));
}

function releaseRunLifecycleLease_(scriptProps, lifecycleContext, runId) {
  if (!lifecycleContext || !lifecycleContext.leasePropKey) return;
  const lease = safeJsonParse_(scriptProps.getProperty(lifecycleContext.leasePropKey) || '{}') || {};
  if (String(lease.run_id || '') !== String(runId || '')) return;
  scriptProps.deleteProperty(lifecycleContext.leasePropKey);
}

function appendRunLifecycleStatus_(runId, status, payload) {
  appendLogRow_({
    row_type: 'ops',
    run_id: runId,
    stage: 'run_lifecycle',
    status: status,
    reason_code: status,
    message: JSON.stringify(Object.assign({ status: status }, payload || {})),
  });
}


function derivePlayerStatsSkipReason_(oddsStage, matchStage) {
  const oddsCount = Number((oddsStage && oddsStage.events && oddsStage.events.length) || 0);
  const matchReasonCodes = (matchStage && matchStage.summary && matchStage.summary.reason_codes) || {};

  if (oddsCount === 0 && Number(matchReasonCodes.schedule_seed_no_odds || 0) > 0) {
    return 'skipped_schedule_only_no_odds';
  }

  return '';
}


function applyOpeningLagActionabilityGate_(runId, config, oddsStage) {
  const stage = oddsStage || { events: [], rows: [], summary: { reason_codes: {}, reason_metadata: {} } };
  const originalEvents = Array.isArray(stage.events) ? stage.events : [];
  const originalRows = Array.isArray(stage.rows) ? stage.rows : [];
  const maxOpeningLagMinutes = Math.max(0, Number(config.MAX_OPENING_LAG_MINUTES || 0));
  const requireOpeningLineProximity = !!config.REQUIRE_OPENING_LINE_PROXIMITY;
  const now = new Date();
  const nowMs = now.getTime();

  let missingOpenTimestamp = 0;
  let openingLagExceeded = 0;
  let openingLagWithinLimit = 0;

  const enrichedEvents = originalEvents.map(function (event) {
    const openTimestamp = event && event.provider_odds_updated_time instanceof Date && !Number.isNaN(event.provider_odds_updated_time.getTime())
      ? event.provider_odds_updated_time
      : null;
    const openingLagMinutes = openTimestamp ? Math.max(0, Math.round((nowMs - openTimestamp.getTime()) / 60000)) : null;
    const evaluated = Object.assign({}, event || {});
    evaluated.open_timestamp = openTimestamp;
    evaluated.opening_lag_minutes = openingLagMinutes;
    evaluated.opening_lag_evaluated_at = now;
    evaluated.is_actionable = true;
    evaluated.reason_code = '';

    if (!openTimestamp) {
      missingOpenTimestamp += 1;
      evaluated.is_actionable = false;
      evaluated.reason_code = 'missing_open_timestamp';
      return evaluated;
    }

    if (requireOpeningLineProximity && maxOpeningLagMinutes > 0 && openingLagMinutes > maxOpeningLagMinutes) {
      openingLagExceeded += 1;
      evaluated.is_actionable = false;
      evaluated.reason_code = 'opening_lag_exceeded';
      return evaluated;
    }

    openingLagWithinLimit += 1;
    return evaluated;
  });

  const rowByKey = {};
  originalRows.forEach(function (row) { rowByKey[row.key] = row; });

  const enrichedRows = enrichedEvents.map(function (event) {
    const key = [event.event_id, event.market, event.outcome].join('|');
    const baseRow = rowByKey[key] || {};
    return Object.assign({}, baseRow, {
      open_timestamp: event.open_timestamp ? event.open_timestamp.toISOString() : '',
      open_timestamp_epoch_ms: event.open_timestamp ? event.open_timestamp.getTime() : '',
      opening_lag_minutes: Number.isFinite(Number(event.opening_lag_minutes)) ? Number(event.opening_lag_minutes) : '',
      opening_lag_evaluated_at: event.opening_lag_evaluated_at ? event.opening_lag_evaluated_at.toISOString() : '',
      is_actionable: event.is_actionable !== false,
      reason_code: event.reason_code || '',
    });
  });

  const actionableEvents = enrichedEvents.filter(function (event) { return event.is_actionable !== false; });

  stage.events = actionableEvents;
  stage.rows = enrichedRows;
  stage.non_actionable_rows = enrichedRows.filter(function (row) { return row.is_actionable === false; });
  stage.skipped_reason_codes = {
    missing_open_timestamp: missingOpenTimestamp,
    opening_lag_exceeded: openingLagExceeded,
  };

  const reasonCodes = stage.summary && stage.summary.reason_codes ? stage.summary.reason_codes : {};
  reasonCodes.opening_lag_within_limit = openingLagWithinLimit;
  reasonCodes.missing_open_timestamp = missingOpenTimestamp;
  reasonCodes.opening_lag_exceeded = openingLagExceeded;
  reasonCodes.odds_actionable = actionableEvents.length;
  reasonCodes.odds_non_actionable = enrichedRows.length - actionableEvents.length;
  if (stage.summary) stage.summary.reason_codes = reasonCodes;
  if (stage.summary) {
    stage.summary.output_count = actionableEvents.length;
    stage.summary.reason_metadata = stage.summary.reason_metadata || {};
    stage.summary.reason_metadata.max_opening_lag_minutes = maxOpeningLagMinutes;
    stage.summary.reason_metadata.require_opening_line_proximity = requireOpeningLineProximity;
  }

  writeOpeningLagSkipState_(runId, {
    max_opening_lag_minutes: maxOpeningLagMinutes,
    require_opening_line_proximity: requireOpeningLineProximity,
    evaluated_count: enrichedRows.length,
    actionable_count: actionableEvents.length,
    missing_open_timestamp: missingOpenTimestamp,
    opening_lag_exceeded: openingLagExceeded,
  });

  return stage;
}

function deriveSignalUpstreamGateReason_(oddsStage, matchStage, playerStatsStage) {
  const oddsCount = Number((oddsStage && oddsStage.events && oddsStage.events.length) || 0);
  const matchedCount = Number((matchStage && matchStage.matchedCount) || 0);
  const matchReasonCodes = (matchStage && matchStage.summary && matchStage.summary.reason_codes) || {};
  const playersWithNonNullStats = Number(
    playerStatsStage
    && playerStatsStage.summary
    && playerStatsStage.summary.reason_metadata
    && playerStatsStage.summary.reason_metadata.players_with_non_null_stats
    || 0
  );

  if (oddsCount === 0 && Number(matchReasonCodes.schedule_seed_no_odds || 0) > 0) {
    return 'schedule_seed_no_odds';
  }

  if (oddsCount > 0 && matchedCount === 0) {
    return 'no_matched_events';
  }

  if (matchedCount > 0 && playersWithNonNullStats === 0) {
    return 'stats_zero_coverage';
  }

  return '';
}

function evaluateRunHealthDiagnostics_(metrics) {
  const fetchedOdds = Number(metrics && metrics.fetched_odds || 0);
  const fetchedSchedule = Number(metrics && metrics.fetched_schedule || 0);
  const matched = Number(metrics && metrics.matched || 0);
  const signalsFound = Number(metrics && metrics.signals_found || 0);
  const playersWithNonNullStats = Number(metrics && metrics.players_with_non_null_stats || 0);
  const sampleUnmatchedCases = (metrics && metrics.sample_unmatched_cases) || [];
  const oddsReasonCodes = Object.assign({}, (metrics && metrics.odds_reason_codes) || {});
  const scheduleReasonCodes = Object.assign({}, (metrics && metrics.schedule_reason_codes) || {});
  const matchReasonCodes = Object.assign({}, (metrics && metrics.match_reason_codes) || {});

  const scheduleOnlyIdle = fetchedOdds === 0
    && fetchedSchedule > 0
    && Number(oddsReasonCodes.odds_refresh_skipped_outside_window || 0) > 0;

  if (scheduleOnlyIdle) {
    const idlePayload = {
      reason_code: 'run_health_expected_idle_outside_odds_window',
      fetched_odds: fetchedOdds,
      fetched_schedule: fetchedSchedule,
      matched: matched,
      signals_found: signalsFound,
      message: 'Odds refresh was intentionally skipped outside the configured odds window; zero matches/signals are expected in this schedule-only run.',
    };

    return {
      is_degraded: false,
      status: 'idle_outside_odds_window',
      reason_code: 'odds_refresh_skipped_outside_window',
      degraded_reason_code: '',
      summary_status: 'success',
      summary_reason_code: 'odds_refresh_skipped_outside_window',
      summary_message: idlePayload.message,
      warning_payload: idlePayload,
      should_emit_warning: false,
    };
  }

  const degraded = fetchedOdds > 0 && matched === 0;
  const statsZeroCoverageDegraded = matched > 0 && playersWithNonNullStats === 0;
  if (statsZeroCoverageDegraded) {
    const statsCoveragePayload = {
      reason_code: 'stats_zero_coverage',
      fetched_odds: fetchedOdds,
      fetched_schedule: fetchedSchedule,
      matched: matched,
      signals_found: signalsFound,
      players_with_non_null_stats: playersWithNonNullStats,
      message: 'Matched events found but player stats coverage is zero; running in fallback-only mode.',
    };

    return {
      is_degraded: true,
      status: 'degraded',
      reason_code: 'stats_zero_coverage',
      degraded_reason_code: 'stats_zero_coverage',
      summary_status: 'degraded',
      summary_reason_code: 'stats_zero_coverage',
      summary_message: statsCoveragePayload.message,
      warning_payload: statsCoveragePayload,
      should_emit_warning: true,
    };
  }

  if (!degraded) {
    return {
      is_degraded: false,
      status: 'healthy',
      reason_code: '',
      degraded_reason_code: '',
      summary_status: 'success',
      summary_reason_code: 'run_success',
      summary_message: 'Pipeline run completed.',
      warning_payload: null,
      should_emit_warning: false,
    };
  }

  const warningPayload = {
    reason_code: 'run_health_no_matches_from_odds',
    fetched_odds: fetchedOdds,
    matched: matched,
    signals_found: signalsFound,
    failure_reasons: {
      schedule_enrichment_no_schedule_events: Number(scheduleReasonCodes.schedule_enrichment_no_schedule_events || 0),
      no_player_match: Number(matchReasonCodes.no_player_match || 0),
    },
    sample_unmatched_events: sampleUnmatchedCases.slice(0, 5).map(function (entry) {
      return {
        odds_event_id: entry.odds_event_id,
        competition: entry.competition,
        player_1: entry.player_1,
        player_2: entry.player_2,
        commence_time: entry.commence_time,
        rejection_code: entry.rejection_code,
      };
    }),
  };

  return {
    is_degraded: true,
    status: 'degraded',
    reason_code: warningPayload.reason_code,
    degraded_reason_code: warningPayload.reason_code,
    summary_status: 'degraded',
    summary_reason_code: warningPayload.reason_code,
    summary_message: 'Pipeline run completed with degraded run-health guard.',
    warning_payload: warningPayload,
    should_emit_warning: true,
  };
}
