function installOrUpdateTriggers() {
  ensureTabsAndConfig_();
  const config = getConfig_();
  const scriptProps = PropertiesService.getScriptProperties();
  const triggerCadence = resolveTriggerCadence_(config, scriptProps);

  const spec = {
    version: 1,
    functionName: 'runEdgeBoard',
    type: 'clock',
    everyMinutes: triggerCadence.effective_every_minutes,
  };

  const signature = JSON.stringify(spec);
  const existingSignature = scriptProps.getProperty(PROPS.PIPELINE_TRIGGER_SIGNATURE);
  const existingPipelineTriggers = ScriptApp.getProjectTriggers().filter((t) => t.getHandlerFunction() === spec.functionName);
  const runId = buildRunId_();

  if (existingSignature === signature && existingPipelineTriggers.length > 0) {
    const verification = verifyTriggerInstallation_(spec, signature, runId, 'trigger_noop');

    appendLogRow_({
      row_type: 'ops',
      run_id: runId,
      stage: 'installOrUpdateTriggers',
      status: 'success',
      reason_code: 'trigger_noop',
      message: JSON.stringify({
        action: 'install_noop',
        trigger_count: verification.trigger_count,
        next_run_estimate: verification.next_run_estimate,
        configured_schedule_minutes: triggerCadence.configured_every_minutes,
        recommended_schedule_minutes: triggerCadence.recommended_every_minutes,
        schedule_tuned: triggerCadence.schedule_tuned,
        lock_clear: verification.lock_clear,
        debounce_clear: verification.debounce_clear,
      }),
      trigger_event: 'trigger_noop',
    });
    return buildMenuTriggerActionResult_('install_noop', verification);
  }

  existingPipelineTriggers.forEach((trigger) => ScriptApp.deleteTrigger(trigger));
  ScriptApp.newTrigger(spec.functionName).timeBased().everyMinutes(spec.everyMinutes).create();
  scriptProps.setProperty(PROPS.PIPELINE_TRIGGER_SIGNATURE, signature);

  const currentTriggerCount = ScriptApp.getProjectTriggers().filter((t) => t.getHandlerFunction() === spec.functionName).length;

  const verification = verifyTriggerInstallation_(spec, signature, runId, 'trigger_reinstalled');

  appendLogRow_({
    row_type: 'ops',
    run_id: runId,
    stage: 'installOrUpdateTriggers',
    status: 'success',
    reason_code: 'trigger_reinstalled',
    message: JSON.stringify({
      action: 'install_or_refresh',
      trigger_count: verification.trigger_count || currentTriggerCount,
      schedule_minutes: spec.everyMinutes,
      configured_schedule_minutes: triggerCadence.configured_every_minutes,
      recommended_schedule_minutes: triggerCadence.recommended_every_minutes,
      schedule_tuned: triggerCadence.schedule_tuned,
      next_run_estimate: verification.next_run_estimate,
      lock_clear: verification.lock_clear,
      debounce_clear: verification.debounce_clear,
    }),
    trigger_event: 'trigger_reinstalled',
  });

  return buildMenuTriggerActionResult_('trigger_reinstalled', verification);
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
  const runId = buildRunId_();
  let removedCount = 0;

  ScriptApp.getProjectTriggers().forEach((trigger) => {
    if (trigger.getHandlerFunction() === 'runEdgeBoard') {
      ScriptApp.deleteTrigger(trigger);
      removedCount += 1;
    }
  });

  const lockState = checkScriptLockClear_();
  const debounceState = checkDebounceState_();
  const remainingTriggers = ScriptApp.getProjectTriggers().filter((t) => t.getHandlerFunction() === 'runEdgeBoard').length;

  appendLogRow_({
    row_type: 'ops',
    run_id: runId,
    stage: 'removePipelineTriggers',
    status: 'success',
    reason_code: 'trigger_removed',
    message: JSON.stringify({
      action: 'trigger_removed',
      removed_count: removedCount,
      trigger_count: remainingTriggers,
      next_run_estimate: '',
      lock_clear: lockState.lock_clear,
      debounce_clear: debounceState.debounce_clear,
    }),
    trigger_event: 'trigger_removed',
  });

  return {
    action: 'trigger_removed',
    run_id: runId,
    removed_count: removedCount,
    trigger_count: remainingTriggers,
    next_run_estimate: '',
    debounce_clear: debounceState.debounce_clear,
    debounce_wait_ms_remaining: debounceState.debounce_wait_ms_remaining,
    lock_clear: lockState.lock_clear,
    lock_check_error: lockState.lock_check_error,
    verification_passed: remainingTriggers === 0,
  };
}

function buildMenuTriggerActionResult_(action, verification) {
  const safe = verification || {};
  return {
    action: String(action || ''),
    run_id: String(safe.run_id || ''),
    trigger_count: Number(safe.trigger_count || 0),
    next_run_estimate: String(safe.next_run_estimate || ''),
    debounce_clear: !!safe.debounce_clear,
    debounce_wait_ms_remaining: Number(safe.debounce_wait_ms_remaining || 0),
    lock_clear: !!safe.lock_clear,
    lock_check_error: String(safe.lock_check_error || ''),
    verification_passed: !!safe.verification_passed,
  };
}

function runEdgeBoard() {
  const runId = buildRunId_();
  const startedAt = new Date();
  const startedAtMs = startedAt.getTime();
  const scriptProps = PropertiesService.getScriptProperties();

  if (scriptProps.getProperty(PROPS.WORKBOOK_RESET_IN_PROGRESS) === 'true') {
    appendLogRow_({
      row_type: 'ops',
      run_id: runId,
      stage: 'runEdgeBoard',
      status: 'skipped',
      reason_code: 'reset_in_progress_skip',
      message: 'Skipped runEdgeBoard because workbook reset is in progress.',
      lock_event: 'reset_in_progress_skip',
    });
    appendLogRow_({
      row_type: 'summary',
      run_id: runId,
      stage: 'runEdgeBoard',
      started_at: startedAt,
      ended_at: new Date(),
      status: 'skipped',
      reason_code: 'reset_in_progress_skip',
      message: 'Skipped because WORKBOOK_RESET_IN_PROGRESS flag is set.',
      lock_event: 'reset_in_progress_skip',
    });
    return;
  }

  ensureTabsAndConfig_();

  const lock = LockService.getScriptLock();
  let lifecycleContext = null;
  let lockAcquired = false;
  let lockAcquiredAtMs = 0;
  const lockAttemptStartedMs = Date.now();

  lockAcquired = tryLock_(lock, 5000);
  if (!lockAcquired) {
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
    appendLogRow_({
      row_type: 'ops',
      run_id: runId,
      stage: 'run_lock',
      status: 'skipped',
      reason_code: 'run_locked_skip',
      message: JSON.stringify({
        lock_event: 'run_locked_skip',
        lock_wait_ms: Math.max(0, Date.now() - lockAttemptStartedMs),
      }),
      lock_event: 'run_locked_skip',
    });
    return;
  }
  lockAcquiredAtMs = Date.now();
  scriptProps.setProperty(PROPS.LAST_PIPELINE_START_TS, String(lockAcquiredAtMs));
  appendLogRow_({
    row_type: 'ops',
    run_id: runId,
    stage: 'run_lock',
    status: 'success',
    reason_code: 'run_lock_acquired',
    message: JSON.stringify({
      lock_event: 'run_lock_acquired',
      lock_wait_ms: Math.max(0, lockAcquiredAtMs - lockAttemptStartedMs),
    }),
    lock_event: 'run_lock_acquired',
  });

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
    maybeEmitReasonAliasDictionary_(config);
    const maxRuntimeMs = resolvePipelineMaxRuntimeMs_(config);
    const productiveMitigation = resolveProductiveOutputMitigationContext_(runId, config);
    const effectiveConfig = productiveMitigation.opening_lag_active
      ? Object.assign({}, config, {
        MAX_OPENING_LAG_MINUTES: config.MAX_OPENING_LAG_MINUTES + productiveMitigation.opening_lag_extra_minutes,
      })
      : config;
    if (productiveMitigation.force_fresh_odds_probe_active) {
      effectiveConfig.ODDS_WINDOW_FORCE_REFRESH = true;
    }
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
    const cadenceControl = resolveCadenceControl_(config, scriptProps, nowMs);
    appendLogRow_({
      row_type: 'ops',
      run_id: runId,
      stage: 'cadence_control',
      status: cadenceControl.tuned ? 'warning' : 'ok',
      reason_code: cadenceControl.tuned ? 'cadence_autotuned' : 'cadence_configured',
      message: JSON.stringify(cadenceControl),
    });
    const debounceMs = cadenceControl.effective_debounce_ms;
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
        ? stageFetchOdds(runId, effectiveConfig, oddsWindowDecision.odds_fetch_window, {
          bypass_stale_fallback: productiveMitigation.force_fresh_odds_probe_active,
        })
        : buildSkippedOddsStage_(runId, oddsWindowDecision.decision_reason_code, oddsWindowDecision.decision_message);
    });
    const fetchedOddsCountBeforeGate = Number((initialOddsStage && initialOddsStage.events && initialOddsStage.events.length) || 0);
    const oddsStage = runCheckpointedStage_(lifecycleContext, 'odds_gate', function () {
      assertPipelineRuntimeBudget_(lifecycleContext, runId);
      return applyOpeningLagActionabilityGate_(runId, effectiveConfig, initialOddsStage);
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
        odds_refresh_skipped_outside_window: oddsWindowDecision.decision_reason_code === 'odds_refresh_skipped_outside_window',
        odds_window_bootstrap_mode: !!oddsWindowDecision.bootstrap_mode,
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

    const signalUpstreamGate = deriveSignalUpstreamGateReason_(oddsStage, scheduleStage, matchStage, playerStatsStage);
    const signalStage = runCheckpointedStage_(lifecycleContext, 'signals', function () {
      assertPipelineRuntimeBudget_(lifecycleContext, runId);
      return stageGenerateSignals(
        runId,
        config,
        oddsStage.events,
        matchStage.rows,
        playerStatsStage.byOddsEventId,
        {
          upstream_gate_reason: signalUpstreamGate.reason,
          upstream_gate_inputs: signalUpstreamGate.inputs,
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

    const signalDecisionSummary = Object.assign({
      run_id: runId,
      sent_count: Number(signalStage.sentCount || 0),
      scored_count: Number(signalStage.scoredCount || 0),
      suppression_counts: {
        cooldown: { total: Number(signalStage.cooldownSuppressedCount || 0), by_reason: { cooldown_suppressed: Number(signalStage.cooldownSuppressedCount || 0) } },
        edge: { total: Number((signalStage.summary && signalStage.summary.reason_codes && signalStage.summary.reason_codes.edge_below_threshold) || 0), by_reason: { edge_below_threshold: Number((signalStage.summary && signalStage.summary.reason_codes && signalStage.summary.reason_codes.edge_below_threshold) || 0) } },
        stale: { total: Number((signalStage.summary && signalStage.summary.reason_codes && signalStage.summary.reason_codes.stale_odds_skip) || 0), by_reason: { stale_odds_skip: Number((signalStage.summary && signalStage.summary.reason_codes && signalStage.summary.reason_codes.stale_odds_skip) || 0) } },
        timing: { total: Number((signalStage.summary && signalStage.summary.reason_codes && signalStage.summary.reason_codes.too_close_to_start_skip) || 0), by_reason: { too_close_to_start_skip: Number((signalStage.summary && signalStage.summary.reason_codes && signalStage.summary.reason_codes.too_close_to_start_skip) || 0) } },
        config: {
          total: Number((signalStage.summary && signalStage.summary.reason_codes && signalStage.summary.reason_codes.notify_disabled) || 0)
            + Number((signalStage.summary && signalStage.summary.reason_codes && signalStage.summary.reason_codes.notify_missing_config) || 0),
          by_reason: {
            notify_disabled: Number((signalStage.summary && signalStage.summary.reason_codes && signalStage.summary.reason_codes.notify_disabled) || 0),
            notify_missing_config: Number((signalStage.summary && signalStage.summary.reason_codes && signalStage.summary.reason_codes.notify_missing_config) || 0),
          },
        },
      },
      sampled_top_suppressions: [],
      alignment_checks: {
        sent_matches_reason_counts: Number(signalStage.sentCount || 0) === Number((signalStage.summary && signalStage.summary.reason_codes && signalStage.summary.reason_codes.sent) || 0),
      },
    }, signalStage.signalDecisionSummary || {});
    const runQualityContract = buildRunQualityContractMetrics_(
      playerStatsStage.summary,
      signalDecisionSummary,
      signalStage.summary
    );
    signalDecisionSummary.quality_contract = runQualityContract;

    logDiagnosticEvent_(config, 'pipeline_stage_counts', {
      run_id: runId,
      fetched_odds: oddsStage.events.length,
      fetched_schedule: scheduleStage.events.length,
      matched: matchStage.matchedCount,
      unmatched: matchStage.unmatchedCount,
      rejected: matchStage.rejectedCount,
      player_stats_rows: playerStatsStage.rows.length,
      signals_found: signalStage.rows.length,
      signals_scored: Number(signalStage.scoredCount || 0),
      signals_sent: signalStage.sentCount,
      cooldown_suppressed: signalStage.cooldownSuppressedCount,
      duplicate_suppressed: signalStage.duplicateSuppressedCount,
      suppression_counts: signalDecisionSummary.suppression_counts,
      sampled_top_suppressions: signalDecisionSummary.sampled_top_suppressions,
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

    const fetchedOddsCount = resolveRunHealthFetchedOddsCount_(fetchedOddsCountBeforeGate, oddsStage);
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
      const playerStatsReasonCodes = cloneReasonCodeMap_((playerStatsStage.summary && playerStatsStage.summary.reason_codes) || {});
      playerStatsReasonCodes.stats_zero_coverage = Number(playerStatsReasonCodes.stats_zero_coverage || 0) + 1;
      playerStatsStage.summary.reason_codes = playerStatsReasonCodes;
    }
    const runHealthMetrics = {
      fetched_odds: fetchedOddsCount,
      fetched_schedule: scheduleStage.events.length,
      matched: matchedCount,
      signals_found: signalsFoundCount,
      players_with_non_null_stats: playersWithNonNullStats,
      sample_unmatched_cases: matchStage.unmatched,
      sample_blocked_odds_cases: oddsStage.non_actionable_rows,
      odds_reason_codes: oddsStage.summary.reason_codes,
      schedule_reason_codes: scheduleStage.summary.reason_codes,
      match_reason_codes: matchStage.summary.reason_codes,
      signal_reason_codes: signalStage.summary.reason_codes,
      player_stats_reason_codes: playerStatsStage.summary.reason_codes,
      opening_lag_blocked_count: Number((oddsStage.skipped_reason_codes && oddsStage.skipped_reason_codes.opening_lag_exceeded) || 0),
      schedule_only_seed_count: Number((matchStage.summary && matchStage.summary.reason_codes && matchStage.summary.reason_codes.schedule_seed_no_odds) || 0),
      odds_non_actionable_count: Number((oddsStage.summary && oddsStage.summary.reason_codes && oddsStage.summary.reason_codes.odds_non_actionable) || 0),
      no_odds_stage_count: Number((matchStage.summary && matchStage.summary.reason_codes && matchStage.summary.reason_codes.no_odds_candidates) || 0),
      stale_odds_skip_count: Number((signalStage.summary && signalStage.summary.reason_codes && signalStage.summary.reason_codes.stale_odds_skip) || 0),
      low_edge_suppressed_count: Number((signalStage.summary && signalStage.summary.reason_codes && signalStage.summary.reason_codes.edge_below_threshold) || 0),
      cooldown_suppressed_count: Number((signalStage.summary && signalStage.summary.reason_codes && signalStage.summary.reason_codes.cooldown_suppressed) || 0),
      edge_quality_diagnostics: signalDecisionSummary && signalDecisionSummary.edge_quality ? signalDecisionSummary.edge_quality : {},
    };
    const dataNotActionableState = updateRunHealthDataNotActionableState_(runId, runHealthMetrics, config);
    const noMatchFromOddsState = updateRunHealthNoMatchFromOddsState_(runId, runHealthMetrics, config);
    const runHealthDiagnostics = evaluateRunHealthDiagnostics_(Object.assign({}, runHealthMetrics, {
      data_not_actionable_streak: Number(dataNotActionableState.consecutive_count || 0),
      data_not_actionable_escalation_threshold: Number(dataNotActionableState.threshold || 0),
      no_match_from_odds_streak: Number(noMatchFromOddsState.consecutive_count || 0),
      no_match_from_odds_degraded_trigger: Number(noMatchFromOddsState.threshold || 0),
      single_run_critical_trigger_enabled: !!config.RUN_HEALTH_SINGLE_RUN_CRITICAL_TRIGGER,
    }));

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

    const stageSummarySnapshots = [
      oddsStage.summary,
      scheduleStage.summary,
      matchStage.summary,
      playerStatsStage.summary,
      signalStage.summary,
      persistStage.summary,
    ].map((summary) => {
      const safeSummary = summary || {};
      return Object.assign({}, safeSummary, {
        reason_codes: cloneReasonCodeMap_(safeSummary.reason_codes || {}),
        reason_metadata: Object.assign({}, safeSummary.reason_metadata || {}),
      });
    });
    const reasonCodeMaps = stageSummarySnapshots.map((summary) => summary.reason_codes);
    const reasonMetadataMaps = stageSummarySnapshots.map((summary) => summary.reason_metadata);

    const combinedReasonCodes = mergeReasonCounts_(reasonCodeMaps);
    const stageReasonCodeMutationDiagnostics = buildStageReasonCodeMutationDiagnostics_([
      { stage: 'stageFetchOdds', summary: oddsStage.summary, snapshot: stageSummarySnapshots[0].reason_codes },
      { stage: 'stageFetchSchedule', summary: scheduleStage.summary, snapshot: stageSummarySnapshots[1].reason_codes },
      { stage: 'stageMatchEvents', summary: matchStage.summary, snapshot: stageSummarySnapshots[2].reason_codes },
      { stage: 'stageFetchPlayerStats', summary: playerStatsStage.summary, snapshot: stageSummarySnapshots[3].reason_codes },
      { stage: 'stageGenerateSignals', summary: signalStage.summary, snapshot: stageSummarySnapshots[4].reason_codes },
      { stage: 'stagePersist', summary: persistStage.summary, snapshot: stageSummarySnapshots[5].reason_codes },
    ]);
    const combinedReasonMetadata = mergeReasonMetadata_(reasonMetadataMaps);
    const stageReasonCodeMaximaViolations = validateStageReasonCodeMaxima_(stageSummarySnapshots);
    const boundedCounterInvariantChecks = buildRunEdgeBoardBoundedCounterInvariantChecks_(scheduleStage.summary, matchStage);
    const boundedCounterInvariantViolations = assertDebugBoundedStageCounters_(config, boundedCounterInvariantChecks);
    const boundedCounterInvariantViolationTriage = triageRunEdgeBoardBoundedCounterInvariantViolations_(boundedCounterInvariantViolations);
    const boundedCounterInvariantEnforcement = enforceInvariant_(config, {
      invariant: 'bounded_stage_counter_invariant_exceeded',
      context: 'runEdgeBoard',
      violations: boundedCounterInvariantViolationTriage.enforceable_violations,
      hard_fail: false,
      error_prefix: 'bounded_stage_counter_invariant_exceeded',
    });
    if (stageReasonCodeMutationDiagnostics.mutated_stages.length > 0 || stageReasonCodeMaximaViolations.length > 0 || boundedCounterInvariantViolations.length > 0) {
      appendLogRow_({
        row_type: 'ops',
        run_id: runId,
        stage: 'reason_code_accumulation_guard',
        status: 'warning',
        reason_code: stageReasonCodeMutationDiagnostics.mutated_stages.length > 0
          ? 'reason_code_map_mutated_after_snapshot'
          : (stageReasonCodeMaximaViolations.length > 0
            ? 'reason_code_counter_exceeds_stage_max'
            : 'bounded_stage_counter_invariant_exceeded'),
        message: JSON.stringify({
          reason_code_map_mutated_after_snapshot: stageReasonCodeMutationDiagnostics,
          reason_code_counter_exceeds_stage_max: stageReasonCodeMaximaViolations,
          bounded_stage_counter_invariant_exceeded: boundedCounterInvariantViolations,
          bounded_stage_counter_invariant_downgraded: boundedCounterInvariantViolationTriage.downgraded_violations,
          invariant_enforcement: boundedCounterInvariantEnforcement,
        }),
      });
    }

    const emptyCycleState = updateBootstrapEmptyCycleState_(runId, oddsStage.rows.length, scheduleStage.events.length);
    if (emptyCycleState.reason_code) {
      combinedReasonCodes[emptyCycleState.reason_code] = (combinedReasonCodes[emptyCycleState.reason_code] || 0) + 1;
    }
    if (runHealthDiagnostics.degraded_reason_code) {
      combinedReasonCodes[runHealthDiagnostics.degraded_reason_code] = (combinedReasonCodes[runHealthDiagnostics.degraded_reason_code] || 0) + 1;
    }
    if (runHealthDiagnostics.summary_reason_code && runHealthDiagnostics.summary_reason_code !== runHealthDiagnostics.degraded_reason_code) {
      combinedReasonCodes[runHealthDiagnostics.summary_reason_code] = (combinedReasonCodes[runHealthDiagnostics.summary_reason_code] || 0) + 1;
    }
    if (emptyCycleState.warning_needed) {
      const bootstrapWatchdogEmission = resolveBootstrapEmptyCycleWatchdogEmission_(emptyCycleState, runHealthDiagnostics);
      appendLogRow_({
        row_type: 'ops',
        run_id: runId,
        stage: 'bootstrap_empty_cycle_watchdog',
        status: bootstrapWatchdogEmission.status,
        reason_code: 'bootstrap_empty_cycle_detected',
        message: JSON.stringify({
          consecutive_empty_cycles: emptyCycleState.consecutive_empty_cycles,
          threshold: emptyCycleState.threshold,
          diagnostics_counter: emptyCycleState.diagnostics_counter,
          last_non_empty_fetch_at: emptyCycleState.last_non_empty_fetch_at || '',
          last_non_empty_fetch_at_utc: emptyCycleState.last_non_empty_fetch_at_utc || '',
          watchdog_emission_mode: bootstrapWatchdogEmission.mode,
          outside_window_expected_idle: bootstrapWatchdogEmission.outside_window_expected_idle,
          material_threshold_change: bootstrapWatchdogEmission.material_threshold_change,
          watchdog_state_unexpected: bootstrapWatchdogEmission.watchdog_state_unexpected,
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
    finalizeProductiveOutputMitigationState_(runId, config, productiveMitigation, productiveOutputState);

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
    const creditBurnRateState = getStateJson_('ODDS_API_BURN_RATE_STATE') || {};
    const creditBurnRateNotification = maybeNotifyCreditBurnRate_(config, runId, creditBurnRateState);
    const rollupEmission = maybeEmitRunRollup_(config, {
      fetched_odds: fetchedOddsCount,
      fetched_schedule: scheduleStage.events.length,
      matched: matchedCount,
      unmatched: matchStage.unmatchedCount,
      signals_found: signalsFoundCount,
      run_health_reason_code: runHealthDiagnostics.reason_code,
      run_mode: oddsWindowDecision.current_refresh_mode || '',
      reason_codes: cloneReasonCodeMap_(combinedReasonCodes),
      stage_summaries: stageSummarySnapshots.map((summary) => Object.assign({}, summary, {
        reason_codes: cloneReasonCodeMap_(summary.reason_codes || {}),
        reason_metadata: Object.assign({}, summary.reason_metadata || {}),
      })),
      watchdog: {
        bootstrap_empty_cycles: emptyCycleState.consecutive_empty_cycles,
        bootstrap_threshold: emptyCycleState.threshold,
        productive_empty_cycles: productiveOutputState.consecutive_count,
        productive_threshold: productiveOutputState.threshold,
        schedule_only_cycles: productiveOutputState.schedule_only_consecutive_count,
        schedule_only_threshold: productiveOutputState.schedule_only_threshold,
      },
    });
    if (rollupEmission.emitted && rollupEmission.rollup) {
      appendLogRow_({
        row_type: 'ops',
        run_id: runId,
        stage: 'run_rollup',
        status: 'success',
        reason_code: 'run_rollup_emitted',
        message: JSON.stringify(rollupEmission.rollup),
      });
    }
    const compactStageSummaries = compactStageSummariesForLog_([
      oddsStage.summary,
      scheduleStage.summary,
      matchStage.summary,
      playerStatsStage.summary,
      signalStage.summary,
      persistStage.summary,
    ], REASON_CODE_ALIAS_SCHEMA_ID);

    const topRejectionReasonsRaw = getTopReasonCodes_(combinedReasonCodes, 10);
    const topRejectionReasonNormalization = normalizeReasonCodeEntriesForDisplay_(topRejectionReasonsRaw, REASON_CODE_ALIAS_SCHEMA_ID);

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
      top_rejection_reasons: topRejectionReasonNormalization.entries,
      top_rejection_reasons_raw: topRejectionReasonsRaw,
      reason_code_display_normalization: topRejectionReasonNormalization.metadata,
      reason_codes: combinedReasonCodes,
      reason_metadata: combinedReasonMetadata,
      reason_code_accumulation_guard: {
        mutation_diagnostics: stageReasonCodeMutationDiagnostics,
        maxima_violations: stageReasonCodeMaximaViolations,
        bounded_counter_violations: boundedCounterInvariantViolations,
        bounded_counter_enforcement: boundedCounterInvariantEnforcement,
      },
      upstream_gate_reason: combinedReasonMetadata.upstream_gate_reason || '',
      run_health: {
        status: runHealthDiagnostics.status,
        reason_code: runHealthDiagnostics.reason_code,
        diagnostics: runHealthDiagnostics.warning_payload,
        edge_quality_diagnostics: signalDecisionSummary && signalDecisionSummary.edge_quality ? signalDecisionSummary.edge_quality : {},
      },
      productive_output_mitigation: productiveMitigation,
      productive_output_watchdog: productiveOutputState,
      credit_burn_rate: creditBurnRateState,
      credit_burn_rate_notification: creditBurnRateNotification,
      signal_decision_summary: signalDecisionSummary,
    };

    const requiredParityStages = ['stageFetchPlayerStats'];
    const gsParityPrecheck = {
      contract_name: 'run_log_export_parity_contract_v1',
      latest_run_ids: [runId],
      summary_presence_by_run_id: { [runId]: true },
      required_stage_summary_presence_by_run_id: { [runId]: { stageFetchPlayerStats: true } },
      parity_status: 'pass',
      reason_code: 'export_parity_contract_pass_precheck',
    };

    const summaryStageSummariesPayload = {
      schema_id: REASON_CODE_ALIAS_SCHEMA_ID,
      stage_summaries: [
        oddsStage.summary,
        scheduleStage.summary,
        matchStage.summary,
        playerStatsStage.summary,
        signalStage.summary,
        persistStage.summary,
      ],
      gs_export_parity_contract: gsParityPrecheck,
    };

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
      opening_lag_blocked_count: runHealthMetrics.opening_lag_blocked_count,
      schedule_only_seed_count: runHealthMetrics.schedule_only_seed_count,
      odds_non_actionable_count: runHealthMetrics.odds_non_actionable_count,
      signals_found: signalStage.sentCount,
      signals_scored: Number(signalStage.scoredCount || 0),
      signal_decision_summary: JSON.stringify(signalDecisionSummary),
      reason_codes: combinedReasonCodes,
      rejection_codes: combinedReasonCodes,
      stage_summaries: JSON.stringify(summaryStageSummariesPayload),
      cooldown_suppressed: signalStage.cooldownSuppressedCount,
      duplicate_suppressed: signalStage.duplicateSuppressedCount,
    });

    const parityContractStatus = buildRunExportParityContractState_(runId, requiredParityStages, {
      latest_run_ids: [runId],
    });
    if (!parityContractStatus.pass) {
      combinedReasonCodes.export_parity_contract_failed = Number(combinedReasonCodes.export_parity_contract_failed || 0) + 1;
      appendLogRow_({
        row_type: 'ops',
        run_id: runId,
        stage: 'export_parity_contract',
        status: 'failed',
        reason_code: 'export_parity_contract_failed',
        message: JSON.stringify(parityContractStatus),
      });
      const parityError = new Error('Export parity contract failed; blocking successful publish state updates.');
      parityError.reason_code = 'export_parity_contract_failed';
      parityError.parity_status = parityContractStatus;
      throw parityError;
    }

    const forceVerboseCapture = !!productiveMitigation.verbose_diagnostics_capture_active;
    if (normalizeLogProfile_(config.LOG_PROFILE || DEFAULT_CONFIG.LOG_PROFILE) === 'verbose' || forceVerboseCapture) {
      setStateValue_('LAST_RUN_VERBOSE_JSON', JSON.stringify(verbosePayload, null, 2));
    } else {
      setStateValue_('LAST_RUN_VERBOSE_JSON', JSON.stringify({
        run_id: runId,
        log_profile: 'compact',
        note: 'Set LOG_PROFILE=verbose to capture full LAST_RUN_VERBOSE_JSON diagnostics payload.',
        reason_codes: compactReasonCodeMapForLog_(combinedReasonCodes, REASON_CODE_ALIAS_SCHEMA_ID),
        stage_count: compactStageSummaries.length,
      }));
    }
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
    markRunLifecycleCompleted_(lifecycleContext, runId);
  } catch (error) {
    const errorMessage = String(error && error.message ? error.message : error);
    const errorReasonCode = String(error && error.reason_code ? error.reason_code : 'run_exception');
    appendRunLifecycleStatus_(runId, 'aborted', {
      reason_code: errorReasonCode,
      message: errorMessage,
    });
    const summaryReasonCodes = {};
    summaryReasonCodes[errorReasonCode] = 1;
    appendLogRow_({
      row_type: 'summary',
      run_id: runId,
      stage: 'runEdgeBoard',
      started_at: startedAt,
      ended_at: new Date(),
      status: 'failed',
      reason_code: errorReasonCode,
      reason_codes: summaryReasonCodes,
      message: errorMessage,
      exception: errorMessage,
      stack: String(error && error.stack ? error.stack : ''),
    });
    throw error;
  } finally {
    if (lockAcquired) {
      const finishedAtMs = Date.now();
      updateRuntimeTelemetry_(scriptProps, startedAtMs, finishedAtMs);
      appendLogRow_({
        row_type: 'ops',
        run_id: runId,
        stage: 'run_lock',
        status: 'success',
        reason_code: 'run_lock_released',
        message: JSON.stringify({
          lock_event: 'run_lock_released',
          lock_held_ms: Math.max(0, finishedAtMs - lockAcquiredAtMs),
        }),
        lock_event: 'run_lock_released',
      });
    }
    releaseRunLifecycleLease_(scriptProps, lifecycleContext, runId);
    if (lockAcquired) {
      lock.releaseLock();
    }
  }
}

function buildRunQualityContractMetrics_(playerStatsSummary, signalDecisionSummary, signalStageSummary) {
  const playerSummary = playerStatsSummary && typeof playerStatsSummary === 'object' ? playerStatsSummary : {};
  const signalSummary = signalDecisionSummary && typeof signalDecisionSummary === 'object' ? signalDecisionSummary : {};
  const stageSummary = signalStageSummary && typeof signalStageSummary === 'object' ? signalStageSummary : {};
  const playerMetadata = playerSummary.reason_metadata && typeof playerSummary.reason_metadata === 'object'
    ? playerSummary.reason_metadata
    : {};
  const coverage = playerMetadata.coverage && typeof playerMetadata.coverage === 'object'
    ? playerMetadata.coverage
    : {};
  const resolvedRate = Number(coverage.resolved_rate);
  let featureCompleteness = Number.isFinite(resolvedRate) ? Math.max(0, Math.min(1, resolvedRate)) : null;
  let featureReasonCode = 'resolved_rate_from_player_stats_coverage';
  if (!Number.isFinite(featureCompleteness)) {
    featureCompleteness = 0;
    featureReasonCode = Number(playerSummary.input_count || 0) === 0
      ? 'upstream_stage_empty_player_stats_default'
      : 'missing_player_stats_coverage_default';
  }

  const edgeQuality = signalSummary.edge_quality && typeof signalSummary.edge_quality === 'object'
    ? signalSummary.edge_quality
    : {};
  const volatility = edgeQuality.edge_volatility_vs_previous_run && typeof edgeQuality.edge_volatility_vs_previous_run === 'object'
    ? edgeQuality.edge_volatility_vs_previous_run
    : {};
  let edgeVolatility = Number(volatility.abs_delta_p95);
  let edgeReasonCode = 'edge_volatility_abs_delta_p95';
  if (!Number.isFinite(edgeVolatility)) {
    edgeVolatility = Number(volatility.abs_delta_mean);
    edgeReasonCode = 'edge_volatility_abs_delta_mean';
  }
  if (!Number.isFinite(edgeVolatility)) {
    edgeVolatility = 0;
    edgeReasonCode = Number(stageSummary.input_count || 0) === 0
      ? 'upstream_stage_empty_generate_signals_default'
      : 'missing_edge_volatility_default';
  }

  return {
    schema: 'run_quality_contract_v1',
    feature_completeness: roundNumber_(featureCompleteness, 4),
    feature_completeness_reason_code: featureReasonCode,
    edge_volatility: roundNumber_(Math.abs(edgeVolatility), 4),
    edge_volatility_reason_code: edgeReasonCode,
  };
}

function resolveCadenceControl_(config, scriptProps, nowMs) {
  const configuredDebounceMs = Math.max(0, Number(config.DUPLICATE_DEBOUNCE_MS || 0));
  const triggerEveryMinutes = Math.max(1, Number(config.PIPELINE_TRIGGER_EVERY_MIN || 15));
  const triggerIntervalMs = triggerEveryMinutes * 60 * 1000;
  const maxSafeDebounceMs = Math.max(0, Math.floor(triggerIntervalMs * 0.8));
  const observedRuntimeMs = Math.max(0, Number(scriptProps.getProperty(PROPS.PIPELINE_RUNTIME_EWMA_MS) || 0));
  const runtimeGuardMs = observedRuntimeMs > 0
    ? Math.round(observedRuntimeMs + Math.max(10000, observedRuntimeMs * 0.25))
    : 0;
  const overlapWindowMs = Math.max(0, runtimeGuardMs - triggerIntervalMs);
  const tunedDebounceMs = runtimeGuardMs > 0
    ? Math.min(maxSafeDebounceMs, runtimeGuardMs)
    : configuredDebounceMs;
  const effectiveDebounceMs = Math.max(0, Math.min(maxSafeDebounceMs, Math.max(configuredDebounceMs, tunedDebounceMs)));
  const lastStartTs = Number(scriptProps.getProperty(PROPS.LAST_PIPELINE_START_TS) || 0);
  const lastStartAgeMs = lastStartTs > 0 ? Math.max(0, nowMs - lastStartTs) : -1;
  const manualOverlapWindowMs = observedRuntimeMs > 0 ? observedRuntimeMs : 0;

  return {
    configured_debounce_ms: configuredDebounceMs,
    effective_debounce_ms: effectiveDebounceMs,
    max_safe_debounce_ms: maxSafeDebounceMs,
    trigger_interval_ms: triggerIntervalMs,
    observed_avg_runtime_ms: observedRuntimeMs,
    expected_overlap_window_ms: overlapWindowMs,
    manual_overlap_window_ms: manualOverlapWindowMs,
    last_start_age_ms: lastStartAgeMs,
    tuned: effectiveDebounceMs !== configuredDebounceMs,
  };
}

function resolveTriggerCadence_(config, scriptProps) {
  const configuredEveryMinutes = Math.max(1, Number(config.PIPELINE_TRIGGER_EVERY_MIN || 15));
  const observedRuntimeMs = Math.max(0, Number(scriptProps.getProperty(PROPS.PIPELINE_RUNTIME_EWMA_MS) || 0));
  const recommendedEveryMinutes = observedRuntimeMs > 0
    ? Math.max(configuredEveryMinutes, Math.ceil((observedRuntimeMs * 1.25) / 60000))
    : configuredEveryMinutes;
  return {
    configured_every_minutes: configuredEveryMinutes,
    recommended_every_minutes: recommendedEveryMinutes,
    effective_every_minutes: recommendedEveryMinutes,
    observed_avg_runtime_ms: observedRuntimeMs,
    schedule_tuned: recommendedEveryMinutes !== configuredEveryMinutes,
  };
}

function updateRuntimeTelemetry_(scriptProps, startedAtMs, endedAtMs) {
  const durationMs = Math.max(0, Number(endedAtMs || 0) - Number(startedAtMs || 0));
  const previousEwmaMs = Math.max(0, Number(scriptProps.getProperty(PROPS.PIPELINE_RUNTIME_EWMA_MS) || 0));
  const previousCount = Math.max(0, Number(scriptProps.getProperty(PROPS.PIPELINE_RUNTIME_SAMPLE_COUNT) || 0));
  const nextCount = previousCount + 1;
  const smoothing = previousCount > 0 ? 0.2 : 1;
  const nextEwmaMs = Math.round(previousEwmaMs > 0
    ? ((smoothing * durationMs) + ((1 - smoothing) * previousEwmaMs))
    : durationMs);

  scriptProps.setProperty(PROPS.PIPELINE_RUNTIME_EWMA_MS, String(nextEwmaMs));
  scriptProps.setProperty(PROPS.PIPELINE_RUNTIME_SAMPLE_COUNT, String(nextCount));
}

function resolveBootstrapEmptyCycleWatchdogEmission_(emptyCycleState, runHealthDiagnostics) {
  const state = emptyCycleState || {};
  const diagnostics = runHealthDiagnostics || {};
  const consecutive = Number(state.consecutive_empty_cycles || 0);
  const threshold = Math.max(1, Number(state.threshold || 0));
  const diagnosticsCounter = Number(state.diagnostics_counter || 0);
  const expectedIdleOutsideWindow = isOutsideWindowExpectedIdleContext_(diagnostics);
  const materialThresholdChange = consecutive === threshold
    || (consecutive > threshold && threshold > 0 && (consecutive % threshold === 0));
  const unexpectedState = consecutive < threshold || diagnosticsCounter < consecutive;
  const summaryMode = expectedIdleOutsideWindow && !materialThresholdChange && !unexpectedState;

  return {
    status: summaryMode ? 'info' : 'warning',
    mode: summaryMode ? 'outside_window_summary' : 'warning',
    outside_window_expected_idle: expectedIdleOutsideWindow,
    material_threshold_change: materialThresholdChange,
    watchdog_state_unexpected: unexpectedState,
  };
}

function isOutsideWindowExpectedIdleContext_(runHealthDiagnostics) {
  const diagnostics = runHealthDiagnostics || {};
  const outsideWindowReason = 'odds_refresh_skipped_outside_window';
  return String(diagnostics.reason_code || '') === outsideWindowReason
    || String(diagnostics.summary_reason_code || '') === outsideWindowReason
    || String(diagnostics.degraded_reason_code || '') === outsideWindowReason
    || String(diagnostics.status || '') === 'idle_outside_odds_window'
    || String(diagnostics.summary_status || '') === 'idle_outside_odds_window'
    || String(diagnostics.warning_payload && diagnostics.warning_payload.reason_code || '') === outsideWindowReason
    || !!(diagnostics.warning_payload && diagnostics.warning_payload.outside_window_expected_idle)
    || !!diagnostics.outside_window_expected_idle;
}

function buildRunEdgeBoardBoundedCounterInvariantChecks_(scheduleSummary, matchStage) {
  const safeScheduleSummary = scheduleSummary || {};
  const safeMatchStage = matchStage || {};
  const safeMatchSummary = safeMatchStage.summary || {};
  const matchInputCount = Number(safeMatchSummary.input_count || 0);
  const scheduleOutputCount = Number(safeScheduleSummary.output_count || 0);
  const matchReasonCodes = safeMatchSummary.reason_codes || {};
  const scheduleSeedNoOddsCount = Number(matchReasonCodes.schedule_seed_no_odds || 0);
  const scheduleSeedNoOddsMode = matchInputCount === 0 && scheduleSeedNoOddsCount > 0;
  const mode = scheduleSeedNoOddsMode ? 'schedule_seed_no_odds' : 'odds_present';
  const diagnosticBoundUsesScheduleSeeds = scheduleSeedNoOddsMode;

  return [
    {
      stage: 'stageMatchEvents',
      mode: mode,
      max_name: 'output_count',
      max: Number(safeMatchSummary.output_count || 0),
      bound_source: 'stageMatchEvents.output_count',
      counters: {
        matched_count: Number(matchReasonCodes.matched_count || 0),
        matched_rows: Number(safeMatchStage.matchedCount || 0),
      },
    },
    {
      stage: 'stageMatchEvents',
      mode: mode,
      max_name: 'input_count',
      max: matchInputCount,
      bound_source: 'stageMatchEvents.input_count',
      counters: {
        rejected_count: Number(matchReasonCodes.rejected_count || 0),
        unmatched_rows: Number(safeMatchStage.unmatchedCount || 0),
      },
    },
    {
      stage: 'stageMatchEvents',
      mode: mode,
      max_name: diagnosticBoundUsesScheduleSeeds ? 'stageFetchSchedule.output_count' : 'input_count',
      max: diagnosticBoundUsesScheduleSeeds ? scheduleOutputCount : matchInputCount,
      bound_source: diagnosticBoundUsesScheduleSeeds
        ? 'stageFetchSchedule.output_count (schedule-seed mode)'
        : 'stageMatchEvents.input_count (odds-present mode)',
      counters: {
        diagnostic_records_written: Number(matchReasonCodes.diagnostic_records_written || 0),
      },
    },
  ];
}

function triageRunEdgeBoardBoundedCounterInvariantViolations_(violations) {
  const downgradedViolations = [];
  const enforceableViolations = [];

  (violations || []).forEach((violation) => {
    const safeViolation = violation || {};
    const mode = String(safeViolation.mode || 'odds_present');
    const counterName = String(safeViolation.counter_name || '');
    const shouldDowngrade = mode === 'schedule_seed_no_odds' && counterName === 'diagnostic_records_written';
    if (shouldDowngrade) {
      downgradedViolations.push(Object.assign({}, safeViolation, {
        downgrade_reason: 'schedule_seed_no_odds_expected_diagnostic_overcount',
      }));
      return;
    }
    enforceableViolations.push(safeViolation);
  });

  return {
    downgraded_violations: downgradedViolations,
    enforceable_violations: enforceableViolations,
  };
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



function resolveProductiveOutputMitigationContext_(runId, config) {
  const mitigationState = getStateJson_('PRODUCTIVE_OUTPUT_MITIGATION_STATE') || {};
  const productiveState = getStateJson_('EMPTY_PRODUCTIVE_OUTPUT_STATE') || {};
  const threshold = Math.max(1, Number(config.EMPTY_PRODUCTIVE_OUTPUT_THRESHOLD || 3));
  const streak = Math.max(0, Number(productiveState.consecutive_count || 0));
  const globallyEnabled = !!config.PRODUCTIVE_OUTPUT_MITIGATION_ENABLED;
  const thresholdReached = streak >= threshold;
  const openingLagExtraMinutes = Math.max(0, Number(config.PRODUCTIVE_OUTPUT_MITIGATION_OPENING_LAG_EXTRA_MINUTES || 0));

  const openingLagActive = globallyEnabled
    && thresholdReached
    && !!config.PRODUCTIVE_OUTPUT_MITIGATION_OPENING_LAG_WIDEN_ENABLED
    && openingLagExtraMinutes > 0;
  const forceFreshOddsProbeActive = globallyEnabled
    && !!config.PRODUCTIVE_OUTPUT_MITIGATION_FORCE_FRESH_ODDS_PROBE_ENABLED
    && !!mitigationState.force_fresh_odds_probe_pending;
  const verboseCaptureActive = globallyEnabled
    && !!config.PRODUCTIVE_OUTPUT_MITIGATION_VERBOSE_DIAGNOSTICS_ENABLED
    && !!mitigationState.verbose_diagnostics_capture_pending;

  const context = {
    enabled: globallyEnabled,
    threshold_reached: thresholdReached,
    streak: streak,
    threshold: threshold,
    opening_lag_active: openingLagActive,
    opening_lag_extra_minutes: openingLagExtraMinutes,
    force_fresh_odds_probe_active: forceFreshOddsProbeActive,
    verbose_diagnostics_capture_active: verboseCaptureActive,
  };

  if (openingLagActive || forceFreshOddsProbeActive || verboseCaptureActive) {
    appendLogRow_({
      row_type: 'ops',
      run_id: runId,
      stage: 'productive_output_mitigation',
      status: 'notice',
      reason_code: 'productive_output_mitigation_activated',
      message: JSON.stringify(context),
    });
  }

  return context;
}

function finalizeProductiveOutputMitigationState_(runId, config, mitigationContext, productiveOutputState) {
  const previous = getStateJson_('PRODUCTIVE_OUTPUT_MITIGATION_STATE') || {};
  const globalEnabled = !!config.PRODUCTIVE_OUTPUT_MITIGATION_ENABLED;
  const warningNeeded = !!(productiveOutputState && productiveOutputState.warning_needed);
  const canForceFresh = globalEnabled && !!config.PRODUCTIVE_OUTPUT_MITIGATION_FORCE_FRESH_ODDS_PROBE_ENABLED;
  const canVerboseCapture = globalEnabled && !!config.PRODUCTIVE_OUTPUT_MITIGATION_VERBOSE_DIAGNOSTICS_ENABLED;

  const next = {
    run_id: runId,
    updated_at: formatLocalIso_(new Date()),
    force_fresh_odds_probe_pending: warningNeeded ? canForceFresh : false,
    verbose_diagnostics_capture_pending: warningNeeded ? canVerboseCapture : false,
    last_activation: {
      opening_lag_active: !!(mitigationContext && mitigationContext.opening_lag_active),
      force_fresh_odds_probe_active: !!(mitigationContext && mitigationContext.force_fresh_odds_probe_active),
      verbose_diagnostics_capture_active: !!(mitigationContext && mitigationContext.verbose_diagnostics_capture_active),
      streak: Number(mitigationContext && mitigationContext.streak || 0),
      threshold: Number(mitigationContext && mitigationContext.threshold || 0),
    },
    previous: {
      force_fresh_odds_probe_pending: !!previous.force_fresh_odds_probe_pending,
      verbose_diagnostics_capture_pending: !!previous.verbose_diagnostics_capture_pending,
    },
  };

  setStateValue_('PRODUCTIVE_OUTPUT_MITIGATION_STATE', JSON.stringify(next));
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
  const fallbackExemptionMaxAgeMinutes = Math.max(0, Number(config.OPENING_LAG_FALLBACK_EXEMPTION_MAX_AGE_MINUTES || 0));
  const fallbackExemptionMaxRowsPerRun = Math.max(0, Number(config.OPENING_LAG_FALLBACK_EXEMPTION_MAX_ROWS_PER_RUN || 0));
  const fallbackExemptionKeyMatchWindowMinutes = Math.max(0, Number(config.OPENING_LAG_FALLBACK_KEY_MATCH_WINDOW_MINUTES || 120));
  const fallbackExemptionKeyMatchMaxAgeMinutes = Math.max(
    fallbackExemptionMaxAgeMinutes,
    Number(config.OPENING_LAG_FALLBACK_KEY_MATCH_MAX_AGE_MINUTES || 240)
  );
  const fallbackExemptionCapMode = String(config.OPENING_LAG_FALLBACK_EXEMPTION_CAP_MODE || 'unlimited_when_zero').toLowerCase();
  const fallbackExemptionAllowedSourcesInput = parseOpeningLagPolicySources_(
    config.OPENING_LAG_FALLBACK_EXEMPTION_ALLOWED_SOURCES_JSON,
    ['fallback_cached_stale', 'fallback_cached_stale_bounded_window']
  );
  const fallbackExemptionDeniedSourcesInput = parseOpeningLagPolicySources_(
    config.OPENING_LAG_FALLBACK_EXEMPTION_DENIED_SOURCES_JSON,
    ['strict_gate', 'fallback_cached_fresh']
  );
  const fallbackExemptionConfigValidation = validateOpeningLagFallbackExemptionConfig_({
    cap_mode: fallbackExemptionCapMode,
    max_rows_per_run: fallbackExemptionMaxRowsPerRun,
    allowed_sources: fallbackExemptionAllowedSourcesInput,
    denied_sources: fallbackExemptionDeniedSourcesInput,
  });
  const fallbackExemptionAllowedSources = fallbackExemptionConfigValidation.allowed_sources;
  const fallbackExemptionDeniedSources = fallbackExemptionConfigValidation.denied_sources;
  const denyAllWhenCapZero = fallbackExemptionConfigValidation.cap_zero_blocks_all;
  const now = new Date();
  const nowMs = now.getTime();

  let missingOpenTimestamp = 0;
  let openingLagExceeded = 0;
  let openingLagWithinLimit = 0;
  let fallbackCachedExempted = 0;
  let fallbackExemptionDeniedSource = 0;
  let fallbackExemptionDeniedAge = 0;
  let fallbackExemptionDeniedCap = 0;
  const fallbackExemptionAgeBuckets = {};
  const fallbackExemptionAgeBucketSummary = {
    '<=60': 0,
    '61-180': 0,
    '181-300': 0,
    '>300': 0,
  };
  const fallbackExemptionEvidenceSampleLimit = 3;
  const fallbackExemptionEvidence = {
    blocked_by_source: [],
    blocked_by_age: [],
    blocked_by_cap: [],
    exempted: [],
  };

  const enrichedEvents = originalEvents.map(function (event) {
    const timestampType = String((event && event.open_timestamp_type) || '');
    const timestampSource = String((event && event.open_timestamp_source) || '');
    const policyTier = String((event && event.opening_lag_policy_tier) || 'strict_gate');
    const strictGate = policyTier === 'strict_gate';

    const openTimestamp = event && event.provider_odds_updated_time instanceof Date && !Number.isNaN(event.provider_odds_updated_time.getTime())
      ? event.provider_odds_updated_time
      : (event && event.open_timestamp instanceof Date && !Number.isNaN(event.open_timestamp.getTime()) ? event.open_timestamp : null);
    const openingLagMinutes = openTimestamp ? Math.max(0, Math.round((nowMs - openTimestamp.getTime()) / 60000)) : null;
    const fallbackAgeBucket = openingLagAgeBucket_(openingLagMinutes);
    if (fallbackAgeBucket) {
      fallbackExemptionAgeBuckets[fallbackAgeBucket] = (fallbackExemptionAgeBuckets[fallbackAgeBucket] || 0) + 1;
    }
    const tuningAgeBucket = openingLagPolicyTuningBucket_(openingLagMinutes);
    if (tuningAgeBucket) {
      fallbackExemptionAgeBucketSummary[tuningAgeBucket] = Number(fallbackExemptionAgeBucketSummary[tuningAgeBucket] || 0) + 1;
    }

    const dynamicExemptionPolicy = resolveOpeningLagFallbackExemptionPolicy_(
      event,
      policyTier,
      fallbackExemptionMaxAgeMinutes,
      fallbackExemptionKeyMatchWindowMinutes,
      fallbackExemptionKeyMatchMaxAgeMinutes,
      nowMs
    );

    const evaluated = Object.assign({}, event || {});
    evaluated.open_timestamp = openTimestamp;
    evaluated.opening_lag_minutes = openingLagMinutes;
    evaluated.opening_lag_evaluated_at = now;
    evaluated.open_timestamp_type = timestampType;
    evaluated.open_timestamp_source = timestampSource;
    evaluated.opening_lag_policy_tier = policyTier;
    evaluated.opening_lag_policy_tier_applied = dynamicExemptionPolicy.policy_tier;
    evaluated.opening_lag_fallback_exemption_max_age_minutes = dynamicExemptionPolicy.max_age_minutes;
    evaluated.opening_lag_fallback_minutes_to_start = dynamicExemptionPolicy.minutes_to_start;
    evaluated.opening_lag_fallback_age_bucket = fallbackAgeBucket || '';
    evaluated.is_actionable = true;
    evaluated.reason_code = '';
    evaluated.fallback_exemption_reason_code = '';

    if (!openTimestamp) {
      missingOpenTimestamp += 1;
      evaluated.is_actionable = false;
      evaluated.reason_code = strictGate ? 'missing_open_timestamp' : 'missing_open_timestamp_fallback';
      return evaluated;
    }

    if (!strictGate) {
      let exemptionDecision = 'opening_lag_fallback_exemption_allowed';
      const eventEvidence = {
        event_id: String((event && event.event_id) || ''),
        policy_tier: policyTier,
        policy_tier_applied: dynamicExemptionPolicy.policy_tier,
        exemption_max_age_minutes: dynamicExemptionPolicy.max_age_minutes,
        opening_lag_minutes: Number.isFinite(Number(openingLagMinutes)) ? Number(openingLagMinutes) : null,
        minutes_to_start: Number.isFinite(Number(dynamicExemptionPolicy.minutes_to_start))
          ? Number(dynamicExemptionPolicy.minutes_to_start)
          : null,
      };

      const sourceAllowed = isOpeningLagFallbackSourceAllowed_(
        policyTier,
        dynamicExemptionPolicy.policy_tier,
        fallbackExemptionAllowedSources,
        fallbackExemptionDeniedSources
      );
      if (!sourceAllowed) {
        exemptionDecision = 'opening_lag_fallback_exemption_denied_source';
        fallbackExemptionDeniedSource += 1;
        pushOpeningLagFallbackEvidence_(fallbackExemptionEvidence.blocked_by_source, eventEvidence, fallbackExemptionEvidenceSampleLimit);
      } else if (dynamicExemptionPolicy.max_age_minutes > 0 && openingLagMinutes > dynamicExemptionPolicy.max_age_minutes) {
        exemptionDecision = 'opening_lag_fallback_exemption_denied_age';
        fallbackExemptionDeniedAge += 1;
        pushOpeningLagFallbackEvidence_(fallbackExemptionEvidence.blocked_by_age, eventEvidence, fallbackExemptionEvidenceSampleLimit);
      } else if ((denyAllWhenCapZero && fallbackExemptionMaxRowsPerRun === 0)
        || (fallbackExemptionMaxRowsPerRun > 0 && fallbackCachedExempted >= fallbackExemptionMaxRowsPerRun)) {
        exemptionDecision = 'opening_lag_fallback_exemption_denied_cap';
        fallbackExemptionDeniedCap += 1;
        pushOpeningLagFallbackEvidence_(fallbackExemptionEvidence.blocked_by_cap, eventEvidence, fallbackExemptionEvidenceSampleLimit);
      }

      evaluated.fallback_exemption_reason_code = exemptionDecision;

      if (exemptionDecision === 'opening_lag_fallback_exemption_allowed') {
        fallbackCachedExempted += 1;
        pushOpeningLagFallbackEvidence_(fallbackExemptionEvidence.exempted, eventEvidence, fallbackExemptionEvidenceSampleLimit);
        evaluated.reason_code = exemptionDecision;
        return evaluated;
      }
    }

    if (requireOpeningLineProximity && maxOpeningLagMinutes > 0 && openingLagMinutes > maxOpeningLagMinutes) {
      openingLagExceeded += 1;
      evaluated.is_actionable = false;
      evaluated.reason_code = 'opening_lag_exceeded';
      return evaluated;
    }

    openingLagWithinLimit += 1;
    evaluated.reason_code = 'opening_lag_within_limit';
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
      open_timestamp_type: event.open_timestamp_type || '',
      open_timestamp_source: event.open_timestamp_source || '',
      opening_lag_policy_tier: event.opening_lag_policy_tier || '',
      opening_lag_policy_tier_applied: event.opening_lag_policy_tier_applied || '',
      opening_lag_fallback_exemption_max_age_minutes: Number.isFinite(Number(event.opening_lag_fallback_exemption_max_age_minutes))
        ? Number(event.opening_lag_fallback_exemption_max_age_minutes)
        : '',
      opening_lag_fallback_minutes_to_start: Number.isFinite(Number(event.opening_lag_fallback_minutes_to_start))
        ? Number(event.opening_lag_fallback_minutes_to_start)
        : '',
      opening_lag_minutes: Number.isFinite(Number(event.opening_lag_minutes)) ? Number(event.opening_lag_minutes) : '',
      opening_lag_fallback_age_bucket: event.opening_lag_fallback_age_bucket || '',
      opening_lag_evaluated_at: event.opening_lag_evaluated_at ? event.opening_lag_evaluated_at.toISOString() : '',
      is_actionable: event.is_actionable !== false,
      fallback_exemption_reason_code: event.fallback_exemption_reason_code || '',
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
    fallback_cached_exempted: fallbackCachedExempted,
  };

  const reasonCodes = stage.summary && stage.summary.reason_codes ? stage.summary.reason_codes : {};
  reasonCodes.opening_lag_within_limit = openingLagWithinLimit;
  reasonCodes.missing_open_timestamp = missingOpenTimestamp;
  reasonCodes.opening_lag_exceeded = openingLagExceeded;
  reasonCodes.opening_lag_fallback_exempted = fallbackCachedExempted;
  reasonCodes.opening_lag_fallback_exemption_allowed = fallbackCachedExempted;
  reasonCodes.opening_lag_fallback_exemption_denied_source = fallbackExemptionDeniedSource;
  reasonCodes.opening_lag_fallback_exemption_denied_age = fallbackExemptionDeniedAge;
  reasonCodes.opening_lag_fallback_exemption_denied_cap = fallbackExemptionDeniedCap;
  reasonCodes.odds_actionable = actionableEvents.length;
  reasonCodes.odds_non_actionable = enrichedRows.length - actionableEvents.length;
  if (stage.summary) stage.summary.reason_codes = reasonCodes;
  if (stage.summary) {
    stage.summary.output_count = actionableEvents.length;
    stage.summary.reason_metadata = stage.summary.reason_metadata || {};
    stage.summary.reason_metadata.max_opening_lag_minutes = maxOpeningLagMinutes;
    stage.summary.reason_metadata.require_opening_line_proximity = requireOpeningLineProximity;
    stage.summary.reason_metadata.fallback_exemption_max_age_minutes = fallbackExemptionMaxAgeMinutes;
    stage.summary.reason_metadata.fallback_exemption_key_match_window_minutes = fallbackExemptionKeyMatchWindowMinutes;
    stage.summary.reason_metadata.fallback_exemption_key_match_max_age_minutes = fallbackExemptionKeyMatchMaxAgeMinutes;
    stage.summary.reason_metadata.fallback_exemption_max_rows_per_run = fallbackExemptionMaxRowsPerRun;
    stage.summary.reason_metadata.fallback_exemption_cap_mode = fallbackExemptionCapMode;
    stage.summary.reason_metadata.fallback_exemption_allowed_sources = Object.keys(fallbackExemptionAllowedSources);
    stage.summary.reason_metadata.fallback_exemption_denied_sources = Object.keys(fallbackExemptionDeniedSources);
    stage.summary.reason_metadata.fallback_exemption_config_validation = fallbackExemptionConfigValidation;
    stage.summary.reason_metadata.fallback_exemption_age_buckets = fallbackExemptionAgeBuckets;
    stage.summary.reason_metadata.fallback_exemption_age_bucket_summary = fallbackExemptionAgeBucketSummary;
    stage.summary.reason_metadata.fallback_exemption_diagnostics = buildOpeningLagFallbackDiagnostics_(fallbackExemptionEvidence, {
      exempted: fallbackCachedExempted,
      blocked_by_source: fallbackExemptionDeniedSource,
      blocked_by_age: fallbackExemptionDeniedAge,
      blocked_by_cap: fallbackExemptionDeniedCap,
    });
  }

  writeOpeningLagSkipState_(runId, {
    max_opening_lag_minutes: maxOpeningLagMinutes,
    require_opening_line_proximity: requireOpeningLineProximity,
    evaluated_count: enrichedRows.length,
    actionable_count: actionableEvents.length,
    missing_open_timestamp: missingOpenTimestamp,
    opening_lag_exceeded: openingLagExceeded,
    opening_lag_fallback_exempted: fallbackCachedExempted,
    opening_lag_fallback_exemption_denied_source: fallbackExemptionDeniedSource,
    opening_lag_fallback_exemption_denied_age: fallbackExemptionDeniedAge,
    opening_lag_fallback_exemption_denied_cap: fallbackExemptionDeniedCap,
    opening_lag_fallback_exemption_key_match_window_minutes: fallbackExemptionKeyMatchWindowMinutes,
    opening_lag_fallback_exemption_key_match_max_age_minutes: fallbackExemptionKeyMatchMaxAgeMinutes,
    opening_lag_fallback_exemption_cap_mode: fallbackExemptionCapMode,
    opening_lag_fallback_exemption_allowed_sources: Object.keys(fallbackExemptionAllowedSources),
    opening_lag_fallback_exemption_denied_sources: Object.keys(fallbackExemptionDeniedSources),
    opening_lag_fallback_exemption_config_validation: fallbackExemptionConfigValidation,
    opening_lag_fallback_exemption_age_bucket_summary: fallbackExemptionAgeBucketSummary,
    opening_lag_fallback_exemption_diagnostics: buildOpeningLagFallbackDiagnostics_(fallbackExemptionEvidence, {
      exempted: fallbackCachedExempted,
      blocked_by_source: fallbackExemptionDeniedSource,
      blocked_by_age: fallbackExemptionDeniedAge,
      blocked_by_cap: fallbackExemptionDeniedCap,
    }),
  });

  return stage;
}

function resolveOpeningLagFallbackExemptionPolicy_(event, policyTier, baseMaxAgeMinutes, keyWindowMinutes, keyWindowMaxAgeMinutes, nowMs) {
  const normalizedTier = String(policyTier || 'strict_gate');
  const baseMaxAge = Math.max(0, Number(baseMaxAgeMinutes || 0));
  const result = {
    policy_tier: normalizedTier,
    max_age_minutes: baseMaxAge,
    minutes_to_start: null,
  };

  if (normalizedTier !== 'fallback_cached_stale') return result;

  const commenceTime = event && event.commence_time instanceof Date && !Number.isNaN(event.commence_time.getTime())
    ? event.commence_time
    : null;
  if (!commenceTime) return result;

  const minutesToStart = (commenceTime.getTime() - nowMs) / 60000;
  result.minutes_to_start = roundNumber_(minutesToStart, 2);
  if (!Number.isFinite(minutesToStart) || minutesToStart < 0) return result;

  const keyWindow = Math.max(0, Number(keyWindowMinutes || 0));
  if (keyWindow <= 0 || minutesToStart > keyWindow) return result;

  result.policy_tier = 'fallback_cached_stale_bounded_window';
  result.max_age_minutes = Math.max(baseMaxAge, Math.max(0, Number(keyWindowMaxAgeMinutes || baseMaxAge)));
  return result;
}

function parseOpeningLagPolicySources_(value, fallbackList) {
  let parsed = null;
  if (Array.isArray(value)) {
    parsed = value;
  } else if (typeof value === 'string') {
    const trimmed = value.trim();
    if (trimmed) {
      try {
        parsed = JSON.parse(trimmed);
      } catch (error) {
        parsed = null;
      }
    }
  }

  if (!Array.isArray(parsed)) {
    parsed = Array.isArray(fallbackList) ? fallbackList : [];
  }

  return parsed.reduce(function (acc, raw) {
    const key = String(raw || '').trim();
    if (!key) return acc;
    acc[key] = true;
    return acc;
  }, {});
}

function validateOpeningLagFallbackExemptionConfig_(config) {
  const safe = config || {};
  const rawCapMode = String(safe.cap_mode || 'unlimited_when_zero').toLowerCase();
  const capMode = normalizeOpeningLagFallbackCapMode_(rawCapMode, 'unlimited_when_zero');
  const maxRowsPerRun = Math.max(0, Number(safe.max_rows_per_run || 0));
  const allowedSources = Object.assign({}, safe.allowed_sources || {});
  const deniedSources = Object.assign({}, safe.denied_sources || {});
  const allowListWasEmpty = Object.keys(allowedSources).length === 0;
  if (allowListWasEmpty) {
    allowedSources.fallback_cached_stale = true;
    allowedSources.fallback_cached_stale_bounded_window = true;
  }
  const capZeroBlocksAll = capMode === 'deny_all_when_zero' && maxRowsPerRun === 0;
  const overlap = Object.keys(allowedSources).filter(function (source) { return !!deniedSources[source]; });
  const invalidCapMode = rawCapMode !== capMode;

  return {
    cap_mode: capMode,
    invalid_cap_mode: invalidCapMode,
    cap_zero_blocks_all: capZeroBlocksAll,
    has_unsafe_cap_zero: capZeroBlocksAll,
    has_allow_deny_overlap: overlap.length > 0,
    allow_deny_overlap_sources: overlap,
    allow_list_empty: allowListWasEmpty,
    allow_list_defaulted: allowListWasEmpty,
    has_config_warning: invalidCapMode || overlap.length > 0 || allowListWasEmpty,
    allowed_sources: allowedSources,
    denied_sources: deniedSources,
  };
}

function normalizeOpeningLagFallbackCapMode_(value, fallback) {
  const normalized = String(value || '').toLowerCase().trim();
  if (normalized === 'deny_all_when_zero' || normalized === 'unlimited_when_zero') {
    return normalized;
  }
  return String(fallback || 'unlimited_when_zero').toLowerCase();
}

function pushOpeningLagFallbackEvidence_(bucket, evidence, limit) {
  const target = Array.isArray(bucket) ? bucket : [];
  if (target.length >= Math.max(1, Number(limit || 1))) return;
  target.push(Object.assign({}, evidence || {}));
}

function buildOpeningLagFallbackDiagnostics_(evidence, counts) {
  const safeEvidence = evidence || {};
  const safeCounts = counts || {};
  const diagnostics = {
    exempted: Number(safeCounts.exempted || 0),
    blocked_by_source: Number(safeCounts.blocked_by_source || 0),
    blocked_by_age: Number(safeCounts.blocked_by_age || 0),
    blocked_by_cap: Number(safeCounts.blocked_by_cap || 0),
    samples: {
      exempted: Array.isArray(safeEvidence.exempted) ? safeEvidence.exempted.slice() : [],
      blocked_by_source: Array.isArray(safeEvidence.blocked_by_source) ? safeEvidence.blocked_by_source.slice() : [],
      blocked_by_age: Array.isArray(safeEvidence.blocked_by_age) ? safeEvidence.blocked_by_age.slice() : [],
      blocked_by_cap: Array.isArray(safeEvidence.blocked_by_cap) ? safeEvidence.blocked_by_cap.slice() : [],
    },
  };
  diagnostics.summary = [
    'exempted=' + diagnostics.exempted,
    'blocked_by_age=' + diagnostics.blocked_by_age,
    'blocked_by_cap=' + diagnostics.blocked_by_cap,
    'sampled=' + (
      diagnostics.samples.exempted.length
      + diagnostics.samples.blocked_by_age.length
      + diagnostics.samples.blocked_by_cap.length
    ),
  ].join(' ');
  return diagnostics;
}

function openingLagAgeBucket_(openingLagMinutes) {
  const lag = Number(openingLagMinutes);
  if (!Number.isFinite(lag) || lag < 0) return '';
  if (lag <= 30) return '0_30m';
  if (lag <= 60) return '31_60m';
  if (lag <= 120) return '61_120m';
  if (lag <= 180) return '121_180m';
  if (lag <= 240) return '181_240m';
  if (lag <= 300) return '241_300m';
  return '301m_plus';
}

function openingLagPolicyTuningBucket_(openingLagMinutes) {
  const lag = Number(openingLagMinutes);
  if (!Number.isFinite(lag) || lag < 0) return '';
  if (lag <= 60) return '<=60';
  if (lag <= 180) return '61-180';
  if (lag <= 300) return '181-300';
  return '>300';
}

function isOpeningLagFallbackSourceAllowed_(policyTier, appliedPolicyTier, allowedSources, deniedSources) {
  const source = String(policyTier || '').trim();
  const applied = String(appliedPolicyTier || '').trim();
  const allowed = allowedSources || {};
  const denied = deniedSources || {};

  if (denied[source] || (applied && denied[applied])) return false;
  if (allowed[source] || (applied && allowed[applied])) return true;

  // Preserve stale fallback quality guardrails while allowing bounded-window variants.
  if (source === 'fallback_cached_stale' && applied === 'fallback_cached_stale_bounded_window') {
    return !!allowed.fallback_cached_stale;
  }

  return false;
}

function deriveSignalUpstreamGateReason_(oddsStage, scheduleStage, matchStage, playerStatsStage) {
  const oddsCount = Number((oddsStage && oddsStage.events && oddsStage.events.length) || 0);
  const oddsReasonCodes = (oddsStage && oddsStage.summary && oddsStage.summary.reason_codes) || {};
  const scheduleReasonCodes = (scheduleStage && scheduleStage.summary && scheduleStage.summary.reason_codes) || {};
  const matchedCount = Number((matchStage && matchStage.matchedCount) || 0);
  const matchReasonCodes = (matchStage && matchStage.summary && matchStage.summary.reason_codes) || {};
  const resolvedWithUsableStatsCount = Number(
    playerStatsStage
    && playerStatsStage.summary
    && playerStatsStage.summary.reason_metadata
    && playerStatsStage.summary.reason_metadata.resolved_with_usable_stats_count
    || 0
  );
  const outOfCohortCount = Number(
    playerStatsStage
    && playerStatsStage.summary
    && playerStatsStage.summary.reason_metadata
    && playerStatsStage.summary.reason_metadata.out_of_cohort_count
    || 0
  );
  const cohortPolicyOutcome = resolvedWithUsableStatsCount === 0 && outOfCohortCount > 0
    ? 'blocked_out_of_cohort'
    : (resolvedWithUsableStatsCount > 0 ? 'eligible' : 'unknown');

  const upstreamInputs = {
    odds_stage_reason: resolvePrimaryStageGateReason_(oddsReasonCodes, [
      'odds_refresh_skipped_outside_window',
      'source_credit_saver_skip',
    ]),
    schedule_stage_reason: resolvePrimaryStageGateReason_(scheduleReasonCodes, [
      'source_credit_saver_skip',
      'schedule_fetch_skipped_outside_window_credit_saver',
    ]),
    match_stage_reason: resolvePrimaryStageGateReason_(matchReasonCodes, [
      'schedule_seed_no_odds',
      'no_schedule_candidates',
      'no_odds_candidates',
    ]),
    resolved_with_usable_stats_count: resolvedWithUsableStatsCount,
    out_of_cohort_count: outOfCohortCount,
    cohort_policy_outcome: cohortPolicyOutcome,
  };

  if (upstreamInputs.odds_stage_reason) {
    return {
      reason: upstreamInputs.odds_stage_reason,
      inputs: upstreamInputs,
    };
  }

  if (upstreamInputs.schedule_stage_reason) {
    return {
      reason: upstreamInputs.schedule_stage_reason,
      inputs: upstreamInputs,
    };
  }

  if (oddsCount === 0 && Number(matchReasonCodes.schedule_seed_no_odds || 0) > 0) {
    return {
      reason: 'schedule_seed_no_odds',
      inputs: upstreamInputs,
    };
  }

  if (oddsCount > 0 && matchedCount === 0) {
    return {
      reason: 'no_matched_events',
      inputs: upstreamInputs,
    };
  }

  if (matchedCount > 0 && resolvedWithUsableStatsCount === 0) {
    return {
      reason: 'stats_zero_coverage',
      inputs: upstreamInputs,
    };
  }

  return {
    reason: '',
    inputs: upstreamInputs,
  };
}

function resolvePrimaryStageGateReason_(reasonCodes, priorityOrder) {
  const codes = reasonCodes || {};
  const priorities = priorityOrder || [];

  for (let i = 0; i < priorities.length; i += 1) {
    const reason = priorities[i];
    if (Number(codes[reason] || 0) > 0) return reason;
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
  const signalReasonCodes = Object.assign({}, (metrics && metrics.signal_reason_codes) || {});
  const playerStatsReasonCodes = Object.assign({}, (metrics && metrics.player_stats_reason_codes) || {});
  const openingLagBlockedCount = Number(metrics && metrics.opening_lag_blocked_count || 0);
  const scheduleOnlySeedCount = Number(metrics && metrics.schedule_only_seed_count || 0);
  const noOddsStageCount = Number(metrics && metrics.no_odds_stage_count || 0);
  const staleOddsSkipCount = Number(metrics && metrics.stale_odds_skip_count || 0);
  const lowEdgeSuppressedCount = Number(metrics && metrics.low_edge_suppressed_count || 0);
  const cooldownSuppressedCount = Number(metrics && metrics.cooldown_suppressed_count || 0);
  const edgeQualityDiagnostics = metrics && metrics.edge_quality_diagnostics && typeof metrics.edge_quality_diagnostics === 'object'
    ? Object.assign({}, metrics.edge_quality_diagnostics)
    : {};
  const dataNotActionableStreak = Math.max(0, Number(metrics && metrics.data_not_actionable_streak || 0));
  const dataNotActionableEscalationThreshold = Math.max(2, Number(metrics && metrics.data_not_actionable_escalation_threshold || 3));
  const noMatchFromOddsStreak = Math.max(0, Number(metrics && metrics.no_match_from_odds_streak || 0));
  const noMatchFromOddsDegradedTrigger = Math.max(1, Number(metrics && metrics.no_match_from_odds_degraded_trigger || 1));
  const singleRunCriticalTriggerEnabled = (metrics && Object.prototype.hasOwnProperty.call(metrics, 'single_run_critical_trigger_enabled'))
    ? !!metrics.single_run_critical_trigger_enabled
    : true;
  const sampledBlockedOdds = sanitizeBlockedOddsSamples_((metrics && metrics.sample_blocked_odds_cases) || []);
  const outsideWindowOddsSkipped = Number(oddsReasonCodes.odds_refresh_skipped_outside_window || 0) > 0;
  const scheduleSkippedOutsideWindowCreditSaver = Number(scheduleReasonCodes.schedule_fetch_skipped_outside_window_credit_saver || 0) > 0;
  const sourceCreditSaverSkip = Number(oddsReasonCodes.source_credit_saver_skip || 0) > 0
    || Number(scheduleReasonCodes.source_credit_saver_skip || 0) > 0;
  const scheduleOnlyIdle = fetchedOdds === 0
    && outsideWindowOddsSkipped
    && (fetchedSchedule > 0 || scheduleSkippedOutsideWindowCreditSaver || sourceCreditSaverSkip);

  if (scheduleOnlyIdle) {
    const idlePayload = {
      reason_code: 'odds_refresh_skipped_outside_window',
      fetched_odds: fetchedOdds,
      fetched_schedule: fetchedSchedule,
      matched: matched,
      signals_found: signalsFound,
      message: scheduleSkippedOutsideWindowCreditSaver
        ? 'Odds refresh was intentionally skipped outside the configured odds window, and schedule fetch was credit-saved; zero matches/signals are expected in this idle run.'
        : sourceCreditSaverSkip
          ? 'Odds refresh was intentionally skipped outside the configured odds window, and upstream source selection activated credit saver; zero matches/signals are expected in this idle run.'
        : 'Odds refresh was intentionally skipped outside the configured odds window; zero matches/signals are expected in this schedule-only run.',
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

  const temporaryNoOddsSignalCount = openingLagBlockedCount + scheduleOnlySeedCount + noOddsStageCount;
  const openingLagScheduleSeedNoOdds = fetchedOdds > 0
    && matched === 0
    && Number(oddsReasonCodes.opening_lag_exceeded || 0) > 0
    && (
      scheduleOnlySeedCount > 0
      || noOddsStageCount > 0
      || Number(matchReasonCodes.schedule_seed_no_odds || 0) > 0
      || Number(matchReasonCodes.no_odds_candidates || 0) > 0
    );

  if (openingLagScheduleSeedNoOdds) {
    const openingLagSeedPayload = Object.assign(buildRunHealthDegradedContract_({
      reason_code: 'run_health_opening_lag_schedule_seed_no_odds',
      odds_reason_codes: oddsReasonCodes,
      schedule_reason_codes: scheduleReasonCodes,
      match_reason_codes: matchReasonCodes,
      signal_reason_codes: signalReasonCodes,
      player_stats_reason_codes: playerStatsReasonCodes,
      opening_lag_blocked_count: openingLagBlockedCount,
      schedule_only_seed_count: scheduleOnlySeedCount,
      no_odds_stage_count: noOddsStageCount,
      stale_odds_skip_count: staleOddsSkipCount,
      low_edge_suppressed_count: lowEdgeSuppressedCount,
      cooldown_suppressed_count: cooldownSuppressedCount,
      sampled_blocked_odds: sampledBlockedOdds,
      sample_unmatched_cases: sampleUnmatchedCases,
    }), {
      reason_code: 'run_health_opening_lag_schedule_seed_no_odds',
      fetched_odds: fetchedOdds,
      fetched_schedule: fetchedSchedule,
      matched: matched,
      signals_found: signalsFound,
      message: 'Fetched odds rows were blocked by opening-lag guard, and downstream matching followed schedule-seed/no-odds path; zero matches/signals are expected while strict odds-quality gate remains active.',
    });

    const shouldEmitOpeningLagSeedWarning = openingLagBlockedCount >= 2
      || scheduleOnlySeedCount >= 2
      || noOddsStageCount >= 2;

    return {
      is_degraded: false,
      status: 'idle_opening_lag_schedule_seed_no_odds',
      reason_code: 'run_health_opening_lag_schedule_seed_no_odds',
      degraded_reason_code: '',
      summary_status: 'success',
      summary_reason_code: 'run_health_opening_lag_schedule_seed_no_odds',
      summary_message: openingLagSeedPayload.message,
      warning_payload: openingLagSeedPayload,
      should_emit_warning: shouldEmitOpeningLagSeedWarning,
    };
  }

  const expectedTemporaryNoOdds = fetchedOdds > 0
    && matched === 0
    && (
      temporaryNoOddsSignalCount > 0
      || Number(matchReasonCodes.schedule_seed_no_odds || 0) > 0
      || Number(matchReasonCodes.no_odds_candidates || 0) > 0
      || Number(oddsReasonCodes.opening_lag_exceeded || 0) > 0
      || outsideWindowOddsSkipped
      || scheduleSkippedOutsideWindowCreditSaver
      || sourceCreditSaverSkip
    );

  if (expectedTemporaryNoOdds) {
    const temporaryNoOddsPayload = Object.assign(buildRunHealthDegradedContract_({
      reason_code: 'run_health_expected_temporary_no_odds',
      odds_reason_codes: oddsReasonCodes,
      schedule_reason_codes: scheduleReasonCodes,
      match_reason_codes: matchReasonCodes,
      signal_reason_codes: signalReasonCodes,
      player_stats_reason_codes: playerStatsReasonCodes,
      opening_lag_blocked_count: openingLagBlockedCount,
      schedule_only_seed_count: scheduleOnlySeedCount,
      no_odds_stage_count: noOddsStageCount,
      stale_odds_skip_count: staleOddsSkipCount,
      low_edge_suppressed_count: lowEdgeSuppressedCount,
      cooldown_suppressed_count: cooldownSuppressedCount,
      sampled_blocked_odds: sampledBlockedOdds,
      sample_unmatched_cases: sampleUnmatchedCases,
    }), {
      reason_code: 'run_health_expected_temporary_no_odds',
      fetched_odds: fetchedOdds,
      fetched_schedule: fetchedSchedule,
      matched: matched,
      signals_found: signalsFound,
      message: 'Fetched odds without matches, but diagnostics indicate a temporary expected no-odds/no-match window.',
    });

    const shouldEmitTemporaryNoOddsWarning = temporaryNoOddsSignalCount >= 3
      || noOddsStageCount >= 2
      || scheduleOnlySeedCount >= 2
      || openingLagBlockedCount >= 2;

    return {
      is_degraded: false,
      status: 'idle_expected_temporary_no_odds',
      reason_code: 'run_health_expected_temporary_no_odds',
      degraded_reason_code: '',
      summary_status: 'success',
      summary_reason_code: 'run_health_expected_temporary_no_odds',
      summary_message: temporaryNoOddsPayload.message,
      warning_payload: temporaryNoOddsPayload,
      should_emit_warning: shouldEmitTemporaryNoOddsWarning,
    };
  }

  const dataNotActionableYet = isRunHealthDataNotActionableYet_(metrics);
  if (dataNotActionableYet) {
    const persistentDataNotActionable = dataNotActionableStreak >= dataNotActionableEscalationThreshold;
    const actionableGapPayload = Object.assign(buildRunHealthDegradedContract_({
      reason_code: persistentDataNotActionable
        ? 'run_health_data_not_actionable_persistent'
        : 'run_health_data_not_actionable_yet',
      odds_reason_codes: oddsReasonCodes,
      schedule_reason_codes: scheduleReasonCodes,
      match_reason_codes: matchReasonCodes,
      signal_reason_codes: signalReasonCodes,
      player_stats_reason_codes: playerStatsReasonCodes,
      opening_lag_blocked_count: openingLagBlockedCount,
      schedule_only_seed_count: scheduleOnlySeedCount,
      no_odds_stage_count: noOddsStageCount,
      stale_odds_skip_count: staleOddsSkipCount,
      low_edge_suppressed_count: lowEdgeSuppressedCount,
      cooldown_suppressed_count: cooldownSuppressedCount,
      sampled_blocked_odds: sampledBlockedOdds,
      sample_unmatched_cases: sampleUnmatchedCases,
    }), {
      reason_code: persistentDataNotActionable
        ? 'run_health_data_not_actionable_persistent'
        : 'run_health_data_not_actionable_yet',
      fetched_odds: fetchedOdds,
      fetched_schedule: fetchedSchedule,
      matched: matched,
      signals_found: signalsFound,
      data_not_actionable_streak: dataNotActionableStreak,
      data_not_actionable_escalation_threshold: dataNotActionableEscalationThreshold,
      message: persistentDataNotActionable
        ? 'Fetched odds are still not actionable after consecutive runs; downstream matches/signals remain empty and now require operator intervention.'
        : 'Fetched odds are not actionable yet (e.g. missing open timestamps or no actionable rows); downstream matches/signals are expected to be empty while data is still warming.',
    });

    return {
      is_degraded: persistentDataNotActionable,
      status: persistentDataNotActionable ? 'degraded' : 'idle_data_not_actionable_yet',
      reason_code: actionableGapPayload.reason_code,
      degraded_reason_code: persistentDataNotActionable ? actionableGapPayload.reason_code : '',
      summary_status: persistentDataNotActionable ? 'degraded' : 'success',
      summary_reason_code: actionableGapPayload.reason_code,
      summary_message: actionableGapPayload.message,
      warning_payload: actionableGapPayload,
      should_emit_warning: persistentDataNotActionable,
    };
  }

  const degraded = fetchedOdds > 0 && matched === 0;
  const statsZeroCoverageDegraded = matched > 0 && playersWithNonNullStats === 0;
  if (statsZeroCoverageDegraded) {
    const statsCoverageReasonCode = resolveStatsCoverageRunHealthReason_(playerStatsReasonCodes);
    const statsCoveragePayload = Object.assign(buildRunHealthDegradedContract_({
      reason_code: statsCoverageReasonCode,
      odds_reason_codes: oddsReasonCodes,
      schedule_reason_codes: scheduleReasonCodes,
      match_reason_codes: matchReasonCodes,
      signal_reason_codes: signalReasonCodes,
      player_stats_reason_codes: playerStatsReasonCodes,
      opening_lag_blocked_count: openingLagBlockedCount,
      schedule_only_seed_count: scheduleOnlySeedCount,
      no_odds_stage_count: noOddsStageCount,
      stale_odds_skip_count: staleOddsSkipCount,
      low_edge_suppressed_count: lowEdgeSuppressedCount,
      cooldown_suppressed_count: cooldownSuppressedCount,
      stats_zero_coverage_count: 1,
      sampled_blocked_odds: sampledBlockedOdds,
      sample_unmatched_cases: sampleUnmatchedCases,
    }), {
      reason_code: statsCoverageReasonCode,
      fetched_odds: fetchedOdds,
      fetched_schedule: fetchedSchedule,
      matched: matched,
      signals_found: signalsFound,
      players_with_non_null_stats: playersWithNonNullStats,
      message: 'Matched events found but player stats coverage is zero; running in fallback-only mode.',
    });

    return {
      is_degraded: singleRunCriticalTriggerEnabled,
      status: singleRunCriticalTriggerEnabled ? 'degraded' : 'warning_stats_zero_coverage',
      reason_code: statsCoverageReasonCode,
      degraded_reason_code: singleRunCriticalTriggerEnabled ? statsCoverageReasonCode : '',
      summary_status: singleRunCriticalTriggerEnabled ? 'degraded' : 'success',
      summary_reason_code: statsCoverageReasonCode,
      summary_message: statsCoveragePayload.message,
      warning_payload: Object.assign({}, statsCoveragePayload, {
        single_run_critical_trigger_enabled: singleRunCriticalTriggerEnabled,
      }),
      should_emit_warning: true,
    };
  }

  const edgeInstabilityDetected = !!edgeQualityDiagnostics.instability_detected;
  if (edgeInstabilityDetected) {
    const edgeQualityPayload = Object.assign(buildRunHealthDegradedContract_({
      reason_code: 'edge_quality_unstable',
      odds_reason_codes: oddsReasonCodes,
      schedule_reason_codes: scheduleReasonCodes,
      match_reason_codes: matchReasonCodes,
      signal_reason_codes: signalReasonCodes,
      player_stats_reason_codes: playerStatsReasonCodes,
      opening_lag_blocked_count: openingLagBlockedCount,
      schedule_only_seed_count: scheduleOnlySeedCount,
      no_odds_stage_count: noOddsStageCount,
      stale_odds_skip_count: staleOddsSkipCount,
      low_edge_suppressed_count: lowEdgeSuppressedCount,
      cooldown_suppressed_count: cooldownSuppressedCount,
      sampled_blocked_odds: sampledBlockedOdds,
      sample_unmatched_cases: sampleUnmatchedCases,
      edge_quality_diagnostics: edgeQualityDiagnostics,
    }), {
      reason_code: 'edge_quality_unstable',
      fetched_odds: fetchedOdds,
      fetched_schedule: fetchedSchedule,
      matched: matched,
      signals_found: signalsFound,
      edge_quality_diagnostics: edgeQualityDiagnostics,
      message: 'Edge distribution volatility breached configured stability threshold versus previous run.',
    });

    return {
      is_degraded: true,
      status: 'degraded',
      reason_code: 'edge_quality_unstable',
      degraded_reason_code: 'edge_quality_unstable',
      summary_status: 'degraded',
      summary_reason_code: 'edge_quality_unstable',
      summary_message: edgeQualityPayload.message,
      warning_payload: edgeQualityPayload,
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

  const warningPayload = Object.assign(buildRunHealthDegradedContract_({
    reason_code: 'run_health_no_matches_from_odds',
    odds_reason_codes: oddsReasonCodes,
    schedule_reason_codes: scheduleReasonCodes,
    match_reason_codes: matchReasonCodes,
    signal_reason_codes: signalReasonCodes,
    player_stats_reason_codes: playerStatsReasonCodes,
    opening_lag_blocked_count: openingLagBlockedCount,
    schedule_only_seed_count: scheduleOnlySeedCount,
    no_odds_stage_count: noOddsStageCount,
    stale_odds_skip_count: staleOddsSkipCount,
    low_edge_suppressed_count: lowEdgeSuppressedCount,
    cooldown_suppressed_count: cooldownSuppressedCount,
    sampled_blocked_odds: sampledBlockedOdds,
    sample_unmatched_cases: sampleUnmatchedCases,
  }), {
    reason_code: 'run_health_no_matches_from_odds',
    fetched_odds: fetchedOdds,
    fetched_schedule: fetchedSchedule,
    matched: matched,
    signals_found: signalsFound,
    no_match_from_odds_streak: noMatchFromOddsStreak,
    no_match_from_odds_degraded_trigger: noMatchFromOddsDegradedTrigger,
  });
  const noMatchFromOddsEscalated = noMatchFromOddsStreak >= noMatchFromOddsDegradedTrigger;
  const noMatchReasonCode = noMatchFromOddsEscalated
    ? 'run_health_no_matches_from_odds_consecutive'
    : 'run_health_no_matches_from_odds_waiting';
  warningPayload.reason_code = noMatchReasonCode;
  warningPayload.no_match_from_odds_streak = noMatchFromOddsStreak;
  warningPayload.no_match_from_odds_degraded_trigger = noMatchFromOddsDegradedTrigger;

  return {
    is_degraded: noMatchFromOddsEscalated,
    status: noMatchFromOddsEscalated ? 'degraded' : 'warning_no_matches_waiting_threshold',
    reason_code: noMatchReasonCode,
    degraded_reason_code: noMatchFromOddsEscalated ? noMatchReasonCode : '',
    summary_status: noMatchFromOddsEscalated ? 'degraded' : 'success',
    summary_reason_code: noMatchReasonCode,
    summary_message: noMatchFromOddsEscalated
      ? 'Pipeline run completed with degraded run-health guard.'
      : 'Pipeline run completed; no-match streak has not reached degraded trigger yet.',
    warning_payload: warningPayload,
    should_emit_warning: true,
  };
}

function isRunHealthDataNotActionableYet_(metrics) {
  const safe = metrics || {};
  const fetchedOdds = Number(safe.fetched_odds || 0);
  const matched = Number(safe.matched || 0);
  const signalsFound = Number(safe.signals_found || 0);
  const oddsReasonCodes = (safe.odds_reason_codes) || {};
  const matchReasonCodes = (safe.match_reason_codes) || {};
  const oddsActionableCount = Number(oddsReasonCodes.odds_actionable || 0);
  const missingOpenTimestampCount = Number(oddsReasonCodes.missing_open_timestamp || 0);
  const noOddsCandidatesCount = Number(matchReasonCodes.no_odds_candidates || 0);
  const scheduleSeedNoOddsCount = Number(matchReasonCodes.schedule_seed_no_odds || 0);

  return fetchedOdds > 0
    && matched === 0
    && signalsFound === 0
    && (
      oddsActionableCount === 0
      || missingOpenTimestampCount > 0
      || noOddsCandidatesCount > 0
      || scheduleSeedNoOddsCount > 0
    );
}

function updateRunHealthDataNotActionableState_(runId, metrics, config) {
  const threshold = Math.max(
    2,
    Number((config && config.RUN_HEALTH_CONSECUTIVE_RUN_DEGRADED_TRIGGER)
      || (config && config.RUN_HEALTH_DATA_NOT_ACTIONABLE_PERSIST_THRESHOLD)
      || 3)
  );
  const previous = getStateJson_('RUN_HEALTH_DATA_NOT_ACTIONABLE_STATE') || {};
  const isDataNotActionableYet = isRunHealthDataNotActionableYet_(metrics);
  const previousCount = Math.max(0, Number(previous.consecutive_count || 0));
  const consecutiveCount = isDataNotActionableYet ? previousCount + 1 : 0;
  const escalated = isDataNotActionableYet && consecutiveCount >= threshold;

  const next = {
    run_id: runId,
    updated_at: formatLocalIso_(new Date()),
    threshold: threshold,
    consecutive_count: consecutiveCount,
    is_data_not_actionable_yet: isDataNotActionableYet,
    escalated: escalated,
  };
  setStateValue_('RUN_HEALTH_DATA_NOT_ACTIONABLE_STATE', JSON.stringify(next));

  return next;
}

function updateRunHealthNoMatchFromOddsState_(runId, metrics, config) {
  const threshold = Math.max(1, Number((config && config.RUN_HEALTH_CONSECUTIVE_RUN_DEGRADED_TRIGGER) || 3));
  const previous = getStateJson_('RUN_HEALTH_NO_MATCH_FROM_ODDS_STATE') || {};
  const fetchedOdds = Number((metrics && metrics.fetched_odds) || 0);
  const matched = Number((metrics && metrics.matched) || 0);
  const isNoMatchFromOdds = fetchedOdds > 0 && matched === 0;
  const previousCount = Math.max(0, Number(previous.consecutive_count || 0));
  const consecutiveCount = isNoMatchFromOdds ? previousCount + 1 : 0;

  const next = {
    run_id: runId,
    updated_at: formatLocalIso_(new Date()),
    threshold: threshold,
    consecutive_count: consecutiveCount,
    is_no_match_from_odds: isNoMatchFromOdds,
    escalated: isNoMatchFromOdds && consecutiveCount >= threshold,
  };
  setStateValue_('RUN_HEALTH_NO_MATCH_FROM_ODDS_STATE', JSON.stringify(next));
  return next;
}

function resolveStatsCoverageRunHealthReason_(playerStatsReasonCodes) {
  const reasonCodes = playerStatsReasonCodes || {};
  if (Number(reasonCodes.stats_top100_filter_excluded || 0) > 0) return 'stats_top100_filter_excluded';
  if (Number(reasonCodes.stats_out_of_cohort || 0) > 0) return 'stats_out_of_cohort';
  if (Number(reasonCodes.stats_rank_unknown || 0) > 0) return 'stats_rank_unknown';
  return 'stats_zero_coverage';
}

function buildRunHealthDegradedContract_(metrics) {
  const safe = metrics || {};
  const sampledBlockedRecords = (safe.sampled_blocked_odds || []).slice(0, 5);
  const sampledBlockedOddsIds = sampledBlockedRecords
    .map(function (entry) { return sanitizeRunHealthText_(entry && entry.odds_event_id, 72); })
    .filter(function (value) { return !!value; })
    .slice(0, 3);
  const openingLagBlockedCount = Number(safe.opening_lag_blocked_count || 0);
  const scheduleOnlySeedCount = Number(safe.schedule_only_seed_count || 0);
  const noOddsStageCount = Number(safe.no_odds_stage_count || 0);
  const staleOddsSkipCount = Number(safe.stale_odds_skip_count || 0);
  const lowEdgeSuppressedCount = Number(safe.low_edge_suppressed_count || 0);
  const cooldownSuppressedCount = Number(safe.cooldown_suppressed_count || 0);
  const statsZeroCoverageCount = Number(safe.stats_zero_coverage_count || 0);
  const noMatchFromOddsStreak = Number(safe.no_match_from_odds_streak || 0);
  const noMatchFromOddsDegradedTrigger = Number(safe.no_match_from_odds_degraded_trigger || 0);
  const edgeQualityDiagnostics = safe.edge_quality_diagnostics && typeof safe.edge_quality_diagnostics === 'object'
    ? Object.assign({}, safe.edge_quality_diagnostics)
    : {};

  const blockerCounts = {
    opening_lag_blocked_count: openingLagBlockedCount,
    schedule_only_seed_count: scheduleOnlySeedCount,
    no_odds_stage_count: noOddsStageCount,
    stale_odds_skip_count: staleOddsSkipCount,
    low_edge_suppressed_count: lowEdgeSuppressedCount,
    cooldown_suppressed_count: cooldownSuppressedCount,
    stats_zero_coverage_count: statsZeroCoverageCount,
    no_match_from_odds_streak: noMatchFromOddsStreak,
    no_match_from_odds_degraded_trigger: noMatchFromOddsDegradedTrigger,
  };

  return {
    run_health_contract_version: 3,
    reason_code: sanitizeRunHealthText_(safe.reason_code, 72),
    blocker_counts: blockerCounts,
    opening_lag_blocked_count: openingLagBlockedCount,
    schedule_only_seed_count: scheduleOnlySeedCount,
    no_odds_stage_count: noOddsStageCount,
    stale_odds_skip_count: staleOddsSkipCount,
    low_edge_suppressed_count: lowEdgeSuppressedCount,
    cooldown_suppressed_count: cooldownSuppressedCount,
    stats_zero_coverage_count: statsZeroCoverageCount,
    failure_reasons: buildRunHealthFailureReasons_({
      opening_lag_blocked_count: openingLagBlockedCount,
      schedule_only_seed_count: scheduleOnlySeedCount,
      no_odds_stage_count: noOddsStageCount,
      stale_odds_skip_count: staleOddsSkipCount,
      low_edge_suppressed_count: lowEdgeSuppressedCount,
      cooldown_suppressed_count: cooldownSuppressedCount,
      stats_zero_coverage_count: statsZeroCoverageCount,
      sampled_blocked_odds_ids: sampledBlockedOddsIds,
    }),
    stage_skipped_reason_counts: deriveStageSkippedReasonCounts_([
      safe.odds_reason_codes || {},
      safe.schedule_reason_codes || {},
      safe.match_reason_codes || {},
      safe.signal_reason_codes || {},
      safe.player_stats_reason_codes || {},
    ]),
    dominant_blocker_categories: resolveDominantRunHealthBlockers_(
      safe.odds_reason_codes || {},
      safe.schedule_reason_codes || {},
      safe.match_reason_codes || {},
      safe.signal_reason_codes || {},
      {
        opening_lag_blocked: openingLagBlockedCount,
        schedule_only_seed: scheduleOnlySeedCount,
        no_odds_stage: noOddsStageCount,
        stale_odds_skip: staleOddsSkipCount,
        low_edge_suppressed: lowEdgeSuppressedCount,
        cooldown_suppressed: cooldownSuppressedCount,
        stats_zero_coverage: statsZeroCoverageCount,
      }
    ),
    sampled_blocked_records: sampledBlockedRecords,
    edge_quality_diagnostics: edgeQualityDiagnostics,
    sampled_blocked_odds: sampledBlockedRecords,
    sample_unmatched_events: (safe.sample_unmatched_cases || []).slice(0, 5).map(function (entry) {
      return {
        odds_event_id: entry.odds_event_id,
        competition: entry.competition,
        player_1: entry.player_1,
        player_2: entry.player_2,
        normalized_odds_players: entry.normalized_odds_players || [],
        normalized_schedule_players: entry.normalized_schedule_players || [],
        nearest_schedule_candidate: entry.nearest_schedule_candidate || null,
        primary_time_delta_min: entry.primary_time_delta_min,
        fallback_time_delta_min: entry.fallback_time_delta_min,
        commence_time: entry.commence_time,
        rejection_code: entry.rejection_code,
      };
    }),
  };
}

function deriveStageSkippedReasonCounts_(reasonCodeMaps) {
  const merged = mergeReasonCounts_(reasonCodeMaps || []);
  const counts = {};
  Object.keys(merged).forEach(function (code) {
    const value = Number(merged[code] || 0);
    if (value <= 0) return;
    if (/(^|_)(skip|skipped|blocked)(_|$)/.test(code)
      || code === 'opening_lag_exceeded'
      || code === 'missing_open_timestamp'
      || code === 'schedule_seed_no_odds'
      || code === 'schedule_enrichment_no_schedule_events'
      || code === 'schedule_missing_player_identity'
      || code === 'no_player_match') {
      counts[code] = value;
    }
  });
  return counts;
}

function buildRunHealthFailureReasons_(counts) {
  const safeCounts = counts || {};
  const blockerSpecs = [
    {
      reason_code: 'opening_lag_blocked',
      count: Number(safeCounts.opening_lag_blocked_count || 0),
      sampled_odds_event_ids: (safeCounts.sampled_blocked_odds_ids || []).slice(0, 3),
    },
    {
      reason_code: 'schedule_only_seed',
      count: Number(safeCounts.schedule_only_seed_count || 0),
    },
    {
      reason_code: 'no_odds_stage',
      count: Number(safeCounts.no_odds_stage_count || 0),
    },
    {
      reason_code: 'stale_odds_skip',
      count: Number(safeCounts.stale_odds_skip_count || 0),
    },
    {
      reason_code: 'low_edge_suppressed',
      count: Number(safeCounts.low_edge_suppressed_count || 0),
    },
    {
      reason_code: 'cooldown_suppressed',
      count: Number(safeCounts.cooldown_suppressed_count || 0),
    },
    {
      reason_code: 'stats_zero_coverage',
      count: Number(safeCounts.stats_zero_coverage_count || 0),
    },
  ];

  return blockerSpecs.filter(function (entry) {
    return Number(entry.count || 0) > 0;
  });
}

function resolveDominantRunHealthBlockers_(oddsReasonCodes, scheduleReasonCodes, matchReasonCodes, signalReasonCodes, explicitCounts) {
  const counts = Object.assign({
    opening_lag_blocked: 0,
    schedule_only_seed: 0,
    no_odds_stage: 0,
    stale_odds_skip: 0,
    low_edge_suppressed: 0,
    cooldown_suppressed: 0,
    stats_zero_coverage: 0,
    no_player_match: 0,
    schedule_missing_player_identity: 0,
    schedule_unavailable: 0,
    no_schedule_candidates: 0,
    no_odds_candidates: 0,
    outside_window_idle_skip: 0,
  }, explicitCounts || {});
  counts.no_player_match += Number((matchReasonCodes && matchReasonCodes.no_player_match) || 0);
  counts.schedule_missing_player_identity += Number((matchReasonCodes && matchReasonCodes.schedule_missing_player_identity) || 0);
  counts.schedule_unavailable += Number((scheduleReasonCodes && scheduleReasonCodes.schedule_enrichment_no_schedule_events) || 0);
  counts.no_schedule_candidates += Number((matchReasonCodes && matchReasonCodes.no_schedule_candidates) || 0);
  counts.no_odds_candidates += Number((matchReasonCodes && matchReasonCodes.no_odds_candidates) || 0);
  counts.stale_odds_skip += Number((signalReasonCodes && signalReasonCodes.stale_odds_skip) || 0);
  counts.low_edge_suppressed += Number((signalReasonCodes && signalReasonCodes.edge_below_threshold) || 0);
  counts.cooldown_suppressed += Number((signalReasonCodes && signalReasonCodes.cooldown_suppressed) || 0);
  counts.outside_window_idle_skip += Number((oddsReasonCodes && oddsReasonCodes.odds_refresh_skipped_outside_window) || 0);
  counts.outside_window_idle_skip += Number((scheduleReasonCodes && scheduleReasonCodes.schedule_fetch_skipped_outside_window_credit_saver) || 0);

  return Object.keys(counts)
    .map(function (category) {
      return {
        category: category,
        count: Number(counts[category] || 0),
      };
    })
    .filter(function (entry) { return entry.count > 0; })
    .sort(function (a, b) { return b.count - a.count; })
    .slice(0, 5);
}

function sanitizeBlockedOddsSamples_(blockedRows) {
  return (blockedRows || []).slice(0, 5).map(function (row) {
    const safe = row || {};
    return {
      odds_event_id: sanitizeRunHealthText_(safe.event_id, 72),
      teams: [sanitizeRunHealthText_(safe.player_1, 48), sanitizeRunHealthText_(safe.player_2, 48)].filter(function (item) { return !!item; }).join(' vs '),
      commence_time: sanitizeRunHealthText_(safe.commence_time, 40),
      open_timestamp: sanitizeRunHealthText_(safe.open_timestamp, 40),
      blocked_reason_code: sanitizeRunHealthText_(safe.reason_code, 48),
    };
  });
}

function sanitizeRunHealthText_(value, maxLen) {
  if (value === null || typeof value === 'undefined') return '';
  const normalized = String(value).replace(/\s+/g, ' ').trim();
  const limit = Math.max(1, Number(maxLen || 64));
  if (normalized.length <= limit) return normalized;
  return normalized.slice(0, limit - 1) + '…';
}

function resolveRunHealthFetchedOddsCount_(fetchedBeforeGateCount, gatedOddsStage) {
  const initialCount = Number(fetchedBeforeGateCount || 0);
  const gatedCount = Number((gatedOddsStage && gatedOddsStage.events && gatedOddsStage.events.length) || 0);
  return Math.max(initialCount, gatedCount);
}

function buildStageReasonCodeMutationDiagnostics_(stageSnapshots) {
  const mutatedStages = [];
  (stageSnapshots || []).forEach((entry) => {
    const stage = entry && entry.stage ? String(entry.stage) : '';
    const summary = entry && entry.summary ? entry.summary : {};
    const baseline = cloneReasonCodeMap_(entry && entry.snapshot ? entry.snapshot : {});
    const current = cloneReasonCodeMap_(summary.reason_codes || {});
    if (!areReasonCodeMapsEquivalent_(baseline, current)) {
      mutatedStages.push({
        stage: stage,
        baseline: baseline,
        current: current,
      });
    }
  });
  return {
    checked_stage_count: Number((stageSnapshots || []).length || 0),
    mutated_stages: mutatedStages,
  };
}

function validateStageReasonCodeMaxima_(stageSummaries) {
  const violations = [];
  (stageSummaries || []).forEach((summary) => {
    const safeSummary = summary || {};
    const stageName = String(safeSummary.stage || '');
    const reasonCodes = cloneReasonCodeMap_(safeSummary.reason_codes || {});
    const maxCount = Math.max(
      Number(safeSummary.input_count || 0),
      Number(safeSummary.output_count || 0)
    );

    Object.keys(reasonCodes).forEach((reasonCode) => {
      if (!/within|allowed/i.test(reasonCode)) return;
      const value = Number(reasonCodes[reasonCode] || 0);
      if (!Number.isFinite(value) || value <= maxCount) return;
      violations.push({
        stage: stageName,
        reason_code: reasonCode,
        value: value,
        max_allowed: maxCount,
        input_count: Number(safeSummary.input_count || 0),
        output_count: Number(safeSummary.output_count || 0),
      });
    });
  });
  return violations;
}
