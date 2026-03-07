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
  const spec = {
    functionName: 'runEdgeBoard',
    everyMinutes: 15,
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
    const config = getConfig_();
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
        current_refresh_mode: oddsWindowDecision.current_refresh_mode || '',
        previous_refresh_mode: oddsWindowDecision.previous_refresh_mode || '',
        transitioned_from_bootstrap_to_active_window: !!oddsWindowDecision.transitioned_from_bootstrap_to_active_window,
        transition_state: oddsWindowDecision.transitioned_from_bootstrap_to_active_window
          ? 'bootstrap_to_active_window'
          : (oddsWindowDecision.bootstrap_mode ? 'bootstrap_active' : 'active_window_or_skipped'),
      }),
    });

    const oddsStage = oddsWindowDecision.should_fetch_odds
      ? stageFetchOdds(runId, config, oddsWindowDecision.odds_fetch_window)
      : buildSkippedOddsStage_(runId, oddsWindowDecision.decision_reason_code, oddsWindowDecision.decision_message);
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

    const scheduleStage = stageFetchSchedule(runId, config, oddsStage.events);
    appendStageLog_(runId, scheduleStage.summary);

    const matchStage = stageMatchEvents(runId, config, oddsStage.events, scheduleStage.events);
    appendStageLog_(runId, matchStage.summary);

    const playerStatsStage = stageFetchPlayerStats(runId, config, oddsStage.events, matchStage.rows);
    appendStageLog_(runId, playerStatsStage.summary);

    const signalStage = stageGenerateSignals(runId, config, oddsStage.events, matchStage.rows, playerStatsStage.byOddsEventId);
    appendStageLog_(runId, signalStage.summary);

    const persistStage = stagePersist(runId, {
      odds: oddsStage.rows,
      schedule: scheduleStage.rows,
      playerStats: playerStatsStage.rows,
      matchMap: matchStage.rows,
      signals: signalStage.rows,
    });
    appendStageLog_(runId, persistStage.summary);

    const combinedReasonCodes = mergeReasonCounts_([
      oddsStage.summary.reason_codes,
      scheduleStage.summary.reason_codes,
      matchStage.summary.reason_codes,
      playerStatsStage.summary.reason_codes,
      signalStage.summary.reason_codes,
      persistStage.summary.reason_codes,
    ]);

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
      status: 'success',
      reason_code: 'run_success',
      message: 'Pipeline run completed.',
      fetched_odds: oddsStage.events.length,
      fetched_schedule: scheduleStage.events.length,
      allowed_tournaments: scheduleStage.allowedCount,
      matched: matchStage.matchedCount,
      unmatched: matchStage.unmatchedCount,
      signals_found: signalStage.sentCount,
      rejection_codes: JSON.stringify(combinedReasonCodes),
      stage_summaries: JSON.stringify(verbosePayload.stage_summaries),
      cooldown_suppressed: signalStage.cooldownSuppressedCount,
      duplicate_suppressed: signalStage.duplicateSuppressedCount,
    });
  } catch (error) {
    appendLogRow_({
      row_type: 'summary',
      run_id: runId,
      stage: 'runEdgeBoard',
      started_at: startedAt,
      ended_at: new Date(),
      status: 'failed',
      reason_code: 'run_exception',
      message: String(error && error.message ? error.message : error),
      exception: String(error && error.message ? error.message : error),
      stack: String(error && error.stack ? error.stack : ''),
    });
    throw error;
  } finally {
    lock.releaseLock();
  }
}
