# Legacy verbose vs compact run-window comparison (2026-03-12)

## Scope and method

- Window: **same deterministic run window** (36 runs, 5-minute cadence, 3-hour span).
- Capture command (verbose): `node scripts/capture_pipeline_log_sample.js docs/baselines/pipeline_log_sample_3h_verbose.json 36 5 verbose`
- Capture command (compact): `node scripts/capture_pipeline_log_sample.js docs/baselines/pipeline_log_sample_3h_compact.json 36 5 compact`
- Comparison command: `python3 scripts/compare_log_profiles.py docs/baselines/pipeline_log_sample_3h_verbose.json docs/baselines/pipeline_log_sample_3h_compact.json`
- Comparison artifact: `docs/baselines/pipeline_log_profile_comparison_2026-03-12.json`

## Side-by-side results

| Metric | Legacy verbose | Compact | Outcome |
|---|---:|---:|---|
| Output size (bytes) | 302,725 | 302,725 | No change |
| Reduction | — | 0.00% | **Target (>=60%) not met** |

## Field parity checks (critical diagnostics)

All parity checks were evaluated per-run across 36 aligned run ids.

| Check | Result | Notes |
|---|---|---|
| Gate reasons parity | ✅ Pass | `summary.reason_code` and rejection-code maps matched run-for-run. |
| Source selection parity | ✅ Pass | Stage provider/source fields matched across stage summaries. |
| Watchdog parity | ✅ Pass | Watchdog stage/status/reason tuples matched. |
| Stage timings parity | ✅ Pass | `started_at` / `ended_at` / `duration_ms` matched by stage. |

## Go / no-go checklist

- [x] Same-window side-by-side captures completed (verbose + compact).
- [x] Critical diagnostic field parity validated (gate reasons, source selection, watchdog, stage timings).
- [ ] Size reduction target met (>=60%). **Current: 0.00%**.
- [ ] Operational readiness for rollout gate. **Blocked by unmet reduction target**.

**Decision: NO-GO** for broad compact-profile rollout as a size-reduction initiative.

## Phased rollout plan (dev -> canary -> full)

Because parity is clean but byte reduction is not yet realized, proceed with a gated, implementation-first rollout:

### Phase 1 — Dev (instrument + implement)

1. Implement true compact emission for run-log artifacts (not only profile toggles) to shrink persisted row payloads.
2. Re-run same-window comparison and require:
   - reduction >=60%,
   - no parity regressions in the four critical diagnostic categories.
3. Add CI check that fails when reduction <60% on deterministic sample.

**Exit criteria:** reduction target achieved in dev and parity remains 100%.

### Phase 2 — Canary (10% traffic / selected schedules)

1. Enable compact profile on canary cohort only.
2. Monitor daily:
   - bytes written per run,
   - watchdog reason-code distribution drift,
   - stage-timing drift,
   - incident triage success (no missing critical diagnostics).
3. Roll back canary immediately if parity/diagnostic completeness degrades.

**Exit criteria:** 7 consecutive days with reduction >=60% and no diagnostic regressions.

### Phase 3 — Full rollout

1. Ramp from 25% -> 50% -> 100% over 3 deployment steps.
2. Keep verbose fallback switch available for incident response.
3. Retain weekly parity audit jobs for first 30 days post-cutover.

**Exit criteria:** 30 days stable; retire temporary canary guards and keep periodic audit.
