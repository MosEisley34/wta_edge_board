# Legacy verbose vs compact run-window comparison (2026-03-12 deterministic rerun)

## Scope and method

- Window: **same deterministic run window** (36 runs, 5-minute cadence, 3-hour span).
- Capture command (verbose): `node scripts/capture_pipeline_log_sample.js docs/baselines/pipeline_log_sample_3h_verbose_2026-03-12.json 36 5 verbose`
- Capture command (compact): `node scripts/capture_pipeline_log_sample.js docs/baselines/pipeline_log_sample_3h_compact_2026-03-12.json 36 5 compact`
- Comparison command: `python3 scripts/compare_log_profiles.py docs/baselines/pipeline_log_sample_3h_verbose_2026-03-12.json docs/baselines/pipeline_log_sample_3h_compact_2026-03-12.json`
- Comparison artifact: `docs/baselines/pipeline_log_profile_comparison_2026-03-12_rerun.json`

## Side-by-side results

| Metric | Legacy verbose | Compact | Outcome |
|---|---:|---:|---|
| Output size (bytes) | 345,493 | 345,493 | No change |
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

**Decision: NO-GO** for compact-profile rollout at this time because reduction remains below the >=60% gate despite parity passing.
