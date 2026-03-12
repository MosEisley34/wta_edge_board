# Legacy verbose vs compact run-window comparison (2026-03-12 deterministic rerun, parity-fix refresh)

## Scope and method

- Window: **same deterministic run window** (36 runs, 5-minute cadence, 3-hour span).
- Capture command (verbose): `node scripts/capture_pipeline_log_sample.js docs/baselines/pipeline_log_sample_3h_verbose_2026-03-12_fix.json 36 5 verbose`
- Capture command (compact): `node scripts/capture_pipeline_log_sample.js docs/baselines/pipeline_log_sample_3h_compact_2026-03-12_fix.json 36 5 compact`
- Comparison command: `python3 scripts/compare_log_profiles.py docs/baselines/pipeline_log_sample_3h_verbose_2026-03-12_fix.json docs/baselines/pipeline_log_sample_3h_compact_2026-03-12_fix.json --baseline-summary docs/baselines/pipeline_log_profile_comparison_2026-03-12.json --summary-json-out docs/baselines/pipeline_log_profile_comparison_2026-03-12_fix.json`
- Comparison artifact: `docs/baselines/pipeline_log_profile_comparison_2026-03-12_fix.json`

## Side-by-side results

| Metric | Legacy verbose | Compact | Outcome |
|---|---:|---:|---|
| Output size (bytes) | 351,901 | 351,901 | No change |
| Reduction | — | 0.00% | **Target (>=60%) not met** |
| Incremental compact bytes vs baseline (`pipeline_log_profile_comparison_2026-03-12.json`) | — | -44,280 bytes | Regression (larger compact artifact) |

## Corrected parity checks and attribution sanity

All checks were evaluated per-run across 36 aligned run ids.

| Check | Result | Notes |
|---|---|---|
| Gate reasons parity | ✅ Pass | `summary.reason_code` and rejection-code maps matched run-for-run. |
| **Counter parity (corrected)** | ✅ Pass | Summary counters (`fetched_odds`, `fetched_schedule`, `allowed_tournaments`, `matched`, `unmatched`, `signals_found`, `rejected`, `cooldown_suppressed`, `duplicate_suppressed`) matched. |
| Source selection parity | ✅ Pass | Stage provider/source fields matched across stage summaries. |
| Watchdog parity | ✅ Pass | Watchdog stage/status/reason tuples matched. |
| Stage timings parity | ✅ Pass | `started_at` / `ended_at` / `duration_ms` matched by stage. |
| **Run-health/degraded attribution sanity** | ✅ Pass | Derived run-health attribution (`healthy` vs `degraded`, and reason-source consistency from watchdog + rejection/run-health codes) matched run-for-run. |

## Delta from prior baseline (quick operator check)

Compared against `docs/baselines/pipeline_log_profile_comparison_2026-03-12_rerun.json`:

- Added explicit parity dimensions for **summary counters** and **run-health attribution sanity** (both mismatch counts are `0`).
- Prior parity report only covered gate reasons, source selection, watchdog, and stage timing.
- Sample size bytes changed from baseline compact **302,725** to **347,005** on this rerun window (**+44,280 bytes**); this does **not** change rollout readiness because percentage reduction remains **0.00%**.
- Critical parity gate still passes; rollout gate still fails solely due to byte-reduction threshold.

## Byte reduction gate (GO / NO-GO)

- Reduction threshold: **>=60%**
- Observed reduction: **0.00%**
- Quality gate failed reasons: `reduction_below_target`

**Decision: NO-GO** for compact-profile rollout at this time because reduction remains below threshold despite parity and attribution sanity checks passing.
