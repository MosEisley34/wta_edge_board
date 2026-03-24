# Runtime diagnostics runbook

## Logging profile guidance

Runtime logging now supports `LOG_PROFILE=compact|verbose` (set in the `Config` sheet).

- `compact` (default): optimized for smaller diagnostic artifacts and faster runs. It keeps only low-verbosity diagnostics and writes a compact `LAST_RUN_VERBOSE_JSON` summary.
- `verbose`: preserves previous full-fidelity diagnostics, including full `LAST_RUN_VERBOSE_JSON` payloads and high-detail logging.

Expected size/performance tradeoff:
- `compact`: reduced `Run_Log`/`State` payload size and less serialization/write overhead.
- `verbose`: larger runtime artifacts and higher write cost, but better for deep incident triage.

## Standard triage bundle (recommended)

Run this wrapper each triage cycle so runtime diagnostics inputs are prepared consistently before analysis:

```bash
scripts/run_triage_bundle.sh [--out-dir ./exports] <file-or-directory> [more paths...]
```

What it does:
1. Runs `scripts/prepare_runtime_exports.sh` to export `Run_Log`/`State` CSV/JSON into `./exports` (default).
2. Immediately invokes `scripts/scan_runtime_diagnostics.sh ./exports` (or your custom `--out-dir`).
3. Prioritize the scanner's **Run-health degraded contract (first-pass triage)** section before any key-specific deep dive.

`prepare_runtime_exports.sh` enforces a single-snapshot Run_Log export contract: `Run_Log.csv` + `Run_Log.json` are regenerated together from the same latest source snapshot, parity-gated, and recorded in `run_log_latest_batch_note.json`.

For compare/gate workflows that require explicit run IDs, always use this fail-fast preflight first:

```bash
scripts/export_parity_precheck.sh --out-dir ./exports_live <run_id_a> <run_id_b> <live_runtime_dir_or_files>
```

Operational contract for compare/gate workflows:
- `scripts/export_parity_precheck.sh` must be the first command in the sequence.
- If wrapper parity/precheck fails, stop triage and re-export immediately.
- After wrapper success, run all downstream commands against `./exports_live` only (`precheck_run_ids.py`, `verify_run_log_parity.py`, `compare_*`, `evaluate_edge_quality.py`).
- Do not mix stale `live_runtime/Run_Log.json` with fresh `Run_Log.csv` in runbook examples or operator commands.

Optional shell muscle-memory helper (ops dotfiles):

```bash
alias rl_precheck='scripts/export_parity_precheck.sh --out-dir ./exports_live'
```


## Quick operational checks (copy/paste)

Use this short flow when validating trigger health and recent runtime diagnostics artifacts:

```bash
# 1) Prepare deterministic exports in ./exports from local files/dirs.
scripts/prepare_runtime_exports.sh --out-dir ./exports <file-or-directory>

# 2) Run local triage scanner (first-pass contract + grouped counts + row samples).
scripts/triage_runtime_diagnostics_local.sh ./exports

# 3) Emit 7-line compact incident summary for chat/ticket updates.
scripts/runtime_diagnostics_summary.py ./exports

# 4) Optional: run the wrapper that does steps (1)+(2) in one command.
scripts/run_triage_bundle.sh --out-dir ./exports <file-or-directory>
```

Operational expectation:
- `prepare_runtime_exports.sh` must produce `runtime_export_manifest.json` plus at least one Run_Log/State artifact.
- `triage_runtime_diagnostics_local.sh` should show `Run-health degraded contract (first-pass triage)` near the top.
- `runtime_diagnostics_summary.py` should print deterministic seven-line summary output including daily status and day-over-day deltas.

## Compare/gate command checklist (run IDs required)

Use this sequence for all run-to-run parity/quality workflows:

```bash
# 1) Mandatory export + parity + run-id precheck wrapper.
scripts/export_parity_precheck.sh --out-dir ./exports_live <run_id_a> <run_id_b> <live_runtime_dir_or_files>

# 2) Optional explicit precheck re-run against the prepared export dir only.
python3 scripts/precheck_run_ids.py <run_id_a> <run_id_b> --export-dir ./exports_live

# 3) Compare/gate commands read only from ./exports_live.
python3 scripts/verify_run_log_parity.py --export-dir ./exports_live
python3 scripts/compare_run_diagnostics.py <run_id_a> <run_id_b> --export-dir ./exports_live
python3 scripts/compare_run_metrics.py <run_id_a> <run_id_b> --input ./exports_live/Run_Log.csv
python3 scripts/evaluate_edge_quality.py ./exports_live --baseline-run-id <run_id_a> --candidate-run-id <run_id_b>
```

## Rolling edge-quality windows (postmortem vs release gate)

Use rolling edge-quality analysis each cycle to produce **two distinct windows**:
- `full_history_trend`: long-horizon trend for postmortems and drift analysis.
- `recent_window_gate`: release-readiness gate scoped to recent runs only.

Recommended command (with explicit recent-window cutoff and persisted artifact):

```bash
python3 scripts/evaluate_edge_quality.py ./exports_live \
  --min-ended-at 2026-03-21T00:00:00Z \
  --rolling-report-out ./docs/baselines/runtime_rollups/edge_quality_rolling_2026-03-24.json
```

Operational gate criteria:
- **GO/NO-GO must use only `recent_window_gate` status counts**.
- Treat any `recent_window_gate.status_counts.fail > 0` as **NO-GO**.
- Treat any `recent_window_gate.status_counts.insufficient_sample > 0` as **NO-GO** until sample sufficiency is restored.
- Treat `recent_window_gate.pair_count == 0` as **NO-GO** (insufficient evidence).
- Use `full_history_trend` only for diagnostics/postmortem context (not release gating).

Auditability requirement:
- Persist rolling outputs as JSON artifacts (`--rolling-report-out`) every cycle.
- Keep the artifact path/date in the incident/release log so both windows remain reconstructable later.

## Operator SLOs for degraded-mode reliability

Use these thresholds for weekly operations review and on-call escalation.

### Max tolerated consecutive degraded runs (by primary cause)

- `run_health_no_matches_from_odds`: **3** consecutive runs maximum.
- `stats_zero_coverage`: **2** consecutive runs maximum.
- `run_health_expected_temporary_no_odds`: **6** consecutive runs maximum before mandatory validation of upstream freshness.
- `run_health_opening_lag_schedule_seed_no_odds`: **6** consecutive runs maximum during expected market-open lag windows.
- `odds_refresh_bootstrap_blocked_by_credit_limit` / `credit_hard_limit_skip_odds`: **1** run maximum before operator intervention.

### Notification delivery success SLO

- Signal and risk notification delivery (`postDiscordWebhook_` outcomes) must maintain:
  - **≥ 99.0% success per rolling 7-day window**, and
  - **≥ 95.0% success per day**.
- Any `notify_http_failed` burst of **3+ consecutive failures** is treated as an incident candidate.

### Mandatory remediation triggers

Trigger remediation immediately when any of these occur:

1. Hard credit protection mode activated (`credit_hard_limit_skip_odds` or bootstrap credit-blocked paths).
2. Notification success falls below daily 95% threshold.
3. Stage-summary vs final-summary reason-code contract mismatch appears in run-health diagnostics.
4. Matcher precheck blockers (`schedule_missing_player_identity`, `schedule_date_misaligned_with_odds`) persist for 2 consecutive runs.

Required remediation checklist:
- Confirm webhook endpoint health and credentials.
- Validate odds/schedule provider freshness windows and parser contracts.
- Re-run deterministic soak replay sequence (below) and archive artifacts.
- Open incident ticket with sampled blocker payloads from triage output.

## Manual usage flow (optional)

1. Run the repeatable export pre-step into the known export directory (`./exports` by default):

```bash
scripts/prepare_runtime_exports.sh [--out-dir ./exports] <file-or-directory> [more paths...]
```

Expected files (at least one required before scanning):
- `./exports/*Run_Log*.csv`
- `./exports/*Run_Log*.json`
- `./exports/*State*.csv`
- `./exports/*State*.json`

Preferred artifact source order during scanner triage:
1. `Run_Log.csv`
2. `Run_Log.json` (fallback)
3. `State.csv`
4. `State.json` only when it is object/record JSON (not list-style key/value state dumps)

`State.json` compatibility note:
- expected schema is object/record JSON that includes runtime fields (for example `stage`, `message`, `reason_code`),
- list-style `State.json` (`[{"key":"...","value":"..."}]`) is intentionally ignored with a warning; scanner falls back to `State.csv` when available.

Documentation generated by the pre-step:
- `./exports/runtime_export_manifest.json` (lists discovered export files, size, and `modified_at_utc` timestamps, plus `generated_at_utc` for the manifest itself)

If none are present, the pre-step fails early with a remediation message.

2. Run diagnostics triage:

```bash
scripts/triage_runtime_diagnostics_local.sh [./exports]
```

(For CI, use `scripts/triage_runtime_diagnostics_ci.sh` and optionally set `RUNTIME_EXPORT_DIR`.)

Optional compact incident summary (for chat handoff / ticket updates):

```bash
scripts/runtime_diagnostics_summary.py ./exports
```

Output is deterministic and intentionally small (7 lines) covering:
- run count + status breakdown,
- daily status snapshot with business-friendly labels (`Runs completed`, `Runs degraded`, `Odds not actionable yet`, `Signals produced`),
- short `What changed since yesterday` deltas,
- top non-zero reason codes,
- stage duration min/avg/p95,
- watchdog trend delta,
- key operational warnings.

Optional knobs:
- `--top-n`
- `--max-stages`
- `--warning-limit`

## Weekly soak replay (CI/Ops, no manual steps)

Run this deterministic sequence weekly (or after reliability-related changes) to replay contract checks and scenario coverage end-to-end.

```bash
# 1) Deterministic script-level regression pack.
pytest -q scripts/tests/test_compare_run_diagnostics.py \
  scripts/tests/test_compare_run_metrics.py \
  scripts/tests/test_runtime_diagnostics_summary.py \
  scripts/tests/test_runtime_periodic_aggregates.py

# 2) Log-profile parity + compact reason-code guardrails.
scripts/ci_profile_parity_gate.sh

# 3) Rebuild runtime exports from latest artifacts and run triage contracts.
scripts/prepare_runtime_exports.sh --out-dir ./exports ./exports
scripts/triage_runtime_diagnostics_ci.sh

# 4) Emit operator-facing deterministic summary + periodic rollup snapshot.
scripts/runtime_diagnostics_summary.py ./exports
scripts/runtime_periodic_aggregates.py ./exports --snapshot-dir ./docs/baselines/runtime_rollups
```

Expected outcome:
- all commands succeed in CI without manual input,
- run-health degraded contract section is present,
- no new reason-code contract mismatch warnings are introduced.

### Periodic historical rollups (for planning/postmortems)

Generate dated aggregate snapshots (blocker mix, productivity ratio, stage latency trends, daily status labels, and day-over-day deltas) into a dedicated historical folder so raw runtime logs stay separate and do not bloat long-term artifacts:

```bash
scripts/runtime_periodic_aggregates.py ./exports --snapshot-dir ./docs/baselines/runtime_rollups
```

Optional deterministic date labeling (useful for backfills/re-runs):

```bash
scripts/runtime_periodic_aggregates.py ./exports --snapshot-dir ./docs/baselines/runtime_rollups --snapshot-date 2026-03-12
```

Snapshot output pattern:
- `docs/baselines/runtime_rollups/runtime_periodic_rollup_YYYY-MM-DD.json`


3. Interpret output:

- Start with `Run-health degraded contract (first-pass triage)` to validate contract version consistency, blocker totals, dominant blocker categories, sampled blocked records, and stage-skipped reason rollups.
- `Grouped counts` shows frequency per diagnostic key.
- `Top matching rows` shows concrete file/row examples for quick drill-down.
- Prioritize investigation by highest-count key and confirm with row previews.
