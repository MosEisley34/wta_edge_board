# Runtime diagnostics runbook

## Signal stake policy spec (canonical contract)

Use this contract for any runtime that converts internal stake units into emitted recommendation values.

### Config schema (`stake_policy`)

```yaml
stake_policy:
  base_unit_size: 1.0
  account_currency: MXN
  minimum_stake_per_currency:
    MXN: 20
  policy_mode: strict_suppress_below_min # strict_suppress_below_min | round_up_to_min
  precision:
    decimals: 2
    rounding_mode: round_half_up
```

Operational requirements:
- `base_unit_size > 0` and numeric.
- `account_currency` must have a configured `minimum_stake_per_currency` entry (starting baseline `MXN: 20`).
- `policy_mode` must be one of `strict_suppress_below_min` or `round_up_to_min`.
- Deterministic precision is fixed at `2` decimals with `round_half_up`.

### Conversion → validation → emission sequence

1. Convert to account-currency value: `raw_stake = signal_stake_units * base_unit_size`.
2. Apply precision normalization: round to 2 decimals (`round_half_up`).
3. Validate against per-currency minimum:
   - at/above minimum: emit unchanged stake;
   - below minimum + `strict_suppress_below_min`: suppress;
   - below minimum + `round_up_to_min`: emit at configured minimum.
4. Emit stake fields on outbound signals:
   - `recommended_stake`
   - `recommended_stake_currency`
   - `min_stake_applied`
   - `stake_policy_decision_reason`

Reason-code contract:
- `at_or_above_min_emit`
- `below_min_suppressed_strict`
- `below_min_rounded_up`
- `stake_policy_config_error`

### Canonical fixture

`scripts/fixtures/stake_policy_mxn20.json` is the canonical fixture for tests/scripts implementing this policy.

## Stake policy rollout plan (shadow → compare → promote)

Use this phased plan to introduce stake policy safely and with measurable impact tracking.

### Phase 1 — shadow mode (compute only, no enforcement)

Goal: compute policy decisions and projected outcomes without changing runtime outputs.

1. Enable stake-policy diagnostics in non-enforcing mode for a short shadow window (recommended: 1–3 days of representative run volume).
2. For each run in the window, record:
   - `projected_suppressions` (`below_min_suppressed_strict` projections),
   - `projected_round_ups` (`below_min_rounded_up` projections),
   - projected reason-code mix from stake-policy summary.
3. Persist run IDs and window boundaries in the rollout log so later comparisons can be reproduced.

Operational note:
- During shadow mode, runtime emitted signals remain baseline behavior; only the projected policy decisions are tracked.

### Phase 2 — baseline vs policy-on comparison on matched windows

Use existing wrappers/scripts so both sides are preflight-gated and run-ID validated.

```bash
# 1) Mandatory deterministic export + run-id preflight.
scripts/export_parity_precheck.sh --out-dir ./exports_live <baseline_run_id> <candidate_run_id> <live_runtime_dir_or_files>

# 2) Diagnostics compare on prepared exports.
scripts/compare_run_diagnostics_preflight.sh --out-dir ./exports_live <baseline_run_id> <candidate_run_id> <live_runtime_dir_or_files>

# 3) Metrics compare (includes suppression + stake-policy summaries).
scripts/compare_run_metrics_preflight.sh --out-dir ./exports_live <baseline_run_id> <candidate_run_id> <live_runtime_dir_or_files>

# 4) Edge-quality gate comparison on same matched run pair.
python3 scripts/evaluate_edge_quality.py ./exports_live --baseline-run-id <baseline_run_id> --candidate-run-id <candidate_run_id>
```

### Phase 3 — KPI delta tracking (required)

For each matched comparison window, track and archive these deltas:

1. **Actionable signals count** (`actionable_signals`/sent-notification proxy where applicable).
2. **Suppression reason mix** (distribution shift across suppression reason codes, including projected stake-policy reasons).
3. **Coverage gate status** (player-stats/coverage gate pass-fail parity).
4. **Edge-quality status distribution** (`pass`/`fail`/`insufficient_sample` and related gate statuses).

Store KPI outputs in dated baseline artifacts and include exact run IDs used for each delta report.

### Phase 4 — promotion criteria (must be met before enforcement)

Document and enforce explicit criteria in release notes for the rollout window. Recommended minimum criteria:

- No regression in coverage gates versus baseline window.
- Stable parity on diagnostics/metrics contracts (no new contract mismatches).
- Acceptable actionable-volume drop versus baseline (team-defined threshold; must be written before evaluation).
- No new edge-quality gate failures in the recent decision window.

If any criterion fails, keep policy in shadow mode, remediate, and re-run matched-window comparisons.

### Phase 5 — enforce in GS runtime (after approval only)

Only after all promotion criteria pass:

1. Switch GS runtime stake-policy mode from shadow to enforcing.
2. Keep daily/weekly KPI tracking active for at least one stabilization window after cutover.
3. Record cutover date/time, config diff, and first enforcing run IDs in the runbook changelog.

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
alias rl_compare_diag='scripts/compare_run_diagnostics_preflight.sh --out-dir ./exports_live'
alias rl_compare_metrics='scripts/compare_run_metrics_preflight.sh --out-dir ./exports_live'
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
# Run from repository root (fails fast outside a git checkout).
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || true)"
[[ -n "$REPO_ROOT" ]] || { echo "Error: run inside a git checkout." >&2; exit 1; }
cd "$REPO_ROOT"

# 1) Mandatory compare wrapper (runs export parity + run-id precheck first).
scripts/compare_run_diagnostics_preflight.sh --out-dir ./exports_live <run_id_a> <run_id_b> <live_runtime_dir_or_files>

# 2) Optional explicit precheck re-run against the prepared export dir only.
python3 scripts/precheck_run_ids.py <run_id_a> <run_id_b> --export-dir ./exports_live

# 3) Additional compare/gate commands read only from ./exports_live.
python3 scripts/verify_run_log_parity.py --export-dir ./exports_live
scripts/compare_run_metrics_preflight.sh --out-dir ./exports_live <run_id_a> <run_id_b> <live_runtime_dir_or_files>
python3 scripts/evaluate_edge_quality.py ./exports_live --baseline-run-id <run_id_a> --candidate-run-id <run_id_b>
```

Evidence artifact contract (required for every comparison report):
- Attach `./exports_live/run_compare_preflight.json` with each diagnostics/metrics comparison output.
- If this artifact is missing, treat the report as non-compliant and rerun the wrapper.

## Operator SOP: refresh JSON from CSV → preflight → compare

Use this copy/paste SOP when an operator wants a deterministic compare packet from local runtime exports.

```bash
# 1) Mirror canonical JSON artifacts from CSV into the export batch directory.
python3 scripts/mirror_runtime_csv_to_json.py --input-dir ./live_runtime --out-dir ./exports_live

# 2) Build a clean, parity-gated export batch from exports_live (explicit contract dir).
scripts/prepare_runtime_exports.sh --out-dir ./exports_live ./exports_live

# 2b) Capture preflight report with strict failure propagation.
bash -c '
  set -euo pipefail
  scripts/export_parity_precheck.sh --out-dir ./exports_live <run_id_a> <run_id_b> ./exports_live \
    | tee ./exports_live/run_compare_preflight.report.log
  [[ -s ./exports_live/run_compare_preflight.json ]] || {
    echo "Error: missing or empty ./exports_live/run_compare_preflight.json" >&2
    exit 1
  }
'

# 3) Compare diagnostics through mandatory preflight wrapper.
scripts/compare_run_diagnostics_preflight.sh --out-dir ./exports_live <run_id_a> <run_id_b> ./exports_live

# 4) Compare metrics through mandatory preflight wrapper.
scripts/compare_run_metrics_preflight.sh --out-dir ./exports_live <run_id_a> <run_id_b> ./exports_live

# 5) Confirm required evidence artifact exists for incident/report attachment.
test -s ./exports_live/run_compare_preflight.json && echo "preflight evidence present"
```

Emergency override policy:
- `scripts/export_parity_precheck.sh --allow-csv-only-triage` requires `--incident-tag <LETTERS-NNN>`.
- compare scripts may only bypass missing preflight sidecar using `--emergency-preflight-override-tag <LETTERS-NNN>`.

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
- Treat any `recent_window_gate.status_counts.legacy_schema_insufficient_feature_contract > 0` as **NO-GO** and triage as historical-schema incompatibility (not a modern completeness regression).
- Treat any `recent_window_gate.status_counts.insufficient_sample > 0` as **NO-GO** until sample sufficiency is restored.
- Treat `recent_window_gate.pair_count == 0` as **NO-GO** (insufficient evidence).
- Use `full_history_trend` only for diagnostics/postmortem context (not release gating).

Auditability requirement:
- Persist rolling outputs as JSON artifacts (`--rolling-report-out`) every cycle.
- Keep the artifact path/date in the incident/release log so both windows remain reconstructable later.

## Daily production SLO job (rolling 3d/7d pair quality)

Run once per day against `./exports_live/Run_Log.csv`:

```bash
scripts/run_daily_edge_quality_slo.sh
```

Gate contract:
- Windows: last `3` and `7` days (configurable via `--windows`).
- A pair is **decisionable for fail-rate** only when both runs meet minimum activity (`matched_events >= 5` and `scored_signals >= 10`).
- Low-activity or missing-activity pairs are tracked in `excluded_pairs` with run IDs, activity counts, and exclusion reasons.
- A window is **decisionable** only if `decisionable_pair_count >= 10` (configurable via `--min-pairs`).
- Status counts tracked per window remain operational context: `pass`, `fail`, `insufficient_sample`.
- Window fail-rate = `decisionable_status_counts.fail / decisionable_pair_count`.
- **Daily gate verdict = fail** when any decisionable window fail-rate exceeds `0.15` (configurable via `--fail-rate-threshold`).
- `insufficient_sample`/low-signal outcomes should not be treated as hard quality failures; use them for operational triage only.

Artifacts:
- Timestamped full output: `reports/edge_quality_daily_slo_<timestamp>.json`.
- Trend-baseline archive: `docs/baselines/edge_quality_slo/edge_quality_daily_slo_summary.jsonl`.

Example with explicit tuning:

```bash
scripts/run_daily_edge_quality_slo.sh \
  --run-log ./exports_live/Run_Log.csv \
  --windows 3,7 \
  --min-pairs 10 \
  --fail-rate-threshold 0.15
```

## GS suppression quality tuning loop (weekly + 3-day before/after)

Use this workflow to reduce non-actionable scoring/suppression churn while keeping player-stats coverage parity intact.

1. Build weekly GS-focused report from run summaries.
2. Inspect top suppression buckets and split avoidable vs expected.
3. Apply **one** suppression/control change at a time, then compare 3-day rolling windows.
4. Require no regression in player-stats coverage/parity.
5. Append measured impact + run IDs to this runbook changelog.

Command (weekly report + tuning comparison):

```bash
python3 scripts/gs_signal_quality_report.py \
  --input ./exports_live/Run_Log.csv \
  --change-run-id <run_id_where_change_started> \
  --weekly-window-days 7 \
  --rolling-window-days 3 \
  --change-label "signal_suppression_precheck_skip_scoring" \
  --json-out ./docs/baselines/runtime_rollups/gs_signal_quality_<date>.json \
  --markdown-out ./docs/baselines/runtime_rollups/gs_signal_quality_<date>.md
```

Control currently available for unattended runtime suppression churn:

- `SIGNAL_SUPPRESSION_PRECHECK_SKIP_SCORING=true` (default): for `too_close_to_start_skip` and `stale_odds_skip`, skip model scoring/h2h work and mark these as unscored suppressions.

### Suppression tuning changelog

Add one entry per change:

```md
### <change_label>
- Change pivot run_id: `<run_id>`
- Before window run_ids (<n>): <comma-separated run ids>
- After window run_ids (<n>): <comma-separated run ids>
- Suppression total Δ: <+/-n>
- Scored signals Δ: <+/-n>
- Sent notifications Δ: <+/-n>
- Player-stats no-regression gate: PASS|FAIL
```

Latest weekly cycle (2026-03-24):

### cooldown_window_150m
- Change pivot run_id: `test-run`
- Before window run_ids (0): none
- After window run_ids (2): test-run, test-run
- Suppression total Δ: +0
- Scored signals Δ: +0
- Sent notifications Δ: +0
- Player-stats coverage Δ: n/a (no requested players in window)
- Player-stats TA parity Δ: n/a (no resolved players in window)
- Player-stats no-regression gate: PASS
- Weekly top suppression buckets (latest exports): cooldown=0, edge=0


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

# 2b) Preflight reference consistency (docs + wrappers).
python3 scripts/ci_preflight_reference_gate.py

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
