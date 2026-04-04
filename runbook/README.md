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
python3 scripts/evaluate_edge_quality.py ./exports_live \
  --baseline-run-id <baseline_run_id> \
  --candidate-run-id <candidate_run_id> \
  --out-json ./artifacts/compare/<baseline_run_id>_vs_<candidate_run_id>.json
```

Artifact query contract (required):
- Use the resolved compare artifact path printed by `evaluate_edge_quality.py` (or your explicit `--out-json` path) for any downstream `jq`/`rg` inspection.
- Do **not** inspect ephemeral `/tmp` compare outputs unless an incident runbook explicitly requires `/tmp` and records why.

### Stake-policy enabled compare lane (policy-on only)

Use this dedicated lane when the compare set is policy-enabled and you need policy-outcome deltas (suppressed/adjusted/passed + reason-code shifts):

```bash
scripts/compare_run_metrics_preflight.sh --out-dir ./exports_live <baseline_run_id> <candidate_run_id> <live_runtime_dir_or_files> -- --stake-policy-enabled
python3 scripts/evaluate_edge_quality.py ./exports_live \
  --baseline-run-id <baseline_run_id> \
  --candidate-run-id <candidate_run_id> \
  --stake-policy-enabled
```

Operator contract:
- Compare reports are tagged with `stake_policy_enabled=true|false`.
- A compare set that mixes `stake_policy_enabled=true` and `stake_policy_enabled=false` run summaries is invalid and must be split into homogeneous lanes before reporting deltas.
- Stake mode is determined by American odds sign:
  - `+odds` → `to_risk` (stake is interpreted directly as risk amount before min-policy checks).
  - `-odds` → `to_win` (stake is interpreted as target win and converted to risk before min-policy checks).
- Unit/floor/bucket handling is deterministic for this lane:
  - **Unit rule:** `raw_stake = signal_stake_units * base_unit_size`.
  - **Minimum rule:** compare normalized `raw_stake` to `minimum_stake_per_currency[account_currency]` (baseline MXN floor is 20).
  - **Bucket rule:** classify each row into `passed` (`at_or_above_min_emit`), `adjusted` (`below_min_rounded_up`), or `suppressed` (`below_min_suppressed_strict`) and track deltas bucket-by-bucket in compare reports.
- Examples (base unit 10 MXN, min 20 MXN):
  - `+120`, `signal_stake_units=1.5` ⇒ `to_risk`, `raw_stake=15` ⇒ strict mode: `suppressed`; round-up mode: `adjusted_to_20`.
  - `-150`, `signal_stake_units=1.0` ⇒ `to_win`, target win `10`, implied risk `15` ⇒ strict mode: `suppressed`; round-up mode: `adjusted_to_20`.
  - `+110`, `signal_stake_units=3.0` ⇒ `to_risk`, `raw_stake=30` ⇒ `passed`.

### Phase 3 — KPI delta tracking (required)

For each matched comparison window, track and archive these deltas:

1. **Actionable signals count** (`actionable_signals`/sent-notification proxy where applicable).
2. **Suppression reason mix** (distribution shift across suppression reason codes, including projected stake-policy reasons).
3. **Coverage gate status** (player-stats/coverage gate pass-fail parity).
4. **Edge-quality status distribution** (`pass`/`fail`/`insufficient_sample` and related gate statuses).

Store KPI outputs in dated baseline artifacts and include exact run IDs used for each delta report.

### Phase 4 — promotion criteria (must be met before enforcement)

Document and enforce explicit criteria in release notes for the rollout window. Policy-on promotion requires **all** of the following:

1. **Signal-quality gate stability**
   - `evaluate_edge_quality.py --stake-policy-enabled` returns `status=pass` (or approved `insufficient_sample` fallback support) across the matched decision window.
   - No new `fail` statuses are introduced versus the policy-disabled baseline lane.
2. **Suppression distribution stability**
   - No material new drift in suppression reason distribution versus baseline (use configured `max_suppression_drift` / `suppression_min_volume` guardrails).
   - Stake-policy reason-code distribution is stable across matched windows (no single new reason dominates unexpectedly without incident review).
   - Interpret `windowed_fallback_result.pairs[].failure_diagnostics.suppression_drift.failing_reasons` before escalating:
     - Treat as a likely **policy regression** when drift repeats across neighboring pairs and the same event IDs persist while minutes-to-start and odds freshness snapshots remain comparable.
     - Treat as a likely **time-window / market-state artifact** when top contributing events are mostly new rotations, minutes-to-start skews toward kickoff windows (`too_close_to_start_skip`), or odds freshness metadata shifts toward stale bursts (`stale_odds_skip`) in only one side of the pair.
     - Use the deterministic `top_contributing_events` sample as first-pass evidence; only rehydrate raw logs when this sample is insufficient for incident classification.
3. **Stake-policy outcome sanity**
   - `suppressed_count`, `adjusted_count`, and `passed_count` deltas are directionally consistent with rollout intent and within predeclared tolerances.
   - Coverage-gate pass/fail parity is maintained versus baseline.
4. **Diagnostics contract parity**
   - No new contract mismatches in diagnostics/metrics wrappers and preflight evidence remains valid.

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

## Standard triage bundle (only supported daily matrix operator path)

Use this wrapper for **all daily matrix generation** and run-to-run diagnostics triage. No other manual command sequence is supported for operators.

```bash
scripts/run_triage_bundle.sh [--out-dir ./exports_live] [--baseline-run-id <run_id>] [--candidate-run-id <run_id>] <file-or-directory> [more paths...]
```

Strict enforced execution order:
0. **runtime export pre-step** (`scripts/prepare_runtime_exports.sh`),
1. **derive run pair** (manual `--baseline-run-id/--candidate-run-id` override or automatic latest pair derivation from exported `Run_Log.json`),
2. **precheck** (`scripts/precheck_run_ids.py --require-gate-prereqs`),
3. **compare diagnostics** (`scripts/compare_run_diagnostics.py`),
4. **edge quality evaluation** (`scripts/evaluate_edge_quality.py`),
5. **summary output** (machine-readable JSON).

Hard stage gates (fail-fast):
- Stop when run IDs are missing/empty.
- Stop when `precheck_stage.json` is missing/empty or precheck exits non-zero.
- Stop when `compare_validation.json` is missing/empty or compare validation exits non-zero.
- Stop when `edge_quality_compare.json` is missing/empty, fails JSON parsing, or is missing required keys: `status`, `gate_verdict`, `reason_code`.

Machine-readable artifacts (per execution):
- `triage_impl_<UTC timestamp>/out/derive_run_pair.json`
- `triage_impl_<UTC timestamp>/out/precheck_stage.json`
- `triage_impl_<UTC timestamp>/out/compare_validation.json`
- `triage_impl_<UTC timestamp>/out/edge_quality_compare.json`
- `triage_impl_<UTC timestamp>/out/triage_summary.json`

Directory lifecycle note:
- `triage_impl_<UTC timestamp>/` is created only after the runtime export pre-step completes, so export workspace refresh cannot delete stage artifacts in-flight.

The final summary JSON includes:
- `status`
- `reason_code`
- `run_ids` (`candidate_run_id`, `baseline_run_id`)
- per-stage `gate_outcomes` (`run_pair_selection`, `precheck`, `compare_validation`, `edge_quality`) with status, reason code, and artifact path
- artifact paths and required-JSON presence flags

Operational note:
- Keep `--out-dir` outside long-lived tracked branches when possible (for example `/tmp/wta_edge_board_triage_exports`) to avoid accidental artifact commits.

Optional shell muscle-memory helper (ops dotfiles):

```bash
alias rl_precheck='scripts/export_parity_precheck.sh --out-dir ./exports_live'
alias rl_compare_diag='scripts/compare_run_diagnostics_preflight.sh --out-dir ./exports_live'
alias rl_compare_metrics='scripts/compare_run_metrics_preflight.sh --out-dir ./exports_live'
```


## Quick operational checks (copy/paste)

Use the supported wrapper path when validating trigger health and generating the daily matrix packet:

```bash
# Single supported operator command:
scripts/run_triage_bundle.sh --out-dir ./exports_live <file-or-directory>
```

Operational expectation:
- `scripts/run_triage_bundle.sh` must finish with `status=pass` and emit `triage_impl_*/out/triage_summary.json`.
- The summary JSON must include non-empty `baseline_run_id` and `candidate_run_id`.
- Daily matrix generation is blocked unless `triage_impl_*/out/edge_quality_compare.json` exists and parses with required keys `status`, `gate_verdict`, `reason_code`.
- If any stage gate fails, treat outputs as invalid and rerun only after remediation.

## Compare/gate command checklist (run IDs required)

Use this sequence for all run-to-run parity/quality workflows:

```bash
# Run from repository root (fails fast outside a git checkout).
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || true)"
[[ -n "$REPO_ROOT" ]] || { echo "Error: run inside a git checkout." >&2; exit 1; }
cd "$REPO_ROOT"

# 1) Mandatory compare wrapper (runs export parity + run-id precheck first).
bash -c '
  set -euo pipefail
  scripts/compare_run_diagnostics_preflight.sh --out-dir ./exports_live <run_id_a> <run_id_b> <live_runtime_dir_or_files> \
    | tee ./exports_live/compare_run_diagnostics.report.log
'

# 2) Optional explicit precheck re-run against the prepared export dir only.
python3 scripts/precheck_run_ids.py <run_id_a> <run_id_b> --export-dir ./exports_live

# 3) Additional compare/gate commands read only from ./exports_live.
python3 scripts/verify_run_log_parity.py --export-dir ./exports_live
bash -c '
  set -euo pipefail
  scripts/compare_run_metrics_preflight.sh --out-dir ./exports_live <run_id_a> <run_id_b> <live_runtime_dir_or_files> \
    | tee ./exports_live/compare_run_metrics.report.log
'
python3 scripts/evaluate_edge_quality.py ./exports_live --baseline-run-id <run_id_a> --candidate-run-id <run_id_b>
```

Evidence artifact contract (required for every comparison report):
- Attach `./exports_live/run_compare_preflight.json` with each diagnostics/metrics comparison output.
- A report is only **successful/valid** when `./exports_live/run_compare_preflight.json` exists and is non-empty.
- If the preflight sidecar is missing, mark compare artifacts invalid and rerun the wrapper before sharing results.

## Operator SOP: refresh JSON from CSV → preflight → compare

Use this copy/paste SOP when an operator wants a deterministic compare packet from local runtime exports.

```bash
# Mandatory run-scoped report hygiene (prevents stale-artifact false positives).
RUN_A="<run_id_a>"
RUN_B="<run_id_b>"
RUN_REPORT_DIR="./reports/prepost_${RUN_A}_vs_${RUN_B}"
RUN_START_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
RUN_START_MARKER="${RUN_REPORT_DIR}/.run_start_${RUN_START_UTC}"

# 0) Clear old run-scoped artifacts before preflight/compare/eval.
rm -rf "$RUN_REPORT_DIR"
mkdir -p "$RUN_REPORT_DIR"
touch "$RUN_START_MARKER"

# 1) Lock one source path and reuse it for mirror + precheck + compare wrappers.
# Operators usually point at a timestamped runtime export batch path like:
#   ./live_runtime/batches/2026-03-24T09-15-00Z
EXPORT_SRC="./live_runtime/batches/2026-03-24T09-15-00Z"
[[ -e "$EXPORT_SRC" ]] || {
  echo "Error: fresh export source missing: $EXPORT_SRC" >&2
  echo "Hint: regenerate or locate the latest live_runtime batch before precheck." >&2
  exit 1
}

# 1a) Mirror canonical JSON artifacts from CSV in the SAME locked source directory.
python3 scripts/mirror_runtime_csv_to_json.py --input-dir "$EXPORT_SRC" --out-dir "$EXPORT_SRC"

# 2) Build a clean, parity-gated export batch from exports_live (explicit contract dir).
scripts/prepare_runtime_exports.sh --out-dir ./exports_live ./exports_live

# WARNING: Mirroring into ./exports_live does NOT repair stale JSON in ./live_runtime.
# If precheck reads ./live_runtime (or $EXPORT_SRC under it), mirror there first.

# 2a.1) Resolve + lock export source run IDs before any compare/evaluate command.
# This MUST pass before invoking:
#   - scripts/compare_run_diagnostics_preflight.sh
#   - scripts/compare_run_metrics_preflight.sh
#   - python3 scripts/evaluate_edge_quality.py
NEW_RUN_ID="<run_id_b>"
export NEW_RUN_ID
PRE_RUN_ID="$(python3 - "$EXPORT_SRC" "$NEW_RUN_ID" <<'PY'
import csv
import sys
from pathlib import Path

export_path = Path(sys.argv[1])
candidate_run_id = sys.argv[2]
run_log_csv = export_path / "Run_Log.csv"
if not run_log_csv.exists():
    print("")
    raise SystemExit(0)

rows = []
with run_log_csv.open("r", encoding="utf-8-sig", newline="") as handle:
    reader = csv.DictReader(handle)
    for idx, row in enumerate(reader):
        run_id = (row.get("run_id") or "").strip()
        timestamp = (row.get("run_utc") or row.get("timestamp_utc") or "").strip()
        if run_id:
            rows.append((idx, run_id, timestamp))

ordered_ids = []
seen = set()
for _idx, run_id, _ts in sorted(rows, key=lambda item: ((item[2] or ""), item[0])):
    if run_id in seen:
        continue
    seen.add(run_id)
    ordered_ids.append(run_id)

try:
    candidate_index = ordered_ids.index(candidate_run_id)
except ValueError:
    print("")
    raise SystemExit(0)

if candidate_index == 0:
    print("")
    raise SystemExit(0)

print(ordered_ids[candidate_index - 1])
PY
)"
export PRE_RUN_ID NEW_RUN_ID
python3 - <<'PY'
import json
import os
import sys

baseline_run_id = (os.environ.get("PRE_RUN_ID") or "").strip()
candidate_run_id = (os.environ.get("NEW_RUN_ID") or "").strip()
if baseline_run_id and candidate_run_id:
    raise SystemExit(0)

print(json.dumps({
    "status": "error",
    "code": "RUN_IDS_MISSING",
    "message": "Baseline/candidate run IDs must both be non-empty before compare/evaluate.",
    "baseline_run_id": baseline_run_id,
    "candidate_run_id": candidate_run_id,
}, sort_keys=True))
raise SystemExit(1)
PY

python3 scripts/precheck_run_ids.py "$PRE_RUN_ID" "$NEW_RUN_ID" --export-dir "$EXPORT_SRC" --recent-limit 10

scripts/export_parity_precheck.sh --out-dir ./exports_live "$PRE_RUN_ID" "$NEW_RUN_ID" "$EXPORT_SRC"

# Stale-source troubleshooting fallback (existing exports_live contract flow).
# If the fresh source path is stale/unavailable, rerun precheck from ./exports_live.
scripts/export_parity_precheck.sh --out-dir ./exports_live <run_id_a> <run_id_b> ./exports_live

# Parity mismatch likely due stale JSON (explicit triage snippet).
# Symptom: precheck/parity says CSV↔JSON mismatch while run IDs are present in CSV.
# Cause: mirror ran against ./exports_live, but precheck/compare still read $EXPORT_SRC (live_runtime path).
python3 scripts/mirror_runtime_csv_to_json.py --input-dir "$EXPORT_SRC" --out-dir "$EXPORT_SRC"
scripts/export_parity_precheck.sh --out-dir ./exports_live <run_id_a> <run_id_b> "$EXPORT_SRC"

# 2b) Capture preflight report with strict failure propagation.
bash -c '
  set -euo pipefail
  scripts/export_parity_precheck.sh --out-dir ./exports_live "$PRE_RUN_ID" "$NEW_RUN_ID" "$EXPORT_SRC" \
    | tee "'"$RUN_REPORT_DIR"'/run_compare_preflight.report.log"
  exit_code=${PIPESTATUS[0]} # zsh: exit_code=${pipestatus[1]}
  if [[ "$exit_code" -ne 0 ]]; then
    exit "$exit_code"
  fi
  [[ -s ./exports_live/run_compare_preflight.json ]] || {
    echo "Error: missing or empty ./exports_live/run_compare_preflight.json" >&2
    exit 1
  }
  [[ ./exports_live/run_compare_preflight.json -nt "'"$RUN_START_MARKER"'" ]] || {
    echo "Error: stale preflight evidence (not created in current run)." >&2
    exit 1
  }
  [[ "'"$RUN_REPORT_DIR"'/run_compare_preflight.report.log" -nt "'"$RUN_START_MARKER"'" ]] || {
    echo "Error: stale preflight report log (not created in current run)." >&2
    exit 1
  }
'

# 3) Compare diagnostics through mandatory preflight wrapper (source-locked via $EXPORT_SRC).
bash -c '
  set -euo pipefail
  scripts/compare_run_diagnostics_preflight.sh --out-dir ./exports_live "$PRE_RUN_ID" "$NEW_RUN_ID" "$EXPORT_SRC" \
    | tee "'"$RUN_REPORT_DIR"'/compare_run_diagnostics.report.log"
  exit_code=${PIPESTATUS[0]} # zsh: exit_code=${pipestatus[1]}
  if [[ "$exit_code" -ne 0 ]]; then
    exit "$exit_code"
  fi
  [[ "'"$RUN_REPORT_DIR"'/compare_run_diagnostics.report.log" -nt "'"$RUN_START_MARKER"'" ]] || {
    echo "Error: stale diagnostics report log (not created in current run)." >&2
    exit 1
  }
'

# 4) Compare metrics through mandatory preflight wrapper (source-locked via $EXPORT_SRC).
bash -c '
  set -euo pipefail
  scripts/compare_run_metrics_preflight.sh --out-dir ./exports_live "$PRE_RUN_ID" "$NEW_RUN_ID" "$EXPORT_SRC" \
    | tee "'"$RUN_REPORT_DIR"'/compare_run_metrics.report.log"
  exit_code=${PIPESTATUS[0]} # zsh: exit_code=${pipestatus[1]}
  if [[ "$exit_code" -ne 0 ]]; then
    exit "$exit_code"
  fi
  [[ "'"$RUN_REPORT_DIR"'/compare_run_metrics.report.log" -nt "'"$RUN_START_MARKER"'" ]] || {
    echo "Error: stale metrics report log (not created in current run)." >&2
    exit 1
  }
'

# 5) Evaluate edge quality only after the same source lock/precheck has succeeded.
SAFE_PRE_RUN_ID="$(printf '%s' "$PRE_RUN_ID" | sed -E 's/[^A-Za-z0-9._-]+/_/g; s/^[._-]+//; s/[._-]+$//')"
SAFE_NEW_RUN_ID="$(printf '%s' "$NEW_RUN_ID" | sed -E 's/[^A-Za-z0-9._-]+/_/g; s/^[._-]+//; s/[._-]+$//')"
[[ -n "$SAFE_PRE_RUN_ID" ]] || SAFE_PRE_RUN_ID="unknown_baseline"
[[ -n "$SAFE_NEW_RUN_ID" ]] || SAFE_NEW_RUN_ID="unknown_candidate"
EDGE_COMPARE_ARTIFACT="./artifacts/compare/${SAFE_PRE_RUN_ID}_vs_${SAFE_NEW_RUN_ID}.json"
python3 scripts/evaluate_edge_quality.py ./exports_live --baseline-run-id "$PRE_RUN_ID" --candidate-run-id "$NEW_RUN_ID" \
  --out-json "$EDGE_COMPARE_ARTIFACT" \
  | tee "$RUN_REPORT_DIR/evaluate_edge_quality.report.log"

[[ -s "$EDGE_COMPARE_ARTIFACT" ]] || {
  echo "Error: missing edge compare artifact at $EDGE_COMPARE_ARTIFACT" >&2
  echo "Hint: regenerate via: python3 scripts/evaluate_edge_quality.py ./exports_live --baseline-run-id \"$PRE_RUN_ID\" --candidate-run-id \"$NEW_RUN_ID\" --out-json \"$EDGE_COMPARE_ARTIFACT\"" >&2
  exit 1
}
python3 - "$EDGE_COMPARE_ARTIFACT" <<'PY'
import json
import sys
path = sys.argv[1]
with open(path, "r", encoding="utf-8") as handle:
    payload = json.load(handle)
required = ("status", "gate_verdict", "reason_code")
missing = [k for k in required if k not in payload]
if missing:
    raise SystemExit(f"Error: edge compare artifact missing required keys: {', '.join(missing)}")
PY

# 6) Strict post-run evidence/report validation (copy/paste as-is).
bash -c '
  set -euo pipefail

  # Fresh/non-empty evidence and report logs.
  test -s ./exports_live/run_compare_preflight.json
  test ./exports_live/run_compare_preflight.json -nt "'"$RUN_START_MARKER"'"
  test -s "'"$RUN_REPORT_DIR"'/run_compare_preflight.report.log"
  test -s "'"$RUN_REPORT_DIR"'/compare_run_diagnostics.report.log"
  test -s "'"$RUN_REPORT_DIR"'/compare_run_metrics.report.log"
  test -s "'"$RUN_REPORT_DIR"'/evaluate_edge_quality.report.log"
  test -s "'"$EDGE_COMPARE_ARTIFACT"'"
  test "'"$RUN_REPORT_DIR"'/run_compare_preflight.report.log" -nt "'"$RUN_START_MARKER"'"
  test "'"$RUN_REPORT_DIR"'/compare_run_diagnostics.report.log" -nt "'"$RUN_START_MARKER"'"
  test "'"$RUN_REPORT_DIR"'/compare_run_metrics.report.log" -nt "'"$RUN_START_MARKER"'"
  test "'"$RUN_REPORT_DIR"'/evaluate_edge_quality.report.log" -nt "'"$RUN_START_MARKER"'"

  # Semantic success markers (must exist).
  rg -F "Precheck passed: both target run IDs are present" "'"$RUN_REPORT_DIR"'/run_compare_preflight.report.log"
  rg -F "Fail-fast preflight complete. Safe to run compare/gate commands." "'"$RUN_REPORT_DIR"'/compare_run_diagnostics.report.log"
  rg -F "Fail-fast preflight complete. Safe to run compare/gate commands." "'"$RUN_REPORT_DIR"'/compare_run_metrics.report.log"

  # Hard-fail markers (must NOT exist).
  ! rg -F "Precheck failed" "'"$RUN_REPORT_DIR"'/run_compare_preflight.report.log" "'"$RUN_REPORT_DIR"'/compare_run_diagnostics.report.log" "'"$RUN_REPORT_DIR"'/compare_run_metrics.report.log"
  ! rg -F "Run log parity check failed" "'"$RUN_REPORT_DIR"'/run_compare_preflight.report.log" "'"$RUN_REPORT_DIR"'/compare_run_diagnostics.report.log" "'"$RUN_REPORT_DIR"'/compare_run_metrics.report.log"
'
echo "preflight evidence and strict semantic report validation passed"
```

### Operator checklist: GS refresh (before any compare or gate run)

Use this quick checklist whenever you refresh GS-side runtime artifacts:

1. Refresh CSV exports from the latest GS/runtime batch path.
2. Mirror CSV → JSON in that same source path:
   - `python3 scripts/mirror_runtime_csv_to_json.py --input-dir "$EXPORT_SRC" --out-dir "$EXPORT_SRC"`
3. Verify newest run ID is present in both `Run_Log.csv` and `Run_Log.json`:
   - `python3 scripts/precheck_run_ids.py <newest_run_id> <anchor_run_id> --export-dir "$EXPORT_SRC" --recent-limit 10`
4. Compare newest run ID against an anchor run ID using the preflight wrapper before diagnostics/metrics/eval:
   - `scripts/export_parity_precheck.sh --out-dir ./exports_live <anchor_run_id> <newest_run_id> "$EXPORT_SRC"`
5. Only run compare/evaluate commands after the anchor-vs-newest preflight succeeds.

Failure-handling notes:
- **Missing run IDs:** stop immediately; do not run compare/evaluate. Re-export fresh CSVs, re-run CSV→JSON mirror, then repeat precheck.
- **Identical PREV/POSTV run IDs:** treat as invalid compare input (no delta window). Select distinct run IDs (older anchor + newest candidate) and rerun precheck.
- **Insufficient sample in edge-quality gate:** use window fallback instead of forcing pass/fail:
  - rerun `evaluate_edge_quality.py` on a larger matched window (for example, include additional recent runs),
  - document fallback scope + run IDs in artifacts/changelog,
  - keep rollout decision in hold state until sufficiency is restored.

Compatibility note:
- Pipeline exit capture uses `exit_code` consistently. In bash use `${PIPESTATUS[0]}`; in zsh use `${pipestatus[1]}`.
- Compare outputs are invalid when `run_compare_preflight.json` is missing for the export batch that produced the report.
- Run-scoped report hygiene is mandatory: clear `reports/prepost_<runA>_vs_<runB>/` before each attempt, recreate it, and accept evidence only when required files are newer than the run-start marker in that directory.

Emergency override policy:
- `scripts/export_parity_precheck.sh --allow-csv-only-triage` requires `--incident-tag <LETTERS-NNN>`.
- compare scripts may only bypass missing preflight sidecar using `--emergency-preflight-override-tag <LETTERS-NNN>`.
- `--player-stats-gate-override-reason` only overrides coverage-threshold failures; schema-integrity failures remain blocking (`schema_missing`).

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
