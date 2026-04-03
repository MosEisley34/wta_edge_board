#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [[ -z "$repo_root" ]]; then
  echo "Error: this command must be run inside a git checkout." >&2
  exit 1
fi
cd "$repo_root"

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_triage_bundle.sh [--out-dir <dir>] [--baseline-run-id <run_id>] [--candidate-run-id <run_id>] <file-or-directory> [more paths...]

Supported operator flow (strict order):
  0) runtime export workspace setup
  1) derive run pair
  2) precheck
  3) compare diagnostics
  4) edge quality evaluation
  5) summary output

Stage gates:
  - stop if run IDs are missing/empty
  - stop if compare validation fails
  - stop if required JSON artifacts are missing

Outputs:
  - triage implementation directory: <out-dir>/triage_impl_<timestamp>/
  - machine-readable final summary: <out-dir>/triage_impl_<timestamp>/out/triage_bundle_summary.json

Notes:
  - This script is the only supported operator path for daily matrix generation.
USAGE
}

out_dir="./exports_live"
manual_baseline_run_id=""
manual_candidate_run_id=""
inputs=()

while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --out-dir)
      shift
      [[ "$#" -gt 0 ]] || { echo "Error: --out-dir requires a value." >&2; usage >&2; exit 1; }
      out_dir="$1"
      ;;
    --baseline-run-id)
      shift
      [[ "$#" -gt 0 ]] || { echo "Error: --baseline-run-id requires a value." >&2; usage >&2; exit 1; }
      manual_baseline_run_id="$1"
      ;;
    --candidate-run-id)
      shift
      [[ "$#" -gt 0 ]] || { echo "Error: --candidate-run-id requires a value." >&2; usage >&2; exit 1; }
      manual_candidate_run_id="$1"
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      inputs+=("$1")
      ;;
  esac
  shift
done

if [[ "${#inputs[@]}" -eq 0 ]]; then
  echo "Error: missing live runtime input path(s)." >&2
  usage >&2
  exit 1
fi

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
triage_impl_dir="${out_dir}/triage_impl_${timestamp}"
out_stage_dir="${triage_impl_dir}/out"
mkdir -p "$out_stage_dir"

derive_pair_json="$out_stage_dir/derive_run_pair.json"
precheck_json="$out_stage_dir/precheck_stage.json"
compare_json="$out_stage_dir/compare_validation.json"
edge_json="$out_stage_dir/edge_quality_compare.json"
summary_json="$out_stage_dir/triage_bundle_summary.json"

baseline_run_id=""
candidate_run_id=""
status="fail"
reason_code="UNINITIALIZED"

write_summary() {
  python3 - "$summary_json" "$status" "$reason_code" "$candidate_run_id" "$baseline_run_id" "$derive_pair_json" "$precheck_json" "$compare_json" "$edge_json" "$triage_impl_dir" "$out_dir" <<'PY'
import json
import os
import sys

(
    summary_path,
    status,
    reason_code,
    candidate_run_id,
    baseline_run_id,
    derive_pair_json,
    precheck_json,
    compare_json,
    edge_json,
    triage_impl_dir,
    export_dir,
) = sys.argv[1:]

payload = {
    "status": status,
    "reason_code": reason_code,
    "candidate_run_id": candidate_run_id or None,
    "baseline_run_id": baseline_run_id or None,
    "artifact_paths": {
        "derive_run_pair_json": derive_pair_json,
        "precheck_stage_json": precheck_json,
        "compare_validation_json": compare_json,
        "edge_quality_compare_json": edge_json,
        "triage_impl_dir": triage_impl_dir,
        "export_dir": export_dir,
    },
    "required_json_exists": {
        "derive_run_pair_json": os.path.isfile(derive_pair_json) and os.path.getsize(derive_pair_json) > 0,
        "precheck_stage_json": os.path.isfile(precheck_json) and os.path.getsize(precheck_json) > 0,
        "compare_validation_json": os.path.isfile(compare_json) and os.path.getsize(compare_json) > 0,
        "edge_quality_compare_json": os.path.isfile(edge_json) and os.path.getsize(edge_json) > 0,
    },
}
os.makedirs(os.path.dirname(summary_path), exist_ok=True)
with open(summary_path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2, sort_keys=True)
print(json.dumps(payload, sort_keys=True))
PY
}

fail_and_exit() {
  reason_code="$1"
  status="fail"
  echo "[triage-bundle] FAIL reason_code=${reason_code}" >&2
  write_summary
  exit 1
}

ensure_required_json() {
  local path="$1"
  local missing_reason="$2"
  if [[ ! -s "$path" ]]; then
    echo "Error: required JSON artifact missing or empty: $path" >&2
    fail_and_exit "$missing_reason"
  fi
}

echo "[0/5] Preparing runtime exports into ${out_dir}"
scripts/prepare_runtime_exports.sh --out-dir "$out_dir" "${inputs[@]}"

echo "[1/5] Deriving run pair"
if [[ -n "${manual_baseline_run_id//[[:space:]]/}" || -n "${manual_candidate_run_id//[[:space:]]/}" ]]; then
  if [[ -z "${manual_baseline_run_id//[[:space:]]/}" || -z "${manual_candidate_run_id//[[:space:]]/}" ]]; then
    fail_and_exit "RUN_IDS_MISSING"
  fi
  baseline_run_id="$manual_baseline_run_id"
  candidate_run_id="$manual_candidate_run_id"
  python3 - "$derive_pair_json" "$baseline_run_id" "$candidate_run_id" <<'PY'
import json
import sys
out, baseline, candidate = sys.argv[1:]
with open(out, "w", encoding="utf-8") as handle:
    json.dump({
        "status": "ok",
        "reason_code": "RUN_PAIR_MANUAL_OVERRIDE",
        "baseline_run_id": baseline,
        "candidate_run_id": candidate,
        "source": "manual_flags",
    }, handle, indent=2, sort_keys=True)
PY
else
  set +e
  python3 - "$out_dir" "$derive_pair_json" <<'PY'
import json
import os
import sys
from datetime import datetime

export_dir, out_path = sys.argv[1:]
run_log_json = os.path.join(export_dir, "Run_Log.json")
if not os.path.isfile(run_log_json):
    with open(out_path, "w", encoding="utf-8") as handle:
        json.dump({"status": "fail", "reason_code": "RUN_IDS_MISSING", "error": f"missing {run_log_json}"}, handle, indent=2, sort_keys=True)
    raise SystemExit(2)
with open(run_log_json, "r", encoding="utf-8") as handle:
    payload = json.load(handle)
rows = payload if isinstance(payload, list) else []

summaries = []
for row in rows:
    if not isinstance(row, dict):
        continue
    if str(row.get("stage") or "").strip() != "runEdgeBoard":
        continue
    run_id = str(row.get("run_id") or "").strip()
    ended_at = str(row.get("ended_at") or "").strip()
    if run_id and ended_at:
        summaries.append((run_id, ended_at))

if not summaries:
    with open(out_path, "w", encoding="utf-8") as handle:
        json.dump({"status": "fail", "reason_code": "RUN_IDS_MISSING", "error": "no runEdgeBoard summary rows with ended_at found"}, handle, indent=2, sort_keys=True)
    raise SystemExit(2)

def parse_ts(value: str):
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.min

summaries.sort(key=lambda item: parse_ts(item[1]), reverse=True)
ordered = []
seen = set()
for run_id, ended_at in summaries:
    if run_id in seen:
        continue
    seen.add(run_id)
    ordered.append({"run_id": run_id, "ended_at": ended_at})
if len(ordered) < 2:
    with open(out_path, "w", encoding="utf-8") as handle:
        json.dump({"status": "fail", "reason_code": "RUN_IDS_MISSING", "error": "fewer than two unique run IDs available"}, handle, indent=2, sort_keys=True)
    raise SystemExit(2)

with open(out_path, "w", encoding="utf-8") as handle:
    json.dump({
        "status": "ok",
        "reason_code": "RUN_PAIR_DERIVED",
        "baseline_run_id": ordered[1]["run_id"],
        "candidate_run_id": ordered[0]["run_id"],
        "source": run_log_json,
        "candidates_considered": ordered[:6],
    }, handle, indent=2, sort_keys=True)
PY
  derive_exit=$?
  set -e
  if [[ "$derive_exit" -ne 0 ]]; then
    fail_and_exit "RUN_IDS_MISSING"
  fi
  baseline_run_id="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["baseline_run_id"])' "$derive_pair_json")"
  candidate_run_id="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["candidate_run_id"])' "$derive_pair_json")"
fi
ensure_required_json "$derive_pair_json" "REQUIRED_JSON_MISSING"
if [[ -z "${baseline_run_id//[[:space:]]/}" || -z "${candidate_run_id//[[:space:]]/}" ]]; then
  fail_and_exit "RUN_IDS_MISSING"
fi

echo "[2/5] Running precheck"
set +e
python3 scripts/precheck_run_ids.py "$baseline_run_id" "$candidate_run_id" --export-dir "$out_dir" --require-gate-prereqs > "$triage_impl_dir/precheck_stdout.log" 2>&1
precheck_exit=$?
set -e
python3 - "$precheck_json" "$precheck_exit" "$baseline_run_id" "$candidate_run_id" "$triage_impl_dir/precheck_stdout.log" <<'PY'
import json
import sys
out, exit_code, baseline, candidate, log_path = sys.argv[1:]
exit_code = int(exit_code)
with open(out, "w", encoding="utf-8") as handle:
    json.dump({
        "status": "ok" if exit_code == 0 else "fail",
        "reason_code": "PRECHECK_PASSED" if exit_code == 0 else "PRECHECK_FAILED",
        "exit_code": exit_code,
        "baseline_run_id": baseline,
        "candidate_run_id": candidate,
        "stdout_log": log_path,
    }, handle, indent=2, sort_keys=True)
PY
ensure_required_json "$precheck_json" "REQUIRED_JSON_MISSING"
if [[ "$precheck_exit" -ne 0 ]]; then
  cat "$triage_impl_dir/precheck_stdout.log" >&2 || true
  fail_and_exit "PRECHECK_FAILED"
fi

echo "[3/5] Comparing diagnostics"
set +e
python3 scripts/compare_run_diagnostics.py "$baseline_run_id" "$candidate_run_id" --export-dir "$out_dir" > "$triage_impl_dir/compare_stdout.log" 2>&1
compare_exit=$?
set -e
python3 - "$compare_json" "$compare_exit" "$baseline_run_id" "$candidate_run_id" "$triage_impl_dir/compare_stdout.log" <<'PY'
import json
import sys
out, exit_code, baseline, candidate, log_path = sys.argv[1:]
exit_code = int(exit_code)
with open(out, "w", encoding="utf-8") as handle:
    json.dump({
        "status": "ok" if exit_code == 0 else "fail",
        "reason_code": "COMPARE_VALIDATION_PASSED" if exit_code == 0 else "COMPARE_VALIDATION_FAILED",
        "exit_code": exit_code,
        "baseline_run_id": baseline,
        "candidate_run_id": candidate,
        "stdout_log": log_path,
    }, handle, indent=2, sort_keys=True)
PY
ensure_required_json "$compare_json" "REQUIRED_JSON_MISSING"
if [[ "$compare_exit" -ne 0 ]]; then
  cat "$triage_impl_dir/compare_stdout.log" >&2 || true
  fail_and_exit "COMPARE_VALIDATION_FAILED"
fi

echo "[4/5] Evaluating edge quality"
python3 scripts/evaluate_edge_quality.py "$out_dir" --baseline-run-id "$baseline_run_id" --candidate-run-id "$candidate_run_id" --out-json "$edge_json" > "$triage_impl_dir/edge_quality_stdout.log" 2>&1 || {
  cat "$triage_impl_dir/edge_quality_stdout.log" >&2 || true
  fail_and_exit "EDGE_QUALITY_EVAL_FAILED"
}
ensure_required_json "$edge_json" "REQUIRED_JSON_MISSING"

echo "[5/5] Writing summary"
status="pass"
reason_code="TRIAGE_BUNDLE_OK"
write_summary

echo "[triage-bundle] Summary JSON: $summary_json"
