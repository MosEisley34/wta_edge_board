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
  1) run-pair selection
  2) precheck
  3) compare diagnostics
  4) edge quality evaluation
  5) summary JSON emission

Hard stage gates:
  - stop if run IDs are missing/empty
  - stop if precheck JSON is missing/invalid or precheck exits non-zero
  - stop if compare validation JSON is missing/invalid or compare exits non-zero
  - stop if edge quality JSON is missing/invalid

Outputs:
  - triage implementation directory: <out-dir>/triage_impl_<timestamp>/
  - machine-readable final summary: <out-dir>/triage_impl_<timestamp>/out/triage_summary.json

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
    -*)
      echo "Error: unsupported option $1" >&2
      usage >&2
      exit 1
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
triage_impl_dir=""
out_stage_dir=""
derive_pair_json=""
precheck_json=""
compare_json=""
edge_json=""
summary_json=""

baseline_run_id=""
candidate_run_id=""
status="fail"
reason_code="UNINITIALIZED"

run_pair_status="not_run"
run_pair_reason_code="NOT_RUN"
precheck_status="not_run"
precheck_reason_code="NOT_RUN"
compare_status="not_run"
compare_reason_code="NOT_RUN"
edge_quality_status="not_run"
edge_quality_reason_code="NOT_RUN"

write_summary() {
  python3 - "$summary_json" "$status" "$reason_code" "$candidate_run_id" "$baseline_run_id" "$derive_pair_json" "$precheck_json" "$compare_json" "$edge_json" "$triage_impl_dir" "$out_dir" "$run_pair_status" "$run_pair_reason_code" "$precheck_status" "$precheck_reason_code" "$compare_status" "$compare_reason_code" "$edge_quality_status" "$edge_quality_reason_code" <<'PY'
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
    run_pair_status,
    run_pair_reason_code,
    precheck_status,
    precheck_reason_code,
    compare_status,
    compare_reason_code,
    edge_quality_status,
    edge_quality_reason_code,
) = sys.argv[1:]

artifact_paths = {
    "derive_run_pair_json": derive_pair_json,
    "precheck_stage_json": precheck_json,
    "compare_validation_json": compare_json,
    "edge_quality_compare_json": edge_json,
    "triage_impl_dir": triage_impl_dir,
    "export_dir": export_dir,
}

required_json_exists = {
    "derive_run_pair_json": os.path.isfile(derive_pair_json) and os.path.getsize(derive_pair_json) > 0,
    "precheck_stage_json": os.path.isfile(precheck_json) and os.path.getsize(precheck_json) > 0,
    "compare_validation_json": os.path.isfile(compare_json) and os.path.getsize(compare_json) > 0,
    "edge_quality_compare_json": os.path.isfile(edge_json) and os.path.getsize(edge_json) > 0,
}

payload = {
    "status": status,
    "reason_code": reason_code,
    "run_ids": {
        "candidate_run_id": candidate_run_id or None,
        "baseline_run_id": baseline_run_id or None,
    },
    "gate_outcomes": {
        "run_pair_selection": {
            "status": run_pair_status,
            "reason_code": run_pair_reason_code,
            "artifact_path": derive_pair_json,
        },
        "precheck": {
            "status": precheck_status,
            "reason_code": precheck_reason_code,
            "artifact_path": precheck_json,
        },
        "compare_validation": {
            "status": compare_status,
            "reason_code": compare_reason_code,
            "artifact_path": compare_json,
        },
        "edge_quality": {
            "status": edge_quality_status,
            "reason_code": edge_quality_reason_code,
            "artifact_path": edge_json,
        },
    },
    "artifact_paths": artifact_paths,
    "required_json_exists": required_json_exists,
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

write_edge_stage_failure() {
  local reason="$1"
  local exit_code="$2"
  local command_used="$3"
  local stderr_log="$4"
  python3 - "$edge_json" "$reason" "$exit_code" "$baseline_run_id" "$candidate_run_id" "$command_used" "$stderr_log" <<'PY'
import json
import sys

(
    out_path,
    reason_code,
    exit_code,
    baseline_run_id,
    candidate_run_id,
    command_used,
    stderr_log_path,
) = sys.argv[1:]

payload = {
    "status": "fail",
    "reason_code": reason_code,
    "gate_verdict": "fail",
    "exit_code": int(exit_code),
    "baseline_run_id": baseline_run_id,
    "candidate_run_id": candidate_run_id,
    "command_used": command_used,
    "stderr_log_path": stderr_log_path,
}

with open(out_path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2, sort_keys=True)
PY
}

write_compare_stage_result() {
  local reason="$1"
  local exit_code="$2"
  local log_path="$3"
  python3 - "$compare_json" "$reason" "$exit_code" "$baseline_run_id" "$candidate_run_id" "$log_path" <<'PY'
import json
import sys

out, reason_code, exit_code, baseline, candidate, log_path = sys.argv[1:]
exit_code = int(exit_code)
with open(out, "w", encoding="utf-8") as handle:
    json.dump({
        "status": "ok" if reason_code == "COMPARE_VALIDATION_PASSED" else "fail",
        "reason_code": reason_code,
        "exit_code": exit_code,
        "baseline_run_id": baseline,
        "candidate_run_id": candidate,
        "stdout_log": log_path,
    }, handle, indent=2, sort_keys=True)
PY
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

triage_impl_dir="${out_dir}/triage_impl_${timestamp}"
out_stage_dir="${triage_impl_dir}/out"
mkdir -p "$out_stage_dir"

derive_pair_json="$out_stage_dir/derive_run_pair.json"
precheck_json="$out_stage_dir/precheck_stage.json"
compare_json="$out_stage_dir/compare_validation.json"
edge_json="$out_stage_dir/edge_quality_compare.json"
summary_json="$out_stage_dir/triage_summary.json"

echo "[1/5] Deriving run pair"
if [[ -n "${manual_baseline_run_id//[[:space:]]/}" || -n "${manual_candidate_run_id//[[:space:]]/}" ]]; then
  if [[ -z "${manual_baseline_run_id//[[:space:]]/}" || -z "${manual_candidate_run_id//[[:space:]]/}" ]]; then
    run_pair_status="fail"
    run_pair_reason_code="RUN_IDS_MISSING"
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
    run_pair_status="fail"
    run_pair_reason_code="RUN_IDS_MISSING"
    fail_and_exit "RUN_IDS_MISSING"
  fi
  baseline_run_id="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["baseline_run_id"])' "$derive_pair_json")"
  candidate_run_id="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["candidate_run_id"])' "$derive_pair_json")"
fi
ensure_required_json "$derive_pair_json" "RUN_PAIR_JSON_MISSING"
if [[ -z "${baseline_run_id//[[:space:]]/}" || -z "${candidate_run_id//[[:space:]]/}" ]]; then
  run_pair_status="fail"
  run_pair_reason_code="RUN_IDS_MISSING"
  fail_and_exit "RUN_IDS_MISSING"
fi
run_pair_status="pass"
run_pair_reason_code="RUN_PAIR_READY"

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
ensure_required_json "$precheck_json" "PRECHECK_JSON_MISSING"
if [[ "$precheck_exit" -ne 0 ]]; then
  precheck_status="fail"
  precheck_reason_code="PRECHECK_FAILED"
  cat "$triage_impl_dir/precheck_stdout.log" >&2 || true
  fail_and_exit "PRECHECK_FAILED"
fi
precheck_status="pass"
precheck_reason_code="PRECHECK_PASSED"

echo "[3/5] Comparing diagnostics"
compare_preflight_log="$triage_impl_dir/compare_preflight.log"
echo "[3/5] Writing compare preflight sidecar"
set +e
python3 - "$out_dir" "$baseline_run_id" "$candidate_run_id" > "$compare_preflight_log" 2>&1 <<'PY'
import sys
from pathlib import Path

repo_root = Path.cwd()
sys.path.insert(0, str(repo_root / "scripts"))
from preflight_guard import write_preflight_sidecar

out_dir, run_a, run_b = sys.argv[1:4]
sidecar_path = write_preflight_sidecar(
    out_dir,
    run_a,
    run_b,
    allow_csv_only_triage=False,
    incident_tag="",
)
print(f"Preflight sidecar written: {sidecar_path}")
PY
compare_preflight_write_exit=$?
set -e
if [[ "$compare_preflight_write_exit" -ne 0 ]]; then
  compare_status="fail"
  compare_reason_code="COMPARE_PREFLIGHT_WRITE_FAILED"
  write_compare_stage_result "COMPARE_PREFLIGHT_WRITE_FAILED" "$compare_preflight_write_exit" "$compare_preflight_log"
  cat "$compare_preflight_log" >&2 || true
  fail_and_exit "COMPARE_PREFLIGHT_WRITE_FAILED"
fi
set +e
python3 - "$out_dir" "$baseline_run_id" "$candidate_run_id" >> "$compare_preflight_log" 2>&1 <<'PY'
import sys
from pathlib import Path

repo_root = Path.cwd()
sys.path.insert(0, str(repo_root / "scripts"))
from preflight_guard import enforce_preflight_guard

out_dir, run_a, run_b = sys.argv[1:4]
status = enforce_preflight_guard(
    out_dir,
    run_a,
    run_b,
    "",
)
print(f"Preflight guard status: {status.get('status')}")
print(f"Preflight evidence: {status.get('sidecar_path', '')}")
PY
compare_preflight_validate_exit=$?
set -e
if [[ "$compare_preflight_validate_exit" -ne 0 ]]; then
  compare_status="fail"
  compare_reason_code="COMPARE_PREFLIGHT_INVALID"
  write_compare_stage_result "COMPARE_PREFLIGHT_INVALID" "$compare_preflight_validate_exit" "$compare_preflight_log"
  cat "$compare_preflight_log" >&2 || true
  fail_and_exit "COMPARE_PREFLIGHT_INVALID"
fi

set +e
python3 scripts/compare_run_diagnostics.py "$baseline_run_id" "$candidate_run_id" --export-dir "$out_dir" > "$triage_impl_dir/compare_stdout.log" 2>&1
compare_exit=$?
set -e
if [[ "$compare_exit" -eq 0 ]]; then
  write_compare_stage_result "COMPARE_VALIDATION_PASSED" "$compare_exit" "$triage_impl_dir/compare_stdout.log"
else
  write_compare_stage_result "COMPARE_VALIDATION_FAILED" "$compare_exit" "$triage_impl_dir/compare_stdout.log"
fi
ensure_required_json "$compare_json" "COMPARE_JSON_MISSING"
if [[ "$compare_exit" -ne 0 ]]; then
  compare_status="fail"
  compare_reason_code="COMPARE_VALIDATION_FAILED"
  cat "$triage_impl_dir/compare_stdout.log" >&2 || true
  fail_and_exit "COMPARE_VALIDATION_FAILED"
fi
compare_status="pass"
compare_reason_code="COMPARE_VALIDATION_PASSED"

echo "[4/5] Evaluating edge quality"
edge_quality_stderr_log="$triage_impl_dir/edge_quality_stderr.log"
edge_quality_command=(
  python3 scripts/evaluate_edge_quality.py "$out_dir"
  --baseline-run-id "$baseline_run_id"
  --candidate-run-id "$candidate_run_id"
  --out-json "$edge_json"
)
printf '%q ' "${edge_quality_command[@]}" > "$triage_impl_dir/edge_quality_command.sh"
echo >> "$triage_impl_dir/edge_quality_command.sh"
set +e
"${edge_quality_command[@]}" > "$triage_impl_dir/edge_quality_stdout.log" 2> "$edge_quality_stderr_log"
edge_quality_exit=$?
set -e
if [[ ! -s "$edge_json" ]]; then
  edge_quality_status="fail"
  edge_quality_reason_code="EDGE_QUALITY_ARTIFACT_MISSING"
  echo "Error: required JSON artifact missing or empty: $edge_json" >&2
  write_edge_stage_failure "EDGE_QUALITY_ARTIFACT_MISSING" "$edge_quality_exit" "$(cat "$triage_impl_dir/edge_quality_command.sh")" "$edge_quality_stderr_log"
  fail_and_exit "EDGE_QUALITY_ARTIFACT_MISSING"
fi
set +e
edge_classification="$(python3 - "$edge_json" <<'PY'
import json
import sys

required_keys = ("status", "gate_verdict", "reason_code")
path = sys.argv[1]

with open(path, "r", encoding="utf-8") as handle:
    payload = json.load(handle)

if not isinstance(payload, dict):
    raise SystemExit("edge quality JSON must be an object")

missing = [key for key in required_keys if key not in payload]
if missing:
    raise SystemExit(f"missing required keys: {', '.join(missing)}")

gate_verdict = str(payload["gate_verdict"]).strip().lower()
if gate_verdict in {"pass", "ok"}:
    print("pass|EDGE_QUALITY_GATE_PASSED")
else:
    print("fail|EDGE_QUALITY_GATE_BLOCKED")
PY
)"
edge_parse_exit=$?
set -e
if [[ "$edge_parse_exit" -ne 0 ]]; then
  edge_quality_status="fail"
  edge_quality_reason_code="EDGE_QUALITY_JSON_INVALID"
  echo "Error: edge quality artifact exists but failed postcondition validation: $edge_json" >&2
  cat "$edge_quality_stderr_log" >&2 || true
  write_edge_stage_failure "EDGE_QUALITY_JSON_INVALID" "$edge_parse_exit" "$(cat "$triage_impl_dir/edge_quality_command.sh")" "$edge_quality_stderr_log"
  fail_and_exit "EDGE_QUALITY_JSON_INVALID"
fi

edge_quality_status="${edge_classification%%|*}"
edge_quality_reason_code="${edge_classification#*|}"

if [[ "$edge_quality_status" != "pass" ]]; then
  status="fail"
  reason_code="$edge_quality_reason_code"
  write_summary
  exit 1
fi

echo "[5/5] Writing summary"
status="pass"
reason_code="TRIAGE_BUNDLE_OK"
write_summary

echo "[triage-bundle] Summary JSON: $summary_json"
