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
  scripts/export_parity_precheck.sh [--out-dir <dir>] [--allow-csv-only-triage --incident-tag <TAG>] <run_id_a> <run_id_b> <file-or-directory> [more paths...]

Fail-fast operational wrapper:
  1) Export Run_Log/State artifacts from a single latest snapshot
  2) Block on Run_Log CSV/JSON parity verification
  3) Block on target run-id precheck before compare/gate steps

Examples:
  scripts/export_parity_precheck.sh runA runB ./runtime
  scripts/export_parity_precheck.sh --out-dir ./exports runA runB ./runtime/Run_Log.csv ./runtime/Run_Log.json
  scripts/export_parity_precheck.sh --out-dir ./exports --allow-csv-only-triage --incident-tag INC-1234 runA runB ./runtime
USAGE
}

out_dir="./exports"
allow_csv_only_triage=0
incident_tag=""

while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --out-dir)
      shift
      if [[ "$#" -eq 0 ]]; then
        echo "Error: --out-dir requires a directory value." >&2
        usage >&2
        exit 1
      fi
      out_dir="$1"
      shift
      ;;
    --allow-csv-only-triage)
      allow_csv_only_triage=1
      shift
      ;;
    --incident-tag)
      shift
      if [[ "$#" -eq 0 ]]; then
        echo "Error: --incident-tag requires a value." >&2
        usage >&2
        exit 1
      fi
      incident_tag="$1"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      break
      ;;
  esac
done

if [[ "$#" -lt 3 ]]; then
  echo "Error: expected <run_id_a> <run_id_b> and at least one input path." >&2
  usage >&2
  exit 1
fi

run_id_a="$1"
run_id_b="$2"
shift 2
inputs=("$@")

if [[ "$allow_csv_only_triage" -eq 1 && -z "$incident_tag" ]]; then
  echo "Error: --allow-csv-only-triage requires --incident-tag <INCIDENT-TAG>." >&2
  exit 1
fi

echo "[1/3] Exporting runtime artifacts into ${out_dir}"
scripts/prepare_runtime_exports.sh --out-dir "$out_dir" "${inputs[@]}"

echo

echo "[2/3] Verifying Run_Log CSV/JSON parity gate"
python3 scripts/verify_run_log_parity.py --export-dir "$out_dir"

echo

echo "[3/3] Running run-id precheck gate"
precheck_args=("$run_id_a" "$run_id_b" --export-dir "$out_dir")
if [[ "$allow_csv_only_triage" -eq 1 ]]; then
  precheck_args+=(--allow-csv-only-triage --allow-csv-only-triage-incident-tag "$incident_tag")
fi
python3 scripts/precheck_run_ids.py "${precheck_args[@]}"

python3 - "$out_dir" "$run_id_a" "$run_id_b" "$allow_csv_only_triage" "$incident_tag" <<'PY'
import sys
from pathlib import Path

repo_root = Path.cwd()
sys.path.insert(0, str(repo_root / "scripts"))
from preflight_guard import write_preflight_sidecar

out_dir, run_a, run_b, allow_csv_only, incident_tag = sys.argv[1:6]
sidecar_path = write_preflight_sidecar(
    out_dir,
    run_a,
    run_b,
    allow_csv_only_triage=allow_csv_only == "1",
    incident_tag=incident_tag,
)
print(f"Preflight sidecar written: {sidecar_path}")
PY

echo

echo "Fail-fast preflight complete. Safe to run compare/gate commands."
