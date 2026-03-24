#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/export_parity_precheck.sh [--out-dir <dir>] <run_id_a> <run_id_b> <file-or-directory> [more paths...]

Fail-fast operational wrapper:
  1) Export Run_Log/State artifacts from a single latest snapshot
  2) Block on Run_Log CSV/JSON parity verification
  3) Block on target run-id precheck before compare/gate steps

Examples:
  scripts/export_parity_precheck.sh runA runB ./runtime
  scripts/export_parity_precheck.sh --out-dir ./exports runA runB ./runtime/Run_Log.csv ./runtime/Run_Log.json
USAGE
}

out_dir="./exports"

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

echo "[1/3] Exporting runtime artifacts into ${out_dir}"
scripts/prepare_runtime_exports.sh --out-dir "$out_dir" "${inputs[@]}"

echo

echo "[2/3] Verifying Run_Log CSV/JSON parity gate"
python3 scripts/verify_run_log_parity.py --export-dir "$out_dir"

echo

echo "[3/3] Running run-id precheck gate"
python3 scripts/precheck_run_ids.py "$run_id_a" "$run_id_b" --export-dir "$out_dir"

echo

echo "Fail-fast preflight complete. Safe to run compare/gate commands."
