#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_triage_bundle.sh [--out-dir <dir>] <file-or-directory> [more paths...]

Standard triage bundle flow:
  1) Export Run_Log/State CSV/JSON artifacts into ./exports (default)
  2) Run runtime diagnostics scan against the export directory

Examples:
  scripts/run_triage_bundle.sh ./runtime
  scripts/run_triage_bundle.sh --out-dir ./exports ./runtime/Run_Log.csv ./runtime/state_dump.json
USAGE
}

out_dir="./exports"
inputs=()

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
  echo "Error: no input paths provided for triage bundle." >&2
  usage >&2
  exit 1
fi

echo "[1/2] Preparing Run_Log/State exports in ${out_dir}"
scripts/prepare_runtime_exports.sh --out-dir "$out_dir" "${inputs[@]}"

echo
echo "[2/2] Scanning runtime diagnostics from ${out_dir}"
scripts/scan_runtime_diagnostics.sh "$out_dir"
