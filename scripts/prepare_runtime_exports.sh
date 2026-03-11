#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/prepare_runtime_exports.sh [--out-dir <dir>] <file-or-directory> [more paths...]

Repeatable pre-step before diagnostics scanning:
  1) Export Run_Log/State CSV/JSON artifacts into ./exports (default)
  2) Verify expected export files exist before running scan

Expected exported files (at least one must exist):
  - ./exports/*Run_Log*.csv
  - ./exports/*Run_Log*.json
  - ./exports/*State*.csv
  - ./exports/*State*.json

Examples:
  scripts/prepare_runtime_exports.sh ./runtime
  scripts/prepare_runtime_exports.sh --out-dir ./exports ./runtime/Run_Log.csv ./runtime/state_dump.json
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
  echo "Error: no input paths provided for export pre-step." >&2
  usage >&2
  exit 1
fi

scripts/export_runtime_artifacts.sh --out-dir "$out_dir" "${inputs[@]}"

if ! find "$out_dir" -maxdepth 1 -type f \
  \( -iname '*run_log*.csv' -o -iname '*run_log*.json' -o -iname '*state*.csv' -o -iname '*state*.json' \) \
  | grep -q .; then
  echo "Error: export pre-step completed but expected runtime files were not found in $out_dir." >&2
  echo "Expected at least one of: $out_dir/*Run_Log*.csv, $out_dir/*Run_Log*.json, $out_dir/*State*.csv, $out_dir/*State*.json" >&2
  echo "Remediation: rerun scripts/prepare_runtime_exports.sh with paths that contain Run_Log/State CSV/JSON artifacts, then run scripts/scan_runtime_diagnostics.sh $out_dir" >&2
  exit 1
fi

echo "Pre-step complete. Exports are ready in $out_dir"
echo "Next command: scripts/scan_runtime_diagnostics.sh $out_dir"
