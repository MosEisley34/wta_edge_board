#!/usr/bin/env bash
set -euo pipefail

EXPORT_DIR="${1:-./exports}"

if [[ ! -d "$EXPORT_DIR" ]]; then
  echo "Error: runtime export directory not found: $EXPORT_DIR" >&2
  echo "Run scripts/prepare_runtime_exports.sh first (default output: ./exports)." >&2
  exit 1
fi

if ! find "$EXPORT_DIR" -maxdepth 1 -type f \( -iname '*run*log*.csv' -o -iname '*run*log*.json' -o -iname '*state*.csv' -o -iname '*state*.json' \) | grep -q .; then
  echo "Error: no exported Run_Log/State CSV/JSON artifacts found in $EXPORT_DIR" >&2
  echo "Run scripts/prepare_runtime_exports.sh before triage." >&2
  exit 1
fi

python3 scripts/verify_run_log_parity.py --export-dir "$EXPORT_DIR"

echo "[triage] First pass: prioritize the Run-health degraded contract section."
scripts/scan_runtime_diagnostics.sh "$EXPORT_DIR"
