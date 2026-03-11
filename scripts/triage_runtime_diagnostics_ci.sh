#!/usr/bin/env bash
set -euo pipefail

EXPORT_DIR="${RUNTIME_EXPORT_DIR:-./exports}"

if [[ ! -d "$EXPORT_DIR" ]]; then
  echo "Error: CI runtime exports directory missing: $EXPORT_DIR" >&2
  echo "Expected Run_Log/State artifacts to be exported before diagnostics triage." >&2
  exit 1
fi

if ! find "$EXPORT_DIR" -maxdepth 1 -type f \( -iname '*run*log*.csv' -o -iname '*run*log*.json' -o -iname '*state*.csv' -o -iname '*state*.json' \) | grep -q .; then
  echo "Error: CI runtime exports missing in $EXPORT_DIR (no Run_Log/State CSV/JSON files)." >&2
  echo "Ensure export step runs before scripts/triage_runtime_diagnostics_ci.sh." >&2
  exit 1
fi

scripts/scan_runtime_diagnostics.sh "$EXPORT_DIR"
