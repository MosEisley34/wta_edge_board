#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CANONICAL_INPUT="${TA_PARITY_INPUT:-$ROOT_DIR/tmp/source_probe_latest/raw/tennisabstract_leadersource_wta.body}"
FALLBACK_INPUT="$ROOT_DIR/tmp/source_probes/raw/tennisabstract_leadersource_wta.body"

if [[ $# -gt 0 ]]; then
  INPUT_PATH="$1"
else
  INPUT_PATH="$CANONICAL_INPUT"
fi

if [[ ! -f "$INPUT_PATH" && "$INPUT_PATH" == "$CANONICAL_INPUT" && -f "$FALLBACK_INPUT" ]]; then
  echo "INFO: canonical leadersource artifact missing; using fallback: $FALLBACK_INPUT" >&2
  INPUT_PATH="$FALLBACK_INPUT"
fi

exec python3 "$ROOT_DIR/scripts/check_ta_parity.py" --input "$INPUT_PATH" "${@:2}"
