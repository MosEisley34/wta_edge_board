#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CANONICAL_INPUT="${TA_PARITY_INPUT:-$ROOT_DIR/tmp/source_probe_latest/raw/tennisabstract_leadersource_wta.body}"
FALLBACK_INPUT="$ROOT_DIR/tmp/source_probes/raw/tennisabstract_leadersource_wta.body"
REGEN_COMMAND='scripts/probe_tennis_sources.sh ./tmp/source_probes'

if [[ $# -gt 0 ]]; then
  INPUT_PATH="$1"
else
  INPUT_PATH="$CANONICAL_INPUT"
fi

if [[ ! -f "$INPUT_PATH" && "$INPUT_PATH" == "$CANONICAL_INPUT" && -f "$FALLBACK_INPUT" ]]; then
  probe_locations=(
    "$ROOT_DIR/tmp/source_probe_latest/raw/tennisabstract_leadersource_wta.body"
    "$ROOT_DIR/tmp/source_probes/raw/tennisabstract_leadersource_wta.body"
    "$ROOT_DIR/tmp/source_probe_*/raw/tennisabstract_leadersource_wta.body"
  )
  found_locations=()
  for probe_path in "${probe_locations[@]}"; do
    for candidate in $probe_path; do
      [[ -f "$candidate" ]] && found_locations+=("$candidate")
    done
  done
  if [[ ${#found_locations[@]} -gt 0 ]]; then
    echo "INFO: canonical leadersource artifact missing; checked common probe outputs and found: ${found_locations[*]}; run '$REGEN_COMMAND' to regenerate tmp/source_probe_latest/raw/tennisabstract_leadersource_wta.body" >&2
  else
    echo "INFO: canonical leadersource artifact missing; checked common probe outputs (none found); run '$REGEN_COMMAND' to regenerate tmp/source_probe_latest/raw/tennisabstract_leadersource_wta.body" >&2
  fi
  echo "INFO: using fallback: $FALLBACK_INPUT" >&2
  INPUT_PATH="$FALLBACK_INPUT"
fi

if [[ ! -f "$INPUT_PATH" && "$INPUT_PATH" == "$CANONICAL_INPUT" && ! -f "$FALLBACK_INPUT" ]]; then
  echo "ERROR: missing canonical leadersource artifact at $CANONICAL_INPUT and fallback at $FALLBACK_INPUT; run scripts/probe_tennis_sources.sh ./tmp/source_probes (expected output: $CANONICAL_INPUT)" >&2
  exit 1
fi

exec python3 "$ROOT_DIR/scripts/check_ta_parity.py" --input "$INPUT_PATH" "${@:2}"
