#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

VERBOSE_PROFILE_JSON="${VERBOSE_PROFILE_JSON:-$ROOT_DIR/docs/baselines/pipeline_log_sample_3h_verbose_2026-03-12.json}"
COMPACT_PROFILE_JSON="${COMPACT_PROFILE_JSON:-$ROOT_DIR/docs/baselines/pipeline_log_sample_3h_compact_2026-03-12.json}"
PROFILE_REDUCTION_TARGET_PCT="${PROFILE_REDUCTION_TARGET_PCT:-60}"
PROFILE_CRITICAL_PARITY_KEYS="${PROFILE_CRITICAL_PARITY_KEYS:-gate_reasons,source_selection,watchdog}"
PROFILE_SUMMARY_JSON_OUT="${PROFILE_SUMMARY_JSON_OUT:-$ROOT_DIR/exports/pipeline_log_profile_ci_summary.json}"

if [[ ! -f "$VERBOSE_PROFILE_JSON" ]]; then
  echo "Error: verbose profile JSON missing: $VERBOSE_PROFILE_JSON" >&2
  exit 1
fi

if [[ ! -f "$COMPACT_PROFILE_JSON" ]]; then
  echo "Error: compact profile JSON missing: $COMPACT_PROFILE_JSON" >&2
  exit 1
fi

echo "Running deterministic profile comparison..."
python3 "$ROOT_DIR/scripts/compare_log_profiles.py" \
  "$VERBOSE_PROFILE_JSON" \
  "$COMPACT_PROFILE_JSON" \
  --target-reduction-pct "$PROFILE_REDUCTION_TARGET_PCT" \
  --critical-parity-keys "$PROFILE_CRITICAL_PARITY_KEYS" \
  --summary-json-out "$PROFILE_SUMMARY_JSON_OUT"

echo "Profile comparison summary artifact: $PROFILE_SUMMARY_JSON_OUT"
