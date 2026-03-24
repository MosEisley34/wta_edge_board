#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/compare_run_metrics_preflight.sh [--out-dir <dir>] [--allow-csv-only-triage --incident-tag <TAG>] <run_id_a> <run_id_b> <live_runtime_dir_or_files> [-- compare_run_metrics_args...]

Always runs scripts/export_parity_precheck.sh first, then scripts/compare_run_metrics.py.
USAGE
}

out_dir="./exports_live"
allow_csv_only_triage=0
incident_tag=""

while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --out-dir)
      shift
      out_dir="$1"
      shift
      ;;
    --allow-csv-only-triage)
      allow_csv_only_triage=1
      shift
      ;;
    --incident-tag)
      shift
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
  usage >&2
  exit 1
fi

run_a="$1"
run_b="$2"
shift 2

runtime_inputs=()
while [[ "$#" -gt 0 ]]; do
  if [[ "$1" == "--" ]]; then
    shift
    break
  fi
  runtime_inputs+=("$1")
  shift
done

if [[ "${#runtime_inputs[@]}" -eq 0 ]]; then
  echo "Error: missing live runtime input path(s)." >&2
  usage >&2
  exit 1
fi

compare_extra_args=("$@")

preflight_args=(--out-dir "$out_dir")
if [[ "$allow_csv_only_triage" -eq 1 ]]; then
  if [[ -z "$incident_tag" ]]; then
    echo "Error: --allow-csv-only-triage requires --incident-tag <INCIDENT-TAG>." >&2
    exit 1
  fi
  preflight_args+=(--allow-csv-only-triage --incident-tag "$incident_tag")
fi

scripts/export_parity_precheck.sh "${preflight_args[@]}" "$run_a" "$run_b" "${runtime_inputs[@]}"
python3 scripts/compare_run_metrics.py "$run_a" "$run_b" --input "$out_dir/Run_Log.csv" "${compare_extra_args[@]}"
