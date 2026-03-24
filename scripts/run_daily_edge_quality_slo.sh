#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_daily_edge_quality_slo.sh [--run-log <path>] [--reports-dir <dir>] [--archive-dir <dir>] [--windows <csv>] [--min-pairs <n>] [--fail-rate-threshold <float>] [--as-of-utc <iso>]

Default production invocation:
  scripts/run_daily_edge_quality_slo.sh
USAGE
}

run_log="./exports_live/Run_Log.csv"
reports_dir="reports"
archive_dir="docs/baselines/edge_quality_slo"
windows="3,7"
min_pairs="10"
fail_rate_threshold="0.15"
as_of_utc=""

while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --run-log)
      shift
      run_log="$1"
      ;;
    --reports-dir)
      shift
      reports_dir="$1"
      ;;
    --archive-dir)
      shift
      archive_dir="$1"
      ;;
    --windows)
      shift
      windows="$1"
      ;;
    --min-pairs)
      shift
      min_pairs="$1"
      ;;
    --fail-rate-threshold)
      shift
      fail_rate_threshold="$1"
      ;;
    --as-of-utc)
      shift
      as_of_utc="$1"
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
  shift
done

if [[ ! -f "${run_log}" ]]; then
  echo "Run log not found: ${run_log}" >&2
  exit 1
fi

cmd=(
  python3 scripts/evaluate_edge_quality.py "${run_log}"
  --daily-slo
  --daily-slo-windows "${windows}"
  --daily-slo-min-pairs "${min_pairs}"
  --daily-slo-fail-rate-threshold "${fail_rate_threshold}"
  --daily-slo-output-dir "${reports_dir}"
  --daily-slo-archive-dir "${archive_dir}"
)
if [[ -n "${as_of_utc}" ]]; then
  cmd+=(--as-of-utc "${as_of_utc}")
fi

"${cmd[@]}"
