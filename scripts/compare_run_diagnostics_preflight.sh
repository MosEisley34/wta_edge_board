#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [[ -z "$repo_root" ]]; then
  echo "Error: this command must be run inside a git checkout." >&2
  exit 1
fi
cd "$repo_root"

usage() {
  cat <<'USAGE'
Usage:
  scripts/compare_run_diagnostics_preflight.sh [--out-dir <dir>] [--allow-csv-only-triage --incident-tag <TAG>] <run_id_a> <run_id_b> <live_runtime_dir_or_files> [-- compare_run_diagnostics_args...]

Always runs scripts/export_parity_precheck.sh first, then scripts/compare_run_diagnostics.py.
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

preflight_sidecar="$out_dir/run_compare_preflight.json"
scripts/export_parity_precheck.sh "${preflight_args[@]}" "$run_a" "$run_b" "${runtime_inputs[@]}"
if [[ ! -s "$preflight_sidecar" ]]; then
  echo "Error: expected non-empty preflight sidecar at $preflight_sidecar." >&2
  exit 1
fi
compare_log="$out_dir/compare_run_diagnostics_preflight.log"
set +e
python3 scripts/compare_run_diagnostics.py "$run_a" "$run_b" --export-dir "$out_dir" "${compare_extra_args[@]}" | tee "$compare_log"
compare_status=${PIPESTATUS[0]}
set -e
if [[ ! -s "$compare_log" ]]; then
  echo "Error: missing diagnostics compare log at $compare_log." >&2
  echo "Hint: regenerate with: scripts/compare_run_diagnostics_preflight.sh --out-dir \"$out_dir\" \"$run_a\" \"$run_b\" ${runtime_inputs[*]}" >&2
  exit 1
fi
python3 scripts/extract_schema_missing_triage.py --limit 20 --out "$out_dir/triage_last20_next.csv" "$compare_log" >/dev/null || true
exit "$compare_status"
