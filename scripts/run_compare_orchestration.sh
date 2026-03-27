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
  scripts/run_compare_orchestration.sh [--out-dir <dir>] [--allow-csv-only-triage --incident-tag <TAG>] [--min-feature-completeness <float>] [--force-full-compare] <run_id_a> <run_id_b> <live_runtime_dir_or_files> [more paths...]

Runs this orchestration sequence:
  1) scripts/export_parity_precheck.sh
  2) scripts/check_feature_completeness_preflight.py (early gate)
  3) scripts/compare_run_diagnostics_preflight.sh
  4) scripts/compare_run_metrics_preflight.sh
  5) python3 scripts/evaluate_edge_quality.py

By default, if candidate runEdgeBoard feature completeness is missing/non-numeric/below floor,
the workflow exits early with reason_code=FEATURE_COMPLETENESS_BELOW_FLOOR.
Use --force-full-compare to continue downstream commands for debugging.
USAGE
}

out_dir="./exports_live"
allow_csv_only_triage=0
incident_tag=""
min_feature_completeness="0.60"
force_full_compare=0

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
    --min-feature-completeness)
      shift
      min_feature_completeness="$1"
      shift
      ;;
    --force-full-compare)
      force_full_compare=1
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
runtime_inputs=("$@")

if [[ "$allow_csv_only_triage" -eq 1 && -z "$incident_tag" ]]; then
  echo "Error: --allow-csv-only-triage requires --incident-tag <INCIDENT-TAG>." >&2
  exit 1
fi

preflight_args=(--out-dir "$out_dir")
if [[ "$allow_csv_only_triage" -eq 1 ]]; then
  preflight_args+=(--allow-csv-only-triage --incident-tag "$incident_tag")
fi

scripts/export_parity_precheck.sh "${preflight_args[@]}" "$run_a" "$run_b" "${runtime_inputs[@]}"

feature_gate_args=(
  --export-dir "$out_dir"
  --baseline-run-id "$run_a"
  --candidate-run-id "$run_b"
  --min-feature-completeness "$min_feature_completeness"
  --report-out "$out_dir/feature_completeness_preflight.json"
)
if [[ "$force_full_compare" -eq 1 ]]; then
  feature_gate_args+=(--force-full-compare)
fi
python3 scripts/check_feature_completeness_preflight.py "${feature_gate_args[@]}"

scripts/compare_run_diagnostics_preflight.sh "${preflight_args[@]}" "$run_a" "$run_b" "${runtime_inputs[@]}"
scripts/compare_run_metrics_preflight.sh "${preflight_args[@]}" "$run_a" "$run_b" "${runtime_inputs[@]}"
python3 scripts/evaluate_edge_quality.py "$out_dir" --baseline-run-id "$run_a" --candidate-run-id "$run_b"
