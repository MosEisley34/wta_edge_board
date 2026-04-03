#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_triage_bundle.sh [--out-dir <dir>] [--rolling-report-out <json>] <file-or-directory> [more paths...]

Standard triage bundle flow:
  1) Export Run_Log/State CSV/JSON artifacts into an operator workspace (default: /tmp/wta_edge_board_triage_exports)
  2) Run runtime diagnostics scan against the export directory
  3) Evaluate edge-quality stability/robustness gate

Important:
  - Prefer --out-dir outside this repository tree to avoid generated artifact merge blockers on main.
  - Example outside-repo workspace: /tmp/wta_edge_board_triage_exports

Examples:
  scripts/run_triage_bundle.sh ./runtime
  scripts/run_triage_bundle.sh --out-dir /tmp/wta_edge_board_triage_exports ./runtime/Run_Log.csv ./runtime/state_dump.json
  scripts/run_triage_bundle.sh --out-dir /tmp/wta_edge_board_triage_exports --rolling-report-out ./docs/baselines/runtime_rollups/edge_quality_rolling_2026-03-24.json ./runtime
USAGE
}

out_dir="${TMPDIR:-/tmp}/wta_edge_board_triage_exports"
rolling_report_out=""
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
    --rolling-report-out)
      shift
      if [[ "$#" -eq 0 ]]; then
        echo "Error: --rolling-report-out requires a file path." >&2
        usage >&2
        exit 1
      fi
      rolling_report_out="$1"
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
  echo "Error: no input paths provided for triage bundle." >&2
  usage >&2
  exit 1
fi

echo "[1/3] Preparing Run_Log/State exports in ${out_dir}"
scripts/prepare_runtime_exports.sh --out-dir "$out_dir" "${inputs[@]}"

echo
echo "[2/3] Scanning runtime diagnostics from ${out_dir}"
echo "[triage] First pass: prioritize the Run-health degraded contract section."
scripts/scan_runtime_diagnostics.sh "$out_dir"


echo
echo "[3/3] Evaluating edge-quality gate from ${out_dir}"
if [[ -n "${rolling_report_out}" ]]; then
  python3 scripts/evaluate_edge_quality.py "${out_dir}" --rolling-report-out "${rolling_report_out}"
else
  python3 scripts/evaluate_edge_quality.py "${out_dir}"
fi
