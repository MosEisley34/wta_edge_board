#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/prepare_runtime_exports.sh [--out-dir <dir>] <file-or-directory> [more paths...]

Repeatable pre-step before diagnostics scanning:
  1) Export Run_Log/State CSV/JSON artifacts into ./exports (default)
  2) Mirror canonical runtime CSV tabs to JSON and enforce CSV↔JSON row parity
  3) Enforce Run_Log CSV/JSON parity for latest batch before publish
  4) Verify expected export files exist before running scan
  5) Always emit deterministic export artifacts even on failure:
     - runtime_export_manifest.json
     - runtime_export_manifest.pointer.json
     - runtime_export_failure.json (failure only)

Expected exported files (all must exist):
  - ./exports/Run_Log.csv
  - ./exports/Run_Log.json
  - ./exports/State.csv
  - ./exports/State.json

The pre-step writes a deterministic manifest and pointer:
  - ./exports/runtime_export_manifest.json
  - ./exports/runtime_export_manifest.pointer.json
If export fails, pointer includes explicit failure artifact path:
  - ./exports/runtime_export_failure.json

Examples:
  scripts/prepare_runtime_exports.sh ./runtime
  scripts/prepare_runtime_exports.sh --out-dir ./exports ./runtime/Run_Log.csv ./runtime/State.json
USAGE
}

out_dir="./exports"
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

manifest_path="$out_dir/runtime_export_manifest.json"
pointer_path="$out_dir/runtime_export_manifest.pointer.json"
failure_artifact_path="$out_dir/runtime_export_failure.json"
required_files=()

write_manifest_artifacts() {
  local status="$1"
  local reason_code="$2"
  local detail_message="$3"
  shift 3
  local files=("$@")

  mkdir -p "$out_dir"
  python3 - "$manifest_path" "$pointer_path" "$failure_artifact_path" "$status" "$reason_code" "$detail_message" "${files[@]}" <<'PY'
import datetime as dt
import json
import os
import sys
from pathlib import Path

manifest_path = Path(sys.argv[1])
pointer_path = Path(sys.argv[2])
failure_path = Path(sys.argv[3])
status = sys.argv[4]
reason_code = sys.argv[5]
detail_message = sys.argv[6]
files = [Path(p) for p in sys.argv[7:]]

expected_patterns = [
    "*Run_Log*.csv",
    "*Run_Log*.json",
    "*State*.csv",
    "*State*.json",
    "runtime_tab_json_mirror_summary.json",
]

manifest = {
    "generated_at_utc": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
    "status": status,
    "reason_code": reason_code,
    "detail_message": detail_message,
    "expected_patterns": expected_patterns,
    "files": [],
}

for path in files:
    if not path.exists() or not path.is_file():
        continue
    stat = path.stat()
    manifest["files"].append(
        {
            "path": str(path),
            "size_bytes": stat.st_size,
            "modified_at_utc": dt.datetime.fromtimestamp(stat.st_mtime, tz=dt.timezone.utc).isoformat(),
        }
    )

manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

pointer = {
    "generated_at_utc": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
    "manifest_path": str(manifest_path),
    "status": status,
}
if status == "failed":
    failure_payload = {
        "generated_at_utc": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
        "status": "failed",
        "reason_code": reason_code,
        "detail_message": detail_message,
        "manifest_path": str(manifest_path),
    }
    failure_path.write_text(json.dumps(failure_payload, indent=2) + "\n", encoding="utf-8")
    pointer["failure_artifact_path"] = str(failure_path)

pointer_path.write_text(json.dumps(pointer, indent=2) + "\n", encoding="utf-8")
PY
}

staging_dir=""

on_exit() {
  local exit_code=$?
  if [[ -n "$staging_dir" && -d "$staging_dir" ]]; then
    rm -rf "$staging_dir"
  fi
  if [[ "$exit_code" -ne 0 ]]; then
    write_manifest_artifacts \
      "failed" \
      "runtime_export_prestep_failed" \
      "prepare_runtime_exports.sh failed before producing a parity-ready export batch" \
      "${required_files[@]}"
    echo "Failure artifacts: $manifest_path (manifest), $pointer_path (pointer), $failure_artifact_path (failure details)" >&2
  fi
}
trap on_exit EXIT

if [[ "${#inputs[@]}" -eq 0 ]]; then
  echo "Error: no input paths provided for export pre-step." >&2
  usage >&2
  exit 1
fi

out_parent="$(dirname "$out_dir")"
out_base="$(basename "$out_dir")"
staging_dir="$(mktemp -d "${out_parent}/.${out_base}.staging.XXXXXX")"

scripts/export_runtime_artifacts.sh --out-dir "$staging_dir" "${inputs[@]}"
python3 scripts/mirror_all_runtime_tabs_to_json.py --export-dir "$staging_dir"
python3 scripts/verify_run_log_parity.py --export-dir "$staging_dir"

rm -rf "$out_dir"
mkdir -p "$out_dir"
cp -a "$staging_dir"/. "$out_dir"/

required_files=(
  "$out_dir/Run_Log.csv"
  "$out_dir/Run_Log.json"
  "$out_dir/State.csv"
  "$out_dir/State.json"
  "$out_dir/runtime_tab_json_mirror_summary.json"
)
missing_files=()
for path in "${required_files[@]}"; do
  if [[ ! -f "$path" ]]; then
    missing_files+=("$path")
  fi
done

if [[ "${#missing_files[@]}" -gt 0 ]]; then
  echo "Error: export pre-step produced an incomplete runtime batch in $out_dir." >&2
  echo "Missing required files:" >&2
  printf '  - %s\n' "${missing_files[@]}" >&2
  echo "Remediation: rerun scripts/prepare_runtime_exports.sh with inputs that include a single source snapshot containing Run_Log.csv, Run_Log.json, State.csv, and State.json." >&2
  exit 1
fi

write_manifest_artifacts \
  "ok" \
  "runtime_export_prestep_pass" \
  "prepare_runtime_exports.sh completed successfully" \
  "${required_files[@]}"

echo "Pre-step complete. Exports are ready in $out_dir"
echo "Manifest: $manifest_path"
echo "Manifest pointer: $pointer_path"
echo "Next command: scripts/scan_runtime_diagnostics.sh $out_dir"
