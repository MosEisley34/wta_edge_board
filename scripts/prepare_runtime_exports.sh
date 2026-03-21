#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/prepare_runtime_exports.sh [--out-dir <dir>] <file-or-directory> [more paths...]

Repeatable pre-step before diagnostics scanning:
  1) Export Run_Log/State CSV/JSON artifacts into ./exports (default)
  2) Verify expected export files exist before running scan

Expected exported files (at least one must exist):
  - ./exports/*Run_Log*.csv
  - ./exports/*Run_Log*.json
  - ./exports/*State*.csv
  - ./exports/*State*.json

The pre-step also writes a timestamped export manifest:
  - ./exports/runtime_export_manifest.json

Examples:
  scripts/prepare_runtime_exports.sh ./runtime
  scripts/prepare_runtime_exports.sh --out-dir ./exports ./runtime/Run_Log.csv ./runtime/state_dump.json
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

if [[ "${#inputs[@]}" -eq 0 ]]; then
  echo "Error: no input paths provided for export pre-step." >&2
  usage >&2
  exit 1
fi

out_parent="$(dirname "$out_dir")"
out_base="$(basename "$out_dir")"
staging_dir="$(mktemp -d "${out_parent}/.${out_base}.staging.XXXXXX")"
cleanup_staging_dir() {
  rm -rf "$staging_dir"
}
trap cleanup_staging_dir EXIT

scripts/export_runtime_artifacts.sh --out-dir "$staging_dir" "${inputs[@]}"
python3 scripts/verify_run_log_parity.py --export-dir "$staging_dir"

rm -rf "$out_dir"
mkdir -p "$out_dir"
cp -a "$staging_dir"/. "$out_dir"/

mapfile -t matched_files < <(
  find "$out_dir" -maxdepth 1 -type f \
    \( -iname '*run_log*.csv' -o -iname '*run_log*.json' -o -iname '*state*.csv' -o -iname '*state*.json' \) \
    | sort
)

if [[ "${#matched_files[@]}" -eq 0 ]]; then
  echo "Error: export pre-step completed but expected runtime files were not found in $out_dir." >&2
  echo "Expected at least one of: $out_dir/*Run_Log*.csv, $out_dir/*Run_Log*.json, $out_dir/*State*.csv, $out_dir/*State*.json" >&2
  echo "Remediation: rerun scripts/prepare_runtime_exports.sh with paths that contain Run_Log/State CSV/JSON artifacts, then run scripts/scan_runtime_diagnostics.sh $out_dir" >&2
  exit 1
fi

manifest_path="$out_dir/runtime_export_manifest.json"
python3 - "$manifest_path" "${matched_files[@]}" <<'PY'
import datetime as dt
import json
import os
import sys

manifest_path = sys.argv[1]
files = sys.argv[2:]

def as_utc_iso(ts: float) -> str:
    return dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).isoformat()

manifest = {
    "generated_at_utc": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
    "expected_patterns": [
        "*Run_Log*.csv",
        "*Run_Log*.json",
        "*State*.csv",
        "*State*.json",
    ],
    "files": [],
}

for path in files:
    stat = os.stat(path)
    manifest["files"].append(
        {
            "path": path,
            "size_bytes": stat.st_size,
            "modified_at_utc": as_utc_iso(stat.st_mtime),
        }
    )

with open(manifest_path, "w", encoding="utf-8") as f:
    json.dump(manifest, f, indent=2)
    f.write("\n")
PY

echo "Pre-step complete. Exports are ready in $out_dir"
echo "Manifest: $manifest_path"
echo "Next command: scripts/scan_runtime_diagnostics.sh $out_dir"
