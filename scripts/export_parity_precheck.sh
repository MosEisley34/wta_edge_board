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
  scripts/export_parity_precheck.sh [--out-dir <dir>] [--allow-csv-only-triage --incident-tag <TAG>] <run_id_a> <run_id_b> <file-or-directory> [more paths...]

Fail-fast operational wrapper:
  1) Export Run_Log/State artifacts from a single latest snapshot
  2) Block on Run_Log CSV/JSON parity verification
  3) Block when any canonical runtime tab CSV/JSON file is missing or row counts mismatch
  4) Block on target run-id precheck before compare/gate steps
  5) Canonical stageFetchPlayerStats sync path (patch CSV, mirror JSON, validate key fields)
  6) Re-run precheck after sync before compare/gate steps
  7) Emit deterministic precheck pointer artifact:
     - success:  <out-dir>/export_parity_precheck.pointer.json
     - failure:  <out-dir>/export_parity_precheck_failure.json

Examples:
  scripts/export_parity_precheck.sh runA runB ./runtime
  scripts/export_parity_precheck.sh --out-dir ./exports runA runB ./runtime/Run_Log.csv ./runtime/Run_Log.json
  scripts/export_parity_precheck.sh --out-dir ./exports --allow-csv-only-triage --incident-tag INC-1234 runA runB ./runtime
USAGE
}

out_dir="./exports"
allow_csv_only_triage=0
incident_tag=""
precheck_pointer_path=""
precheck_failure_path=""

write_precheck_failure_artifacts() {
  local reason_code="$1"
  local detail_message="$2"
  mkdir -p "$out_dir"
  python3 - "$out_dir" "$precheck_pointer_path" "$precheck_failure_path" "$reason_code" "$detail_message" <<'PY'
import datetime as dt
import json
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
pointer_path = Path(sys.argv[2])
failure_path = Path(sys.argv[3])
reason_code = sys.argv[4]
detail_message = sys.argv[5]
manifest_path = out_dir / "runtime_export_manifest.json"
manifest_pointer_path = out_dir / "runtime_export_manifest.pointer.json"

failure_payload = {
    "generated_at_utc": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
    "status": "failed",
    "reason_code": reason_code,
    "detail_message": detail_message,
    "manifest_path": str(manifest_path),
    "manifest_exists": manifest_path.is_file(),
    "manifest_pointer_path": str(manifest_pointer_path),
}
failure_path.write_text(json.dumps(failure_payload, indent=2) + "\n", encoding="utf-8")

pointer_payload = {
    "generated_at_utc": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
    "status": "failed",
    "manifest_path": str(manifest_path),
    "manifest_exists": manifest_path.is_file(),
    "failure_artifact_path": str(failure_path),
}
pointer_path.write_text(json.dumps(pointer_payload, indent=2) + "\n", encoding="utf-8")
PY
}

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
      shift
      ;;
    --allow-csv-only-triage)
      allow_csv_only_triage=1
      shift
      ;;
    --incident-tag)
      shift
      if [[ "$#" -eq 0 ]]; then
        echo "Error: --incident-tag requires a value." >&2
        usage >&2
        exit 1
      fi
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

precheck_pointer_path="$out_dir/export_parity_precheck.pointer.json"
precheck_failure_path="$out_dir/export_parity_precheck_failure.json"

on_exit() {
  local exit_code=$?
  if [[ "$exit_code" -ne 0 ]]; then
    write_precheck_failure_artifacts \
      "export_parity_precheck_failed" \
      "export_parity_precheck.sh failed before preflight completed"
    echo "Precheck failure artifacts: $precheck_pointer_path (pointer), $precheck_failure_path (failure details)" >&2
  fi
}
trap on_exit EXIT

if [[ "$#" -lt 3 ]]; then
  echo "Error: expected <run_id_a> <run_id_b> and at least one input path." >&2
  usage >&2
  exit 1
fi

run_id_a="$1"
run_id_b="$2"
shift 2
inputs=("$@")

if [[ "$allow_csv_only_triage" -eq 1 && -z "$incident_tag" ]]; then
  echo "Error: --allow-csv-only-triage requires --incident-tag <INCIDENT-TAG>." >&2
  exit 1
fi

canonical_source_dirs=()
for input_path in "${inputs[@]}"; do
  if [[ -d "$input_path" ]]; then
    canonical_source_dirs+=("$input_path")
  fi
done

if [[ "${#canonical_source_dirs[@]}" -gt 0 ]]; then
  echo "[0/6] Auto-mirroring canonical CSV tabs in source directory inputs (when present)"
  python3 - "${canonical_source_dirs[@]}" <<'PY'
import json
import subprocess
import sys
from pathlib import Path

repo_root = Path.cwd()
sys.path.insert(0, str(repo_root / "scripts"))
from preflight_guard import CANONICAL_RUNTIME_TABS, evaluate_raw_tab_completeness

dirs = [Path(path).resolve() for path in sys.argv[1:]]
seen: set[Path] = set()
for source_dir in dirs:
    if source_dir in seen:
        continue
    seen.add(source_dir)
    present_csv_tabs = [
        tab for tab in CANONICAL_RUNTIME_TABS if (source_dir / f"{tab}.csv").is_file()
    ]
    if not present_csv_tabs:
        print(f"- skip {source_dir}: no canonical CSV tabs found")
        continue

    print(f"- mirror {source_dir}: detected canonical CSV tabs: {', '.join(present_csv_tabs)}")
    for tab in present_csv_tabs:
        json_path = source_dir / f"{tab}.json"
        if json_path.is_file():
            json_path.unlink()

    proc = subprocess.run(
        [
            "python3",
            str(repo_root / "scripts" / "mirror_all_runtime_tabs_to_json.py"),
            "--export-dir",
            str(source_dir),
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise SystemExit(
            "Auto-mirror failed while regenerating canonical tab JSON files in source directory "
            f"{source_dir}.\n{proc.stderr.strip()}"
        )

    result = evaluate_raw_tab_completeness(str(source_dir))
    mismatched = [item for item in result.get("mismatched_tabs", []) if item.get("tab") in present_csv_tabs]
    missing_json_tabs = [
        tab for tab in present_csv_tabs if not (source_dir / f"{tab}.json").is_file()
    ]
    if mismatched or missing_json_tabs:
        detail = {
            "source_dir": str(source_dir),
            "present_csv_tabs": present_csv_tabs,
            "missing_json_tabs": missing_json_tabs,
            "mismatched_tabs": mismatched,
        }
        print(json.dumps(detail, indent=2), file=sys.stderr)
        raise SystemExit(
            "Preflight raw-tab parity still mismatched after auto-mirror regeneration. "
            "Source snapshot contains stale canonical tab JSON files that did not self-heal."
        )
PY
  echo
fi

echo "[1/6] Exporting runtime artifacts into ${out_dir}"
scripts/prepare_runtime_exports.sh --out-dir "$out_dir" "${inputs[@]}"

echo

echo "[2/6] Verifying Run_Log CSV/JSON parity gate"
python3 scripts/verify_run_log_parity.py --export-dir "$out_dir"

echo

echo "[3/6] Verifying canonical runtime tab completeness gate (9 CSV + 9 JSON with row-count parity)"
python3 - "$out_dir" <<'PY'
import json
import sys
from pathlib import Path

repo_root = Path.cwd()
sys.path.insert(0, str(repo_root / "scripts"))
from preflight_guard import evaluate_raw_tab_completeness

export_dir = sys.argv[1]
result = evaluate_raw_tab_completeness(export_dir)
if not result.get("is_complete"):
    print(json.dumps(result, indent=2), file=sys.stderr)
    raise SystemExit(
        "Preflight raw-tab completeness gate failed: missing and/or mismatched canonical runtime tabs detected."
    )
PY

echo

echo "[4/6] Running run-id precheck gate"
precheck_args=("$run_id_a" "$run_id_b" --export-dir "$out_dir" --require-gate-prereqs)
if [[ "$allow_csv_only_triage" -eq 1 ]]; then
  precheck_args+=(--allow-csv-only-triage --allow-csv-only-triage-incident-tag "$incident_tag")
fi
python3 scripts/precheck_run_ids.py "${precheck_args[@]}"

echo

echo "[5/6] Canonical stageFetchPlayerStats sync (CSV -> JSON mirror + key-field validation)"
python3 scripts/sync_stage_player_stats_contract.py --export-dir "$out_dir" --run-id "$run_id_a" --run-id "$run_id_b" --stage stageFetchPlayerStats

echo

echo "[6/6] Re-running run-id precheck after sync"
python3 scripts/precheck_run_ids.py "${precheck_args[@]}"

python3 - "$out_dir" "$run_id_a" "$run_id_b" "$allow_csv_only_triage" "$incident_tag" <<'PY'
import sys
from pathlib import Path

repo_root = Path.cwd()
sys.path.insert(0, str(repo_root / "scripts"))
from preflight_guard import write_preflight_sidecar

out_dir, run_a, run_b, allow_csv_only, incident_tag = sys.argv[1:6]
sidecar_path = write_preflight_sidecar(
    out_dir,
    run_a,
    run_b,
    allow_csv_only_triage=allow_csv_only == "1",
    incident_tag=incident_tag,
)
print(f"Preflight sidecar written: {sidecar_path}")
PY

python3 - "$out_dir" "$precheck_pointer_path" <<'PY'
import datetime as dt
import json
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
pointer_path = Path(sys.argv[2])
manifest_path = out_dir / "runtime_export_manifest.json"

pointer_payload = {
    "generated_at_utc": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
    "status": "ok",
    "manifest_path": str(manifest_path),
    "manifest_exists": manifest_path.is_file(),
    "failure_artifact_path": None,
}
pointer_path.write_text(json.dumps(pointer_payload, indent=2) + "\n", encoding="utf-8")
print(f"Precheck export pointer written: {pointer_path}")
PY

echo

echo "Fail-fast preflight complete. Safe to run compare/gate commands only when summary/stage contract checklist reports compare_ready=true for both runs."
