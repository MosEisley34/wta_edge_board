#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/export_runtime_artifacts.sh [--out-dir <dir>] <file-or-directory> [more paths...]

Copy Run_Log/State runtime artifacts (CSV/JSON) into a known export directory.
Default output directory: ./exports

Matching rules:
  - extension: .csv or .json
  - canonical basenames only: Run_Log(.csv/.json) and State(.csv/.json) (case-insensitive)

Batch contract:
  - Run_Log.csv, Run_Log.json, State.csv, and State.json are exported together from one source snapshot directory.
  - all four files are written with a shared batch timestamp to prevent mixed-staleness analysis.

Examples:
  scripts/export_runtime_artifacts.sh ./runtime_dump
  scripts/export_runtime_artifacts.sh --out-dir ./exports ./runtime/Run_Log.csv ./runtime/State.json
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
  echo "Error: no input paths provided for artifact export." >&2
  usage >&2
  exit 1
fi

python3 - "$out_dir" "${inputs[@]}" <<'PY'
import datetime as dt
import os
import re
import shutil
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
paths = [Path(p) for p in sys.argv[2:]]

name_ok = re.compile(r"(run[_-]?log|state)", re.IGNORECASE)
ext_ok = {".csv", ".json"}

missing = []
candidates = []
for p in paths:
    if not p.exists():
        missing.append(str(p))
        continue
    if p.is_file():
        candidates.append(p)
        continue
    for root, _, names in os.walk(p):
        root_p = Path(root)
        for name in names:
            candidates.append(root_p / name)

selected = []
for f in candidates:
    if not f.is_file():
        continue
    if f.suffix.lower() not in ext_ok:
        continue
    if not name_ok.search(f.stem):
        continue
    selected.append(f.resolve())

for p in missing:
    print(f"Warning: path does not exist and was skipped: {p}", file=sys.stderr)

if not selected:
    print(
        "Error: no Run_Log/State CSV/JSON artifacts found to export. "
        "Provide runtime artifact paths or directories containing those files.",
        file=sys.stderr,
    )
    sys.exit(1)

out_dir.mkdir(parents=True, exist_ok=True)

REMEDIATION = (
    "Remediation: re-export runtime artifacts so the source snapshot contains "
    "Run_Log.csv, Run_Log.json, State.csv, and State.json, then rerun "
    "scripts/prepare_runtime_exports.sh."
)


def _canonical_name(path: Path) -> str | None:
    lower_stem = path.stem.lower()
    if "note" in lower_stem or "manifest" in lower_stem:
        return None
    canonical_stems = {"run_log", "run-log", "runlog"}
    if lower_stem in canonical_stems:
        if path.suffix.lower() == ".json":
            return "Run_Log.json"
        if path.suffix.lower() == ".csv":
            return "Run_Log.csv"
        return None
    state_stems = {"state"}
    if lower_stem in state_stems:
        if path.suffix.lower() == ".json":
            return "State.json"
        if path.suffix.lower() == ".csv":
            return "State.csv"
    return None


def _pick_latest_complete_snapshot(paths: list[Path]) -> dict[str, Path]:
    by_parent: dict[Path, dict[str, list[Path]]] = {}
    for src in paths:
        canonical_name = _canonical_name(src)
        if canonical_name is None:
            continue
        parent_map = by_parent.setdefault(
            src.parent,
            {
                "Run_Log.csv": [],
                "Run_Log.json": [],
                "State.csv": [],
                "State.json": [],
            },
        )
        parent_map[canonical_name].append(src)

    complete: list[tuple[float, dict[str, Path]]] = []
    for grouped in by_parent.values():
        if any(not grouped[name] for name in grouped):
            continue
        chosen = {name: max(items, key=lambda p: p.stat().st_mtime) for name, items in grouped.items()}
        snapshot_mtime = max(path.stat().st_mtime for path in chosen.values())
        complete.append((snapshot_mtime, chosen))

    if not complete:
        raise RuntimeError(
            "Runtime artifact export failed: no source snapshot directory contains all required "
            "files (Run_Log.csv, Run_Log.json, State.csv, State.json).\n"
            f"{REMEDIATION}"
        )

    _, snapshot = max(complete, key=lambda row: row[0])
    return snapshot


selected_unique = sorted(set(selected))
snapshot_sources = _pick_latest_complete_snapshot(selected_unique)

written = []
for target_name in ("Run_Log.csv", "Run_Log.json", "State.csv", "State.json"):
    src = snapshot_sources[target_name]
    dst = out_dir / target_name
    shutil.copy2(src, dst)
    written.append((src, dst))

# Normalize exported artifact timestamps to one batch timestamp to keep the full batch aligned.
batch_export_ts = dt.datetime.now(tz=dt.timezone.utc).timestamp()
for _, dst in written:
    os.utime(dst, (batch_export_ts, batch_export_ts))

print(f"Exported runtime artifacts: {len(written)}")
print(f"Output directory: {out_dir.resolve()}")
for src, dst in written:
    print(f"- {src} -> {dst}")
PY
