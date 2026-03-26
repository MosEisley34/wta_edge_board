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
  - basename contains "run_log" or "state" (case-insensitive)

Run_Log contract:
  - Run_Log.csv and Run_Log.json are exported as a paired snapshot from the same latest source directory.
  - both are written with a shared batch timestamp to prevent mixed-staleness analysis.

Examples:
  scripts/export_runtime_artifacts.sh ./runtime_dump
  scripts/export_runtime_artifacts.sh --out-dir ./exports ./runtime/Run_Log.csv ./runtime/state_dump.json
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
import os
import re
import shutil
import sys
import datetime as dt
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

def _canonical_run_log_name(path: Path) -> str | None:
    lower_stem = path.stem.lower()
    if "note" in lower_stem or "manifest" in lower_stem:
        return None
    canonical_stems = {"run_log", "run-log", "runlog"}
    if lower_stem not in canonical_stems:
        return None
    if path.suffix.lower() == ".json":
        return "Run_Log.json"
    if path.suffix.lower() == ".csv":
        return "Run_Log.csv"
    return None


def _pick_latest_run_log_pair(paths: list[Path]) -> tuple[Path, Path]:
    by_parent: dict[Path, dict[str, list[Path]]] = {}
    for src in paths:
        canonical_name = _canonical_run_log_name(src)
        if canonical_name is None:
            continue
        parent_map = by_parent.setdefault(src.parent, {"csv": [], "json": []})
        parent_map[src.suffix.lower().lstrip(".")].append(src)

    paired: list[tuple[float, Path, Path]] = []
    for _, grouped in by_parent.items():
        csv_paths = grouped["csv"]
        json_paths = grouped["json"]
        if not csv_paths or not json_paths:
            continue
        latest_csv = max(csv_paths, key=lambda p: p.stat().st_mtime)
        latest_json = max(json_paths, key=lambda p: p.stat().st_mtime)
        snapshot_mtime = max(latest_csv.stat().st_mtime, latest_json.stat().st_mtime)
        paired.append((snapshot_mtime, latest_csv, latest_json))

    if not paired:
        raise RuntimeError(
            "Run_Log export failed: could not find a directory snapshot containing both "
            "Run_Log CSV and JSON. Export both from the same source snapshot and retry."
        )

    _, csv_src, json_src = max(paired, key=lambda row: row[0])
    return csv_src, json_src


selected_unique = sorted(set(selected))
run_log_csv_src, run_log_json_src = _pick_latest_run_log_pair(selected_unique)

written = []
for src, target_name in (
    (run_log_csv_src, "Run_Log.csv"),
    (run_log_json_src, "Run_Log.json"),
):
    dst = out_dir / target_name
    shutil.copy2(src, dst)
    written.append((src, dst))

# Normalize exported Run_Log timestamps to one batch timestamp to keep CSV+JSON aligned.
batch_export_ts = dt.datetime.now(tz=dt.timezone.utc).timestamp()
for _, dst in written:
    os.utime(dst, (batch_export_ts, batch_export_ts))

# Copy State artifacts after canonical Run_Log export. Keep all candidates but deduplicate names.
state_selected = [
    src for src in selected_unique
    if src.suffix.lower() in ext_ok and "state" in src.stem.lower()
]
used_names: dict[str, int] = {"Run_Log.csv": 1, "Run_Log.json": 1}
for src in state_selected:
    base = src.name
    n = used_names.get(base, 0)
    if n == 0:
        target_name = base
    else:
        stem = src.stem
        suffix = src.suffix
        target_name = f"{stem}__{n+1}{suffix}"
    used_names[base] = n + 1

    dst = out_dir / target_name
    shutil.copy2(src, dst)
    written.append((src, dst))

print(f"Exported runtime artifacts: {len(written)}")
print(f"Output directory: {out_dir.resolve()}")
for src, dst in written:
    print(f"- {src} -> {dst}")
PY
