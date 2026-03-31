#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/export_runtime_artifacts.sh [--out-dir <dir>] <file-or-directory> [more paths...]

Copy canonical runtime tab artifacts (CSV/JSON) into a known export directory.
Default output directory: ./exports

Matching rules:
  - extension: .csv or .json
  - canonical basenames only: Config, Run_Log, Raw_Odds, Raw_Schedule, Raw_Player_Stats, Match_Map, Signals, State, ProviderHealth (.csv/.json), case-insensitive

Batch contract:
  - Run_Log.csv, Run_Log.json, State.csv, and State.json are exported together from one source snapshot directory.
  - any additional canonical tab artifacts found in that same snapshot directory are also exported.
  - all exported files are written with a shared batch timestamp to prevent mixed-staleness analysis.

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

name_ok = re.compile(r"(config|run[_-]?log|raw[_-]?odds|raw[_-]?schedule|raw[_-]?player[_-]?stats|match[_-]?map|signals|state|provider[_-]?health)", re.IGNORECASE)
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
    stem_aliases = {
        "config": "Config",
        "run_log": "Run_Log",
        "run-log": "Run_Log",
        "runlog": "Run_Log",
        "raw_odds": "Raw_Odds",
        "raw-odds": "Raw_Odds",
        "raw_schedule": "Raw_Schedule",
        "raw-schedule": "Raw_Schedule",
        "raw_player_stats": "Raw_Player_Stats",
        "raw-player-stats": "Raw_Player_Stats",
        "match_map": "Match_Map",
        "match-map": "Match_Map",
        "signals": "Signals",
        "state": "State",
        "providerhealth": "ProviderHealth",
        "provider_health": "ProviderHealth",
        "provider-health": "ProviderHealth",
    }
    canonical_stem = stem_aliases.get(lower_stem)
    if not canonical_stem:
        return None
    suffix = path.suffix.lower()
    if suffix == ".json":
        return f"{canonical_stem}.json"
    if suffix == ".csv":
        return f"{canonical_stem}.csv"
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
        parent_map.setdefault(canonical_name, []).append(src)

    complete: list[tuple[float, dict[str, Path]]] = []
    for grouped in by_parent.values():
        required_names = ("Run_Log.csv", "Run_Log.json", "State.csv", "State.json")
        if any(not grouped.get(name) for name in required_names):
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

snapshot_dir = next(iter(snapshot_sources.values())).parent
canonical_by_name: dict[str, list[Path]] = {}
for item in snapshot_dir.iterdir():
    if not item.is_file():
        continue
    canonical_name = _canonical_name(item)
    if canonical_name is None:
        continue
    canonical_by_name.setdefault(canonical_name, []).append(item)

written = []
for target_name in sorted(canonical_by_name):
    src = max(canonical_by_name[target_name], key=lambda p: p.stat().st_mtime)
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
