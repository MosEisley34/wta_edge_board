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

used_names: dict[str, int] = {}
written = []
for src in sorted(set(selected)):
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
