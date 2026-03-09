#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/scan_runtime_diagnostics.sh <file-or-directory> [more paths...]

Scan exported runtime artifacts (Run_Log/State CSV or JSON) for diagnostic keys:
  - provider_returned_null_features
  - ta_h2h_empty_table
  - missing_stats
  - schedule_enrichment_h2h_missing

Examples:
  scripts/scan_runtime_diagnostics.sh ./exports/Run_Log.csv
  scripts/scan_runtime_diagnostics.sh ./exports/run_logs ./exports/state_dump.json
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ "$#" -eq 0 ]]; then
  echo "Error: no input paths provided. Pass explicit runtime artifact files/directories." >&2
  usage >&2
  exit 1
fi

python3 - "$@" <<'PY'
import csv
import json
import os
import sys
from collections import defaultdict

KEYS = [
    "provider_returned_null_features",
    "ta_h2h_empty_table",
    "missing_stats",
    "schedule_enrichment_h2h_missing",
]
MAX_EXAMPLES_PER_KEY = 5


def is_supported_file(path: str) -> bool:
    lower = path.lower()
    return lower.endswith('.csv') or lower.endswith('.json')


def collect_files(paths):
    files = []
    missing = []
    for p in paths:
        if not os.path.exists(p):
            missing.append(p)
            continue
        if os.path.isfile(p):
            if is_supported_file(p):
                files.append(os.path.abspath(p))
            continue
        for root, _, names in os.walk(p):
            for name in names:
                full = os.path.join(root, name)
                if os.path.isfile(full) and is_supported_file(full):
                    files.append(os.path.abspath(full))
    return sorted(set(files)), missing


def iter_json_records(path):
    with open(path, 'r', encoding='utf-8', errors='replace') as f:
        data = f.read().strip()
    if not data:
        return

    # Handle NDJSON first.
    ndjson_records = []
    ndjson_ok = True
    for idx, line in enumerate(data.splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        try:
            ndjson_records.append((idx, json.loads(line)))
        except Exception:
            ndjson_ok = False
            break
    if ndjson_ok and ndjson_records:
        for idx, record in ndjson_records:
            yield idx, record
        return

    # Fallback: regular JSON (array/object/scalar).
    parsed = json.loads(data)
    if isinstance(parsed, list):
        for idx, item in enumerate(parsed, start=1):
            yield idx, item
    else:
        yield 1, parsed


def iter_csv_records(path):
    with open(path, 'r', encoding='utf-8', errors='replace', newline='') as f:
        reader = csv.DictReader(f)
        if reader.fieldnames:
            for idx, row in enumerate(reader, start=2):
                yield idx, row
        else:
            f.seek(0)
            for idx, line in enumerate(f, start=1):
                yield idx, {'raw_line': line.rstrip('\n')}


def normalize_record_text(record):
    if isinstance(record, (str, int, float, bool)) or record is None:
        return str(record)
    try:
        return json.dumps(record, ensure_ascii=False, sort_keys=True)
    except Exception:
        return str(record)


def main():
    input_paths = sys.argv[1:]
    files, missing_paths = collect_files(input_paths)

    for p in missing_paths:
        print(f"Warning: path does not exist and was skipped: {p}", file=sys.stderr)

    if not files:
        print(
            "Error: no CSV/JSON runtime artifacts found in provided paths. "
            "Pass exported Run_Log/State files or directories containing them.",
            file=sys.stderr,
        )
        sys.exit(1)

    counts = defaultdict(int)
    examples = defaultdict(list)
    scanned_records = 0

    for path in files:
        lower = path.lower()
        try:
            records_iter = iter_csv_records(path) if lower.endswith('.csv') else iter_json_records(path)
            for row_num, record in records_iter:
                scanned_records += 1
                text = normalize_record_text(record)
                for key in KEYS:
                    if key in text:
                        counts[key] += 1
                        if len(examples[key]) < MAX_EXAMPLES_PER_KEY:
                            preview = text if len(text) <= 260 else text[:257] + '...'
                            examples[key].append((path, row_num, preview))
        except Exception as exc:
            print(f"Warning: failed to parse {path}: {exc}", file=sys.stderr)

    print("Runtime diagnostics scan")
    print(f"Scanned files: {len(files)}")
    print(f"Scanned records: {scanned_records}")
    print()

    print("Grouped counts")
    for key in KEYS:
        print(f"- {key}: {counts[key]}")

    print()
    print(f"Top matching rows (up to {MAX_EXAMPLES_PER_KEY} per key)")
    for key in KEYS:
        print(f"\n[{key}]")
        if not examples[key]:
            print("  (no matches)")
            continue
        for path, row_num, preview in examples[key]:
            print(f"  - {path}:{row_num} :: {preview}")


if __name__ == '__main__':
    main()
PY
