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
RUN_HEALTH_SAMPLE_LIMIT = 5
RUN_HEALTH_REQUIRED_FIELDS = [
    "run_health_contract_version",
    "reason_code",
    "blocker_counts",
    "stage_skipped_reason_counts",
    "dominant_blocker_categories",
    "sampled_blocked_records",
    "sample_unmatched_events",
]
PLAYER_STATS_OVERLAP_WARN_THRESHOLD = 0.6


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


def normalize_reason_metadata(record):
    reason_metadata = record.get('reason_metadata')
    if isinstance(reason_metadata, dict):
        return reason_metadata
    if isinstance(reason_metadata, str) and reason_metadata.strip():
        try:
            parsed = json.loads(reason_metadata)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return {}
    return {}


def ingest_player_stats_coverage(reason_metadata, coverage_rows):
    if not isinstance(reason_metadata, dict):
        return
    try:
        requested = int(float(reason_metadata.get('requested_player_count')))
        resolved = int(float(reason_metadata.get('resolved_player_count')))
        unresolved = int(float(reason_metadata.get('unresolved_player_count')))
        overlap = float(reason_metadata.get('overlap_ratio'))
    except (TypeError, ValueError):
        return
    if requested < 0 or resolved < 0 or unresolved < 0:
        return
    top_unresolved = reason_metadata.get('top_unresolved_player_samples')
    coverage_rows.append({
        'requested': requested,
        'resolved': resolved,
        'unresolved': unresolved,
        'overlap': overlap,
        'top_unresolved': top_unresolved if isinstance(top_unresolved, list) else [],
    })


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
    run_health_contract_counts = defaultdict(int)
    run_health_stage_skipped_totals = defaultdict(float)
    run_health_blocker_totals = defaultdict(float)
    run_health_dominant = defaultdict(float)
    run_health_samples = []
    run_health_missing_fields = defaultdict(int)
    player_stats_coverage_rows = []
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

                stage = str(record.get('stage') or '')
                if stage == 'stageFetchPlayerStats':
                    ingest_player_stats_coverage(normalize_reason_metadata(record), player_stats_coverage_rows)
                if stage == 'runEdgeBoard':
                    stage_summaries = record.get('stage_summaries')
                    if isinstance(stage_summaries, str):
                        try:
                            stage_summaries = json.loads(stage_summaries)
                        except Exception:
                            stage_summaries = None
                    if isinstance(stage_summaries, list):
                        for summary in stage_summaries:
                            if not isinstance(summary, dict):
                                continue
                            if str(summary.get('stage') or '') != 'stageFetchPlayerStats':
                                continue
                            ingest_player_stats_coverage(normalize_reason_metadata(summary), player_stats_coverage_rows)
                payload = None
                if stage == 'run_health_guard':
                    message = record.get('message')
                    if isinstance(message, dict):
                        payload = message
                    elif isinstance(message, str) and message.strip():
                        try:
                            parsed = json.loads(message)
                            if isinstance(parsed, dict):
                                payload = parsed
                        except Exception:
                            payload = None
                if isinstance(payload, dict):
                    contract_version = int(payload.get('run_health_contract_version') or 0)
                    run_health_contract_counts[contract_version] += 1

                    for field in RUN_HEALTH_REQUIRED_FIELDS:
                        if field not in payload:
                            run_health_missing_fields[field] += 1
                            continue
                        value = payload.get(field)
                        if value is None:
                            run_health_missing_fields[field] += 1

                    for key, value in (payload.get('stage_skipped_reason_counts') or {}).items():
                        try:
                            numeric = float(value)
                        except (TypeError, ValueError):
                            continue
                        if numeric > 0:
                            run_health_stage_skipped_totals[str(key)] += numeric

                    blocker_counts_payload = payload.get('blocker_counts') or {}
                    for key in (
                        'opening_lag_blocked_count',
                        'schedule_only_seed_count',
                        'no_odds_stage_count',
                        'stale_odds_skip_count',
                        'low_edge_suppressed_count',
                        'cooldown_suppressed_count',
                        'stats_zero_coverage_count',
                    ):
                        raw_value = blocker_counts_payload.get(key, payload.get(key))
                        try:
                            numeric = float(raw_value or 0)
                        except (TypeError, ValueError):
                            numeric = 0
                        if numeric > 0:
                            run_health_blocker_totals[key] += numeric

                    for entry in payload.get('dominant_blocker_categories') or []:
                        if not isinstance(entry, dict):
                            continue
                        category = str(entry.get('category') or '')
                        if not category:
                            continue
                        try:
                            numeric = float(entry.get('count') or 0)
                        except (TypeError, ValueError):
                            continue
                        if numeric > 0:
                            run_health_dominant[category] += numeric

                    if len(run_health_samples) < RUN_HEALTH_SAMPLE_LIMIT:
                        run_health_samples.append({
                            'path': path,
                            'row': row_num,
                            'reason_code': str(payload.get('reason_code') or ''),
                            'sampled_blocked_records': payload.get('sampled_blocked_records') or payload.get('sampled_blocked_odds') or [],
                            'sample_unmatched_events': payload.get('sample_unmatched_events') or [],
                        })
        except Exception as exc:
            print(f"Warning: failed to parse {path}: {exc}", file=sys.stderr)

    print("Runtime diagnostics scan")
    print(f"Scanned files: {len(files)}")
    print(f"Scanned records: {scanned_records}")
    print()

    print("Run-health degraded contract (first-pass triage)")
    total_run_health_records = sum(run_health_contract_counts.values())
    print(f"- degraded run_health_guard records: {total_run_health_records}")
    if total_run_health_records:
        versions = ", ".join(
            f"v{version}:{count}" for version, count in sorted(run_health_contract_counts.items())
        )
        print(f"- contract versions: {versions}")

        if run_health_missing_fields:
            missing_part = "; ".join(
                f"{field}:{count}" for field, count in sorted(run_health_missing_fields.items(), key=lambda item: (-item[1], item[0]))
            )
        else:
            missing_part = "none"
        print(f"- contract field gaps: {missing_part}")

        if run_health_blocker_totals:
            blockers = sorted(run_health_blocker_totals.items(), key=lambda item: (-item[1], item[0]))
            blocker_part = "; ".join(
                f"{k}:{int(v) if float(v).is_integer() else round(v, 2)}" for k, v in blockers
            )
        else:
            blocker_part = "none"
        print(f"- blocker counts: {blocker_part}")

        if run_health_dominant:
            dominant = sorted(run_health_dominant.items(), key=lambda item: (-item[1], item[0]))[:5]
            dominant_part = "; ".join(
                f"{k}:{int(v) if float(v).is_integer() else round(v, 2)}" for k, v in dominant
            )
        else:
            dominant_part = "none"
        print(f"- dominant categories: {dominant_part}")

        if run_health_stage_skipped_totals:
            skipped = sorted(run_health_stage_skipped_totals.items(), key=lambda item: (-item[1], item[0]))[:8]
            skipped_part = "; ".join(
                f"{k}:{int(v) if float(v).is_integer() else round(v, 2)}" for k, v in skipped
            )
        else:
            skipped_part = "none"
        print(f"- stage-skipped reason rollups: {skipped_part}")

        print(f"- sampled blocked records (up to {RUN_HEALTH_SAMPLE_LIMIT})")
        for sample in run_health_samples:
            blocked = sample['sampled_blocked_records']
            unmatched = sample['sample_unmatched_events']
            blocked_preview = blocked[0] if blocked else {}
            unmatched_preview = unmatched[0] if unmatched else {}
            print(
                f"  - {sample['path']}:{sample['row']} reason={sample['reason_code']} "
                f"blocked_odds={json.dumps(blocked_preview, ensure_ascii=False, sort_keys=True)} "
                f"unmatched={json.dumps(unmatched_preview, ensure_ascii=False, sort_keys=True)}"
            )
    else:
        print("- none")
    print()

    print("Player-stats acquisition coverage")
    if player_stats_coverage_rows:
        latest = player_stats_coverage_rows[-1]
        low_overlap_rows = [row for row in player_stats_coverage_rows if row['overlap'] < PLAYER_STATS_OVERLAP_WARN_THRESHOLD]
        avg_overlap = sum(row['overlap'] for row in player_stats_coverage_rows) / len(player_stats_coverage_rows)
        print(f"- rows with coverage metadata: {len(player_stats_coverage_rows)}")
        print(
            f"- latest coverage requested={latest['requested']} resolved={latest['resolved']} "
            f"unresolved={latest['unresolved']} overlap_ratio={latest['overlap']:.3f}"
        )
        print(
            f"- overlap warning threshold={PLAYER_STATS_OVERLAP_WARN_THRESHOLD:.2f} "
            f"rows_below_threshold={len(low_overlap_rows)} avg_overlap={avg_overlap:.3f}"
        )
        unresolved_samples = latest.get('top_unresolved') or []
        print(
            "- latest unresolved player samples: "
            + (", ".join(str(name) for name in unresolved_samples[:5]) if unresolved_samples else "none")
        )
    else:
        print("- none")
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
