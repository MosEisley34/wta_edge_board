#!/usr/bin/env python3
"""Mirror runtime CSV artifacts into JSON artifacts.

This utility converts canonical runtime CSV artifacts:
- Run_Log.csv -> Run_Log.json
- State.csv   -> State.json

Behavior contract:
- If CSV exists and is newer than JSON (or JSON is missing), mirror CSV to JSON.
- If CSV exists but is older/equal to JSON, keep JSON as the source of truth.
- If CSV is missing but JSON already exists, log a skip message and continue.
- If both CSV and JSON are missing for an artifact, exit with a remediation error.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

from runtime_artifact_codec import normalize_run_log_row


ARTIFACTS = (
    ("Run_Log", "Run_Log.csv", "Run_Log.json"),
    ("State", "State.csv", "State.json"),
)

RUN_LOG_SCHEMA_MAP: dict[str, str] = {
    "feature_completeness": "float",
    "matched_events": "int",
    "scored_signals": "int",
    "no_hit_no_events_from_source_count": "int",
    "no_hit_events_outside_time_window_count": "int",
    "no_hit_tournament_filter_excluded_count": "int",
    "no_hit_odds_present_but_match_failed_count": "int",
    "no_hit_schema_invalid_metrics_count": "int",
}

NUMERIC_FIELD_TOKENS = {
    "count",
    "counts",
    "total",
    "totals",
    "duration",
    "ms",
    "minute",
    "minutes",
    "hour",
    "hours",
    "lag",
    "matched",
    "scored",
    "signals",
    "score",
    "threshold",
    "streak",
    "coverage",
    "pct",
    "percent",
    "rate",
}
FLOAT_FIELD_TOKENS = {"rate", "pct", "percent", "coverage", "completeness"}
NON_NUMERIC_FIELD_TOKENS = {"id", "key", "hash"}
RUN_WARNING_CAP_PER_RUN = 10
_INT_LIKE_RE = re.compile(r"^[+-]?\d+$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Mirror Run_Log.csv/State.csv into Run_Log.json/State.json using a directory contract."
        )
    )
    parser.add_argument(
        "--input-dir",
        required=True,
        help="Directory containing runtime artifacts (Run_Log.csv / State.csv and/or JSON counterparts).",
    )
    parser.add_argument(
        "--out-dir",
        default="",
        help="Optional output directory for mirrored JSON files. Defaults to --input-dir.",
    )
    return parser.parse_args()


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _coerce_value_by_rule(value: Any, expected_type: str) -> tuple[Any, bool]:
    if value is None:
        return None, False
    if isinstance(value, str):
        value = value.strip()
        if value == "":
            return None, False
    if expected_type == "float":
        try:
            return float(value), False
        except (TypeError, ValueError):
            return None, True
    if expected_type == "int":
        if isinstance(value, bool):
            return None, True
        if isinstance(value, int):
            return value, False
        try:
            numeric = float(value)
            if numeric.is_integer():
                return int(numeric), False
        except (TypeError, ValueError):
            pass
        return None, True
    return value, False


def _is_type_valid(value: Any, expected_type: str) -> bool:
    if value is None:
        return True
    if expected_type == "float":
        return isinstance(value, float)
    if expected_type == "int":
        return isinstance(value, int) and not isinstance(value, bool)
    return True


def _infer_numeric_field_type(field: str, value: Any) -> str | None:
    if field in RUN_LOG_SCHEMA_MAP:
        return RUN_LOG_SCHEMA_MAP[field]
    if value is None:
        return None
    text = value.strip() if isinstance(value, str) else str(value).strip()
    if not text:
        return None
    lowered_field = field.lower()
    tokens = [token for token in lowered_field.split("_") if token]
    if any(token in NON_NUMERIC_FIELD_TOKENS for token in tokens):
        return None
    if not any(token in NUMERIC_FIELD_TOKENS for token in tokens):
        return None
    if any(token in FLOAT_FIELD_TOKENS for token in tokens):
        return "float"
    if _INT_LIKE_RE.match(text):
        return "int"
    try:
        numeric = float(text)
    except (TypeError, ValueError):
        return "int"
    return "int" if numeric.is_integer() else "float"


def _emit_schema_warning_summary(violation_counts: Counter[tuple[str, str, str]]) -> None:
    if not violation_counts:
        return
    per_run_counts: Counter[str] = Counter()
    for (run_id, _, _), count in violation_counts.items():
        per_run_counts[run_id] += count

    print(
        "Warning: Run_Log export type violations detected. "
        f"Summary (deduped by run_id/stage/field; max {RUN_WARNING_CAP_PER_RUN} rows per run):",
        file=sys.stderr,
    )
    print("  run_id | stage | field | occurrences", file=sys.stderr)
    print("  ------ | ----- | ----- | -----------", file=sys.stderr)

    emitted_per_run: Counter[str] = Counter()
    suppressed_per_run: Counter[str] = Counter()
    for run_id, stage, field in sorted(violation_counts):
        count = violation_counts[(run_id, stage, field)]
        if emitted_per_run[run_id] >= RUN_WARNING_CAP_PER_RUN:
            suppressed_per_run[run_id] += count
            continue
        print(
            f"  {run_id or '<missing>'} | {stage or '<missing>'} | {field} | {count}",
            file=sys.stderr,
        )
        emitted_per_run[run_id] += 1

    if suppressed_per_run:
        print("  ... | ... | ... | ...", file=sys.stderr)
        for run_id in sorted(suppressed_per_run):
            print(
                "  "
                f"{run_id or '<missing>'} | <suppressed> | <suppressed> | {suppressed_per_run[run_id]}",
                file=sys.stderr,
            )

    print("  totals by run_id:", file=sys.stderr)
    for run_id in sorted(per_run_counts):
        print(f"  - {run_id or '<missing>'}: {per_run_counts[run_id]} violation(s)", file=sys.stderr)


def _apply_run_log_typing(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    violation_counts: Counter[tuple[str, str, str]] = Counter()
    for idx, row in enumerate(rows, start=1):
        converted: dict[str, Any] = normalize_run_log_row(dict(row))
        schema_errors: list[dict[str, Any]] = []
        for field, raw_value in tuple(converted.items()):
            expected_type = _infer_numeric_field_type(field, raw_value)
            if not expected_type:
                continue
            raw_value = converted.get(field)
            coerced_value, had_violation = _coerce_value_by_rule(raw_value, expected_type)
            converted[field] = coerced_value
            if had_violation:
                run_id = str(converted.get("run_id") or "")
                stage = str(converted.get("stage") or "")
                violation_counts[(run_id, stage, field)] += 1
                violation = {
                    "field": field,
                    "expected": f"{expected_type}_or_null",
                    "actual": type(raw_value).__name__,
                    "raw": raw_value,
                    "action": "set_null",
                }
                schema_errors.append(violation)
        if schema_errors:
            converted["schema_violation"] = {
                "artifact": "Run_Log",
                "code": "run_log_row_schema_violation",
                "row": idx,
                "errors": schema_errors,
            }
        normalized.append(converted)
    _emit_schema_warning_summary(violation_counts)
    return normalized


def _write_json_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(rows, handle, indent=2)
        handle.write("\n")


def _is_newer(source: Path, target: Path) -> bool:
    return source.stat().st_mtime > target.stat().st_mtime


def main() -> int:
    args = parse_args()

    input_dir = Path(args.input_dir).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve() if args.out_dir else input_dir

    if not input_dir.is_dir():
        print(f"Error: --input-dir is not a directory: {input_dir}", file=sys.stderr)
        return 1

    missing_both: list[str] = []

    for label, csv_name, json_name in ARTIFACTS:
        csv_path = input_dir / csv_name
        out_json_path = out_dir / json_name
        input_json_path = input_dir / json_name

        if csv_path.is_file():
            existing_json = out_json_path if out_json_path.is_file() else input_json_path
            should_mirror_from_csv = not existing_json.is_file() or _is_newer(csv_path, existing_json)

            if should_mirror_from_csv:
                rows = _read_csv_rows(csv_path)
                if label == "Run_Log":
                    rows = _apply_run_log_typing(rows)
                _write_json_rows(out_json_path, rows)
                print(f"Mirrored {csv_path} -> {out_json_path} ({len(rows)} rows)")
            else:
                if existing_json != out_json_path:
                    out_json_path.parent.mkdir(parents=True, exist_ok=True)
                    out_json_path.write_text(existing_json.read_text(encoding='utf-8'), encoding='utf-8')
                    print(
                        "Skip: "
                        f"{csv_path} is not newer than JSON; reused {existing_json} -> {out_json_path}"
                    )
                else:
                    print(
                        "Skip: "
                        f"{csv_path} is not newer than JSON; keeping existing JSON {existing_json}"
                    )
            continue

        if out_json_path.is_file() or input_json_path.is_file():
            existing = out_json_path if out_json_path.is_file() else input_json_path
            if existing != out_json_path:
                out_json_path.parent.mkdir(parents=True, exist_ok=True)
                out_json_path.write_text(existing.read_text(encoding="utf-8"), encoding="utf-8")
                print(
                    f"Skip: {csv_path} is missing; reused existing JSON {existing} -> {out_json_path}"
                )
            else:
                print(f"Skip: {csv_path} is missing; keeping existing JSON {existing}")
            continue

        missing_both.append(label)

    if missing_both:
        missing_list = ", ".join(missing_both)
        print(
            "Error: missing both CSV and JSON runtime artifacts for: "
            f"{missing_list}.\n"
            f"Remediation: provide {input_dir}/Run_Log.csv and/or {input_dir}/State.csv, "
            "or place existing Run_Log.json/State.json under the input directory "
            "(or --out-dir when reusing an already-mirrored batch) and rerun.",
            file=sys.stderr,
        )
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
