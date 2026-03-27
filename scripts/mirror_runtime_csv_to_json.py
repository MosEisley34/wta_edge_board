#!/usr/bin/env python3
"""Mirror runtime CSV artifacts into JSON artifacts.

This utility converts canonical runtime CSV artifacts:
- Run_Log.csv -> Run_Log.json
- State.csv   -> State.json

Behavior contract:
- If CSV exists, read and mirror it to JSON.
- If CSV is missing but JSON already exists, log a skip message and continue.
- If both CSV and JSON are missing for an artifact, exit with a remediation error.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any


ARTIFACTS = (
    ("Run_Log", "Run_Log.csv", "Run_Log.json"),
    ("State", "State.csv", "State.json"),
)

RUN_LOG_TYPE_RULES: dict[str, type] = {
    "feature_completeness": float,
    "matched_events": int,
    "scored_signals": int,
    "no_hit_no_events_from_source_count": int,
    "no_hit_events_outside_time_window_count": int,
    "no_hit_tournament_filter_excluded_count": int,
    "no_hit_odds_present_but_match_failed_count": int,
    "no_hit_schema_invalid_metrics_count": int,
}
RUN_LOG_NUMERIC_METRIC_FIELDS = ("feature_completeness", "matched_events", "scored_signals")


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


def _coerce_value_by_rule(value: Any, expected_type: type) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if value == "":
            return None
    if expected_type is float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return value
    if expected_type is int:
        if isinstance(value, bool):
            return value
        if isinstance(value, int):
            return value
        try:
            numeric = float(value)
            if numeric.is_integer():
                return int(numeric)
        except (TypeError, ValueError):
            pass
        return value
    return value


def _is_type_valid(value: Any, expected_type: type) -> bool:
    if value is None:
        return True
    if expected_type is float:
        return isinstance(value, float)
    if expected_type is int:
        return isinstance(value, int) and not isinstance(value, bool)
    return isinstance(value, expected_type)


def _apply_run_log_typing(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for idx, row in enumerate(rows, start=1):
        converted: dict[str, Any] = dict(row)
        schema_errors: list[dict[str, Any]] = []
        for field, expected_type in RUN_LOG_TYPE_RULES.items():
            converted[field] = _coerce_value_by_rule(converted.get(field), expected_type)
            if not _is_type_valid(converted.get(field), expected_type):
                run_id = str(converted.get("run_id") or "")
                stage = str(converted.get("stage") or "")
                print(
                    (
                        "Warning: Run_Log export type violation "
                        f"run_id={run_id or '<missing>'} stage={stage or '<missing>'} "
                        f"field={field} expected={expected_type.__name__}|null "
                        f"actual={type(converted.get(field)).__name__} row={idx}"
                    ),
                    file=sys.stderr,
                )
        _sanitize_feature_completeness_shape(converted, row_number=idx, schema_errors=schema_errors)
        _sanitize_quality_contract_numeric_metrics(converted, row_number=idx, schema_errors=schema_errors)
        if schema_errors:
            converted["schema_error"] = {
                "artifact": "Run_Log",
                "code": "run_log_row_schema_violation",
                "row": idx,
                "errors": schema_errors,
            }
        normalized.append(converted)
    return normalized


def _sanitize_feature_completeness_shape(
    row: dict[str, Any], row_number: int, schema_errors: list[dict[str, Any]] | None = None
) -> None:
    value = row.get("feature_completeness")
    if value is None or value == "":
        row["feature_completeness"] = None
        return
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return

    detail = None
    if isinstance(value, str):
        trimmed = value.strip()
        if trimmed.startswith("{") or trimmed.startswith("["):
            try:
                detail = json.loads(trimmed)
            except Exception:
                detail = {"raw": trimmed}
    elif isinstance(value, dict):
        detail = value

    if detail is not None:
        row["feature_completeness_detail"] = detail
        row["reason_alias_payload"] = detail

    row["feature_completeness"] = None
    row["schema_violation"] = "run_log_row_schema_violation"
    row["field_type_error"] = "feature_completeness_expected_numeric_or_null"
    if isinstance(schema_errors, list):
        schema_errors.append(
            {
                "field": "feature_completeness",
                "expected": "numeric_or_null",
                "actual": type(value).__name__,
                "action": "sanitized",
            }
        )
    run_id = str(row.get("run_id") or "")
    stage = str(row.get("stage") or "")
    print(
        (
            "Warning: Run_Log schema violation "
            f"run_id={run_id or '<missing>'} stage={stage or '<missing>'} "
            f"field=feature_completeness row={row_number}"
        ),
        file=sys.stderr,
    )


def _capture_non_scalar_metric_detail(
    row: dict[str, Any],
    field: str,
    raw_value: Any,
    parsed_value: Any,
) -> None:
    detail_field = f"{field}_detail"
    detail_json_field = f"{field}_detail_json"
    row[detail_field] = parsed_value if isinstance(parsed_value, (dict, list)) else raw_value
    try:
        row[detail_json_field] = json.dumps(row[detail_field], sort_keys=True)
    except Exception:
        row[detail_json_field] = str(raw_value)


def _sanitize_quality_contract_numeric_metrics(
    row: dict[str, Any], row_number: int, schema_errors: list[dict[str, Any]]
) -> None:
    for field in RUN_LOG_NUMERIC_METRIC_FIELDS:
        value = row.get(field)
        if value is None or value == "":
            row[field] = None
            continue
        if isinstance(value, bool):
            parsed_value: Any = value
        elif isinstance(value, str):
            stripped = value.strip()
            if stripped == "":
                row[field] = None
                continue
            if stripped.startswith("{") or stripped.startswith("["):
                try:
                    parsed_value = json.loads(stripped)
                except Exception:
                    parsed_value = stripped
            else:
                parsed_value = stripped
        else:
            parsed_value = value

        if field == "feature_completeness":
            if isinstance(parsed_value, (int, float)) and not isinstance(parsed_value, bool):
                row[field] = float(parsed_value)
                continue
        else:
            if isinstance(parsed_value, int) and not isinstance(parsed_value, bool):
                row[field] = parsed_value
                continue
            if isinstance(parsed_value, float) and parsed_value.is_integer():
                row[field] = int(parsed_value)
                continue

        _capture_non_scalar_metric_detail(row, field, value, parsed_value)
        row[field] = None
        row["schema_violation"] = "run_log_row_schema_violation"
        row["field_type_error"] = f"{field}_expected_numeric_or_null"
        schema_errors.append(
            {
                "field": field,
                "expected": "numeric_or_null",
                "actual": type(parsed_value).__name__,
                "action": "sanitized",
            }
        )
        run_id = str(row.get("run_id") or "")
        stage = str(row.get("stage") or "")
        print(
            (
                "Warning: Run_Log schema violation "
                f"run_id={run_id or '<missing>'} stage={stage or '<missing>'} "
                f"field={field} row={row_number}"
            ),
            file=sys.stderr,
        )


def _write_json_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(rows, handle, indent=2)
        handle.write("\n")


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
            rows = _read_csv_rows(csv_path)
            if label == "Run_Log":
                rows = _apply_run_log_typing(rows)
            _write_json_rows(out_json_path, rows)
            print(f"Mirrored {csv_path} -> {out_json_path} ({len(rows)} rows)")
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
