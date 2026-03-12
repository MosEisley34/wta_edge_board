#!/usr/bin/env python3
"""Emit a compact, deterministic summary for runtime diagnostics artifacts."""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any
import re

from pipeline_log_adapter import adapt_run_log_record_for_legacy

SUPPORTED_EXTENSIONS = (".csv", ".json")
WATCHDOG_METRIC_KEYS = ("streak_count", "consecutive_empty_cycles", "diagnostics_counter")
NON_SUCCESS_STATUSES = {"warning", "failed", "error", "notice"}


def _parse_json_like(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if not isinstance(value, str) or not value:
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None


def _is_supported_file(path: str) -> bool:
    return path.lower().endswith(SUPPORTED_EXTENSIONS)


def _collect_files(paths: list[str]) -> tuple[list[str], list[str]]:
    files: list[str] = []
    missing: list[str] = []
    for path in paths:
        if not os.path.exists(path):
            missing.append(path)
            continue
        if os.path.isfile(path):
            if _is_supported_file(path):
                files.append(os.path.abspath(path))
            continue
        for root, _, names in os.walk(path):
            for name in names:
                full = os.path.join(root, name)
                if os.path.isfile(full) and _is_supported_file(full):
                    files.append(os.path.abspath(full))
    return sorted(set(files)), missing


def _iter_json_records(path: str):
    with open(path, "r", encoding="utf-8", errors="replace") as handle:
        data = handle.read().strip()
    if not data:
        return

    ndjson = []
    ndjson_ok = True
    for idx, line in enumerate(data.splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        try:
            ndjson.append((idx, json.loads(line)))
        except json.JSONDecodeError:
            ndjson_ok = False
            break
    if ndjson_ok and ndjson:
        for idx, record in ndjson:
            yield idx, record
        return

    parsed = json.loads(data)
    if isinstance(parsed, list):
        for idx, row in enumerate(parsed, start=1):
            yield idx, row
    else:
        yield 1, parsed


def _iter_csv_records(path: str):
    with open(path, "r", encoding="utf-8", errors="replace", newline="") as handle:
        reader = csv.DictReader(handle)
        for idx, row in enumerate(reader, start=2):
            yield idx, row


def _normalize_record(record: dict[str, Any]) -> dict[str, Any]:
    if int(record.get("schema_version") or 0) == 2 or "et" in record:
        return adapt_run_log_record_for_legacy(record)
    return dict(record)


def _parse_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    normalized = value
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    normalized = re.sub(r"Z([+-]\d\d:\d\d)$", r"\1", normalized)
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _format_ms(values: list[int]) -> str:
    sorted_values = sorted(values)
    avg = sum(sorted_values) / len(sorted_values)
    idx = max(0, min(len(sorted_values) - 1, math.ceil(0.95 * len(sorted_values)) - 1))
    p95 = sorted_values[idx]
    return f"min={sorted_values[0]},avg={avg:.1f},p95={p95}"


def build_summary(paths: list[str], top_n: int, max_stages: int, warning_limit: int) -> list[str]:
    files, missing = _collect_files(paths)
    if missing:
        raise FileNotFoundError(f"Missing input paths: {', '.join(missing)}")
    if not files:
        raise FileNotFoundError("No CSV/JSON runtime artifacts found in input paths.")

    run_summary_status: dict[str, tuple[int, str]] = {}
    run_last_status: dict[str, tuple[int, str]] = {}
    status_counts: Counter[str] = Counter()
    reason_totals: defaultdict[str, float] = defaultdict(float)
    fallback_reason_counts: Counter[str] = Counter()
    stage_durations: defaultdict[str, list[int]] = defaultdict(list)
    watchdog_points: list[tuple[int, int, str]] = []
    warning_reasons: Counter[str] = Counter()

    row_idx = 0
    for path in files:
        is_csv = path.lower().endswith(".csv")
        iterator = _iter_csv_records(path) if is_csv else _iter_json_records(path)
        if iterator is None:
            continue
        for _, raw_record in iterator:
            row_idx += 1
            if not isinstance(raw_record, dict):
                continue
            record = _normalize_record(raw_record)
            run_id = str(record.get("run_id") or "")
            stage = str(record.get("stage") or "")
            status = str(record.get("status") or "unknown") or "unknown"
            reason_code = str(record.get("reason_code") or "")

            if run_id:
                run_last_status[run_id] = (row_idx, status)

            if str(record.get("row_type") or "") == "summary" or stage == "runEdgeBoard":
                if run_id:
                    run_summary_status[run_id] = (row_idx, status)

            fallback_reason_counts[reason_code] += 1 if reason_code else 0

            row_type = str(record.get("row_type") or "")

            reason_codes = record.get("reason_codes")
            if isinstance(reason_codes, str):
                parsed_reasons = _parse_json_like(reason_codes)
                reason_codes = parsed_reasons if isinstance(parsed_reasons, dict) else None
            message = _parse_json_like(record.get("message"))
            if not isinstance(reason_codes, dict) and isinstance(message, dict):
                reason_codes = message.get("reason_codes") if isinstance(message.get("reason_codes"), dict) else None
            if isinstance(reason_codes, dict) and row_type == "stage":
                for code, value in sorted(reason_codes.items(), key=lambda item: str(item[0])):
                    try:
                        numeric = float(value)
                    except (TypeError, ValueError):
                        continue
                    if numeric > 0:
                        reason_totals[str(code)] += numeric

            start = _parse_timestamp(record.get("started_at"))
            end = _parse_timestamp(record.get("ended_at"))
            if start and end and stage.startswith("stage"):
                duration_ms = int((end - start).total_seconds() * 1000)
                if duration_ms >= 0:
                    stage_durations[stage].append(duration_ms)

            stage_summaries = record.get("stage_summaries")
            if isinstance(stage_summaries, str):
                parsed_summaries = _parse_json_like(stage_summaries)
                stage_summaries = parsed_summaries if isinstance(parsed_summaries, list) else None
            if isinstance(stage_summaries, list):
                for summary in stage_summaries:
                    if not isinstance(summary, dict):
                        continue
                    summary_stage = str(summary.get("stage") or "")
                    if not summary_stage.startswith("stage"):
                        continue
                    try:
                        duration_ms = int(float(summary.get("duration_ms") or 0))
                    except (TypeError, ValueError):
                        continue
                    if duration_ms >= 0:
                        stage_durations[summary_stage].append(duration_ms)

            is_watchdog = "watchdog" in stage or reason_code == "watchdog_recovered"
            if is_watchdog:
                metric = None
                if isinstance(message, dict):
                    for key in WATCHDOG_METRIC_KEYS:
                        if key in message:
                            try:
                                metric = int(float(message[key]))
                            except (TypeError, ValueError):
                                metric = None
                            if metric is not None:
                                watchdog_points.append((row_idx, metric, key))
                                break

            if status.lower() in NON_SUCCESS_STATUSES:
                warning_reasons[reason_code or "unknown_reason"] += 1

    run_status_source = run_summary_status if run_summary_status else run_last_status
    for _, status in sorted(run_status_source.values(), key=lambda item: item[0]):
        status_counts[status] += 1

    run_total = len(run_status_source)
    status_part = ",".join(f"{key}:{status_counts[key]}" for key in sorted(status_counts)) or "none"

    if reason_totals:
        top_reasons = sorted(reason_totals.items(), key=lambda item: (-item[1], item[0]))[:top_n]
        reason_part = ";".join(f"{code}:{int(val) if float(val).is_integer() else round(val,2)}" for code, val in top_reasons)
    else:
        top_primary = sorted(((k, v) for k, v in fallback_reason_counts.items() if k), key=lambda item: (-item[1], item[0]))[:top_n]
        reason_part = ";".join(f"{code}:{count}" for code, count in top_primary) or "none"

    stage_lines: list[str] = []
    for stage in sorted(stage_durations)[:max_stages]:
        stage_lines.append(f"{stage}[{_format_ms(stage_durations[stage])}]")
    stage_part = ";".join(stage_lines) if stage_lines else "none"

    if watchdog_points:
        points = sorted(watchdog_points, key=lambda item: item[0])
        start_val = points[0][1]
        end_val = points[-1][1]
        delta = end_val - start_val
        metric_key = points[-1][2]
        watchdog_part = f"{metric_key}:start={start_val},end={end_val},delta={delta:+d},n={len(points)}"
    else:
        watchdog_part = "none"

    warning_bits = []
    if status_counts:
        non_success_total = sum(v for k, v in status_counts.items() if k.lower() in NON_SUCCESS_STATUSES)
        if non_success_total:
            warning_bits.append(f"non_success_runs:{non_success_total}")
    for code, count in sorted(warning_reasons.items(), key=lambda item: (-item[1], item[0]))[:warning_limit]:
        warning_bits.append(f"{code}:{count}")
    if watchdog_points:
        points = sorted(watchdog_points, key=lambda item: item[0])
        if points[-1][1] - points[0][1] > 0:
            warning_bits.append("watchdog_trend_up")
    warnings_part = ";".join(warning_bits) if warning_bits else "none"

    return [
        f"runs total={run_total} status={status_part}",
        f"top_reason_codes {reason_part}",
        f"stage_duration_ms {stage_part}",
        f"watchdog_trend {watchdog_part}",
        f"warnings {warnings_part}",
    ]


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Emit compact runtime diagnostics summary from Run_Log/State CSV/JSON artifacts."
    )
    parser.add_argument("paths", nargs="+", help="Artifact files or directories to scan.")
    parser.add_argument("--top-n", type=int, default=6, help="Top non-zero reason codes to include (default: 6).")
    parser.add_argument(
        "--max-stages",
        type=int,
        default=8,
        help="Maximum stage duration entries to include in output (default: 8).",
    )
    parser.add_argument(
        "--warning-limit",
        type=int,
        default=4,
        help="Maximum warning reason entries to include (default: 4).",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        lines = build_summary(args.paths, top_n=args.top_n, max_stages=args.max_stages, warning_limit=args.warning_limit)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    for line in lines:
        print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
