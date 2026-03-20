#!/usr/bin/env python3
"""Build periodic runtime aggregate snapshots without copying raw runtime logs."""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipeline_log_adapter import (
    LEGACY_UNK_REASON_CODE_CANONICAL_MAP,
    REASON_CODE_ALIAS_DICTIONARIES,
    REASON_CODE_ALIAS_SCHEMA_ID,
    adapt_run_log_record_for_legacy,
)

SUPPORTED_EXTENSIONS = (".csv", ".json")


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


def _parse_json_like(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if not isinstance(value, str) or not value:
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None


def _parse_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    normalized = value
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    normalized = re.sub(r"Z([+-]\d\d:\d\d)$", r"\1", normalized)
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _extract_duration_ms(summary: dict[str, Any]) -> int | None:
    start = _parse_timestamp(summary.get("started_at"))
    end = _parse_timestamp(summary.get("ended_at"))
    if start and end:
        duration = int((end - start).total_seconds() * 1000)
        return duration if duration >= 0 else None
    try:
        duration = int(float(summary.get("duration_ms") or 0))
    except (TypeError, ValueError):
        return None
    return duration if duration >= 0 else None


def _percentile(values: list[int], p: float) -> int:
    if not values:
        return 0
    sorted_values = sorted(values)
    idx = max(0, min(len(sorted_values) - 1, math.ceil(p * len(sorted_values)) - 1))
    return sorted_values[idx]


def _bucket_date(ts: datetime) -> str:
    return ts.date().isoformat()


def _collect_fallback_aliases(record: dict[str, Any], message: dict[str, Any] | None) -> dict[str, str]:
    fallback_aliases: dict[str, str] = dict(LEGACY_UNK_REASON_CODE_CANONICAL_MAP)

    def _merge(obj: Any):
        if not isinstance(obj, dict):
            return
        for alias, canonical in obj.items():
            alias_text = str(alias)
            canonical_text = str(canonical)
            if alias_text and canonical_text:
                fallback_aliases[alias_text] = canonical_text

    if isinstance(message, dict):
        _merge(message.get("fallback_aliases"))
        reason_metadata = message.get("reason_metadata")
        if isinstance(reason_metadata, dict):
            _merge(reason_metadata.get("fallback_aliases"))
    _merge(record.get("fallback_aliases"))
    return fallback_aliases


def _normalize_reason_code_for_display(code: str, fallback_aliases: dict[str, str]) -> tuple[str, bool]:
    canonical_reason = fallback_aliases.get(code, code)
    display_alias = (REASON_CODE_ALIAS_DICTIONARIES.get(REASON_CODE_ALIAS_SCHEMA_ID) or {}).get(canonical_reason, canonical_reason)
    return display_alias, display_alias != code


def build_periodic_aggregates(paths: list[str], cadence: str = "daily") -> dict[str, Any]:
    files, missing = _collect_files(paths)
    if missing:
        raise FileNotFoundError(f"Missing input paths: {', '.join(missing)}")
    if not files:
        raise FileNotFoundError("No CSV/JSON runtime artifacts found in input paths.")
    if cadence != "daily":
        raise ValueError("Only --cadence daily is currently supported.")

    run_bucket: dict[str, str] = {}
    run_status: dict[str, str] = {}
    run_productivity: dict[str, tuple[float, int, int]] = {}
    run_summary_metrics: dict[str, tuple[int, int]] = {}
    run_stage_ms: defaultdict[str, dict[str, int]] = defaultdict(dict)
    bucket_blockers: defaultdict[str, defaultdict[str, float]] = defaultdict(lambda: defaultdict(float))
    run_reason_codes: dict[str, dict[str, float]] = {}
    reason_alias_normalization_applied = False

    for path in files:
        iterator = _iter_csv_records(path) if path.lower().endswith(".csv") else _iter_json_records(path)
        if iterator is None:
            continue
        for _, raw_record in iterator:
            if not isinstance(raw_record, dict):
                continue
            record = _normalize_record(raw_record)
            run_id = str(record.get("run_id") or "")
            stage = str(record.get("stage") or "")
            row_type = str(record.get("row_type") or "")

            # run_health_guard blocker mix
            if stage == "run_health_guard":
                message = _parse_json_like(record.get("message"))
                if isinstance(message, dict):
                    blocker_counts = message.get("blocker_counts")
                    if isinstance(blocker_counts, dict):
                        ts = _parse_timestamp(record.get("ended_at") or record.get("started_at"))
                        bucket = _bucket_date(ts or datetime.now(timezone.utc))
                        for key, value in blocker_counts.items():
                            try:
                                numeric = float(value)
                            except (TypeError, ValueError):
                                continue
                            if numeric > 0:
                                bucket_blockers[bucket][str(key)] += numeric

            is_summary = row_type == "summary" or stage == "runEdgeBoard"
            if not (is_summary and run_id):
                continue

            status = str(record.get("status") or "unknown") or "unknown"
            run_status[run_id] = status

            ended = _parse_timestamp(record.get("ended_at"))
            started = _parse_timestamp(record.get("started_at"))
            inferred = ended or started or datetime.now(timezone.utc)
            run_bucket[run_id] = _bucket_date(inferred)

            matched_raw = record.get("matched")
            unmatched_raw = record.get("unmatched")
            matched = 0
            unmatched = 0
            has_explicit = False
            try:
                if matched_raw is not None and matched_raw != "":
                    matched = int(float(matched_raw))
                    has_explicit = True
                if unmatched_raw is not None and unmatched_raw != "":
                    unmatched = int(float(unmatched_raw))
                    has_explicit = True
            except (TypeError, ValueError):
                has_explicit = False

            if has_explicit and matched + unmatched > 0:
                run_productivity[run_id] = (matched / (matched + unmatched), matched, unmatched)
            elif status.lower() == "success":
                run_productivity[run_id] = (1.0, 0, 0)
            else:
                run_productivity[run_id] = (0.0, 0, 0)

            reason_codes = record.get("reason_codes")
            if isinstance(reason_codes, str):
                parsed_reasons = _parse_json_like(reason_codes)
                reason_codes = parsed_reasons if isinstance(parsed_reasons, dict) else None
            message = _parse_json_like(record.get("message"))
            if not isinstance(reason_codes, dict) and isinstance(message, dict):
                reason_codes = message.get("reason_codes") if isinstance(message.get("reason_codes"), dict) else None

            fallback_aliases = _collect_fallback_aliases(record, message if isinstance(message, dict) else None)
            display_reason_codes: dict[str, float] = {}
            if isinstance(reason_codes, dict):
                for code, raw_value in reason_codes.items():
                    try:
                        numeric = float(raw_value)
                    except (TypeError, ValueError):
                        continue
                    if numeric <= 0:
                        continue
                    display_code, changed = _normalize_reason_code_for_display(str(code), fallback_aliases)
                    display_reason_codes[display_code] = display_reason_codes.get(display_code, 0.0) + numeric
                    reason_alias_normalization_applied = reason_alias_normalization_applied or changed
            if display_reason_codes:
                run_reason_codes[run_id] = display_reason_codes

            odds_not_actionable = 0
            signals_produced = 0
            try:
                odds_not_actionable = int(float(record.get("odds_not_actionable")))
            except (TypeError, ValueError):
                if isinstance(reason_codes, dict):
                    try:
                        odds_not_actionable = int(float(reason_codes.get("odds_non_actionable") or 0))
                    except (TypeError, ValueError):
                        odds_not_actionable = 0
            try:
                signals_produced = int(float(record.get("signals_found")))
            except (TypeError, ValueError):
                if isinstance(reason_codes, dict):
                    try:
                        signals_produced = int(float(reason_codes.get("signals_generated") or 0))
                    except (TypeError, ValueError):
                        signals_produced = 0
            run_summary_metrics[run_id] = (max(0, odds_not_actionable), max(0, signals_produced))

            stage_summaries = record.get("stage_summaries")
            if isinstance(stage_summaries, str):
                parsed = _parse_json_like(stage_summaries)
                stage_summaries = parsed if isinstance(parsed, list) else None
            if isinstance(stage_summaries, list):
                for item in stage_summaries:
                    if not isinstance(item, dict):
                        continue
                    stage_name = str(item.get("stage") or "")
                    if not stage_name.startswith("stage"):
                        continue
                    duration_ms = _extract_duration_ms(item)
                    if duration_ms is not None:
                        run_stage_ms[run_id][stage_name] = duration_ms

    bucket_runs: defaultdict[str, list[str]] = defaultdict(list)
    for run_id, bucket in run_bucket.items():
        bucket_runs[bucket].append(run_id)

    rollups = []
    for bucket in sorted(bucket_runs):
        run_ids = sorted(bucket_runs[bucket])
        stage_values: defaultdict[str, list[int]] = defaultdict(list)
        success_runs = 0
        degraded_runs = 0
        productivity_ratios: list[float] = []
        matched_total = 0
        unmatched_total = 0
        odds_not_actionable_total = 0
        signals_produced_total = 0
        reason_totals: defaultdict[str, float] = defaultdict(float)

        for run_id in run_ids:
            status = run_status.get(run_id, "").lower()
            if status == "success":
                success_runs += 1
            elif status in {"warning", "failed", "error", "notice"}:
                degraded_runs += 1
            ratio, matched, unmatched = run_productivity.get(run_id, (0.0, 0, 0))
            productivity_ratios.append(ratio)
            matched_total += matched
            unmatched_total += unmatched
            odds_not_actionable, signals_produced = run_summary_metrics.get(run_id, (0, 0))
            odds_not_actionable_total += odds_not_actionable
            signals_produced_total += signals_produced
            for code, numeric in (run_reason_codes.get(run_id) or {}).items():
                reason_totals[code] += numeric

            for stage_name, duration_ms in run_stage_ms.get(run_id, {}).items():
                stage_values[stage_name].append(duration_ms)

        blocker_totals = dict(sorted(bucket_blockers.get(bucket, {}).items(), key=lambda item: (-item[1], item[0])))
        blocker_total_sum = sum(blocker_totals.values())
        blocker_mix = []
        if blocker_total_sum > 0:
            for key, val in blocker_totals.items():
                blocker_mix.append(
                    {
                        "blocker": key,
                        "count": int(val) if float(val).is_integer() else round(val, 2),
                        "share": round(val / blocker_total_sum, 4),
                    }
                )

        stage_latency = {}
        for stage_name in sorted(stage_values):
            samples = stage_values[stage_name]
            stage_latency[stage_name] = {
                "samples": len(samples),
                "avg_ms": round(sum(samples) / len(samples), 1),
                "p95_ms": _percentile(samples, 0.95),
            }

        rollups.append(
            {
                "period_start": bucket,
                "cadence": cadence,
                "runs": len(run_ids),
                "success_runs": success_runs,
                "success_ratio": round(success_runs / len(run_ids), 4) if run_ids else 0,
                "productivity": {
                    "avg_ratio": round(sum(productivity_ratios) / len(productivity_ratios), 4) if productivity_ratios else 0,
                    "matched_total": matched_total,
                    "unmatched_total": unmatched_total,
                },
                "daily_status": {
                    "runs_completed": success_runs,
                    "runs_degraded": degraded_runs,
                    "odds_not_actionable_yet": odds_not_actionable_total,
                    "signals_produced": signals_produced_total,
                },
                "blocker_mix": blocker_mix,
                "stage_latency_trends": stage_latency,
                "top_reason_codes": [
                    {
                        "reason_code": code,
                        "count": int(val) if float(val).is_integer() else round(val, 2),
                    }
                    for code, val in sorted(reason_totals.items(), key=lambda item: (-item[1], item[0]))[:6]
                ],
            }
        )

    for idx, rollup in enumerate(rollups):
        if idx == 0:
            rollup["what_changed_since_yesterday"] = "not enough history yet"
            continue
        prev = rollups[idx - 1]
        cur_status = rollup.get("daily_status", {})
        prev_status = prev.get("daily_status", {})
        rollup["what_changed_since_yesterday"] = {
            "from_period": prev.get("period_start"),
            "runs_completed_delta": cur_status.get("runs_completed", 0) - prev_status.get("runs_completed", 0),
            "runs_degraded_delta": cur_status.get("runs_degraded", 0) - prev_status.get("runs_degraded", 0),
            "odds_not_actionable_yet_delta": cur_status.get("odds_not_actionable_yet", 0) - prev_status.get("odds_not_actionable_yet", 0),
            "signals_produced_delta": cur_status.get("signals_produced", 0) - prev_status.get("signals_produced", 0),
        }

    return {
        "schema": "runtime_periodic_rollup_v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_file_count": len(files),
        "cadence": cadence,
        "rollups": rollups,
        "notes": "Historical rollups only; raw runtime logs remain outside this snapshot artifact.",
        "metadata": {
            "reason_alias_normalization_applied": reason_alias_normalization_applied,
            "reason_alias_normalization_scope": "presentation_only_historical_compat",
        },
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate periodic aggregate snapshots from runtime diagnostics artifacts."
    )
    parser.add_argument("paths", nargs="+", help="Runtime artifact files/directories (CSV/JSON).")
    parser.add_argument("--cadence", default="daily", choices=["daily"], help="Rollup cadence (default: daily).")
    parser.add_argument(
        "--snapshot-dir",
        default="docs/baselines/runtime_rollups",
        help="Directory to store dated snapshot artifacts.",
    )
    parser.add_argument(
        "--snapshot-date",
        default="",
        help="Date label for artifact filename (YYYY-MM-DD). Defaults to current UTC date.",
    )
    parser.add_argument(
        "--stdout",
        action="store_true",
        help="Print snapshot JSON to stdout in addition to writing the dated artifact.",
    )
    return parser.parse_args(argv)


def _resolve_snapshot_path(snapshot_dir: str, snapshot_date: str) -> Path:
    if snapshot_date:
        datetime.strptime(snapshot_date, "%Y-%m-%d")
        date_label = snapshot_date
    else:
        date_label = datetime.now(timezone.utc).date().isoformat()
    return Path(snapshot_dir) / f"runtime_periodic_rollup_{date_label}.json"


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        snapshot = build_periodic_aggregates(args.paths, cadence=args.cadence)
        out_path = _resolve_snapshot_path(args.snapshot_dir, args.snapshot_date)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as handle:
        json.dump(snapshot, handle, indent=2)
        handle.write("\n")

    print(f"Wrote periodic rollup snapshot: {out_path}")
    print("Snapshot contains blocker mix, productivity ratio, and stage latency trend aggregates.")
    if args.stdout:
        print(json.dumps(snapshot, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
