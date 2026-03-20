#!/usr/bin/env python3
import argparse
import csv
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

METRICS = ["MATCH_CT", "NO_P_MATCH", "REJ_CT", "STATS_ENR", "STATS_MISS_A", "STATS_MISS_B"]


def _parse_json(value: Any, fallback: Any) -> Any:
    if value is None or value == "":
        return fallback
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return fallback
    return fallback


def load_rows(path: Path) -> List[Dict[str, Any]]:
    if path.suffix.lower() == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, list) else []

    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            rows.append(dict(row))
    return rows


def _pick_run_summary(rows: List[Dict[str, Any]], run_id: str) -> Dict[str, Any]:
    last = None
    for row in rows:
        if str(row.get("run_id", "")) != run_id:
            continue
        if str(row.get("row_type", "")) == "summary" and str(row.get("stage", "")) == "runEdgeBoard":
            last = row
    return last or {}


def _metric_counts(summary: Dict[str, Any]) -> Dict[str, int]:
    reason_codes = _parse_json(summary.get("reason_codes"), {})
    if not isinstance(reason_codes, dict):
        reason_codes = {}
    return {metric: int(reason_codes.get(metric, 0) or 0) for metric in METRICS}


def _suppression_counts(summary: Dict[str, Any]) -> Dict[str, int]:
    parsed = _parse_json(summary.get("signal_decision_summary"), {})
    counts: Dict[str, int] = {}
    suppression = parsed.get("suppression_counts") if isinstance(parsed, dict) else {}
    if not isinstance(suppression, dict):
        return counts

    for category in sorted(suppression.keys()):
        bucket = suppression.get(category, {})
        if not isinstance(bucket, dict):
            continue
        by_reason = bucket.get("by_reason", {})
        if not isinstance(by_reason, dict):
            continue
        for reason in sorted(by_reason.keys()):
            counts[reason] = counts.get(reason, 0) + int(by_reason.get(reason, 0) or 0)
    return counts


def _stage_durations(summary: Dict[str, Any]) -> Dict[str, int]:
    stage_summaries = _parse_json(summary.get("stage_summaries"), [])
    durations: Dict[str, int] = {}
    if not isinstance(stage_summaries, list):
        return durations
    for stage in stage_summaries:
        if not isinstance(stage, dict):
            continue
        name = str(stage.get("stage") or "").strip()
        if not name:
            continue
        durations[name] = int(stage.get("duration_ms", 0) or 0)
    return durations


def _format_side_by_side(title: str, run_a: str, run_b: str, values_a: Dict[str, int], values_b: Dict[str, int]) -> List[str]:
    keys = sorted(set(values_a.keys()) | set(values_b.keys()))
    if not keys:
        keys = ["none"]
        values_a = {"none": 0}
        values_b = {"none": 0}

    key_width = max(len(title), max(len(k) for k in keys), 10)
    a_width = max(len(run_a), 10)
    b_width = max(len(run_b), 10)
    lines = [f"\n[{title}]", f"{'metric':<{key_width}}  {run_a:>{a_width}}  {run_b:>{b_width}}  {'delta':>8}"]
    for key in keys:
        left = int(values_a.get(key, 0) or 0)
        right = int(values_b.get(key, 0) or 0)
        lines.append(f"{key:<{key_width}}  {left:>{a_width}}  {right:>{b_width}}  {right-left:>+8}")
    return lines


def build_report(rows: List[Dict[str, Any]], run_a: str, run_b: str) -> str:
    summary_a = _pick_run_summary(rows, run_a)
    summary_b = _pick_run_summary(rows, run_b)

    if not summary_a or not summary_b:
        missing = []
        if not summary_a:
            missing.append(run_a)
        if not summary_b:
            missing.append(run_b)
        raise ValueError(f"Missing runEdgeBoard summary rows for run_id(s): {', '.join(missing)}")

    lines = [f"run_comparator left={run_a} right={run_b}"]
    lines.extend(_format_side_by_side("core_metrics", run_a, run_b, _metric_counts(summary_a), _metric_counts(summary_b)))
    lines.extend(_format_side_by_side("signal_suppression_reasons", run_a, run_b, _suppression_counts(summary_a), _suppression_counts(summary_b)))
    lines.extend(_format_side_by_side("per_stage_duration_ms", run_a, run_b, _stage_durations(summary_a), _stage_durations(summary_b)))
    return "\n".join(lines)


def _resolve_default_input(path_arg: str) -> Path:
    path = Path(path_arg)
    if path.exists():
        return path
    if path_arg.endswith("Run_Log.csv"):
        json_fallback = path.with_suffix(".json")
        if json_fallback.exists():
            return json_fallback
    raise FileNotFoundError(f"Input not found: {path_arg}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare two runs with deterministic side-by-side metrics")
    parser.add_argument("run_a")
    parser.add_argument("run_b")
    parser.add_argument("--input", default="exports_live/Run_Log.csv", help="Run log CSV or JSON path")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_path = _resolve_default_input(args.input)
    rows = load_rows(input_path)
    print(build_report(rows, args.run_a, args.run_b))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
