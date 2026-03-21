#!/usr/bin/env python3
import argparse
import csv
import json
import os
from pathlib import Path
from collections import Counter
from typing import Any, Dict, List, Tuple
from check_player_stats_coverage import GateConfig, evaluate_player_stats_gate

METRICS = ["MATCH_CT", "NO_P_MATCH", "REJ_CT", "STATS_ENR", "STATS_MISS_A", "STATS_MISS_B"]
DISALLOWED_RUN_REASON_CODES = {"run_debounced_skip", "run_locked_skip"}
REQUIRED_STAGE_CHAIN = (
    "stageFetchOdds",
    "stageFetchSchedule",
    "stageMatchEvents",
    "stageFetchPlayerStats",
    "stageGenerateSignals",
    "stagePersist",
)
DEBUG_WATERMARK = (
    "### NON-APPROVAL DEBUG OUTPUT ### "
    "gate failed; verdict publication remains blocked."
)


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


def _normalize_stage_summaries_payload(value: Any) -> List[Dict[str, Any]]:
    parsed = _parse_json(value, None)
    if isinstance(parsed, list):
        return [row for row in parsed if isinstance(row, dict)]
    if isinstance(parsed, dict):
        nested = parsed.get("stage_summaries")
        if isinstance(nested, list):
            return [row for row in nested if isinstance(row, dict)]
    return []


def _extract_stage_summaries(summary: Dict[str, Any]) -> List[Dict[str, Any]]:
    stage_summaries = _normalize_stage_summaries_payload(summary.get("stage_summaries"))
    if stage_summaries:
        return stage_summaries
    return _normalize_stage_summaries_payload(summary.get("message"))


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


def _run_rows(rows: List[Dict[str, Any]], run_id: str) -> List[Dict[str, Any]]:
    return [row for row in rows if str(row.get("run_id", "")) == run_id]


def _run_has_disallowed_skip(rows: List[Dict[str, Any]], run_id: str) -> str | None:
    for row in _run_rows(rows, run_id):
        reason_code = str(row.get("reason_code", "")).strip().lower()
        if reason_code in DISALLOWED_RUN_REASON_CODES:
            return reason_code
    return None


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
    stage_summaries = _extract_stage_summaries(summary)
    durations: Dict[str, int] = {}
    for stage in stage_summaries:
        name = str(stage.get("stage") or "").strip()
        if not name:
            continue
        durations[name] = int(stage.get("duration_ms", 0) or 0)
    return durations


def _parse_reason_metadata(value: Any) -> Dict[str, Any]:
    parsed = _parse_json(value, {})
    return parsed if isinstance(parsed, dict) else {}


def _extract_stage_fetch_player_stats_metadata(summary: Dict[str, Any]) -> Dict[str, Any]:
    for stage in _extract_stage_summaries(summary):
        if str(stage.get("stage") or "").strip() != "stageFetchPlayerStats":
            continue
        metadata = _parse_reason_metadata(stage.get("reason_metadata"))
        if metadata:
            return metadata
    return {}


def _pick_int(metadata: Dict[str, Any], keys: Tuple[str, ...]) -> int:
    for key in keys:
        value = metadata.get(key)
        try:
            return int(float(value))
        except (TypeError, ValueError):
            continue
    return 0


def _pick_reason_map(metadata: Dict[str, Any], keys: Tuple[str, ...]) -> Dict[str, int]:
    for key in keys:
        candidate = metadata.get(key)
        if not isinstance(candidate, dict):
            continue
        out: Dict[str, int] = {}
        for code in sorted(candidate.keys(), key=lambda item: str(item)):
            try:
                out[str(code)] = int(float(candidate[code]))
            except (TypeError, ValueError):
                continue
        if out:
            return out
    return {}


def _player_stats_comparator(summary: Dict[str, Any]) -> Dict[str, Any]:
    metadata = _extract_stage_fetch_player_stats_metadata(summary)
    resolved = max(0, _pick_int(metadata, ("resolved_player_count", "resolved_players_count")))
    requested = max(0, _pick_int(metadata, ("requested_player_count", "total_player_count", "total_players_count")))
    if requested == 0:
        requested = resolved + max(0, _pick_int(metadata, ("unresolved_player_count", "unresolved_players_count")))
    unresolved_total = max(0, requested - resolved)
    ta_resolved = max(0, _pick_int(metadata, ("resolved_via_ta_count", "ta_resolved_count")))
    provider_fallback = max(
        0,
        _pick_int(
            metadata,
            (
                "resolved_via_provider_fallback_count",
                "provider_fallback_resolved_count",
                "resolved_via_source_fallback_count",
            ),
        ),
    )
    model_fallback = max(0, _pick_int(metadata, ("resolved_via_model_fallback_count", "model_fallback_resolved_count")))
    unresolved_a = max(0, _pick_int(metadata, ("unresolved_player_a_count", "unresolved_side_a_count")))
    unresolved_b = max(0, _pick_int(metadata, ("unresolved_player_b_count", "unresolved_side_b_count")))
    top_reasons = _pick_reason_map(
        metadata,
        (
            "fallback_reason_counts",
            "top_fallback_reason_counts",
            "fallback_reasons",
            "fallback_reason_breakdown",
        ),
    )
    return {
        "resolved": resolved,
        "requested": requested,
        "ta_resolved": ta_resolved,
        "provider_fallback_resolved": provider_fallback,
        "model_fallback_resolved": model_fallback,
        "unresolved_player_a": unresolved_a,
        "unresolved_player_b": unresolved_b,
        "unresolved_total": unresolved_total,
        "fallback_reason_counts": top_reasons,
    }


def _run_stage_chain(rows: List[Dict[str, Any]], run_id: str) -> List[str]:
    stages: List[str] = []
    for row in _run_rows(rows, run_id):
        stage = str(row.get("stage", "")).strip()
        if stage and stage != "runEdgeBoard" and stage not in stages:
            stages.append(stage)
    if stages:
        return stages

    summary = _pick_run_summary(rows, run_id)
    for stage_summary in _extract_stage_summaries(summary):
        stage = str(stage_summary.get("stage", "")).strip()
        if stage and stage not in stages:
            stages.append(stage)
    return stages


def _validate_run_pair(rows: List[Dict[str, Any]], run_a: str, run_b: str) -> None:
    failures: List[str] = []
    for run_id in (run_a, run_b):
        disallowed_reason = _run_has_disallowed_skip(rows, run_id)
        if disallowed_reason:
            failures.append(f"{run_id}: disallowed reason_code `{disallowed_reason}`")
        stage_chain = _run_stage_chain(rows, run_id)
        missing_stage_names = [stage for stage in REQUIRED_STAGE_CHAIN if stage not in stage_chain]
        if missing_stage_names:
            failures.append(
                f"{run_id}: missing stage chain entries ({', '.join(missing_stage_names)})"
            )
    if failures:
        details = "; ".join(failures)
        raise ValueError(
            "Comparison auto-failed: replacement run IDs required before producing pre/post verdict. "
            f"Details: {details}."
        )


def _reason_distributions(rows: List[Dict[str, Any]], run_id: str) -> Dict[str, Dict[str, int]]:
    by_stage: Dict[str, Counter] = {}
    for row in _run_rows(rows, run_id):
        stage = str(row.get("stage", "")).strip()
        if not stage:
            continue
        by_stage.setdefault(stage, Counter())
        reason = str(row.get("reason_code", "")).strip()
        if reason:
            by_stage[stage][reason] += 1
        message = _parse_json(row.get("message"), {})
        if isinstance(message, dict):
            reason_codes = message.get("reason_codes")
            if isinstance(reason_codes, dict):
                for reason_code, count in reason_codes.items():
                    try:
                        by_stage[stage][str(reason_code)] += int(float(count))
                    except (TypeError, ValueError):
                        continue
    return {
        stage: {reason: int(counter[reason]) for reason in sorted(counter.keys())}
        for stage, counter in sorted(by_stage.items())
    }


def _discover_coverage_metadata_payloads(rows: List[Dict[str, Any]], run_id: str) -> List[Dict[str, Any]]:
    payloads: List[Dict[str, Any]] = []
    for row in _run_rows(rows, run_id):
        stage = str(row.get("stage", "")).strip()
        if stage != "stageFetchPlayerStats":
            continue
        message = _parse_json(row.get("message"), {})
        if isinstance(message, dict):
            metadata = _parse_reason_metadata(message.get("reason_metadata"))
            if metadata:
                payloads.append(metadata)

    summary = _pick_run_summary(rows, run_id)
    stage_metadata = _extract_stage_fetch_player_stats_metadata(summary)
    if stage_metadata:
        payloads.append(stage_metadata)
    return payloads


def _debug_gate_failure_report(
    rows: List[Dict[str, Any]],
    run_a: str,
    run_b: str,
    gate_report: Dict[str, Any],
) -> str:
    lines = [DEBUG_WATERMARK, "# gate_report", json.dumps(gate_report, indent=2, sort_keys=True)]
    for run_id in (run_a, run_b):
        stage_chain = _run_stage_chain(rows, run_id)
        missing = [stage for stage in REQUIRED_STAGE_CHAIN if stage not in stage_chain]
        lines.append(f"\n## run={run_id} stage_chain_presence")
        lines.append(f"present_stages: {', '.join(stage_chain) if stage_chain else 'none'}")
        lines.append(f"missing_required_stages: {', '.join(missing) if missing else 'none'}")
        lines.append(f"\n## run={run_id} reason_code_distributions")
        lines.append(json.dumps(_reason_distributions(rows, run_id), indent=2, sort_keys=True))
        lines.append(f"\n## run={run_id} discovered_coverage_metadata_payloads")
        lines.append(json.dumps(_discover_coverage_metadata_payloads(rows, run_id), indent=2, sort_keys=True))
    return "\n".join(lines)


def _format_side_by_side(title: str, run_a: str, run_b: str, values_a: Dict[str, float], values_b: Dict[str, float]) -> List[str]:
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
        left = float(values_a.get(key, 0) or 0)
        right = float(values_b.get(key, 0) or 0)
        if left.is_integer() and right.is_integer():
            left_text = str(int(left))
            right_text = str(int(right))
            delta_text = f"{int(right - left):+d}"
        else:
            left_text = f"{left:.2f}"
            right_text = f"{right:.2f}"
            delta_text = f"{(right - left):+.2f}"
        lines.append(f"{key:<{key_width}}  {left_text:>{a_width}}  {right_text:>{b_width}}  {delta_text:>8}")
    return lines


def build_report(rows: List[Dict[str, Any]], run_a: str, run_b: str) -> str:
    _validate_run_pair(rows, run_a, run_b)
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
    player_stats_a = _player_stats_comparator(summary_a)
    player_stats_b = _player_stats_comparator(summary_b)

    lines.extend(
        _format_side_by_side(
            "player_stats_coverage_pct",
            run_a,
            run_b,
            {"coverage_pct": round(100.0 * player_stats_a["resolved"] / player_stats_a["requested"], 2) if player_stats_a["requested"] else 0.0},
            {"coverage_pct": round(100.0 * player_stats_b["resolved"] / player_stats_b["requested"], 2) if player_stats_b["requested"] else 0.0},
        )
    )
    lines.extend(
        _format_side_by_side(
            "player_stats_source_mix_counts",
            run_a,
            run_b,
            {
                "ta_resolved": player_stats_a["ta_resolved"],
                "provider_fallback_resolved": player_stats_a["provider_fallback_resolved"],
                "model_fallback_resolved": player_stats_a["model_fallback_resolved"],
            },
            {
                "ta_resolved": player_stats_b["ta_resolved"],
                "provider_fallback_resolved": player_stats_b["provider_fallback_resolved"],
                "model_fallback_resolved": player_stats_b["model_fallback_resolved"],
            },
        )
    )
    lines.extend(
        _format_side_by_side(
            "player_stats_unresolved_by_side",
            run_a,
            run_b,
            {
                "unresolved_player_a": player_stats_a["unresolved_player_a"],
                "unresolved_player_b": player_stats_a["unresolved_player_b"],
                "unresolved_total": player_stats_a["unresolved_total"],
            },
            {
                "unresolved_player_a": player_stats_b["unresolved_player_a"],
                "unresolved_player_b": player_stats_b["unresolved_player_b"],
                "unresolved_total": player_stats_b["unresolved_total"],
            },
        )
    )
    top_reasons_a = Counter(player_stats_a["fallback_reason_counts"])
    top_reasons_b = Counter(player_stats_b["fallback_reason_counts"])
    union_reasons = sorted(set(top_reasons_a) | set(top_reasons_b), key=lambda code: (-(top_reasons_a[code] + top_reasons_b[code]), code))
    if not union_reasons:
        union_reasons = ["none"]
        top_reasons_a["none"] = 0
        top_reasons_b["none"] = 0
    lines.extend(
        _format_side_by_side(
            "player_stats_top_fallback_reason_deltas",
            run_a,
            run_b,
            {code: int(top_reasons_a[code]) for code in union_reasons[:10]},
            {code: int(top_reasons_b[code]) for code in union_reasons[:10]},
        )
    )
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
    parser.add_argument(
        "--skip-player-stats-coverage-gate",
        action="store_true",
        help="Skip player-stats coverage gate (not recommended; emergency/manual debugging only).",
    )
    parser.add_argument(
        "--player-stats-gate-override-reason",
        default=os.getenv("PLAYER_STATS_COVERAGE_GATE_OVERRIDE", ""),
        help="Override reason for gate failures; non-empty value allows comparison output publication.",
    )
    parser.add_argument(
        "--player-stats-min-resolved-rate",
        type=float,
        default=float(os.getenv("PLAYER_STATS_MIN_RESOLVED_RATE", "0.60")),
        help="Minimum candidate resolved rate for player-stats gate (default: 0.60).",
    )
    parser.add_argument(
        "--player-stats-max-unresolved-players",
        type=int,
        default=int(os.getenv("PLAYER_STATS_MAX_UNRESOLVED_PLAYERS", "8")),
        help="Maximum candidate unresolved players for player-stats gate (default: 8).",
    )
    parser.add_argument(
        "--player-stats-max-missing-side-increase",
        type=int,
        default=int(os.getenv("PLAYER_STATS_MAX_MISSING_SIDE_INCREASE", "0")),
        help="Maximum allowed increase in STATS_MISS_A/STATS_MISS_B vs baseline (default: 0).",
    )
    parser.add_argument(
        "--emit-debug-on-gate-fail",
        action="store_true",
        help=(
            "Emit non-verdict diagnostics when player-stats gate fails. "
            "Output is explicitly non-approval and verdict publication remains blocked."
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_path = _resolve_default_input(args.input)
    rows = load_rows(input_path)
    if not args.skip_player_stats_coverage_gate:
        gate_config = GateConfig(
            min_resolved_rate=max(0.0, min(1.0, float(args.player_stats_min_resolved_rate))),
            max_unresolved_players=max(0, int(args.player_stats_max_unresolved_players)),
            max_missing_side_increase=max(0, int(args.player_stats_max_missing_side_increase)),
            override_reason=str(args.player_stats_gate_override_reason or "").strip(),
        )
        gate_report = evaluate_player_stats_gate(rows, args.run_a, args.run_b, gate_config)
        if gate_report.get("status") == "fail":
            if args.emit_debug_on_gate_fail:
                print(_debug_gate_failure_report(rows, args.run_a, args.run_b, gate_report))
            else:
                print(json.dumps(gate_report, indent=2, sort_keys=True))
            raise SystemExit(
                "Player-stats coverage gate failed; aborting pre/post verdict publication. "
                "Use --player-stats-gate-override-reason with approved incident context to bypass."
            )
        if gate_report.get("status") == "override":
            print("# player_stats_coverage_gate: override active")
            print(json.dumps(gate_report, indent=2, sort_keys=True))
    print(build_report(rows, args.run_a, args.run_b))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
