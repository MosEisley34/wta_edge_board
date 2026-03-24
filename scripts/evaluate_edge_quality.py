#!/usr/bin/env python3
"""Edge-quality gate for prediction stability and feature robustness."""

from __future__ import annotations

import argparse
import csv
import glob
import json
import os
from dataclasses import dataclass
from statistics import quantiles
from pathlib import Path
from typing import Any

from pipeline_log_adapter import (
    REASON_CODE_ALIAS_DICTIONARIES,
    REASON_CODE_ALIAS_SCHEMA_ID,
    _expand_reason_map,
)


@dataclass(frozen=True)
class EdgeQualityGateConfig:
    min_feature_completeness: float = 0.60
    max_edge_volatility: float = 0.03
    min_scored_signals_for_volatility: int = 10
    min_matched_events_for_volatility: int = 5
    volatility_context_min_pairs: int = 4
    volatility_context_quantile: float = 0.90
    volatility_context_ceiling_factor: float = 1.25
    max_suppression_drift: float = 0.50
    suppression_min_volume: int = 2


def _parse_json_like(value: Any, fallback: Any) -> Any:
    if value in (None, ""):
        return fallback
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return fallback
        try:
            return json.loads(value)
        except Exception:
            return fallback
    return fallback


def _iter_run_log_paths(path_or_dir: str) -> list[str]:
    candidate = Path(path_or_dir)
    if candidate.is_file():
        lower = candidate.name.lower()
        if lower.endswith('.json') or lower.endswith('.csv'):
            return [str(candidate)]
        return []

    found: list[str] = []
    seen: set[str] = set()
    for pattern in ("**/*Run_Log*.json", "**/*Run_Log*.csv"):
        for path in glob.glob(os.path.join(path_or_dir, pattern), recursive=True):
            normalized = os.path.normpath(path)
            if normalized in seen:
                continue
            seen.add(normalized)
            found.append(normalized)
    return sorted(found)


def load_run_log_rows(path_or_dir: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in _iter_run_log_paths(path_or_dir):
        if path.lower().endswith(".json"):
            payload = json.loads(Path(path).read_text(encoding="utf-8"))
            if isinstance(payload, list):
                rows.extend(row for row in payload if isinstance(row, dict))
            continue
        with open(path, "r", encoding="utf-8", newline="") as handle:
            rows.extend(dict(row) for row in csv.DictReader(handle))
    return rows


def _pick_run_summary(rows: list[dict[str, Any]], run_id: str) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for row in rows:
        if str(row.get("run_id") or "") != run_id:
            continue
        if str(row.get("row_type") or "") == "summary" and str(row.get("stage") or "") == "runEdgeBoard":
            summary = row
    return summary


def _latest_run_ids(rows: list[dict[str, Any]]) -> list[str]:
    ordered: list[str] = []
    for row in rows:
        if str(row.get("row_type") or "") != "summary" or str(row.get("stage") or "") != "runEdgeBoard":
            continue
        run_id = str(row.get("run_id") or "")
        if run_id:
            ordered.append(run_id)
    unique = list(dict.fromkeys(ordered))
    if len(unique) < 2:
        raise ValueError("Need at least two runEdgeBoard summary rows to evaluate edge quality gate.")
    return unique[-2:]


def _extract_stage_summaries(summary_row: dict[str, Any]) -> list[dict[str, Any]]:
    direct = _parse_json_like(summary_row.get("stage_summaries"), None)
    if isinstance(direct, list):
        return [row for row in direct if isinstance(row, dict)]
    if isinstance(direct, dict):
        nested = direct.get("stage_summaries")
        if isinstance(nested, list):
            return [row for row in nested if isinstance(row, dict)]
    message = _parse_json_like(summary_row.get("message"), {})
    if isinstance(message, dict):
        nested = message.get("stage_summaries")
        if isinstance(nested, list):
            return [row for row in nested if isinstance(row, dict)]
        if isinstance(nested, dict) and isinstance(nested.get("stage_summaries"), list):
            return [row for row in nested.get("stage_summaries") if isinstance(row, dict)]
    return []


def _reason_code_totals(summary_row: dict[str, Any]) -> tuple[dict[str, float], str | None]:
    payload = _parse_json_like(summary_row.get("reason_codes"), {})
    fallback_aliases = _parse_json_like(summary_row.get("fallback_aliases"), {})
    schema_id = str(summary_row.get("schema_id") or REASON_CODE_ALIAS_SCHEMA_ID)

    reason_map = payload
    if isinstance(payload, dict) and isinstance(payload.get("reason_codes"), dict):
        reason_map = payload.get("reason_codes")
        fallback_aliases = payload.get("fallback_aliases") or fallback_aliases
        schema_id = str(payload.get("schema_id") or schema_id)

    if not isinstance(reason_map, dict):
        return {}, "unsupported_artifact_shape_reason_codes"
    if not isinstance(fallback_aliases, dict):
        fallback_aliases = {}

    expanded = _expand_reason_map(reason_map, schema_id=schema_id, fallback_aliases=fallback_aliases)
    canonical_to_alias = REASON_CODE_ALIAS_DICTIONARIES.get(schema_id) or {}
    normalized: dict[str, float] = {}
    for key, value in expanded.items():
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            continue
        normalized[str(key)] = normalized.get(str(key), 0.0) + numeric
        alias = canonical_to_alias.get(str(key))
        if alias:
            normalized[alias] = normalized.get(alias, 0.0) + numeric
    return normalized, None


def _extract_feature_completeness(summary_row: dict[str, Any]) -> tuple[float | None, str | None]:
    for field in ("feature_completeness", "player_stats_feature_completeness", "stats_feature_completeness"):
        try:
            value = float(summary_row.get(field))
            if value >= 0:
                return value, None
        except (TypeError, ValueError):
            continue

    for stage in _extract_stage_summaries(summary_row):
        if str(stage.get("stage") or "") != "stageFetchPlayerStats":
            continue
        metadata = _parse_json_like(stage.get("reason_metadata"), {})
        if not isinstance(metadata, dict):
            metadata = {}
        coverage = _parse_json_like(metadata.get("coverage"), {})
        if isinstance(coverage, dict):
            value = coverage.get("resolved_rate")
            try:
                return float(value), None
            except (TypeError, ValueError):
                pass
        requested = metadata.get("requested_player_count", metadata.get("players_total", 0))
        resolved = metadata.get("resolved_player_count", metadata.get("resolved_with_usable_stats_count", 0))
        try:
            requested_n = float(requested)
            resolved_n = float(resolved)
            return ((resolved_n / requested_n) if requested_n > 0 else 0.0), None
        except (TypeError, ValueError, ZeroDivisionError):
            continue
        try:
            input_n = float(stage.get("input_count"))
            output_n = float(stage.get("output_count"))
            if input_n > 0:
                return max(0.0, min(1.0, output_n / input_n)), None
        except (TypeError, ValueError, ZeroDivisionError):
            continue

    reason_totals, reason_diag = _reason_code_totals(summary_row)
    enriched = float(reason_totals.get("stats_enriched", reason_totals.get("STATS_ENR", 0.0)) or 0.0)
    missing_a = float(reason_totals.get("stats_missing_player_a", reason_totals.get("STATS_MISS_A", 0.0)) or 0.0)
    missing_b = float(reason_totals.get("stats_missing_player_b", reason_totals.get("STATS_MISS_B", 0.0)) or 0.0)
    denom = enriched + missing_a + missing_b
    if denom > 0:
        return max(0.0, min(1.0, enriched / denom)), None
    if reason_diag:
        return None, reason_diag
    if summary_row.get("reason_codes") not in (None, ""):
        return None, "unsupported_artifact_shape_reason_codes"
    return None, "missing_field_feature_completeness"


def _extract_signal_summary(summary_row: dict[str, Any]) -> dict[str, Any]:
    parsed = _parse_json_like(summary_row.get("signal_decision_summary"), {})
    return parsed if isinstance(parsed, dict) else {}


def _flatten_suppression_counts(signal_summary: dict[str, Any]) -> dict[str, int]:
    suppression = signal_summary.get("suppression_counts") if isinstance(signal_summary, dict) else {}
    if not isinstance(suppression, dict):
        return {}
    counts: dict[str, int] = {}
    for group_data in suppression.values():
        if not isinstance(group_data, dict):
            continue
        reasons = group_data.get("by_reason")
        if not isinstance(reasons, dict):
            continue
        for reason, value in reasons.items():
            try:
                counts[str(reason)] = counts.get(str(reason), 0) + int(float(value))
            except (TypeError, ValueError):
                continue
    return counts


def _edge_volatility(summary_row: dict[str, Any], signal_summary: dict[str, Any]) -> tuple[float | None, str | None]:
    for field in (
        "edge_volatility",
        "edge_volatility_vs_previous_run",
        "edge_volatility_abs_delta_p95",
        "edge_volatility_abs_delta_mean",
    ):
        raw = summary_row.get(field)
        if isinstance(raw, dict):
            for nested_key in ("abs_delta_p95", "abs_delta_mean", "delta_p95"):
                try:
                    return abs(float(raw.get(nested_key))), None
                except (TypeError, ValueError):
                    continue
        try:
            return abs(float(raw)), None
        except (TypeError, ValueError):
            pass

    edge_quality = signal_summary.get("edge_quality") if isinstance(signal_summary, dict) else {}
    if not isinstance(edge_quality, dict):
        edge_quality = {}
    volatility = edge_quality.get("edge_volatility_vs_previous_run")
    if not isinstance(volatility, dict):
        volatility = {}
    for key in ("abs_delta_p95", "abs_delta_mean", "delta_p95"):
        value = volatility.get(key)
        try:
            return abs(float(value)), None
        except (TypeError, ValueError):
            continue

    for stage in _extract_stage_summaries(summary_row):
        if str(stage.get("stage") or "").strip() != "stageGenerateSignals":
            continue
        metadata = _parse_json_like(stage.get("reason_metadata"), {})
        if not isinstance(metadata, dict):
            metadata = {}
        for key in ("edge_volatility_vs_previous_run", "edge_volatility"):
            candidate = metadata.get(key)
            if isinstance(candidate, dict):
                for nested_key in ("abs_delta_p95", "abs_delta_mean", "delta_p95"):
                    try:
                        return abs(float(candidate.get(nested_key))), None
                    except (TypeError, ValueError):
                        continue
        try:
            produced = float(stage.get("output_count"))
            considered = float(stage.get("input_count"))
            if considered > 0:
                return abs(produced - considered) / considered, None
        except (TypeError, ValueError, ZeroDivisionError):
            continue
    if summary_row.get("signal_decision_summary") not in (None, "") or summary_row.get("stage_summaries") not in (None, ""):
        return None, "missing_field_edge_volatility"
    return None, "unsupported_artifact_shape_edge_volatility"


def _extract_scored_signals(summary_row: dict[str, Any], signal_summary: dict[str, Any]) -> int | None:
    candidate_fields = (
        "scored_signals",
        "scored_signals_count",
        "scored_signal_count",
        "signals_scored",
        "signal_count",
    )
    for field in candidate_fields:
        try:
            return max(0, int(float(summary_row.get(field))))
        except (TypeError, ValueError):
            pass
        try:
            return max(0, int(float(signal_summary.get(field))))
        except (TypeError, ValueError, AttributeError):
            pass

    for stage in _extract_stage_summaries(summary_row):
        if str(stage.get("stage") or "").strip() != "stageGenerateSignals":
            continue
        for key in ("output_count", "input_count"):
            try:
                return max(0, int(float(stage.get(key))))
            except (TypeError, ValueError):
                continue
    return None


def _extract_matched_events(summary_row: dict[str, Any]) -> int | None:
    for stage in _extract_stage_summaries(summary_row):
        if str(stage.get("stage") or "").strip() != "stageMatchEvents":
            continue
        reason_codes = _parse_json_like(stage.get("reason_codes"), {})
        if not isinstance(reason_codes, dict):
            reason_codes = {}
        for key in ("matched_count", "MATCH_CT"):
            try:
                return max(0, int(float(reason_codes.get(key))))
            except (TypeError, ValueError):
                continue
        try:
            return max(0, int(float(stage.get("output_count"))))
        except (TypeError, ValueError):
            continue
    return None


def _context_value(summary_row: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = summary_row.get(key)
        if value not in (None, ""):
            return str(value)
    return ""


def _volatility_context_key(summary_row: dict[str, Any]) -> tuple[str, str]:
    tournament = _context_value(
        summary_row,
        "tournament_id",
        "tournament_key",
        "tournament",
        "competition_id",
        "competition",
    )
    time_block = _context_value(
        summary_row,
        "time_block",
        "window_block",
        "window_key",
        "schedule_day",
        "run_date",
    )
    return tournament, time_block


def _adaptive_volatility_ceiling(
    rows: list[dict[str, Any]],
    baseline_summary: dict[str, Any],
    candidate_summary: dict[str, Any],
    config: EdgeQualityGateConfig,
) -> dict[str, Any]:
    target_contexts = {_volatility_context_key(baseline_summary), _volatility_context_key(candidate_summary)}
    context_values: list[float] = []
    for row in rows:
        if str(row.get("row_type") or "") != "summary" or str(row.get("stage") or "") != "runEdgeBoard":
            continue
        if _volatility_context_key(row) not in target_contexts:
            continue
        volatility, _ = _edge_volatility(row, _extract_signal_summary(row))
        if volatility is None:
            continue
        context_values.append(abs(float(volatility)))

    default = float(config.max_edge_volatility)
    if len(context_values) < max(1, int(config.volatility_context_min_pairs)):
        return {"ceiling": default, "source": "configured", "sample_size": len(context_values)}

    quantile = min(0.99, max(0.5, float(config.volatility_context_quantile)))
    q = quantiles(context_values, n=100, method="inclusive")[int(round(quantile * 100)) - 1]
    contextual_ceiling = q * float(config.volatility_context_ceiling_factor)
    return {
        "ceiling": max(default, contextual_ceiling),
        "source": "contextual_window_pairs",
        "sample_size": len(context_values),
        "quantile": quantile,
    }


def _snapshot(rows: list[dict[str, Any]], run_id: str) -> dict[str, Any]:
    summary = _pick_run_summary(rows, run_id)
    if not summary:
        raise ValueError(f"Missing runEdgeBoard summary row for run_id={run_id}")
    signal_summary = _extract_signal_summary(summary)
    feature_completeness, feature_diag = _extract_feature_completeness(summary)
    edge_volatility, edge_diag = _edge_volatility(summary, signal_summary)
    diagnostics: dict[str, str] = {}
    if feature_diag:
        diagnostics["feature_completeness_reason_code"] = feature_diag
    if edge_diag:
        diagnostics["edge_volatility_reason_code"] = edge_diag
    return {
        "run_id": run_id,
        "feature_completeness": feature_completeness,
        "edge_volatility": edge_volatility,
        "scored_signals": _extract_scored_signals(summary, signal_summary),
        "matched_events": _extract_matched_events(summary),
        "suppression_counts": _flatten_suppression_counts(signal_summary),
        "diagnostics": diagnostics,
        "context": {
            "tournament": _volatility_context_key(summary)[0],
            "time_block": _volatility_context_key(summary)[1],
        },
    }


def evaluate_edge_quality_gate(
    rows: list[dict[str, Any]],
    baseline_run_id: str,
    candidate_run_id: str,
    config: EdgeQualityGateConfig,
) -> dict[str, Any]:
    baseline = _snapshot(rows, baseline_run_id)
    candidate = _snapshot(rows, candidate_run_id)

    failures: list[str] = []
    warnings: list[str] = []
    candidate_feature_completeness = candidate["feature_completeness"]
    if candidate_feature_completeness is None:
        diag = candidate["diagnostics"].get("feature_completeness_reason_code", "missing_field_feature_completeness")
        failures.append(f"missing_feature_completeness_metric reason_code={diag}")
    elif candidate_feature_completeness < config.min_feature_completeness:
        failures.append(
            "feature_completeness_below_floor "
            f"(candidate={candidate_feature_completeness:.4f} < floor={config.min_feature_completeness:.4f})"
        )

    candidate_edge_volatility = candidate["edge_volatility"]
    scored_signals = candidate.get("scored_signals")
    matched_events = candidate.get("matched_events")
    sample_known = scored_signals is not None and matched_events is not None
    sufficient_scored = (scored_signals or 0) >= config.min_scored_signals_for_volatility
    sufficient_matched = (matched_events or 0) >= config.min_matched_events_for_volatility
    enough_sample_for_volatility = (not sample_known) or (sufficient_scored and sufficient_matched)
    adaptive_ceiling_info = _adaptive_volatility_ceiling(
        rows=rows,
        baseline_summary=_pick_run_summary(rows, baseline_run_id),
        candidate_summary=_pick_run_summary(rows, candidate_run_id),
        config=config,
    )
    effective_volatility_ceiling = float(adaptive_ceiling_info["ceiling"])
    if candidate_edge_volatility is None:
        diag = candidate["diagnostics"].get("edge_volatility_reason_code", "missing_field_edge_volatility")
        failures.append(f"missing_edge_volatility_metric reason_code={diag}")
    elif sample_known and not enough_sample_for_volatility:
        warnings.append(
            "insufficient_sample_for_edge_volatility "
            f"(scored_signals={scored_signals} required>={config.min_scored_signals_for_volatility}; "
            f"matched_events={matched_events} required>={config.min_matched_events_for_volatility})"
        )
    elif candidate_edge_volatility > effective_volatility_ceiling:
        failures.append(
            "edge_volatility_above_ceiling "
            f"(candidate={candidate_edge_volatility:.4f} > ceiling={effective_volatility_ceiling:.4f})"
        )

    suppression_reasons = sorted(set(baseline["suppression_counts"].keys()) | set(candidate["suppression_counts"].keys()))
    suppression_drifts: dict[str, float] = {}
    for reason in suppression_reasons:
        base = int(baseline["suppression_counts"].get(reason, 0))
        cand = int(candidate["suppression_counts"].get(reason, 0))
        denom = max(base, 1)
        drift = abs(cand - base) / float(denom)
        suppression_drifts[reason] = drift
        if max(base, cand) >= config.suppression_min_volume and drift > config.max_suppression_drift:
            failures.append(
                "suppression_drift_exceeded "
                f"(reason={reason} baseline={base} candidate={cand} drift={drift:.4f} > bound={config.max_suppression_drift:.4f})"
            )

    status = "fail" if failures else ("insufficient_sample" if warnings else "pass")
    return {
        "status": status,
        "baseline": baseline,
        "candidate": candidate,
        "thresholds": {
            "min_feature_completeness": config.min_feature_completeness,
            "max_edge_volatility": config.max_edge_volatility,
            "min_scored_signals_for_volatility": config.min_scored_signals_for_volatility,
            "min_matched_events_for_volatility": config.min_matched_events_for_volatility,
            "max_suppression_drift": config.max_suppression_drift,
            "suppression_min_volume": config.suppression_min_volume,
        },
        "effective_volatility_ceiling": adaptive_ceiling_info,
        "suppression_drifts": suppression_drifts,
        "warnings": warnings,
        "failures": failures,
    }


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Evaluate edge-quality gate from Run_Log exports.")
    parser.add_argument("path", help="Run_Log file or export directory")
    parser.add_argument("--baseline-run-id", default="", help="Baseline run_id (defaults to second-latest)")
    parser.add_argument("--candidate-run-id", default="", help="Candidate run_id (defaults to latest)")
    parser.add_argument("--min-feature-completeness", type=float, default=0.60)
    parser.add_argument("--max-edge-volatility", type=float, default=0.03)
    parser.add_argument("--min-scored-signals-for-volatility", type=int, default=10)
    parser.add_argument("--min-matched-events-for-volatility", type=int, default=5)
    parser.add_argument("--volatility-context-min-pairs", type=int, default=4)
    parser.add_argument("--volatility-context-quantile", type=float, default=0.90)
    parser.add_argument("--volatility-context-ceiling-factor", type=float, default=1.25)
    parser.add_argument("--max-suppression-drift", type=float, default=0.50)
    parser.add_argument("--suppression-min-volume", type=int, default=2)
    return parser


def main() -> int:
    parser = _build_arg_parser()
    args = parser.parse_args()

    rows = load_run_log_rows(args.path)
    if not rows:
        parser.error(f"No Run_Log rows found under `{args.path}`")

    if args.baseline_run_id and args.candidate_run_id:
        run_pair = [args.baseline_run_id, args.candidate_run_id]
    else:
        run_pair = _latest_run_ids(rows)

    report = evaluate_edge_quality_gate(
        rows=rows,
        baseline_run_id=run_pair[0],
        candidate_run_id=run_pair[1],
        config=EdgeQualityGateConfig(
            min_feature_completeness=args.min_feature_completeness,
            max_edge_volatility=args.max_edge_volatility,
            min_scored_signals_for_volatility=max(0, int(args.min_scored_signals_for_volatility)),
            min_matched_events_for_volatility=max(0, int(args.min_matched_events_for_volatility)),
            volatility_context_min_pairs=max(1, int(args.volatility_context_min_pairs)),
            volatility_context_quantile=float(args.volatility_context_quantile),
            volatility_context_ceiling_factor=max(0.0, float(args.volatility_context_ceiling_factor)),
            max_suppression_drift=args.max_suppression_drift,
            suppression_min_volume=max(0, int(args.suppression_min_volume)),
        ),
    )

    print(json.dumps(report, indent=2, sort_keys=True))
    return 1 if report["status"] != "pass" else 0


if __name__ == "__main__":
    raise SystemExit(main())
