#!/usr/bin/env python3
"""Edge-quality gate for prediction stability and feature robustness."""

from __future__ import annotations

import argparse
import csv
import glob
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from statistics import quantiles
from pathlib import Path
from typing import Any

from pipeline_log_adapter import (
    REASON_CODE_ALIAS_DICTIONARIES,
    REASON_CODE_ALIAS_SCHEMA_ID,
    _expand_reason_map,
)
from stake_policy import StakePolicyConfig, summarize_run_stake_policy


@dataclass(frozen=True)
class EdgeQualityGateConfig:
    min_feature_completeness: float = 0.60
    max_edge_volatility: float = 0.03
    min_scored_signals_for_volatility: int = 10
    min_matched_events_for_volatility: int = 5
    volatility_sample_window_runs: int = 1
    volatility_context_min_pairs: int = 4
    volatility_context_quantile: float = 0.90
    volatility_context_ceiling_factor: float = 1.25
    max_suppression_drift: float = 0.50
    suppression_min_volume: int = 2
    stake_policy_enabled: bool = False
    stake_policy_min_stake_mxn: float = 10.0
    stake_policy_round_to_min: bool = False


@dataclass(frozen=True)
class DailyEdgeQualitySLOConfig:
    window_days: tuple[int, ...] = (3, 7)
    min_pairs_per_window: int = 10
    fail_rate_threshold: float = 0.15


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


def _summary_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        row
        for row in rows
        if str(row.get("row_type") or "") == "summary" and str(row.get("stage") or "") == "runEdgeBoard"
    ]


def _rolling_run_ids(rows: list[dict[str, Any]], min_ended_at: str = "") -> list[str]:
    threshold = _parse_timestamp(min_ended_at) if min_ended_at else None
    ordered: list[str] = []
    for row in _summary_rows(rows):
        run_id = str(row.get("run_id") or "")
        if not run_id:
            continue
        ended = _parse_timestamp(row.get("ended_at")) or _parse_timestamp(row.get("started_at"))
        if threshold and (ended is None or ended < threshold):
            continue
        ordered.append(run_id)
    return list(dict.fromkeys(ordered))


def _run_pairs_from_ids(run_ids: list[str]) -> list[tuple[str, str]]:
    return [(run_ids[idx - 1], run_ids[idx]) for idx in range(1, len(run_ids))]


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
    quality_contract = _extract_quality_contract(_extract_signal_summary(summary_row))
    try:
        value = float(quality_contract.get("feature_completeness"))
        if value >= 0:
            return value, None
    except (TypeError, ValueError):
        pass

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
            if requested_n > 0:
                return (resolved_n / requested_n), None
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
    if isinstance(quality_contract, dict) and quality_contract.get("feature_completeness_reason_code"):
        return None, str(quality_contract.get("feature_completeness_reason_code"))
    if reason_diag:
        return None, reason_diag
    if summary_row.get("reason_codes") not in (None, ""):
        return None, "unsupported_artifact_shape_reason_codes"
    return None, "missing_field_feature_completeness"


def _run_schema_markers(summary_row: dict[str, Any]) -> dict[str, Any]:
    markers: list[str] = []

    payload = _parse_json_like(summary_row.get("reason_codes"), {})
    schema_candidates: list[str] = []
    for candidate in (summary_row.get("schema_id"), payload.get("schema_id") if isinstance(payload, dict) else None):
        if candidate in (None, ""):
            continue
        schema_candidates.append(str(candidate))

    non_current_schema_ids = sorted({schema for schema in schema_candidates if schema != REASON_CODE_ALIAS_SCHEMA_ID})
    if non_current_schema_ids:
        markers.append("reason_code_schema_id_legacy")

    stage_raw = summary_row.get("stage_summaries")
    stage_parsed = _parse_json_like(stage_raw, None)
    stage_rows = _extract_stage_summaries(summary_row)
    stage_explicitly_empty = stage_parsed == [] or (
        isinstance(stage_parsed, dict) and stage_parsed.get("stage_summaries") == []
    )
    if stage_raw not in (None, "") and not stage_rows and not stage_explicitly_empty:
        markers.append("stage_summaries_legacy_shape")

    required_fields = (
        "feature_completeness",
        "player_stats_feature_completeness",
        "stats_feature_completeness",
        "stage_summaries",
        "reason_codes",
    )
    has_required_fields = any(summary_row.get(field) not in (None, "") for field in required_fields)
    if not has_required_fields:
        markers.append("missing_feature_contract_required_fields")

    return {
        "legacy_feature_contract": bool(markers),
        "markers": markers,
        "schema_ids": sorted(set(schema_candidates)),
        "non_current_schema_ids": non_current_schema_ids,
    }


def _extract_signal_summary(summary_row: dict[str, Any]) -> dict[str, Any]:
    parsed = _parse_json_like(summary_row.get("signal_decision_summary"), {})
    return parsed if isinstance(parsed, dict) else {}

def _extract_quality_contract(signal_summary: dict[str, Any]) -> dict[str, Any]:
    quality_contract = signal_summary.get("quality_contract") if isinstance(signal_summary, dict) else {}
    return quality_contract if isinstance(quality_contract, dict) else {}


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
    quality_contract = _extract_quality_contract(signal_summary)
    try:
        return abs(float(quality_contract.get("edge_volatility"))), None
    except (TypeError, ValueError):
        pass

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
        if isinstance(quality_contract, dict) and quality_contract.get("edge_volatility_reason_code"):
            return None, str(quality_contract.get("edge_volatility_reason_code"))
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


def _snapshot(rows: list[dict[str, Any]], run_id: str, config: EdgeQualityGateConfig) -> dict[str, Any]:
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
    schema_markers = _run_schema_markers(summary)
    stake_policy_summary = summarize_run_stake_policy(
        rows,
        run_id,
        StakePolicyConfig(
            enabled=bool(config.stake_policy_enabled),
            minimum_stake_mxn=float(config.stake_policy_min_stake_mxn),
            round_to_min=bool(config.stake_policy_round_to_min),
        ),
    )
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
        "schema_markers": schema_markers,
        "stake_policy_summary": stake_policy_summary,
    }


def _status_bucket(report: dict[str, Any]) -> str:
    status = str(report.get("status") or "")
    if status == "pass":
        return "pass"
    if status == "fail":
        return "true_fail"
    if status == "insufficient_sample":
        return "insufficient_sample"
    return "schema_missing"


def _find_run_index(run_ids: list[str], run_id: str) -> int:
    for idx, item in enumerate(run_ids):
        if item == run_id:
            return idx
    return -1


def _window_sample_activity(
    rows: list[dict[str, Any]],
    run_ids: list[str],
    anchor_run_id: str,
    window_runs: int,
) -> dict[str, Any]:
    window = max(1, int(window_runs))
    anchor_index = _find_run_index(run_ids, anchor_run_id)
    if anchor_index < 0:
        return {"known": False, "run_ids": [], "scored_signals": None, "matched_events": None}
    start_idx = max(0, anchor_index - window + 1)
    selected_ids = run_ids[start_idx : anchor_index + 1]
    scored_total = 0
    matched_total = 0
    known = True
    for run_id in selected_ids:
        summary = _pick_run_summary(rows, run_id)
        if not summary:
            known = False
            break
        signal_summary = _extract_signal_summary(summary)
        scored = _extract_scored_signals(summary, signal_summary)
        matched = _extract_matched_events(summary)
        if scored is None or matched is None:
            known = False
            break
        scored_total += int(scored)
        matched_total += int(matched)
    if not known:
        return {"known": False, "run_ids": selected_ids, "scored_signals": None, "matched_events": None}
    return {"known": True, "run_ids": selected_ids, "scored_signals": scored_total, "matched_events": matched_total}


def _sample_volume_review(rows: list[dict[str, Any]], config: EdgeQualityGateConfig) -> dict[str, Any]:
    summaries = _summary_rows(rows)
    total_runs = len(summaries)
    min_scored = int(config.min_scored_signals_for_volatility)
    min_matched = int(config.min_matched_events_for_volatility)
    sufficient = 0
    for summary in summaries:
        signal_summary = _extract_signal_summary(summary)
        scored = _extract_scored_signals(summary, signal_summary)
        matched = _extract_matched_events(summary)
        if scored is None or matched is None:
            continue
        if int(scored) >= min_scored and int(matched) >= min_matched:
            sufficient += 1
    run_ids = _rolling_run_ids(rows)
    pair_count = max(0, len(run_ids) - 1)
    return {
        "min_scored_signals_for_volatility": min_scored,
        "min_matched_events_for_volatility": min_matched,
        "summary_run_count": total_runs,
        "rolling_pair_count": pair_count,
        "run_count_meeting_minimum": sufficient,
        "run_meeting_rate": (sufficient / total_runs) if total_runs else None,
    }


def evaluate_edge_quality_gate(
    rows: list[dict[str, Any]],
    baseline_run_id: str,
    candidate_run_id: str,
    config: EdgeQualityGateConfig,
    ordered_run_ids: list[str] | None = None,
) -> dict[str, Any]:
    baseline = _snapshot(rows, baseline_run_id, config)
    candidate = _snapshot(rows, candidate_run_id, config)

    failures: list[str] = []
    warnings: list[str] = []
    high_visibility_warnings: list[str] = []
    if not bool(config.stake_policy_enabled):
        high_visibility_warnings.append(
            "HIGH_VISIBILITY_STAKE_POLICY_DISABLED "
            "(stake_policy_summary counters are not policy outcomes; enable --stake-policy-enabled to validate behavior)"
        )
    candidate_feature_completeness = candidate["feature_completeness"]
    if candidate_feature_completeness is None:
        if candidate.get("schema_markers", {}).get("legacy_feature_contract"):
            markers = ",".join(candidate.get("schema_markers", {}).get("markers", [])) or "unknown"
            warnings.append(f"legacy_schema_insufficient_feature_contract markers={markers}")
        else:
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
    sample_strategy = "candidate_only"
    effective_sample = {
        "known": sample_known,
        "scored_signals": scored_signals,
        "matched_events": matched_events,
        "run_ids": [candidate_run_id],
    }
    enough_sample_for_volatility = (not sample_known) or (sufficient_scored and sufficient_matched)
    if (not enough_sample_for_volatility) and int(config.volatility_sample_window_runs) > 1:
        candidate_window = _window_sample_activity(
            rows=rows,
            run_ids=ordered_run_ids or _rolling_run_ids(rows),
            anchor_run_id=candidate_run_id,
            window_runs=int(config.volatility_sample_window_runs),
        )
        if candidate_window["known"]:
            effective_sample = candidate_window
            sample_strategy = "candidate_window_aggregate"
            sufficient_scored = int(candidate_window["scored_signals"]) >= config.min_scored_signals_for_volatility
            sufficient_matched = int(candidate_window["matched_events"]) >= config.min_matched_events_for_volatility
            enough_sample_for_volatility = sufficient_scored and sufficient_matched
    adaptive_ceiling_info = _adaptive_volatility_ceiling(
        rows=rows,
        baseline_summary=_pick_run_summary(rows, baseline_run_id),
        candidate_summary=_pick_run_summary(rows, candidate_run_id),
        config=config,
    )
    effective_volatility_ceiling = float(adaptive_ceiling_info["ceiling"])
    if sample_strategy != "candidate_only" and enough_sample_for_volatility:
        warnings.append(
            "aggregated_sample_used_for_edge_volatility "
            f"(window_runs={config.volatility_sample_window_runs}; runs={','.join(effective_sample['run_ids'])})"
        )

    if candidate_edge_volatility is None:
        diag = candidate["diagnostics"].get("edge_volatility_reason_code", "missing_field_edge_volatility")
        failures.append(f"missing_edge_volatility_metric reason_code={diag}")
    elif effective_sample["known"] and not enough_sample_for_volatility:
        warnings.append(
            "insufficient_sample_for_edge_volatility "
            f"(scored_signals={effective_sample['scored_signals']} required>={config.min_scored_signals_for_volatility}; "
            f"matched_events={effective_sample['matched_events']} required>={config.min_matched_events_for_volatility}; "
            f"strategy={sample_strategy})"
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

    has_legacy_schema_warning = any(item.startswith("legacy_schema_insufficient_feature_contract") for item in warnings)
    has_insufficient_sample_warning = any(item.startswith("insufficient_sample_for_edge_volatility") for item in warnings)
    has_schema_failure = any(item.startswith("missing_feature_completeness_metric") for item in failures) or any(
        item.startswith("missing_edge_volatility_metric") for item in failures
    )
    if failures:
        status = "schema_missing" if has_schema_failure else "fail"
    elif has_legacy_schema_warning:
        status = "schema_missing"
    elif has_insufficient_sample_warning:
        status = "insufficient_sample"
    else:
        status = "pass"
    return {
        "status": status,
        "baseline": baseline,
        "candidate": candidate,
        "thresholds": {
            "min_feature_completeness": config.min_feature_completeness,
            "max_edge_volatility": config.max_edge_volatility,
            "min_scored_signals_for_volatility": config.min_scored_signals_for_volatility,
            "min_matched_events_for_volatility": config.min_matched_events_for_volatility,
            "volatility_sample_window_runs": config.volatility_sample_window_runs,
            "max_suppression_drift": config.max_suppression_drift,
            "suppression_min_volume": config.suppression_min_volume,
            "stake_policy_enabled": config.stake_policy_enabled,
            "stake_policy_min_stake_mxn": config.stake_policy_min_stake_mxn,
            "stake_policy_round_to_min": config.stake_policy_round_to_min,
        },
        "effective_volatility_ceiling": adaptive_ceiling_info,
        "sample_assessment": {
            "strategy": sample_strategy,
            "known": bool(effective_sample["known"]),
            "scored_signals": effective_sample["scored_signals"],
            "matched_events": effective_sample["matched_events"],
            "run_ids": effective_sample["run_ids"],
            "enough_sample_for_volatility": enough_sample_for_volatility,
        },
        "suppression_drifts": suppression_drifts,
        "warnings": warnings,
        "high_visibility_warnings": high_visibility_warnings,
        "failures": failures,
    }


def evaluate_rolling_edge_quality(
    rows: list[dict[str, Any]],
    config: EdgeQualityGateConfig,
    min_ended_at: str = "",
) -> dict[str, Any]:
    full_run_ids = _rolling_run_ids(rows)
    recent_run_ids = _rolling_run_ids(rows, min_ended_at=min_ended_at)

    def _evaluate_window(window_run_ids: list[str], label: str, include_go_nogo: bool) -> dict[str, Any]:
        pair_reports: list[dict[str, Any]] = []
        for baseline_run_id, candidate_run_id in _run_pairs_from_ids(window_run_ids):
            report = evaluate_edge_quality_gate(
                rows=rows,
                baseline_run_id=baseline_run_id,
                candidate_run_id=candidate_run_id,
                config=config,
                ordered_run_ids=window_run_ids,
            )
            pair_reports.append(report)

        status_counts = {
            "pass": 0,
            "true_fail": 0,
            "insufficient_sample": 0,
            "schema_missing": 0,
        }
        for report in pair_reports:
            status_counts[_status_bucket(report)] += 1
        summary: dict[str, Any] = {
            "window": label,
            "run_count": len(window_run_ids),
            "pair_count": len(pair_reports),
            "status_counts": status_counts,
            "pairs": pair_reports,
        }
        if include_go_nogo:
            fail_count = status_counts["true_fail"]
            insufficient_count = status_counts["insufficient_sample"]
            schema_missing_count = status_counts["schema_missing"]
            if summary["pair_count"] <= 0:
                go_no_go = "no-go"
                reason = "insufficient_recent_window_pairs"
            elif fail_count > 0:
                go_no_go = "no-go"
                reason = "recent_window_edge_quality_failures_present"
            elif schema_missing_count > 0:
                go_no_go = "no-go"
                reason = "recent_window_schema_missing_present"
            elif insufficient_count > 0:
                go_no_go = "no-go"
                reason = "recent_window_insufficient_sample_present"
            else:
                go_no_go = "go"
                reason = "recent_window_all_pairs_passed"
            summary["go_no_go"] = go_no_go
            summary["go_no_go_reason"] = reason
        return summary

    return {
        "schema": "edge_quality_rolling_report_v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "min_ended_at": min_ended_at,
        "sample_volume_review": _sample_volume_review(rows, config),
        "full_history_trend": _evaluate_window(full_run_ids, label="full_history", include_go_nogo=False),
        "recent_window_gate": _evaluate_window(recent_run_ids, label="recent_window", include_go_nogo=True),
    }


def _resolve_as_of_utc(as_of_utc: str = "") -> datetime:
    parsed = _parse_timestamp(as_of_utc.strip()) if as_of_utc else None
    return parsed or datetime.now(timezone.utc)


def _window_threshold_iso(as_of: datetime, window_days: int) -> str:
    floor = as_of - timedelta(days=max(1, int(window_days)))
    return floor.isoformat()


def evaluate_daily_edge_quality_slo(
    rows: list[dict[str, Any]],
    gate_config: EdgeQualityGateConfig,
    slo_config: DailyEdgeQualitySLOConfig,
    as_of_utc: str = "",
) -> dict[str, Any]:
    as_of = _resolve_as_of_utc(as_of_utc)
    window_reports: list[dict[str, Any]] = []
    aggregate_status_counts = {"pass": 0, "true_fail": 0, "insufficient_sample": 0, "schema_missing": 0}
    aggregate_decisionable_status_counts = {"pass": 0, "true_fail": 0}
    excluded_pair_count = 0

    for window_days in sorted(set(int(item) for item in slo_config.window_days if int(item) > 0)):
        min_ended_at = _window_threshold_iso(as_of, window_days)
        run_ids = _rolling_run_ids(rows, min_ended_at=min_ended_at)
        pair_reports = [
            evaluate_edge_quality_gate(
                rows=rows,
                baseline_run_id=baseline_run_id,
                candidate_run_id=candidate_run_id,
                config=gate_config,
                ordered_run_ids=run_ids,
            )
            for baseline_run_id, candidate_run_id in _run_pairs_from_ids(run_ids)
        ]

        status_counts = {"pass": 0, "true_fail": 0, "insufficient_sample": 0, "schema_missing": 0}
        for report in pair_reports:
            status_counts[_status_bucket(report)] += 1

        decisionable_status_counts = {"pass": 0, "true_fail": 0}
        excluded_pairs: list[dict[str, Any]] = []
        min_scored = int(gate_config.min_scored_signals_for_volatility)
        min_matched = int(gate_config.min_matched_events_for_volatility)
        for report in pair_reports:
            baseline = report.get("baseline") or {}
            candidate = report.get("candidate") or {}
            baseline_scored = baseline.get("scored_signals")
            candidate_scored = candidate.get("scored_signals")
            baseline_matched = baseline.get("matched_events")
            candidate_matched = candidate.get("matched_events")
            reasons: list[str] = []

            if baseline_scored is None or candidate_scored is None:
                reasons.append("missing_scored_signals")
            elif baseline_scored < min_scored or candidate_scored < min_scored:
                reasons.append("low_scored_signals")

            if baseline_matched is None or candidate_matched is None:
                reasons.append("missing_matched_events")
            elif baseline_matched < min_matched or candidate_matched < min_matched:
                reasons.append("low_matched_events")

            bucket = _status_bucket(report)
            if reasons or bucket in {"insufficient_sample", "schema_missing"}:
                if bucket == "insufficient_sample":
                    reasons.append("status_insufficient_sample")
                if bucket == "schema_missing":
                    reasons.append("status_schema_missing")
                excluded_pairs.append(
                    {
                        "baseline_run_id": baseline.get("run_id"),
                        "candidate_run_id": candidate.get("run_id"),
                        "status": report.get("status"),
                        "reasons": reasons,
                        "baseline_activity": {
                            "scored_signals": baseline_scored,
                            "matched_events": baseline_matched,
                        },
                        "candidate_activity": {
                            "scored_signals": candidate_scored,
                            "matched_events": candidate_matched,
                        },
                    }
                )
                continue

            status = str(report.get("status") or "")
            if status == "fail":
                decisionable_status_counts["true_fail"] += 1
            else:
                decisionable_status_counts["pass"] += 1

        decisionable_pair_count = decisionable_status_counts["pass"] + decisionable_status_counts["true_fail"]
        decisionable = decisionable_pair_count >= int(slo_config.min_pairs_per_window)
        fail_rate = (
            decisionable_status_counts["true_fail"] / decisionable_pair_count if decisionable_pair_count else None
        )
        verdict = "insufficient_sample"
        if decisionable:
            verdict = "fail" if (fail_rate or 0.0) > float(slo_config.fail_rate_threshold) else "pass"

        window_report = {
            "window_days": window_days,
            "min_ended_at": min_ended_at,
            "run_count": len(run_ids),
            "pair_count": len(pair_reports),
            "decisionable_pair_count": decisionable_pair_count,
            "decisionable": decisionable,
            "status_counts": status_counts,
            "decisionable_status_counts": decisionable_status_counts,
            "excluded_pair_count": len(excluded_pairs),
            "excluded_pairs": excluded_pairs,
            "fail_rate": fail_rate,
            "fail_rate_threshold": float(slo_config.fail_rate_threshold),
            "verdict": verdict,
            "pairs": pair_reports,
        }
        window_reports.append(window_report)
        aggregate_status_counts["pass"] += status_counts["pass"]
        aggregate_status_counts["true_fail"] += status_counts["true_fail"]
        aggregate_status_counts["insufficient_sample"] += status_counts["insufficient_sample"]
        aggregate_status_counts["schema_missing"] += status_counts["schema_missing"]
        aggregate_decisionable_status_counts["pass"] += decisionable_status_counts["pass"]
        aggregate_decisionable_status_counts["true_fail"] += decisionable_status_counts["true_fail"]
        excluded_pair_count += len(excluded_pairs)

    decisionable_windows = [window for window in window_reports if bool(window.get("decisionable"))]
    failing_decisionable_windows = [
        window for window in decisionable_windows if str(window.get("verdict") or "") == "fail"
    ]

    if not decisionable_windows:
        gate_verdict = "insufficient_sample"
        gate_reason = "no_decisionable_windows"
    elif failing_decisionable_windows:
        gate_verdict = "fail"
        gate_reason = "decisionable_window_fail_rate_exceeded_threshold"
    else:
        gate_verdict = "pass"
        gate_reason = "all_decisionable_windows_within_fail_rate_threshold"

    stake_policy_aggregate = {"suppressed_count": 0, "adjusted_count": 0, "passed_count": 0, "missing_stake_count": 0}
    for window in window_reports:
        for pair in window.get("pairs", []):
            for side in ("baseline", "candidate"):
                summary = (pair.get(side) or {}).get("stake_policy_summary") or {}
                for key in stake_policy_aggregate:
                    stake_policy_aggregate[key] += int(summary.get(key, 0) or 0)

    return {
        "schema": "edge_quality_daily_slo_v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "as_of_utc": as_of.isoformat(),
        "min_pairs_per_window": int(slo_config.min_pairs_per_window),
        "fail_rate_threshold": float(slo_config.fail_rate_threshold),
        "sample_volume_review": _sample_volume_review(rows, gate_config),
        "aggregate_status_counts": aggregate_status_counts,
        "aggregate_decisionable_status_counts": aggregate_decisionable_status_counts,
        "excluded_pair_count": excluded_pair_count,
        "window_reports": window_reports,
        "gate_verdict": gate_verdict,
        "gate_reason": gate_reason,
        "decisionable_window_count": len(decisionable_windows),
        "failing_decisionable_window_count": len(failing_decisionable_windows),
        "stake_policy_summary_counts": stake_policy_aggregate,
    }


def write_daily_slo_artifacts(
    report: dict[str, Any],
    output_dir: str,
    archive_dir: str = "",
) -> tuple[Path, Path]:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    reports_path = Path(output_dir)
    reports_path.mkdir(parents=True, exist_ok=True)
    daily_output = reports_path / f"edge_quality_daily_slo_{stamp}.json"
    daily_output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    archive_root = Path(archive_dir) if archive_dir else reports_path / "archive"
    archive_root.mkdir(parents=True, exist_ok=True)
    archive_summary = archive_root / "edge_quality_daily_slo_summary.jsonl"
    summary_row = {
        "generated_at_utc": report.get("generated_at_utc"),
        "as_of_utc": report.get("as_of_utc"),
        "gate_verdict": report.get("gate_verdict"),
        "gate_reason": report.get("gate_reason"),
        "decisionable_window_count": report.get("decisionable_window_count"),
        "failing_decisionable_window_count": report.get("failing_decisionable_window_count"),
        "aggregate_status_counts": report.get("aggregate_status_counts"),
        "aggregate_decisionable_status_counts": report.get("aggregate_decisionable_status_counts"),
        "excluded_pair_count": report.get("excluded_pair_count"),
        "windows": [
            {
                "window_days": row.get("window_days"),
                "pair_count": row.get("pair_count"),
                "decisionable_pair_count": row.get("decisionable_pair_count"),
                "excluded_pair_count": row.get("excluded_pair_count"),
                "decisionable": row.get("decisionable"),
                "fail_rate": row.get("fail_rate"),
                "verdict": row.get("verdict"),
            }
            for row in report.get("window_reports") or []
        ],
    }
    with archive_summary.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(summary_row, sort_keys=True) + "\n")

    return daily_output, archive_summary


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Evaluate edge-quality gate from Run_Log exports.")
    parser.add_argument("path", help="Run_Log file or export directory")
    parser.add_argument("--baseline-run-id", default="", help="Baseline run_id (defaults to second-latest)")
    parser.add_argument("--candidate-run-id", default="", help="Candidate run_id (defaults to latest)")
    parser.add_argument("--min-feature-completeness", type=float, default=0.60)
    parser.add_argument("--max-edge-volatility", type=float, default=0.03)
    parser.add_argument("--min-scored-signals-for-volatility", type=int, default=10)
    parser.add_argument("--min-matched-events-for-volatility", type=int, default=5)
    parser.add_argument(
        "--volatility-sample-window-runs",
        type=int,
        default=1,
        help="Optionally aggregate candidate-adjacent runs when checking volatility sample minimums (default: 1).",
    )
    parser.add_argument("--volatility-context-min-pairs", type=int, default=4)
    parser.add_argument("--volatility-context-quantile", type=float, default=0.90)
    parser.add_argument("--volatility-context-ceiling-factor", type=float, default=1.25)
    parser.add_argument("--max-suppression-drift", type=float, default=0.50)
    parser.add_argument("--suppression-min-volume", type=int, default=2)
    parser.add_argument("--stake-policy-enabled", action="store_true", help="Enable stake-policy evaluation summaries.")
    parser.add_argument("--stake-policy-min-stake-mxn", type=float, default=10.0, help="Minimum MXN stake floor (default: 10).")
    parser.add_argument("--stake-policy-round-to-min", action="store_true", help="Adjust below-min stake to floor instead of suppressing.")
    parser.add_argument(
        "--min-ended-at",
        default="",
        help="Optional ISO-8601 cutoff for rolling window candidate runs (e.g. 2026-03-21T00:00:00Z).",
    )
    parser.add_argument(
        "--rolling-report-out",
        default="",
        help="Optional output path for rolling edge-quality report JSON artifact.",
    )
    parser.add_argument(
        "--daily-slo",
        action="store_true",
        help="Evaluate daily production SLO windows (3/7 days by default) and publish reports.",
    )
    parser.add_argument(
        "--daily-slo-windows",
        default="3,7",
        help="Comma-separated day windows for daily SLO (default: 3,7).",
    )
    parser.add_argument("--daily-slo-min-pairs", type=int, default=10)
    parser.add_argument("--daily-slo-fail-rate-threshold", type=float, default=0.15)
    parser.add_argument("--daily-slo-output-dir", default="reports")
    parser.add_argument("--daily-slo-archive-dir", default="docs/baselines/edge_quality_slo")
    parser.add_argument(
        "--as-of-utc",
        default="",
        help="Optional ISO-8601 timestamp for deterministic daily-SLO window boundaries.",
    )
    return parser


def main() -> int:
    parser = _build_arg_parser()
    args = parser.parse_args()

    rows = load_run_log_rows(args.path)
    if not rows:
        parser.error(f"No Run_Log rows found under `{args.path}`")

    config = EdgeQualityGateConfig(
        min_feature_completeness=args.min_feature_completeness,
        max_edge_volatility=args.max_edge_volatility,
        min_scored_signals_for_volatility=max(0, int(args.min_scored_signals_for_volatility)),
        min_matched_events_for_volatility=max(0, int(args.min_matched_events_for_volatility)),
        volatility_sample_window_runs=max(1, int(args.volatility_sample_window_runs)),
        volatility_context_min_pairs=max(1, int(args.volatility_context_min_pairs)),
        volatility_context_quantile=float(args.volatility_context_quantile),
        volatility_context_ceiling_factor=max(0.0, float(args.volatility_context_ceiling_factor)),
        max_suppression_drift=args.max_suppression_drift,
        suppression_min_volume=max(0, int(args.suppression_min_volume)),
        stake_policy_enabled=bool(args.stake_policy_enabled),
        stake_policy_min_stake_mxn=max(0.0, float(args.stake_policy_min_stake_mxn)),
        stake_policy_round_to_min=bool(args.stake_policy_round_to_min),
    )

    if args.daily_slo:
        window_days = tuple(
            int(item.strip())
            for item in str(args.daily_slo_windows).split(",")
            if item.strip()
        )
        if not window_days:
            parser.error("--daily-slo-windows must include at least one positive integer day window.")
        slo_config = DailyEdgeQualitySLOConfig(
            window_days=window_days,
            min_pairs_per_window=max(1, int(args.daily_slo_min_pairs)),
            fail_rate_threshold=max(0.0, float(args.daily_slo_fail_rate_threshold)),
        )
        report = evaluate_daily_edge_quality_slo(
            rows=rows,
            gate_config=config,
            slo_config=slo_config,
            as_of_utc=str(args.as_of_utc or "").strip(),
        )
        daily_path, summary_path = write_daily_slo_artifacts(
            report=report,
            output_dir=str(args.daily_slo_output_dir or "reports"),
            archive_dir=str(args.daily_slo_archive_dir or ""),
        )
        report["artifacts"] = {
            "daily_report_path": str(daily_path),
            "summary_archive_path": str(summary_path),
        }
        print(json.dumps(report, indent=2, sort_keys=True))
        return 1 if str(report.get("gate_verdict") or "") == "fail" else 0

    if args.baseline_run_id and args.candidate_run_id:
        run_pair = [args.baseline_run_id, args.candidate_run_id]
        report = evaluate_edge_quality_gate(
            rows=rows,
            baseline_run_id=run_pair[0],
            candidate_run_id=run_pair[1],
            config=config,
        )
        print(json.dumps(report, indent=2, sort_keys=True))
        return 1 if report["status"] != "pass" else 0

    if args.baseline_run_id or args.candidate_run_id:
        parser.error("--baseline-run-id and --candidate-run-id must be provided together.")

    report = evaluate_rolling_edge_quality(
        rows=rows,
        config=config,
        min_ended_at=str(args.min_ended_at or "").strip(),
    )
    if args.rolling_report_out:
        out_path = Path(args.rolling_report_out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(json.dumps(report, indent=2, sort_keys=True))
    recent_status_counts = (report.get("recent_window_gate") or {}).get("status_counts") or {}
    if int(recent_status_counts.get("true_fail", 0)) > 0:
        return 1
    if int(recent_status_counts.get("insufficient_sample", 0)) > 0:
        return 1
    if int(recent_status_counts.get("schema_missing", 0)) > 0:
        return 1
    if int((report.get("recent_window_gate") or {}).get("pair_count", 0)) <= 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
