#!/usr/bin/env python3
"""Edge-quality gate for prediction stability and feature robustness."""

from __future__ import annotations

import argparse
import csv
import glob
import json
import os
import re
import sys
import tempfile
import traceback
from collections import Counter
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
from runtime_artifact_codec import normalize_run_log_row
from run_summary_cardinality import (
    is_run_edgeboard_summary_row,
    merge_run_summary_rows_for_cardinality,
)
from preflight_guard import enforce_preflight_guard
from schedule_context import compute_schedule_context, fallback_schedule_context, schedule_context_from_export_dir

RUN_LOG_TYPED_FIELDS: dict[str, type] = {
    "feature_completeness": float,
    "matched_events": int,
    "scored_signals": int,
    "no_hit_no_events_from_source_count": int,
    "no_hit_events_outside_time_window_count": int,
    "no_hit_tournament_filter_excluded_count": int,
    "no_hit_odds_present_but_match_failed_count": int,
    "no_hit_schema_invalid_metrics_count": int,
}
RUN_LOG_JSON_OBJECT_FIELDS: tuple[str, ...] = (
    "reason_alias_payload",
    "reason_aliases",
    "fallback_aliases",
    "reason_code_aliases",
)

STANDARD_COMPARE_RUNBOOK_PATH = "runbook/README.md#phase-2--baseline-vs-policy-on-comparison-on-matched-windows"
STAKE_POLICY_ENABLED_COMPARE_RUNBOOK_PATH = (
    "runbook/README.md#stake-policy-enabled-compare-lane-policy-on-only"
)


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
    volatility_dynamic_scaling_enabled: bool = True
    volatility_dynamic_small_sample_loosen_max: float = 0.20
    volatility_dynamic_large_sample_tighten_max: float = 0.10
    volatility_dynamic_target_sample_multiplier: float = 3.0
    max_suppression_drift: float = 0.50
    suppression_min_volume: int = 2
    low_volume_mode_enabled: bool = True
    low_volume_upcoming_match_count_trigger: int = 4
    low_volume_remaining_pairs_trigger: int = -1
    low_volume_min_scored_signals_for_volatility: int = 6
    low_volume_min_matched_events_for_volatility: int = 3
    stake_policy_enabled: bool = False
    stake_policy_min_stake_mxn: float = 20.0
    stake_policy_round_to_min: bool = False


@dataclass(frozen=True)
class DailyEdgeQualitySLOConfig:
    window_days: tuple[int, ...] = (3, 7)
    min_pairs_per_window: int = 10
    fail_rate_threshold: float = 0.15
    min_scored_signals_by_window: dict[int, int] | None = None
    min_matched_events_by_window: dict[int, int] | None = None


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


def _coerce_typed_field(value: Any, expected_type: type) -> Any:
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


def _normalize_typed_run_log_fields(row: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    for field, expected_type in RUN_LOG_TYPED_FIELDS.items():
        normalized[field] = _coerce_typed_field(normalized.get(field), expected_type)
    for field in RUN_LOG_JSON_OBJECT_FIELDS:
        parsed = _parse_json_like(normalized.get(field), None)
        if isinstance(parsed, dict):
            normalized[field] = parsed
        elif normalized.get(field) in ("", None):
            normalized[field] = {}
    _validate_run_log_row_schema(normalized)
    return normalized


def _validate_run_log_row_schema(row: dict[str, Any]) -> None:
    value = row.get("feature_completeness")
    if value in (None, ""):
        row["feature_completeness"] = None
        return
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return

    detail = None
    if isinstance(value, dict):
        detail = value
    elif isinstance(value, str):
        trimmed = value.strip()
        if trimmed.startswith("{") or trimmed.startswith("["):
            try:
                detail = json.loads(trimmed)
            except Exception:
                detail = {"raw": trimmed}

    if detail is not None:
        row.setdefault("feature_completeness_detail", detail)
        row.setdefault("reason_alias_payload", detail)
    row["feature_completeness"] = None
    row["schema_violation"] = "run_log_row_schema_violation"
    row["field_type_error"] = "feature_completeness_expected_numeric_or_null"


def load_run_log_rows(path_or_dir: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in _iter_run_log_paths(path_or_dir):
        source_kind = "json" if path.lower().endswith(".json") else "csv"
        if path.lower().endswith(".json"):
            payload = json.loads(Path(path).read_text(encoding="utf-8"))
            if isinstance(payload, list):
                for row in payload:
                    if not isinstance(row, dict):
                        continue
                    normalized = _normalize_typed_run_log_fields(normalize_run_log_row(dict(row)))
                    normalized.setdefault("_source_file", path)
                    normalized.setdefault("_source_kind", source_kind)
                    rows.append(normalized)
            continue
        with open(path, "r", encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                normalized = _normalize_typed_run_log_fields(normalize_run_log_row(dict(row)))
                normalized.setdefault("_source_file", path)
                normalized.setdefault("_source_kind", source_kind)
                rows.append(normalized)
    return rows


def _run_summary_selection_diagnostics(
    run_rows: list[dict[str, Any]],
    qualifying_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    stage_values = sorted({str(row.get("stage") or "").strip() for row in run_rows if str(row.get("stage") or "").strip()})
    source_files = sorted(
        {
            str(row.get("_source_file") or "unknown")
            for row in run_rows
            if str(row.get("_source_file") or "").strip()
        }
    )
    source_kinds = sorted(
        {
            str(row.get("_source_kind") or "unknown")
            for row in run_rows
            if str(row.get("_source_kind") or "").strip()
        }
    )
    qualifying_row_diagnostics: list[dict[str, Any]] = []
    for row in qualifying_rows:
        row_diagnostic = {
            "run_id": str(row.get("run_id") or ""),
            "row_type": str(row.get("row_type") or ""),
            "stage": str(row.get("stage") or ""),
            "started_at": str(row.get("started_at") or ""),
            "ended_at": str(row.get("ended_at") or ""),
        }
        merged_from_sources = row.get("merged_from_sources")
        if isinstance(merged_from_sources, list) and merged_from_sources:
            row_diagnostic["merged_from_sources"] = [str(item) for item in merged_from_sources]
        qualifying_row_diagnostics.append(row_diagnostic)
    return {
        "qualifying_row_count": len(qualifying_rows),
        "stages_seen": stage_values,
        "source_files": source_files,
        "source_kinds": source_kinds,
        "qualifying_rows": qualifying_row_diagnostics,
    }


def _pick_run_summary(rows: list[dict[str, Any]], run_id: str, strict_cardinality: bool = False) -> dict[str, Any]:
    run_rows = [row for row in rows if str(row.get("run_id") or "") == run_id]
    qualifying_rows = [row for row in run_rows if is_run_edgeboard_summary_row(row)]
    if len(qualifying_rows) == 1:
        return _normalize_legacy_summary_row(qualifying_rows[0])
    if not strict_cardinality:
        return {}
    diagnostics = _run_summary_selection_diagnostics(run_rows, qualifying_rows)
    raise ValueError(
        f"Expected exactly one runEdgeBoard summary row for run_id={run_id}; "
        f"selection_diagnostics={json.dumps(diagnostics, sort_keys=True)}"
    )


def _summary_stake_policy_enabled(summary: dict[str, Any]) -> bool | None:
    signal_summary = _extract_signal_summary(summary)
    stake_policy_summary = _parse_json_like(signal_summary.get("stake_policy_summary"), {})
    if not isinstance(stake_policy_summary, dict):
        return None
    enabled_value = stake_policy_summary.get("enabled")
    if enabled_value is None:
        return None
    if isinstance(enabled_value, bool):
        return enabled_value
    if isinstance(enabled_value, str):
        lowered = enabled_value.strip().lower()
        if lowered in {"true", "1", "yes"}:
            return True
        if lowered in {"false", "0", "no"}:
            return False
        return None
    return bool(enabled_value)


def _resolve_compare_set_policy_tags(
    rows: list[dict[str, Any]],
    compare_set_run_ids: list[str],
    fallback_enabled: bool,
) -> dict[str, bool]:
    tags: dict[str, bool] = {}
    for run_id in compare_set_run_ids:
        summary = _pick_run_summary(rows, run_id)
        explicit = _summary_stake_policy_enabled(summary)
        tags[run_id] = bool(fallback_enabled if explicit is None else explicit)
    return tags


def _latest_run_ids(rows: list[dict[str, Any]]) -> list[str]:
    selected, _ = _select_latest_run_ids(rows, include_cancelled=False, diagnostics_limit=0)
    return selected


def _contains_cancellation_marker(value: Any) -> bool:
    if value in (None, ""):
        return False
    text = str(value).strip().lower()
    if not text:
        return False
    return any(marker in text for marker in ("cancelled", "canceled", "cancel"))


def _summary_has_cancellation_marker(summary_row: dict[str, Any]) -> tuple[bool, list[str]]:
    fields_to_check = (
        "status",
        "stage_status",
        "run_status",
        "reason_code",
        "message",
    )
    diagnostics: list[str] = []
    for field in fields_to_check:
        raw_value = summary_row.get(field)
        if _contains_cancellation_marker(raw_value):
            diagnostics.append(f"{field}={str(raw_value)!r}")

    for stage in _extract_stage_summaries(summary_row):
        stage_name = str(stage.get("stage") or "unknown_stage")
        stage_status = stage.get("status")
        if _contains_cancellation_marker(stage_status):
            diagnostics.append(f"stage[{stage_name}].status={str(stage_status)!r}")
        stage_reason_code = stage.get("reason_code")
        if _contains_cancellation_marker(stage_reason_code):
            diagnostics.append(f"stage[{stage_name}].reason_code={str(stage_reason_code)!r}")
    return bool(diagnostics), diagnostics

def _infer_run_role(summary_row: dict[str, Any]) -> str:
    role_keys = (
        "compare_role",
        "run_role",
        "role",
        "lane_role",
        "policy_lane",
        "variant_role",
    )
    for key in role_keys:
        raw = summary_row.get(key)
        if raw in (None, ""):
            continue
        value = str(raw).strip().lower()
        if value in {"baseline", "control", "reference", "policy_off"}:
            return "baseline"
        if value in {"candidate", "treatment", "variant", "policy_on"}:
            return "candidate"
    run_id = str(summary_row.get("run_id") or "").strip().lower()
    if run_id:
        if any(token in run_id for token in ("baseline", "control", "reference", "policy-off", "policy_off")):
            return "baseline"
        if any(token in run_id for token in ("candidate", "treatment", "variant", "policy-on", "policy_on")):
            return "candidate"
    return "unknown"


def _infer_run_source(summary_row: dict[str, Any]) -> str:
    source_keys = (
        "run_source",
        "source",
        "source_kind",
        "lane",
        "variant",
        "policy_tag",
    )
    for key in source_keys:
        raw = summary_row.get(key)
        if raw in (None, ""):
            continue
        return str(raw).strip().lower()
    stake_policy_enabled = _summary_stake_policy_enabled(summary_row)
    if stake_policy_enabled is not None:
        return "policy_on" if stake_policy_enabled else "policy_off"
    return "unknown"


def _strict_pair_precondition_diagnostics(baseline: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    baseline_matched_events = int(baseline.get("matched_events") or 0)
    baseline_terminal_reason = str(baseline.get("no_hit_terminal_reason_code") or "none")
    baseline_outside_window = baseline_terminal_reason == "events_outside_time_window"
    baseline_window_counter = int((baseline.get("no_hit_counters") or {}).get("no_hit_events_outside_time_window_count", 0) or 0)
    candidate_window_counter = int((candidate.get("no_hit_counters") or {}).get("no_hit_events_outside_time_window_count", 0) or 0)
    baseline_window_constrained = baseline_outside_window or baseline_window_counter > 0
    candidate_window_constrained = (str(candidate.get("no_hit_terminal_reason_code") or "none") == "events_outside_time_window") or candidate_window_counter > 0

    reason_codes: list[str] = []
    if baseline_matched_events <= 0:
        reason_codes.append("invalid_strict_pair_baseline")
        reason_codes.append("baseline_has_no_matched_events")
    if baseline_outside_window:
        reason_codes.append("invalid_strict_pair_baseline")
        reason_codes.append("baseline_terminal_events_outside_time_window")
    if baseline_window_constrained != candidate_window_constrained:
        reason_codes.append("invalid_strict_pair_baseline")
        reason_codes.append("window_constraint_mismatch")

    normalized_reason_codes = sorted(set(reason_codes))
    return {
        "ok": len(normalized_reason_codes) == 0,
        "reason_codes": normalized_reason_codes,
        "baseline": {
            "run_id": baseline.get("run_id"),
            "matched_events": baseline_matched_events,
            "no_hit_terminal_reason_code": baseline_terminal_reason,
            "no_hit_events_outside_time_window_count": baseline_window_counter,
            "window_constrained": baseline_window_constrained,
        },
        "candidate": {
            "run_id": candidate.get("run_id"),
            "no_hit_terminal_reason_code": str(candidate.get("no_hit_terminal_reason_code") or "none"),
            "no_hit_events_outside_time_window_count": candidate_window_counter,
            "window_constrained": candidate_window_constrained,
        },
    }


def _strict_pair_operational_sample_pre_gate(
    baseline: dict[str, Any],
    candidate: dict[str, Any],
    config: EdgeQualityGateConfig,
    minimums: dict[str, Any] | None = None,
) -> dict[str, Any]:
    minimums = minimums or {}
    min_scored = max(
        1,
        int(minimums.get("min_scored_signals_for_volatility", config.min_scored_signals_for_volatility)),
    )
    min_matched = max(
        1,
        int(minimums.get("min_matched_events_for_volatility", config.min_matched_events_for_volatility)),
    )
    baseline_scored = int(baseline.get("scored_signals") or 0)
    baseline_matched = int(baseline.get("matched_events") or 0)
    candidate_scored = int(candidate.get("scored_signals") or 0)
    candidate_matched = int(candidate.get("matched_events") or 0)

    reason_codes: list[str] = []
    if baseline_scored < min_scored:
        reason_codes.append("baseline_scored_signals_below_minimum")
    if baseline_matched < min_matched:
        reason_codes.append("baseline_matched_events_below_minimum")
    if candidate_scored < min_scored:
        reason_codes.append("candidate_scored_signals_below_minimum")
    if candidate_matched < min_matched:
        reason_codes.append("candidate_matched_events_below_minimum")

    return {
        "ok": len(reason_codes) == 0,
        "reason_code": "operational_sample_thresholds_met" if not reason_codes else "insufficient_operational_sample",
        "reason_codes": reason_codes,
        "minimums": {
            "min_scored_signals_for_volatility": min_scored,
            "min_matched_events_for_volatility": min_matched,
        },
        "baseline": {
            "run_id": baseline.get("run_id"),
            "scored_signals": baseline_scored,
            "matched_events": baseline_matched,
        },
        "candidate": {
            "run_id": candidate.get("run_id"),
            "scored_signals": candidate_scored,
            "matched_events": candidate_matched,
        },
    }


def _invalid_strict_pair_result(
    baseline: dict[str, Any],
    candidate: dict[str, Any],
    config: EdgeQualityGateConfig,
    reason_codes: list[str],
    status: str = "insufficient_sample",
    reason_code: str = "strict_pair_precondition_failed",
    pre_gate_details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    canonical_stake_policy = StakePolicyConfig.from_legacy(
        enabled=bool(config.stake_policy_enabled),
        minimum_stake_mxn=float(config.stake_policy_min_stake_mxn),
        round_to_min=bool(config.stake_policy_round_to_min),
    ).with_canonicalized_fields()
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
            "stake_policy": canonical_stake_policy.canonical_policy(),
        },
        "effective_volatility_ceiling": {"ceiling": float(config.max_edge_volatility), "source": "configured", "sample_size": 0},
        "sample_assessment": {
            "strategy": "strict_pair_precondition_failed",
            "known": True,
            "scored_signals": candidate.get("scored_signals"),
            "matched_events": candidate.get("matched_events"),
            "run_ids": [candidate.get("run_id")],
            "enough_sample_for_volatility": False,
            "reason_code": reason_code,
        },
        "suppression_drifts": {},
        "suppression_drift_details": {},
        "failure_diagnostics": {"suppression_drift": {"failing_reasons": {}}},
        "warnings": [f"{reason_code} reason_codes={','.join(reason_codes)}"],
        "high_visibility_warnings": [],
        "failures": [],
        "strict_pair_precondition": {
            "ok": False,
            "reason_codes": sorted(set(reason_codes)),
            "fallback_route": "windowed_fallback_result",
        },
        "strict_pair_operational_pre_gate": pre_gate_details,
    }



def _select_latest_run_ids(
    rows: list[dict[str, Any]],
    *,
    include_cancelled: bool,
    diagnostics_limit: int = 6,
) -> tuple[list[str], list[str]]:
    latest_summary_by_run: dict[str, dict[str, Any]] = {}
    latest_ordered_run_ids: list[str] = []
    for row in rows:
        if str(row.get("row_type") or "") != "summary" or str(row.get("stage") or "") != "runEdgeBoard":
            continue
        run_id = str(row.get("run_id") or "").strip()
        if not run_id:
            continue
        latest_summary_by_run[run_id] = row
        if run_id in latest_ordered_run_ids:
            latest_ordered_run_ids.remove(run_id)
        latest_ordered_run_ids.append(run_id)

    diagnostics: list[str] = []
    selected_newest_first: list[str] = []
    selected_role = "unknown"
    selected_source = "unknown"
    for run_id in reversed(latest_ordered_run_ids):
        summary = latest_summary_by_run[run_id]
        role = _infer_run_role(summary)
        source = _infer_run_source(summary)
        is_cancelled, cancellation_reasons = _summary_has_cancellation_marker(summary)
        if is_cancelled and not include_cancelled:
            diagnostics.append(
                f"run_id={run_id} rejected (cancelled marker found: {', '.join(cancellation_reasons)})"
            )
            continue
        if not selected_newest_first:
            if is_cancelled and include_cancelled:
                diagnostics.append(
                    f"run_id={run_id} accepted (cancelled marker retained due to --include-cancelled; role={role}; source={source})"
                )
            else:
                diagnostics.append(
                    f"run_id={run_id} accepted (latest non-cancelled candidate; role={role}; source={source})"
                )
            selected_newest_first.append(run_id)
            selected_role = role
            selected_source = source
            continue

        if selected_role == "candidate" and role == "candidate":
            diagnostics.append(
                f"run_id={run_id} rejected (role/source mismatch: candidate_like_pair_disallowed; run_role={role}; selected_role={selected_role}; reason_code=invalid_strict_pair_baseline)"
            )
            continue
        if selected_role != "unknown" and role != "unknown" and selected_role == role and selected_source == source:
            diagnostics.append(
                f"run_id={run_id} rejected (role/source mismatch: identical_role_and_source; run_role={role}; run_source={source}; reason_code=invalid_strict_pair_baseline)"
            )
            continue

        if is_cancelled and include_cancelled:
            diagnostics.append(
                f"run_id={run_id} accepted (cancelled marker retained due to --include-cancelled; selected as baseline; role={role}; source={source}; candidate_role={selected_role}; candidate_source={selected_source})"
            )
        else:
            diagnostics.append(
                f"run_id={run_id} accepted (selected as baseline; role={role}; source={source}; candidate_role={selected_role}; candidate_source={selected_source})"
            )
        selected_newest_first.append(run_id)
        if len(selected_newest_first) >= 2:
            break

    if len(selected_newest_first) < 2:
        diagnostic_suffix = ""
        if diagnostics:
            preview = diagnostics if diagnostics_limit <= 0 else diagnostics[: max(1, diagnostics_limit)]
            diagnostic_suffix = " Diagnostics: " + " | ".join(preview)
        raise ValueError(
            "Need at least two runEdgeBoard summary rows to evaluate edge quality gate after cancellation filter."
            + diagnostic_suffix
        )

    selected_oldest_first = list(reversed(selected_newest_first))
    if diagnostics_limit > 0:
        return selected_oldest_first, diagnostics[:diagnostics_limit]
    return selected_oldest_first, diagnostics


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


def _normalize_legacy_stage_rows(stage_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str]]:
    normalized: list[dict[str, Any]] = []
    markers: list[str] = []
    stage_name_aliases = {
        "matchEvents": "stageMatchEvents",
        "generateSignals": "stageGenerateSignals",
        "fetchPlayerStats": "stageFetchPlayerStats",
    }
    for row in stage_rows:
        if not isinstance(row, dict):
            continue
        stage_name_raw = str(row.get("stage") or row.get("name") or "").strip()
        if not stage_name_raw:
            continue
        stage_name = stage_name_aliases.get(stage_name_raw, stage_name_raw)
        if not stage_name.startswith("stage"):
            stage_name = f"stage{stage_name[:1].upper()}{stage_name[1:]}"
        converted: dict[str, Any] = {"stage": stage_name}
        if row.get("reason_codes") is not None:
            converted["reason_codes"] = row.get("reason_codes")
        elif row.get("reasonCodes") is not None:
            converted["reason_codes"] = row.get("reasonCodes")
            markers.append("legacy_stage_reason_codes_alias_reasonCodes")
        for source_key, target_key in (
            ("input_count", "input_count"),
            ("input", "input_count"),
            ("output_count", "output_count"),
            ("output", "output_count"),
        ):
            if row.get(source_key) not in (None, ""):
                converted[target_key] = row.get(source_key)
        if row.get("reason_metadata") is not None:
            converted["reason_metadata"] = row.get("reason_metadata")
        elif row.get("reasonMetadata") is not None:
            converted["reason_metadata"] = row.get("reasonMetadata")
            markers.append("legacy_stage_reason_metadata_alias_reasonMetadata")
        normalized.append(converted)
    return normalized, markers


def _normalize_legacy_summary_row(summary_row: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(summary_row)
    markers: list[str] = []
    missing_reconstruction: list[str] = []
    schema_id = str(normalized.get("schema_id") or "")
    stage_blob = _parse_json_like(normalized.get("stage_summaries"), None)
    stage_has_legacy_rows = isinstance(stage_blob, dict) and isinstance(stage_blob.get("legacy_stage_rows"), list)
    legacy_hint = bool(
        schema_id and schema_id != REASON_CODE_ALIAS_SCHEMA_ID
        or stage_has_legacy_rows
        or any(
            key in normalized
            for key in (
                "playerStatsFeatureCompleteness",
                "featureCoverage",
                "edgeVolatility",
                "edgeVolatilityVsPreviousRun",
                "signals_generated",
                "events_matched",
            )
        )
    )

    if normalized.get("feature_completeness") in (None, ""):
        for source_key in (
            "player_stats_feature_completeness",
            "stats_feature_completeness",
            "playerStatsFeatureCompleteness",
            "featureCoverage",
            "player_stats_resolved_rate",
        ):
            if normalized.get(source_key) in (None, ""):
                continue
            normalized["feature_completeness"] = normalized.get(source_key)
            if source_key != "feature_completeness":
                markers.append(f"legacy_alias_feature_completeness_from_{source_key}")
            break
        if normalized.get("feature_completeness") in (None, ""):
            missing_reconstruction.append("feature_completeness")

    if normalized.get("edge_volatility") in (None, ""):
        for source_key in ("edgeVolatility", "edgeVolatilityVsPreviousRun", "edge_volatility_abs_delta_p95"):
            if normalized.get(source_key) in (None, ""):
                continue
            normalized["edge_volatility"] = normalized.get(source_key)
            markers.append(f"legacy_alias_edge_volatility_from_{source_key}")
            break
        if normalized.get("edge_volatility") in (None, ""):
            missing_reconstruction.append("edge_volatility")

    if normalized.get("stage_summaries") in (None, ""):
        legacy_stage_bundle = _parse_json_like(normalized.get("legacy_stage_summaries"), None)
        if not isinstance(legacy_stage_bundle, dict):
            legacy_stage_bundle = _parse_json_like(normalized.get("stage_summary"), None)
        if not isinstance(legacy_stage_bundle, dict):
            legacy_stage_bundle = _parse_json_like(normalized.get("stage_summaries"), None)
        legacy_stage_rows = []
        if isinstance(legacy_stage_bundle, dict):
            candidate = legacy_stage_bundle.get("legacy_stage_rows")
            if isinstance(candidate, list):
                legacy_stage_rows = candidate
        if legacy_stage_rows:
            converted, stage_markers = _normalize_legacy_stage_rows(legacy_stage_rows)
            if converted:
                normalized["stage_summaries"] = converted
                markers.append("legacy_stage_summaries_normalized")
                markers.extend(stage_markers)

    if normalized.get("scored_signals") in (None, ""):
        for source_key in ("signals_generated", "generated_signals", "signal_output_count"):
            if normalized.get(source_key) in (None, ""):
                continue
            normalized["scored_signals"] = normalized.get(source_key)
            markers.append(f"legacy_alias_scored_signals_from_{source_key}")
            break

    if normalized.get("matched_events") in (None, ""):
        for source_key in ("matched_count", "events_matched", "match_events_count"):
            if normalized.get(source_key) in (None, ""):
                continue
            normalized["matched_events"] = normalized.get(source_key)
            markers.append(f"legacy_alias_matched_events_from_{source_key}")
            break

    if markers or (legacy_hint and missing_reconstruction):
        normalized["_legacy_normalization"] = {
            "applied_markers": sorted(set(markers)),
            "missing_reconstruction": sorted(set(missing_reconstruction)),
        }
    return normalized


def _collect_fallback_aliases(summary_row: dict[str, Any], payload: dict[str, Any] | None = None) -> dict[str, str]:
    fallback_aliases: dict[str, str] = {}

    def _merge(candidate: Any) -> None:
        if not isinstance(candidate, dict):
            return
        for alias, canonical in candidate.items():
            alias_text = str(alias or "").strip()
            canonical_text = str(canonical or "").strip()
            if not alias_text or not canonical_text:
                continue
            fallback_aliases[alias_text] = canonical_text

    _merge(summary_row.get("fallback_aliases"))
    _merge(summary_row.get("reason_code_aliases"))
    if isinstance(payload, dict):
        _merge(payload.get("fallback_aliases"))
        _merge(payload.get("reason_code_aliases"))
    for stage in _extract_stage_summaries(summary_row):
        _merge(stage.get("fallback_aliases"))
        metadata = _parse_json_like(stage.get("reason_metadata"), {})
        if isinstance(metadata, dict):
            _merge(metadata.get("fallback_aliases"))
            _merge(metadata.get("reason_code_aliases"))
    return fallback_aliases


def _reason_code_totals(summary_row: dict[str, Any]) -> tuple[dict[str, float], str | None, dict[str, Any]]:
    payload = _parse_json_like(summary_row.get("reason_codes"), {})
    schema_id = str(summary_row.get("schema_id") or REASON_CODE_ALIAS_SCHEMA_ID)
    resolution = {
        "fallback_only_aliases_used": [],
        "canonical_precedence_applied": [],
    }

    reason_map = payload
    if isinstance(payload, dict) and isinstance(payload.get("reason_codes"), dict):
        reason_map = payload.get("reason_codes")
        schema_id = str(payload.get("schema_id") or schema_id)

    if not isinstance(reason_map, dict):
        return {}, "unsupported_artifact_shape_reason_codes", resolution
    fallback_aliases = _collect_fallback_aliases(summary_row, payload if isinstance(payload, dict) else None)

    canonical_to_alias = REASON_CODE_ALIAS_DICTIONARIES.get(schema_id) or {}
    alias_to_canonical = {alias: canonical for canonical, alias in canonical_to_alias.items()}
    canonical_values: dict[str, float] = {}
    fallback_values: dict[str, float] = {}
    for raw_key, raw_value in reason_map.items():
        key = str(raw_key)
        try:
            numeric = float(raw_value)
        except (TypeError, ValueError):
            continue
        canonical_key = alias_to_canonical.get(key) or fallback_aliases.get(key) or key
        if canonical_key == key:
            canonical_values[canonical_key] = canonical_values.get(canonical_key, 0.0) + numeric
            continue
        if key in fallback_aliases and key not in alias_to_canonical:
            resolution["fallback_only_aliases_used"].append(key)
            fallback_values[canonical_key] = fallback_values.get(canonical_key, 0.0) + numeric
        else:
            canonical_values[canonical_key] = canonical_values.get(canonical_key, 0.0) + numeric

    expanded = _expand_reason_map(
        canonical_values if canonical_values else reason_map,
        schema_id=schema_id,
        fallback_aliases=fallback_aliases,
    )
    for canonical_key, numeric in fallback_values.items():
        if canonical_key not in expanded:
            expanded[canonical_key] = numeric
    for canonical_key in sorted(set(canonical_values).intersection(fallback_values)):
        resolution["canonical_precedence_applied"].append(canonical_key)
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
    resolution["fallback_only_aliases_used"] = sorted(set(str(item) for item in resolution["fallback_only_aliases_used"]))
    resolution["canonical_precedence_applied"] = sorted(
        set(str(item) for item in resolution["canonical_precedence_applied"])
    )
    return normalized, None, resolution


def _extract_feature_completeness(summary_row: dict[str, Any]) -> tuple[float | None, str | None]:
    if str(summary_row.get("field_type_error") or "").find("feature_completeness") >= 0:
        return None, str(summary_row.get("field_type_error"))
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

    reason_totals, reason_diag, _ = _reason_code_totals(summary_row)
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


def _normalize_event_ids(value: Any) -> list[str]:
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, list):
        return []
    event_ids: list[str] = []
    for item in value:
        event_id = str(item or "").strip()
        if event_id:
            event_ids.append(event_id)
    return sorted(set(event_ids))


def _extract_suppression_reason_context(summary_row: dict[str, Any], signal_summary: dict[str, Any]) -> dict[str, dict[str, Any]]:
    context: dict[str, dict[str, Any]] = {}

    def _merge_reason(reason: str, payload: dict[str, Any]) -> None:
        reason_key = str(reason or "").strip()
        if not reason_key or not isinstance(payload, dict):
            return
        entry = context.setdefault(reason_key, {})
        for key in ("count", "raw_count", "minutes_to_start_snapshot", "odds_freshness_metadata"):
            if payload.get(key) is not None:
                entry[key] = payload.get(key)
        if payload.get("event_ids") is not None:
            merged_ids = _normalize_event_ids(entry.get("event_ids")) + _normalize_event_ids(payload.get("event_ids"))
            entry["event_ids"] = sorted(set(merged_ids))

    def _merge_container(payload: Any) -> None:
        if not isinstance(payload, dict):
            return
        for reason, reason_payload in payload.items():
            if isinstance(reason_payload, dict):
                _merge_reason(str(reason), reason_payload)

    if isinstance(signal_summary, dict):
        _merge_container(signal_summary.get("suppression_reason_context"))
        _merge_container(signal_summary.get("suppression_reasons_context"))

    for stage in _extract_stage_summaries(summary_row):
        if str(stage.get("stage") or "").strip() != "stageGenerateSignals":
            continue
        metadata = _parse_json_like(stage.get("reason_metadata"), {})
        if not isinstance(metadata, dict):
            continue
        _merge_container(metadata.get("suppression_reason_context"))
        _merge_container(metadata.get("suppression_reasons_context"))
    return context


def _build_suppression_drift_failure_diagnostics(
    reason: str,
    baseline: dict[str, Any],
    candidate: dict[str, Any],
) -> dict[str, Any]:
    base = int(baseline["suppression_counts"].get(reason, 0) or 0)
    cand = int(candidate["suppression_counts"].get(reason, 0) or 0)
    denominator = max(base, 1)
    numerator = abs(cand - base)
    drift = numerator / float(denominator)

    baseline_context = (baseline.get("suppression_reason_context") or {}).get(reason) or {}
    candidate_context = (candidate.get("suppression_reason_context") or {}).get(reason) or {}
    baseline_events = _normalize_event_ids(baseline_context.get("event_ids"))
    candidate_events = _normalize_event_ids(candidate_context.get("event_ids"))

    event_delta: dict[str, int] = {}
    for event_id in baseline_events:
        event_delta[event_id] = event_delta.get(event_id, 0) - 1
    for event_id in candidate_events:
        event_delta[event_id] = event_delta.get(event_id, 0) + 1

    top_contributing_events = [
        {"event_id": event_id, "delta": delta}
        for event_id, delta in sorted(event_delta.items(), key=lambda item: (-abs(item[1]), item[0]))[:5]
    ]

    return {
        "reason": reason,
        "drift_fraction": {
            "numerator": numerator,
            "denominator": denominator,
            "value": drift,
            "formula": "abs(candidate_count - baseline_count) / max(baseline_count, 1)",
        },
        "raw_counts": {
            "baseline": base,
            "candidate": cand,
            "delta": cand - base,
        },
        "event_ids": {
            "baseline": baseline_events,
            "candidate": candidate_events,
        },
        "minutes_to_start_snapshot": {
            "baseline": baseline_context.get("minutes_to_start_snapshot"),
            "candidate": candidate_context.get("minutes_to_start_snapshot"),
        },
        "odds_freshness_metadata": {
            "baseline": baseline_context.get("odds_freshness_metadata"),
            "candidate": candidate_context.get("odds_freshness_metadata"),
        },
        "top_contributing_events": top_contributing_events,
    }


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
    for field in ("matched_events", "matched_events_count", "events_matched", "matched_count"):
        try:
            return max(0, int(float(summary_row.get(field))))
        except (TypeError, ValueError):
            continue
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


def _resolve_sample_assessment_reason(
    *,
    sample_known: bool,
    scored_signals: int | None,
    matched_events: int | None,
    min_scored_signals_for_volatility: int,
    min_matched_events_for_volatility: int,
) -> tuple[bool, str]:
    if not sample_known:
        return True, "sample_counts_unknown"
    scored_value = max(0, int(scored_signals or 0))
    matched_value = max(0, int(matched_events or 0))
    effective_scored_floor = max(1, int(min_scored_signals_for_volatility))
    effective_matched_floor = max(0, int(min_matched_events_for_volatility))
    if scored_value == 0:
        return False, "no_scored_signals"
    if scored_value < effective_scored_floor:
        return False, "insufficient_scored_signals"
    if matched_value < effective_matched_floor:
        return False, "insufficient_matched_events"
    return True, "sufficient_sample"


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


def _dynamic_volatility_scale_factor(
    config: EdgeQualityGateConfig,
    effective_sample: dict[str, Any],
    adaptive_ceiling_info: dict[str, Any],
) -> dict[str, Any]:
    if not bool(config.volatility_dynamic_scaling_enabled):
        return {"multiplier": 1.0, "source": "disabled"}
    if not effective_sample.get("known"):
        return {"multiplier": 1.0, "source": "unknown_sample"}

    min_scored = max(1, int(config.min_scored_signals_for_volatility))
    min_matched = max(1, int(config.min_matched_events_for_volatility))
    scored = max(0, int(effective_sample.get("scored_signals") or 0))
    matched = max(0, int(effective_sample.get("matched_events") or 0))
    scored_ratio = scored / float(min_scored)
    matched_ratio = matched / float(min_matched)
    coverage_ratio = min(scored_ratio, matched_ratio)

    target_ratio = max(1.0, float(config.volatility_dynamic_target_sample_multiplier))
    normalized_ratio = min(1.0, max(0.0, coverage_ratio / target_ratio))
    loosen_max = max(0.0, float(config.volatility_dynamic_small_sample_loosen_max))
    tighten_max = max(0.0, float(config.volatility_dynamic_large_sample_tighten_max))
    sample_multiplier = (1.0 + loosen_max) - ((loosen_max + tighten_max) * normalized_ratio)

    cadence_multiplier = 1.0
    context_sample_size = int(adaptive_ceiling_info.get("sample_size") or 0)
    context_target = max(1, int(config.volatility_context_min_pairs) * 2)
    if context_sample_size > 0 and context_target > 0:
        context_ratio = min(1.0, max(0.0, context_sample_size / float(context_target)))
        cadence_multiplier = (1.0 + (loosen_max * 0.5)) - ((loosen_max * 0.5 + tighten_max * 0.5) * context_ratio)

    combined = sample_multiplier * cadence_multiplier
    combined = min(1.0 + loosen_max, max(1.0 - tighten_max, combined))
    return {
        "multiplier": combined,
        "source": "sample_and_context_dynamic",
        "coverage_ratio": coverage_ratio,
        "target_ratio": target_ratio,
        "context_sample_size": context_sample_size,
    }


def _snapshot(rows: list[dict[str, Any]], run_id: str, config: EdgeQualityGateConfig) -> dict[str, Any]:
    deduped_rows, _ = merge_run_summary_rows_for_cardinality(rows)
    summary = _pick_run_summary(deduped_rows, run_id, strict_cardinality=True)
    signal_summary = _extract_signal_summary(summary)
    feature_completeness, feature_diag = _extract_feature_completeness(summary)
    edge_volatility, edge_diag = _edge_volatility(summary, signal_summary)
    diagnostics: dict[str, str] = {}
    if feature_diag:
        diagnostics["feature_completeness_reason_code"] = feature_diag
    if edge_diag:
        diagnostics["edge_volatility_reason_code"] = edge_diag
    _, _, reason_resolution = _reason_code_totals(summary)
    if reason_resolution.get("canonical_precedence_applied"):
        diagnostics["reason_canonical_precedence_applied"] = ",".join(
            reason_resolution["canonical_precedence_applied"]
        )
    if reason_resolution.get("fallback_only_aliases_used"):
        diagnostics["reason_fallback_only_aliases_used"] = ",".join(reason_resolution["fallback_only_aliases_used"])
    legacy_normalization = summary.get("_legacy_normalization") if isinstance(summary, dict) else None
    if isinstance(legacy_normalization, dict):
        markers = legacy_normalization.get("applied_markers")
        missing = legacy_normalization.get("missing_reconstruction")
        if isinstance(markers, list) and markers:
            diagnostics["legacy_normalization_markers"] = ",".join(str(item) for item in markers)
        if isinstance(missing, list) and missing:
            diagnostics["legacy_missing_reconstruction"] = ",".join(str(item) for item in missing)
    schema_markers = _run_schema_markers(summary)
    stake_policy_summary = summarize_run_stake_policy(
        rows,
        run_id,
        StakePolicyConfig.from_legacy(
            enabled=bool(config.stake_policy_enabled),
            minimum_stake_mxn=float(config.stake_policy_min_stake_mxn),
            round_to_min=bool(config.stake_policy_round_to_min),
        ),
    )
    no_hit_counters = {}
    for field in (
        "no_hit_no_events_from_source_count",
        "no_hit_events_outside_time_window_count",
        "no_hit_tournament_filter_excluded_count",
        "no_hit_odds_present_but_match_failed_count",
        "no_hit_schema_invalid_metrics_count",
    ):
        try:
            no_hit_counters[field] = int(float(summary.get(field, 0) or 0))
        except (TypeError, ValueError):
            no_hit_counters[field] = 0
    no_hit_terminal_reason_code = str(summary.get("no_hit_terminal_reason_code") or "none")
    source_fetch_counts: dict[str, int | None] = {}
    for field in ("fetched_odds", "fetched_schedule"):
        raw_value = summary.get(field)
        if raw_value in (None, ""):
            source_fetch_counts[field] = None
            continue
        try:
            source_fetch_counts[field] = max(0, int(float(raw_value)))
        except (TypeError, ValueError):
            source_fetch_counts[field] = None
    def _int_or_none(field: str) -> int | None:
        raw_value = summary.get(field)
        if raw_value in (None, ""):
            return None
        try:
            return int(float(raw_value))
        except (TypeError, ValueError):
            return None

    odds_refresh_executed = _int_or_none("odds_refresh_executed")
    odds_refresh_skipped_outside_window = _int_or_none("odds_refresh_skipped_outside_window")
    opening_lag_exceeded = _int_or_none("opening_lag_exceeded")
    opening_lag_within_limit = _int_or_none("opening_lag_within_limit")
    baseline_window_counter = int(no_hit_counters.get("no_hit_events_outside_time_window_count", 0) or 0)
    terminal_reason = str(no_hit_terminal_reason_code or "none")
    outside_window_dominant = terminal_reason == "events_outside_time_window" or baseline_window_counter > 0
    opening_lag_dominant = bool((opening_lag_exceeded or 0) > 0 and (opening_lag_within_limit or 0) <= 0)
    odds_refresh_executed_effective = bool((odds_refresh_executed or 0) > 0)
    if odds_refresh_executed is None:
        odds_refresh_executed_effective = not bool((odds_refresh_skipped_outside_window or 0) > 0)
    actionable_window_run = odds_refresh_executed_effective and not outside_window_dominant and not opening_lag_dominant
    return {
        "run_id": run_id,
        "feature_completeness": feature_completeness,
        "edge_volatility": edge_volatility,
        "scored_signals": _extract_scored_signals(summary, signal_summary),
        "matched_events": _extract_matched_events(summary),
        "suppression_counts": _flatten_suppression_counts(signal_summary),
        "suppression_reason_context": _extract_suppression_reason_context(summary, signal_summary),
        "diagnostics": diagnostics,
        "context": {
            "tournament": _volatility_context_key(summary)[0],
            "time_block": _volatility_context_key(summary)[1],
        },
        "no_hit_counters": no_hit_counters,
        "source_fetch_counts": source_fetch_counts,
        "no_hit_terminal_reason_code": no_hit_terminal_reason_code,
        "actionable_window_run": actionable_window_run,
        "run_actionability_context": {
            "odds_refresh_executed": odds_refresh_executed,
            "odds_refresh_skipped_outside_window": odds_refresh_skipped_outside_window,
            "opening_lag_exceeded": opening_lag_exceeded,
            "opening_lag_within_limit": opening_lag_within_limit,
            "outside_window_dominant": outside_window_dominant,
            "opening_lag_dominant": opening_lag_dominant,
        },
        "schema_markers": schema_markers,
        "stake_policy_summary": stake_policy_summary,
    }


def _insufficient_source_activity_pre_gate(snapshot: dict[str, Any]) -> dict[str, Any]:
    counters = snapshot.get("no_hit_counters") or {}
    no_events_from_source = int(counters.get("no_hit_no_events_from_source_count", 0) or 0)
    fetch_counts = snapshot.get("source_fetch_counts") or {}
    fetched_odds = fetch_counts.get("fetched_odds")
    fetched_schedule = fetch_counts.get("fetched_schedule")
    known_fetch_counts = fetched_odds is not None and fetched_schedule is not None
    low_source_fetch_activity = bool(known_fetch_counts and int(fetched_odds) <= 0 and int(fetched_schedule) <= 0)
    triggered = no_events_from_source > 0 or low_source_fetch_activity
    return {
        "triggered": triggered,
        "reason_code": "insufficient_source_activity" if triggered else "none",
        "details": {
            "no_events_from_source_count": no_events_from_source,
            "fetched_odds": fetched_odds,
            "fetched_schedule": fetched_schedule,
            "known_fetch_counts": known_fetch_counts,
            "zero_fetch_counts": low_source_fetch_activity,
        },
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


def _sample_confidence(
    *,
    sample_known: bool,
    scored_signals: int | None,
    matched_events: int | None,
    min_scored: int,
    min_matched: int,
) -> float | None:
    if not sample_known:
        return None
    scored_floor = max(1, int(min_scored))
    matched_floor = max(1, int(min_matched))
    scored_ratio = max(0.0, float(int(scored_signals or 0)) / float(scored_floor))
    matched_ratio = max(0.0, float(int(matched_events or 0)) / float(matched_floor))
    return min(1.0, min(scored_ratio, matched_ratio))


def _detect_low_volume_mode(
    *,
    summary_row: dict[str, Any],
    candidate_run_id: str,
    ordered_run_ids: list[str],
    config: EdgeQualityGateConfig,
) -> dict[str, Any]:
    if not bool(config.low_volume_mode_enabled):
        return {"active": False, "reasons": [], "evidence": {"mode_enabled": False}}

    reasons: list[str] = []
    evidence: dict[str, Any] = {"mode_enabled": True}
    schedule_context = _resolve_summary_schedule_context(summary_row)
    evidence["schedule_context"] = schedule_context

    stage_tokens: list[str] = []
    for key in ("competition_stage", "tournament_stage", "draw_stage", "round", "round_name"):
        value = summary_row.get(key)
        if value not in (None, ""):
            stage_tokens.append(str(value).strip().lower())
    for stage in _extract_stage_summaries(summary_row):
        metadata = _parse_json_like(stage.get("reason_metadata"), {})
        if not isinstance(metadata, dict):
            continue
        for key in ("competition_stage", "tournament_stage", "draw_stage", "round", "round_name"):
            value = metadata.get(key)
            if value not in (None, ""):
                stage_tokens.append(str(value).strip().lower())
    stage_tokens.extend([str(token) for token in schedule_context.get("stage_tokens", []) if str(token).strip()])
    evidence["stage_tokens"] = stage_tokens
    if any(token in {"semifinal", "semi_final", "semi-final", "final", "quarterfinal", "quarter-final"} for token in stage_tokens):
        reasons.append("competition_stage_near_finals")

    upcoming_matches: int | None = None
    upcoming_match_count_source = "unknown"
    scoped_upcoming_candidates = (
        ("same_tournament_context_upcoming_match_count", "context_scope"),
        ("tournament_tier_upcoming_match_count", "tournament_scope"),
    )
    for key, source_label in scoped_upcoming_candidates:
        scoped_value = schedule_context.get(key)
        if isinstance(scoped_value, int):
            upcoming_matches = max(0, int(scoped_value))
            upcoming_match_count_source = source_label
            break
    if upcoming_matches is None:
        for key in ("upcoming_match_count", "upcoming_matches_count", "remaining_matches", "remaining_match_count"):
            value = summary_row.get(key)
            if value in (None, ""):
                continue
            try:
                upcoming_matches = max(0, int(float(value)))
                upcoming_match_count_source = "summary_row"
                break
            except (TypeError, ValueError):
                continue
    if upcoming_matches is None:
        for stage in _extract_stage_summaries(summary_row):
            metadata = _parse_json_like(stage.get("reason_metadata"), {})
            if not isinstance(metadata, dict):
                continue
            for key in ("upcoming_match_count", "upcoming_matches_count", "remaining_matches", "remaining_match_count"):
                value = metadata.get(key)
                if value in (None, ""):
                    continue
                try:
                    upcoming_matches = max(0, int(float(value)))
                    upcoming_match_count_source = "stage_reason_metadata"
                    break
                except (TypeError, ValueError):
                    continue
            if upcoming_matches is not None:
                break
    if upcoming_matches is None:
        global_schedule_count = schedule_context.get("global_upcoming_match_count")
        if bool(schedule_context.get("has_schedule_rows")) and isinstance(global_schedule_count, int):
            upcoming_matches = max(0, int(global_schedule_count))
            upcoming_match_count_source = "global_schedule_fallback"
    evidence["upcoming_match_count"] = upcoming_matches
    evidence["upcoming_match_count_source"] = upcoming_match_count_source
    if upcoming_matches is not None and upcoming_matches <= int(config.low_volume_upcoming_match_count_trigger):
        if upcoming_match_count_source == "context_scope":
            reasons.append("low_volume_by_context_scope")
        elif upcoming_match_count_source == "tournament_scope":
            reasons.append("low_volume_by_tournament_scope")
        elif upcoming_match_count_source == "global_schedule_fallback":
            reasons.append("low_volume_by_global_scope_fallback")
        else:
            reasons.append("upcoming_match_count_below_trigger")

    remaining_pairs = None
    candidate_index = _find_run_index(ordered_run_ids, candidate_run_id)
    if candidate_index >= 0:
        remaining_pairs = max(0, (len(ordered_run_ids) - 1) - candidate_index)
    evidence["remaining_pairs_after_candidate"] = remaining_pairs
    remaining_pairs_trigger = int(config.low_volume_remaining_pairs_trigger)
    if remaining_pairs is not None and remaining_pairs_trigger >= 0 and remaining_pairs <= remaining_pairs_trigger:
        reasons.append("few_remaining_pairs_in_window")

    return {"active": bool(reasons), "reasons": sorted(set(reasons)), "evidence": evidence}


def _resolve_threshold_profile(
    *,
    summary_row: dict[str, Any],
    candidate_run_id: str,
    ordered_run_ids: list[str],
    config: EdgeQualityGateConfig,
) -> dict[str, Any]:
    profile_map = {
        "strict_default": {
            "min_scored_signals_for_volatility": max(1, int(config.min_scored_signals_for_volatility)),
            "min_matched_events_for_volatility": max(0, int(config.min_matched_events_for_volatility)),
            "rationale": "strict defaults enforced",
        },
        "low_volume_semifinal_final": {
            "min_scored_signals_for_volatility": max(1, int(config.low_volume_min_scored_signals_for_volatility)),
            "min_matched_events_for_volatility": max(0, int(config.low_volume_min_matched_events_for_volatility)),
            "rationale": "semifinal/final stage with low remaining volume allows relaxed floors",
        },
        "ultra_low_volume_single_match": {
            "min_scored_signals_for_volatility": max(
                1,
                int(
                    max(
                        int(config.min_scored_signals_for_volatility),
                        int(config.low_volume_min_scored_signals_for_volatility),
                    )
                ),
            ),
            "min_matched_events_for_volatility": max(
                0,
                int(
                    max(
                        int(config.min_matched_events_for_volatility),
                        int(config.low_volume_min_matched_events_for_volatility),
                    )
                ),
            ),
            "rationale": "single-match residual volume uses conservative fallback thresholds",
        },
    }

    low_volume_mode = _detect_low_volume_mode(
        summary_row=summary_row,
        candidate_run_id=candidate_run_id,
        ordered_run_ids=ordered_run_ids,
        config=config,
    )
    evidence = dict(low_volume_mode.get("evidence") or {})
    schedule_context = evidence.get("schedule_context")
    if not isinstance(schedule_context, dict):
        schedule_context = _resolve_summary_schedule_context(summary_row)
        evidence["schedule_context"] = schedule_context
    reasons: list[str] = []

    reason_totals, _, _ = _reason_code_totals(summary_row)
    raw_schedule_upserts = None
    for key in ("raw_schedule_upserts", "SCH_UPS"):
        try:
            raw_schedule_upserts = max(0, int(float(reason_totals.get(key))))
            break
        except (TypeError, ValueError):
            continue
    evidence["raw_schedule_upserts"] = raw_schedule_upserts
    if raw_schedule_upserts is not None and raw_schedule_upserts <= 1:
        reasons.append("raw_schedule_single_match_signal")

    stage_tokens = [str(token) for token in (evidence.get("stage_tokens") or [])]
    near_finals = any(token in {"semifinal", "semi_final", "semi-final", "final"} for token in stage_tokens)
    evidence["near_finals"] = near_finals
    if near_finals:
        reasons.append("tournament_phase_semifinal_or_final")
    if bool(schedule_context.get("has_schedule_rows")) and not bool(schedule_context.get("stage_inference_available")):
        reasons.append("schedule_stage_inference_unavailable")

    upcoming_matches = evidence.get("upcoming_match_count")
    upcoming_matches_source = str(evidence.get("upcoming_match_count_source") or "")
    if isinstance(upcoming_matches, int) and upcoming_matches <= int(config.low_volume_upcoming_match_count_trigger):
        if upcoming_matches_source == "context_scope":
            reasons.append("low_volume_by_context_scope")
        elif upcoming_matches_source == "tournament_scope":
            reasons.append("low_volume_by_tournament_scope")
        elif upcoming_matches_source == "global_schedule_fallback":
            reasons.append("global_volume_fallback_kept_strict")
        else:
            reasons.append("upcoming_matches_low_volume")

    remaining_pairs = evidence.get("remaining_pairs_after_candidate")
    if isinstance(remaining_pairs, int):
        remaining_pairs_trigger = int(config.low_volume_remaining_pairs_trigger)
        if remaining_pairs_trigger >= 0 and remaining_pairs <= remaining_pairs_trigger:
            reasons.append("remaining_pairs_below_trigger")
        if remaining_pairs <= 0:
            reasons.append("single_remaining_pair_or_less")

    if not bool(config.low_volume_mode_enabled):
        active_profile = "strict_default"
        reasons = ["low_volume_mode_disabled"]
    elif ("raw_schedule_single_match_signal" in reasons) or (isinstance(upcoming_matches, int) and upcoming_matches <= 1):
        active_profile = "ultra_low_volume_single_match"
    elif near_finals and bool(low_volume_mode.get("active")):
        active_profile = "low_volume_semifinal_final"
    else:
        active_profile = "strict_default"
        decisive_reasons = {
            "tournament_phase_semifinal_or_final",
            "upcoming_matches_low_volume",
            "low_volume_by_context_scope",
            "low_volume_by_tournament_scope",
            "raw_schedule_single_match_signal",
        }
        non_relaxing_explicit_reasons = {"global_volume_fallback_kept_strict"}
        if not any(reason in decisive_reasons for reason in reasons):
            if any(reason in non_relaxing_explicit_reasons for reason in reasons):
                reasons = sorted(set(reasons + ["insufficient_low_volume_evidence_keep_strict"]))
            else:
                reasons = ["insufficient_low_volume_evidence_keep_strict"]

    profile = profile_map[active_profile]
    strict_profile = profile_map["strict_default"]
    sample_floors = {
        "min_scored_signals_for_volatility": int(profile["min_scored_signals_for_volatility"]),
        "min_matched_events_for_volatility": int(profile["min_matched_events_for_volatility"]),
    }
    fallback_mode_used = (
        active_profile != "strict_default"
        and (
            sample_floors["min_scored_signals_for_volatility"] < strict_profile["min_scored_signals_for_volatility"]
            or sample_floors["min_matched_events_for_volatility"] < strict_profile["min_matched_events_for_volatility"]
        )
    )

    return {
        "active_profile": active_profile,
        "activation_reasons": sorted(set(reasons)),
        "evidence_snapshot": evidence,
        "rationale": str(profile["rationale"]),
        "sample_floors": sample_floors,
        "fallback_mode_used": fallback_mode_used,
        "low_volume_mode": low_volume_mode,
        "schedule_context": schedule_context,
    }


def _build_compare_report_schedule_context(
    *,
    export_dir: str,
    threshold_schedule_context: dict[str, Any] | None,
    remaining_pairs_after_candidate: int,
) -> dict[str, Any]:
    if str(export_dir or "").strip():
        schedule_context = schedule_context_from_export_dir(export_dir)
    elif isinstance(threshold_schedule_context, dict):
        schedule_context = threshold_schedule_context
    else:
        schedule_context = fallback_schedule_context("export_dir_not_provided")
    if not isinstance(schedule_context, dict):
        schedule_context = fallback_schedule_context("schedule_context_unavailable")
    schedule_context.setdefault("upcoming_match_count", 0)
    schedule_context.setdefault("global_upcoming_match_count", int(schedule_context.get("upcoming_match_count") or 0))
    schedule_context.setdefault("tournament_tier_upcoming_match_count", None)
    schedule_context.setdefault("same_tournament_context_upcoming_match_count", None)
    schedule_context.setdefault("stage_tokens", [])
    schedule_context["remaining_pairs_after_candidate"] = int(remaining_pairs_after_candidate)
    return schedule_context


def _resolve_summary_schedule_context(summary_row: dict[str, Any]) -> dict[str, Any]:
    candidate_keys = ("raw_schedule_rows", "raw_schedule", "raw_schedule_payload", "Raw_Schedule")
    for key in candidate_keys:
        parsed = _parse_json_like(summary_row.get(key), None)
        if isinstance(parsed, (dict, list)):
            return compute_schedule_context(parsed)
    for stage in _extract_stage_summaries(summary_row):
        metadata = _parse_json_like(stage.get("reason_metadata"), {})
        if not isinstance(metadata, dict):
            continue
        for key in candidate_keys:
            parsed = _parse_json_like(metadata.get(key), None)
            if isinstance(parsed, (dict, list)):
                return compute_schedule_context(parsed)
    return compute_schedule_context([])


def _top_volatility_contributors(
    baseline: dict[str, Any],
    candidate: dict[str, Any],
    limit: int = 5,
) -> list[dict[str, Any]]:
    contributions: list[dict[str, Any]] = []
    baseline_counts = baseline.get("suppression_counts") or {}
    candidate_counts = candidate.get("suppression_counts") or {}
    for key in sorted(set(baseline_counts.keys()) | set(candidate_counts.keys())):
        baseline_value = int(baseline_counts.get(key, 0) or 0)
        candidate_value = int(candidate_counts.get(key, 0) or 0)
        delta = candidate_value - baseline_value
        if delta == 0:
            continue
        contributions.append(
            {
                "kind": "suppression_reason",
                "key": key,
                "baseline": baseline_value,
                "candidate": candidate_value,
                "delta": delta,
                "abs_delta": abs(delta),
            }
        )

    for segment in ("tournament", "time_block"):
        base_segment = str((baseline.get("context") or {}).get(segment) or "")
        cand_segment = str((candidate.get("context") or {}).get(segment) or "")
        if base_segment != cand_segment:
            contributions.append(
                {
                    "kind": "context_segment",
                    "key": segment,
                    "baseline": base_segment,
                    "candidate": cand_segment,
                    "delta": None,
                    "abs_delta": 1,
                }
            )

    contributions.sort(key=lambda item: (-int(item.get("abs_delta", 0) or 0), str(item.get("key") or "")))
    return contributions[: max(1, int(limit))]


def evaluate_edge_quality_gate(
    rows: list[dict[str, Any]],
    baseline_run_id: str,
    candidate_run_id: str,
    config: EdgeQualityGateConfig,
    ordered_run_ids: list[str] | None = None,
) -> dict[str, Any]:
    canonical_stake_policy = StakePolicyConfig.from_legacy(
        enabled=bool(config.stake_policy_enabled),
        minimum_stake_mxn=float(config.stake_policy_min_stake_mxn),
        round_to_min=bool(config.stake_policy_round_to_min),
    ).with_canonicalized_fields()
    baseline = _snapshot(rows, baseline_run_id, config)
    candidate = _snapshot(rows, candidate_run_id, config)
    source_activity_pre_gate = _insufficient_source_activity_pre_gate(candidate)
    source_activity_insufficient = bool(source_activity_pre_gate.get("triggered"))

    failures: list[str] = []
    warnings: list[str] = []
    high_visibility_warnings: list[str] = []
    if not bool(config.stake_policy_enabled):
        high_visibility_warnings.append(
            "HIGH_VISIBILITY_STAKE_POLICY_DISABLED "
            "(stake_policy_summary counters are not policy outcomes; enable --stake-policy-enabled to validate behavior)"
        )
    candidate_feature_completeness = candidate["feature_completeness"]
    if source_activity_insufficient:
        details = source_activity_pre_gate.get("details") or {}
        warnings.append(
            "insufficient_source_activity "
            f"(no_events_from_source={details.get('no_events_from_source_count')}; "
            f"fetched_odds={details.get('fetched_odds')}; "
            f"fetched_schedule={details.get('fetched_schedule')})"
        )
    else:
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
    ordered_ids = ordered_run_ids or _rolling_run_ids(rows)
    candidate_summary_row = _pick_run_summary(rows, candidate_run_id)
    threshold_profile = _resolve_threshold_profile(
        summary_row=candidate_summary_row,
        candidate_run_id=candidate_run_id,
        ordered_run_ids=ordered_ids,
        config=config,
    )
    threshold_profile_name = str(threshold_profile["active_profile"])
    threshold_profile_rationale = str(threshold_profile["rationale"])
    fallback_profile_used = bool(threshold_profile["fallback_mode_used"])
    sample_floor_scored = max(1, int(config.min_scored_signals_for_volatility))
    sample_floor_matched = max(0, int(config.min_matched_events_for_volatility))
    sample_known = scored_signals is not None and matched_events is not None
    sample_strategy = "candidate_only"
    effective_sample = {
        "known": sample_known,
        "scored_signals": scored_signals,
        "matched_events": matched_events,
        "run_ids": [candidate_run_id],
    }
    enough_sample_for_volatility, sample_reason_code = _resolve_sample_assessment_reason(
        sample_known=sample_known,
        scored_signals=scored_signals,
        matched_events=matched_events,
        min_scored_signals_for_volatility=sample_floor_scored,
        min_matched_events_for_volatility=sample_floor_matched,
    )
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
            enough_sample_for_volatility, sample_reason_code = _resolve_sample_assessment_reason(
                sample_known=bool(candidate_window["known"]),
                scored_signals=candidate_window["scored_signals"],
                matched_events=candidate_window["matched_events"],
                min_scored_signals_for_volatility=sample_floor_scored,
                min_matched_events_for_volatility=sample_floor_matched,
            )
    strict_sample_result = {"enough": bool(enough_sample_for_volatility), "reason_code": str(sample_reason_code)}
    if (not enough_sample_for_volatility) and threshold_profile_name != "strict_default":
        low_volume_scored = int((threshold_profile.get("sample_floors") or {}).get("min_scored_signals_for_volatility", sample_floor_scored))
        low_volume_matched = int((threshold_profile.get("sample_floors") or {}).get("min_matched_events_for_volatility", sample_floor_matched))
        low_volume_enough, low_volume_reason = _resolve_sample_assessment_reason(
            sample_known=bool(effective_sample["known"]),
            scored_signals=effective_sample["scored_signals"],
            matched_events=effective_sample["matched_events"],
            min_scored_signals_for_volatility=low_volume_scored,
            min_matched_events_for_volatility=low_volume_matched,
        )
        if low_volume_enough:
            enough_sample_for_volatility = True
            sample_reason_code = "low_volume_profile_sufficient_sample"
            sample_floor_scored = low_volume_scored
            sample_floor_matched = low_volume_matched
            warnings.append(
                "low_volume_threshold_profile_applied "
                f"(scored_floor={low_volume_scored}; matched_floor={low_volume_matched}; "
                f"profile={threshold_profile_name}; "
                f"reasons={','.join((threshold_profile.get('activation_reasons') or ['none']))})"
            )
    confidence = _sample_confidence(
        sample_known=bool(effective_sample["known"]),
        scored_signals=effective_sample["scored_signals"],
        matched_events=effective_sample["matched_events"],
        min_scored=sample_floor_scored,
        min_matched=sample_floor_matched,
    )
    confidence_threshold = 1.0
    confidence_adequate = confidence is None or confidence >= confidence_threshold
    volatility_gate_phase = "strict_gate"
    if source_activity_insufficient:
        volatility_gate_phase = "pre_gate_insufficient_source_activity"
    elif confidence is not None and not confidence_adequate:
        volatility_gate_phase = "pre_gate_insufficient_sample"
    adaptive_ceiling_info = _adaptive_volatility_ceiling(
        rows=rows,
        baseline_summary=_pick_run_summary(rows, baseline_run_id),
        candidate_summary=_pick_run_summary(rows, candidate_run_id),
        config=config,
    )
    dynamic_scale_info = _dynamic_volatility_scale_factor(
        config=config,
        effective_sample=effective_sample,
        adaptive_ceiling_info=adaptive_ceiling_info,
    )
    effective_volatility_ceiling = float(adaptive_ceiling_info["ceiling"]) * float(dynamic_scale_info["multiplier"])
    adaptive_ceiling_info = dict(adaptive_ceiling_info)
    adaptive_ceiling_info["ceiling_after_dynamic_scale"] = effective_volatility_ceiling
    if sample_strategy != "candidate_only" and enough_sample_for_volatility:
        warnings.append(
            "aggregated_sample_used_for_edge_volatility "
            f"(window_runs={config.volatility_sample_window_runs}; runs={','.join(effective_sample['run_ids'])})"
        )

    volatility_gate_diagnostic = {
        "phase": volatility_gate_phase,
        "pre_gate": {
            "sample_confidence": confidence,
            "confidence_threshold": confidence_threshold,
            "insufficient_sample": bool(not confidence_adequate) if confidence is not None else False,
        },
        "strict_gate": {
            "enforced": bool(confidence_adequate) and not source_activity_insufficient,
            "hard_fail_applied": False,
        },
        "comparison": {
            "observed_volatility": candidate_edge_volatility,
            "threshold_used": effective_volatility_ceiling,
            "pair_count": 1,
            "decision_count": 1 if confidence_adequate else 0,
            "sample_preconditions": {
                "known": bool(effective_sample["known"]),
                "strategy": sample_strategy,
                "reason_code": sample_reason_code,
                "scored_signals": effective_sample["scored_signals"],
                "matched_events": effective_sample["matched_events"],
                "min_scored_signals_for_volatility": sample_floor_scored,
                "min_matched_events_for_volatility": sample_floor_matched,
                "enough_sample_for_volatility": bool(enough_sample_for_volatility),
            },
            "source_activity_pre_gate": source_activity_pre_gate,
        },
        "top_contributors": _top_volatility_contributors(baseline=baseline, candidate=candidate),
    }

    if not source_activity_insufficient:
        if candidate_edge_volatility is None:
            diag = candidate["diagnostics"].get("edge_volatility_reason_code", "missing_field_edge_volatility")
            failures.append(f"missing_edge_volatility_metric reason_code={diag}")
        elif effective_sample["known"] and not enough_sample_for_volatility:
            warnings.append(
                "insufficient_sample_for_edge_volatility "
                f"(scored_signals={effective_sample['scored_signals']} required>={sample_floor_scored}; "
                f"matched_events={effective_sample['matched_events']} required>={sample_floor_matched}; "
                f"strategy={sample_strategy}; reason_code={sample_reason_code})"
            )
        elif not confidence_adequate:
            warnings.append(
                "insufficient_sample_for_edge_volatility "
                f"(confidence={confidence:.3f} threshold>={confidence_threshold:.3f}; "
                f"strategy={sample_strategy}; reason_code=insufficient_sample_confidence)"
            )
        elif candidate_edge_volatility > effective_volatility_ceiling:
            volatility_gate_diagnostic["strict_gate"]["hard_fail_applied"] = True
            failures.append(
                "edge_volatility_above_ceiling "
                f"(candidate={candidate_edge_volatility:.4f} > ceiling={effective_volatility_ceiling:.4f})"
            )

    suppression_reasons = sorted(set(baseline["suppression_counts"].keys()) | set(candidate["suppression_counts"].keys()))
    suppression_drifts: dict[str, float] = {}
    suppression_drift_details: dict[str, dict[str, Any]] = {}
    failure_diagnostics = {
        "suppression_drift": {
            "failing_reasons": {},
        }
    }
    can_evaluate_drift = ((not effective_sample["known"]) or enough_sample_for_volatility) and not source_activity_insufficient
    if can_evaluate_drift:
        for reason in suppression_reasons:
            base = int(baseline["suppression_counts"].get(reason, 0))
            cand = int(candidate["suppression_counts"].get(reason, 0))
            denom = max(base, 1)
            numerator = abs(cand - base)
            drift = numerator / float(denom)
            suppression_drifts[reason] = drift
            suppression_drift_details[reason] = {
                "reason": reason,
                "numerator": numerator,
                "denominator": denom,
                "baseline_count": base,
                "candidate_count": cand,
                "delta_count": cand - base,
                "drift": drift,
            }
            if max(base, cand) >= config.suppression_min_volume and drift > config.max_suppression_drift:
                failure_diagnostics["suppression_drift"]["failing_reasons"][reason] = _build_suppression_drift_failure_diagnostics(
                    reason=reason,
                    baseline=baseline,
                    candidate=candidate,
                )
                failures.append(
                    "suppression_drift_exceeded "
                    f"(reason={reason} baseline={base} candidate={cand} drift={drift:.4f} > bound={config.max_suppression_drift:.4f})"
                )

    has_insufficient_sample_warning = any(item.startswith("insufficient_sample_for_edge_volatility") for item in warnings)
    has_schema_failure = any(item.startswith("missing_feature_completeness_metric") for item in failures) or any(
        item.startswith("missing_edge_volatility_metric") for item in failures
    )
    if failures:
        candidate_terminal_reason = str(candidate.get("no_hit_terminal_reason_code") or "none")
        baseline_terminal_reason = str(baseline.get("no_hit_terminal_reason_code") or "none")
        candidate_matched_events = int(candidate.get("matched_events") or 0)
        baseline_matched_events = int(baseline.get("matched_events") or 0)
        candidate_has_no_actionable_hits = candidate_matched_events <= 0 or candidate_terminal_reason != "none"
        baseline_has_no_actionable_hits = baseline_matched_events <= 0 or baseline_terminal_reason != "none"
        if candidate_has_no_actionable_hits:
            failures.append(f"dominant_no_hit_reason (candidate={candidate_terminal_reason})")
        elif baseline_has_no_actionable_hits:
            failures.append(f"dominant_no_hit_reason_baseline (baseline={baseline_terminal_reason})")
        status = "schema_missing" if has_schema_failure else "fail"
    elif source_activity_insufficient or has_insufficient_sample_warning:
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
            "low_volume_mode_enabled": config.low_volume_mode_enabled,
            "low_volume_upcoming_match_count_trigger": config.low_volume_upcoming_match_count_trigger,
            "low_volume_remaining_pairs_trigger": config.low_volume_remaining_pairs_trigger,
            "low_volume_min_scored_signals_for_volatility": config.low_volume_min_scored_signals_for_volatility,
            "low_volume_min_matched_events_for_volatility": config.low_volume_min_matched_events_for_volatility,
            "volatility_sample_window_runs": config.volatility_sample_window_runs,
            "volatility_dynamic_scaling_enabled": config.volatility_dynamic_scaling_enabled,
            "volatility_dynamic_small_sample_loosen_max": config.volatility_dynamic_small_sample_loosen_max,
            "volatility_dynamic_large_sample_tighten_max": config.volatility_dynamic_large_sample_tighten_max,
            "volatility_dynamic_target_sample_multiplier": config.volatility_dynamic_target_sample_multiplier,
            "max_suppression_drift": config.max_suppression_drift,
            "suppression_min_volume": config.suppression_min_volume,
            "stake_policy_enabled": config.stake_policy_enabled,
            "stake_policy_min_stake_mxn": config.stake_policy_min_stake_mxn,
            "stake_policy_round_to_min": config.stake_policy_round_to_min,
            "stake_policy": canonical_stake_policy.canonical_policy(),
        },
        "effective_volatility_ceiling": adaptive_ceiling_info,
        "effective_volatility_dynamic_scale": dynamic_scale_info,
        "sample_assessment": {
            "strategy": sample_strategy,
            "known": bool(effective_sample["known"]),
            "scored_signals": effective_sample["scored_signals"],
            "matched_events": effective_sample["matched_events"],
            "run_ids": effective_sample["run_ids"],
            "enough_sample_for_volatility": enough_sample_for_volatility,
            "reason_code": source_activity_pre_gate.get("reason_code") if source_activity_insufficient else sample_reason_code,
            "confidence": confidence,
            "confidence_threshold": confidence_threshold,
            "confidence_adequate": confidence_adequate,
            "strict_default_result": strict_sample_result,
        },
        "threshold_profile": {
            "active_profile": threshold_profile_name,
            "rationale": threshold_profile_rationale,
            "fallback_mode_used": fallback_profile_used,
            "activation_reasons": list(threshold_profile.get("activation_reasons") or []),
            "evidence_snapshot": dict(threshold_profile.get("evidence_snapshot") or {}),
            "schedule_context": dict(threshold_profile.get("schedule_context") or {}),
            "sample_floors": {
                "min_scored_signals_for_volatility": sample_floor_scored,
                "min_matched_events_for_volatility": sample_floor_matched,
            },
            "low_volume_mode": threshold_profile.get("low_volume_mode") or {},
        },
        "volatility_diagnostic": volatility_gate_diagnostic,
        "suppression_drifts": suppression_drifts,
        "suppression_drift_details": suppression_drift_details,
        "failure_diagnostics": failure_diagnostics,
        "warnings": warnings,
        "high_visibility_warnings": high_visibility_warnings,
        "failures": failures,
    }


def _windowed_fallback_assessment(
    rows: list[dict[str, Any]],
    config: EdgeQualityGateConfig,
    baseline_run_id: str,
    candidate_run_id: str,
    recent_run_window_radius: int,
    min_neighboring_pairs: int,
    ordered_run_ids: list[str] | None = None,
) -> dict[str, Any]:
    run_ids = ordered_run_ids or _rolling_run_ids(rows)
    baseline_idx = _find_run_index(run_ids, baseline_run_id)
    candidate_idx = _find_run_index(run_ids, candidate_run_id)
    radius = max(1, int(recent_run_window_radius))
    if baseline_idx < 0 or candidate_idx < 0:
        return {
            "label": "fallback_window_assessment",
            "triggered_by_status": "insufficient_sample",
            "available": False,
            "reason": "candidate_or_baseline_not_found_in_ordered_runs",
        }

    anchor_low = min(baseline_idx, candidate_idx)
    anchor_high = max(baseline_idx, candidate_idx)
    start_idx = max(0, anchor_low - radius)
    end_idx = min(len(run_ids), anchor_high + radius + 1)
    required_pairs = max(1, int(min_neighboring_pairs))
    while (end_idx - start_idx - 1) < required_pairs and (start_idx > 0 or end_idx < len(run_ids)):
        if start_idx > 0:
            start_idx -= 1
        if (end_idx - start_idx - 1) >= required_pairs:
            break
        if end_idx < len(run_ids):
            end_idx += 1
    min_scored = int(config.min_scored_signals_for_volatility)
    min_matched = int(config.min_matched_events_for_volatility)

    def _window_sample_counts(window_ids: list[str]) -> dict[str, Any]:
        scored_total = 0
        matched_total = 0
        known = True
        for run_id in window_ids:
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
        return {
            "known": known,
            "run_ids": window_ids,
            "scored_signals": scored_total if known else None,
            "matched_events": matched_total if known else None,
            "enough_sample_for_decision": known and scored_total >= min_scored and matched_total >= min_matched,
        }

    while True:
        window_pair_count = end_idx - start_idx - 1
        if window_pair_count < required_pairs:
            break
        candidate_window_ids = run_ids[start_idx + 1 : end_idx]
        sample_counts = _window_sample_counts(candidate_window_ids)
        if sample_counts["enough_sample_for_decision"]:
            break
        if start_idx == 0 and end_idx == len(run_ids):
            break
        if start_idx > 0:
            start_idx -= 1
        if end_idx < len(run_ids):
            end_idx += 1

    window_run_ids = run_ids[start_idx:end_idx]
    candidate_window_ids = run_ids[start_idx + 1 : end_idx]
    sample_counts = _window_sample_counts(candidate_window_ids)
    pair_reports = [
        evaluate_edge_quality_gate(
            rows=rows,
            baseline_run_id=prior_run_id,
            candidate_run_id=next_run_id,
            config=config,
            ordered_run_ids=run_ids,
        )
        for prior_run_id, next_run_id in _run_pairs_from_ids(window_run_ids)
    ]
    status_counts = {"pass": 0, "true_fail": 0, "insufficient_sample": 0, "schema_missing": 0}
    for report in pair_reports:
        status_counts[_status_bucket(report)] += 1

    sample_ready = bool(sample_counts["enough_sample_for_decision"]) or not bool(sample_counts["known"])
    decision_support_status = "insufficient_sample"
    if sample_ready and status_counts["true_fail"] > 0:
        decision_support_status = "fail"
    elif sample_ready and status_counts["pass"] > 0:
        decision_support_status = "pass"
    elif sample_counts["enough_sample_for_decision"] and status_counts["insufficient_sample"] > 0:
        decision_support_status = "pass"
    elif status_counts["insufficient_sample"] > 0:
        decision_support_status = "insufficient_sample"
    elif status_counts["schema_missing"] > 0:
        decision_support_status = "schema_missing"

    return {
        "label": "fallback_window_assessment",
        "triggered_by_status": "insufficient_sample",
        "available": True,
        "window_radius_runs": radius,
        "min_neighboring_pairs": required_pairs,
        "window_run_ids": window_run_ids,
        "pair_count": len(pair_reports),
        "status_counts": status_counts,
        "decision_support_status": decision_support_status,
        "effective_sample_counts": {
            "known": sample_counts["known"],
            "scored_signals": sample_counts["scored_signals"],
            "matched_events": sample_counts["matched_events"],
            "run_ids": sample_counts["run_ids"],
            "min_scored_signals_for_volatility": min_scored,
            "min_matched_events_for_volatility": min_matched,
            "enough_sample_for_decision": sample_counts["enough_sample_for_decision"],
        },
        "pairs": pair_reports,
    }


def evaluate_edge_quality_compare_report(
    rows: list[dict[str, Any]],
    baseline_run_id: str,
    candidate_run_id: str,
    config: EdgeQualityGateConfig,
    ordered_run_ids: list[str] | None = None,
    fallback_recent_run_window_radius: int = 3,
    fallback_min_neighboring_pairs: int = 4,
    export_dir: str = "",
) -> dict[str, Any]:
    canonical_stake_policy = StakePolicyConfig.from_legacy(
        enabled=bool(config.stake_policy_enabled),
        minimum_stake_mxn=float(config.stake_policy_min_stake_mxn),
        round_to_min=bool(config.stake_policy_round_to_min),
    ).with_canonicalized_fields()
    compare_set_run_ids = ordered_run_ids or [baseline_run_id, candidate_run_id]
    compare_set_policy_tags = _resolve_compare_set_policy_tags(
        rows=rows,
        compare_set_run_ids=compare_set_run_ids,
        fallback_enabled=bool(config.stake_policy_enabled),
    )
    observed_tags = set(compare_set_policy_tags.values())
    if len(observed_tags) > 1:
        mixed_runs = ", ".join(
            f"{run_id}={str(compare_set_policy_tags[run_id]).lower()}" for run_id in compare_set_run_ids
        )
        raise ValueError(
            "Mixed stake_policy_enabled states detected within compare set; "
            f"refuse to produce comparison report. compare_set={mixed_runs}"
        )

    threshold_profile = _resolve_threshold_profile(
        summary_row=_pick_run_summary(rows, candidate_run_id),
        candidate_run_id=candidate_run_id,
        ordered_run_ids=ordered_run_ids or _rolling_run_ids(rows),
        config=config,
    )
    baseline_snapshot = _snapshot(rows, baseline_run_id, config)
    candidate_snapshot = _snapshot(rows, candidate_run_id, config)
    profile_floors = threshold_profile.get("sample_floors") or {}
    strict_pair_operational_pre_gate = _strict_pair_operational_sample_pre_gate(
        baseline=baseline_snapshot,
        candidate=candidate_snapshot,
        config=config,
        minimums=profile_floors,
    )
    low_volume_mode = threshold_profile.get("low_volume_mode") or {}
    if (not strict_pair_operational_pre_gate["ok"]) and str(threshold_profile.get("active_profile")) != "strict_default":
        low_scored = max(1, int(profile_floors.get("min_scored_signals_for_volatility", config.min_scored_signals_for_volatility)))
        low_matched = max(1, int(profile_floors.get("min_matched_events_for_volatility", config.min_matched_events_for_volatility)))
        baseline_ok = int(baseline_snapshot.get("scored_signals") or 0) >= low_scored and int(
            baseline_snapshot.get("matched_events") or 0
        ) >= low_matched
        candidate_ok = int(candidate_snapshot.get("scored_signals") or 0) >= low_scored and int(
            candidate_snapshot.get("matched_events") or 0
        ) >= low_matched
        if baseline_ok and candidate_ok:
            strict_pair_operational_pre_gate = dict(strict_pair_operational_pre_gate)
            strict_pair_operational_pre_gate["ok"] = True
            strict_pair_operational_pre_gate["reason_code"] = "operational_sample_thresholds_met_low_volume_profile"
            strict_pair_operational_pre_gate["reason_codes"] = []
            strict_pair_operational_pre_gate["minimums"] = {
                "min_scored_signals_for_volatility": low_scored,
                "min_matched_events_for_volatility": low_matched,
            }
            strict_pair_operational_pre_gate["threshold_profile"] = {
                "active_profile": str(threshold_profile.get("active_profile") or "strict_default"),
                "fallback_mode_used": bool(threshold_profile.get("fallback_mode_used")),
                "rationale": str(threshold_profile.get("rationale") or "strict operational thresholds relaxed during low-volume period"),
                "activation_reasons": list(threshold_profile.get("activation_reasons") or []),
                "evidence_snapshot": dict(threshold_profile.get("evidence_snapshot") or {}),
                "low_volume_mode": low_volume_mode,
            }
    strict_pair_operational_pre_gate.setdefault(
        "threshold_profile",
        {
            "active_profile": str(threshold_profile.get("active_profile") or "strict_default"),
            "fallback_mode_used": bool(threshold_profile.get("fallback_mode_used")),
            "rationale": str(threshold_profile.get("rationale") or "strict operational thresholds enforced"),
            "activation_reasons": list(threshold_profile.get("activation_reasons") or []),
            "evidence_snapshot": dict(threshold_profile.get("evidence_snapshot") or {}),
            "low_volume_mode": low_volume_mode,
        },
    )
    strict_pair_preconditions = _strict_pair_precondition_diagnostics(baseline_snapshot, candidate_snapshot)
    if not strict_pair_operational_pre_gate["ok"]:
        pair_level_result = _invalid_strict_pair_result(
            baseline=baseline_snapshot,
            candidate=candidate_snapshot,
            config=config,
            reason_codes=list(strict_pair_operational_pre_gate.get("reason_codes") or ["insufficient_operational_sample"]),
            status="insufficient_operational_sample",
            reason_code="strict_pair_operational_pre_gate_failed",
            pre_gate_details=strict_pair_operational_pre_gate,
        )
    elif strict_pair_preconditions["ok"]:
        pair_level_result = evaluate_edge_quality_gate(
            rows=rows,
            baseline_run_id=baseline_run_id,
            candidate_run_id=candidate_run_id,
            config=config,
            ordered_run_ids=ordered_run_ids,
        )
    else:
        pair_level_result = _invalid_strict_pair_result(
            baseline=baseline_snapshot,
            candidate=candidate_snapshot,
            config=config,
            reason_codes=list(strict_pair_preconditions.get("reason_codes") or ["invalid_strict_pair_baseline"]),
            pre_gate_details=strict_pair_operational_pre_gate,
        )

    fallback_result: dict[str, Any] | None = None
    if str(pair_level_result.get("status") or "") == "insufficient_sample":
        fallback_result = _windowed_fallback_assessment(
            rows=rows,
            config=config,
            baseline_run_id=baseline_run_id,
            candidate_run_id=candidate_run_id,
            recent_run_window_radius=max(1, int(fallback_recent_run_window_radius)),
            min_neighboring_pairs=max(1, int(fallback_min_neighboring_pairs)),
            ordered_run_ids=ordered_run_ids,
        )

    strict_status = str(pair_level_result.get("status") or "unknown")
    strict_status_reason = str((pair_level_result.get("sample_assessment") or {}).get("reason_code") or strict_status)
    windowed_status = "not_triggered"
    if fallback_result is not None:
        fallback_result["triggered_by_reason"] = strict_status_reason
        if bool(fallback_result.get("available")):
            windowed_status = str(fallback_result.get("decision_support_status") or "insufficient_sample")
        else:
            windowed_status = "unavailable"

    decision_authoritative_source = "strict_pair_gate"
    decision_authoritative_status = strict_status
    if strict_status == "insufficient_sample" and windowed_status not in {"not_triggered", "unavailable"}:
        decision_authoritative_source = "windowed_fallback_result"
        decision_authoritative_status = windowed_status

    gate_verdict = "blocked_insufficient_sample"
    blocked_by_strict_sample_in_low_volume = False
    if not strict_pair_operational_pre_gate["ok"]:
        if str(threshold_profile.get("active_profile")) != "strict_default":
            gate_verdict = "blocked_low_volume_strict_sample"
            blocked_by_strict_sample_in_low_volume = True
        else:
            gate_verdict = "blocked_insufficient_operational_sample"
    elif not strict_pair_preconditions["ok"]:
        gate_verdict = "blocked_insufficient_sample"
    elif decision_authoritative_status == "fail":
        gate_verdict = "failed_quality_regression"
    elif decision_authoritative_status == "pass":
        gate_verdict = "passed_quality_gate"
    elif (
        strict_status == "insufficient_sample"
        and decision_authoritative_status == "insufficient_sample"
        and bool(((pair_level_result.get("threshold_profile") or {}).get("low_volume_mode") or {}).get("active"))
    ):
        gate_verdict = "blocked_low_volume_strict_sample"
        blocked_by_strict_sample_in_low_volume = True

    baseline_stake_summary = (pair_level_result.get("baseline") or {}).get("stake_policy_summary") or {}
    candidate_stake_summary = (pair_level_result.get("candidate") or {}).get("stake_policy_summary") or {}
    policy_delta_metrics = {}
    for key in (
        "signal_rows_evaluated",
        "suppressed_count",
        "adjusted_count",
        "passed_count",
        "missing_stake_count",
    ):
        baseline_value = int(baseline_stake_summary.get(key, 0) or 0)
        candidate_value = int(candidate_stake_summary.get(key, 0) or 0)
        policy_delta_metrics[key] = {
            "baseline": baseline_value,
            "candidate": candidate_value,
            "delta": candidate_value - baseline_value,
        }
    baseline_mode_counts = baseline_stake_summary.get("stake_mode_counts") or {}
    candidate_mode_counts = candidate_stake_summary.get("stake_mode_counts") or {}
    stake_mode_used_shift = {}
    for mode in sorted(set(baseline_mode_counts.keys()) | set(candidate_mode_counts.keys())):
        baseline_value = int(baseline_mode_counts.get(mode, 0) or 0)
        candidate_value = int(candidate_mode_counts.get(mode, 0) or 0)
        stake_mode_used_shift[mode] = {
            "baseline": baseline_value,
            "candidate": candidate_value,
            "delta": candidate_value - baseline_value,
        }

    baseline_adjustment_counts = baseline_stake_summary.get("adjustment_reason_counts") or {}
    candidate_adjustment_counts = candidate_stake_summary.get("adjustment_reason_counts") or {}
    adjustment_reason_code_shift = {}
    for reason in sorted(set(baseline_adjustment_counts.keys()) | set(candidate_adjustment_counts.keys())):
        baseline_value = int(baseline_adjustment_counts.get(reason, 0) or 0)
        candidate_value = int(candidate_adjustment_counts.get(reason, 0) or 0)
        adjustment_reason_code_shift[reason] = {
            "baseline": baseline_value,
            "candidate": candidate_value,
            "delta": candidate_value - baseline_value,
        }

    baseline_final_risk = baseline_stake_summary.get("final_risk_mxn_aggregates") or {}
    candidate_final_risk = candidate_stake_summary.get("final_risk_mxn_aggregates") or {}
    final_risk_mxn_aggregate_shift = {}
    for metric in ("count", "mean", "median"):
        baseline_value = float(baseline_final_risk.get(metric, 0.0) or 0.0)
        candidate_value = float(candidate_final_risk.get(metric, 0.0) or 0.0)
        final_risk_mxn_aggregate_shift[metric] = {
            "baseline": baseline_value,
            "candidate": candidate_value,
            "delta": candidate_value - baseline_value,
        }

    baseline_reason_counts = baseline_stake_summary.get("reason_counts") or {}
    candidate_reason_counts = candidate_stake_summary.get("reason_counts") or {}
    reason_code_shift = {}
    for reason_code in sorted(set(baseline_reason_counts.keys()) | set(candidate_reason_counts.keys())):
        baseline_value = int(baseline_reason_counts.get(reason_code, 0) or 0)
        candidate_value = int(candidate_reason_counts.get(reason_code, 0) or 0)
        reason_code_shift[reason_code] = {
            "baseline": baseline_value,
            "candidate": candidate_value,
            "delta": candidate_value - baseline_value,
        }

    runbook_branch = "standard_compare_runbook"
    runbook_path = STANDARD_COMPARE_RUNBOOK_PATH
    if bool(config.stake_policy_enabled):
        runbook_branch = "stake_policy_enabled_compare_runbook"
        runbook_path = STAKE_POLICY_ENABLED_COMPARE_RUNBOOK_PATH

    fallback_status_counts = {"pass": 0, "insufficient_sample": 0, "true_fail": 0, "schema_missing": 0}
    if isinstance(fallback_result, dict):
        raw_status_counts = fallback_result.get("status_counts") or {}
        for key in tuple(fallback_status_counts.keys()):
            fallback_status_counts[key] = int(raw_status_counts.get(key, 0) or 0)
    window_run_ids = []
    if isinstance(fallback_result, dict):
        raw_window_run_ids = fallback_result.get("window_run_ids") or []
        if isinstance(raw_window_run_ids, list):
            window_run_ids = [str(item) for item in raw_window_run_ids if str(item or "").strip()]
    window_run_id_range: dict[str, Any] = {"first": None, "last": None}
    if window_run_ids:
        window_run_id_range = {"first": window_run_ids[0], "last": window_run_ids[-1]}
    window_health_summary = {
        "status_counts": fallback_status_counts,
        "triggered_by_reason": (
            str((fallback_result or {}).get("triggered_by_reason") or strict_status_reason)
            if strict_status == "insufficient_sample"
            else "not_triggered"
        ),
        "triggered_by_status": (
            str((fallback_result or {}).get("triggered_by_status") or strict_status)
            if strict_status == "insufficient_sample"
            else "not_triggered"
        ),
        "window_run_ids": {
            "count": len(window_run_ids),
            "range": window_run_id_range,
        },
    }
    pair_threshold_profile = dict(pair_level_result.get("threshold_profile") or {})
    pair_threshold_evidence = dict(pair_threshold_profile.get("evidence_snapshot") or {})
    pair_threshold_schedule_context = pair_threshold_evidence.get("schedule_context")
    if not isinstance(pair_threshold_schedule_context, dict):
        candidate_threshold_schedule_context = pair_threshold_profile.get("schedule_context")
        if isinstance(candidate_threshold_schedule_context, dict):
            pair_threshold_schedule_context = candidate_threshold_schedule_context
    if not isinstance(pair_threshold_schedule_context, dict):
        pair_threshold_schedule_context = fallback_schedule_context("threshold_schedule_context_unavailable")

    candidate_run_order = ordered_run_ids or _rolling_run_ids(rows)
    remaining_pairs_after_candidate = 0
    if candidate_run_id in candidate_run_order:
        candidate_index = candidate_run_order.index(candidate_run_id)
        remaining_pairs_after_candidate = max(0, len(candidate_run_order) - candidate_index - 1)
    top_level_schedule_context = _build_compare_report_schedule_context(
        export_dir=export_dir,
        threshold_schedule_context=pair_threshold_schedule_context,
        remaining_pairs_after_candidate=remaining_pairs_after_candidate,
    )
    pair_threshold_evidence["schedule_context"] = top_level_schedule_context
    pair_threshold_profile["evidence_snapshot"] = pair_threshold_evidence
    pair_threshold_profile["schedule_context"] = top_level_schedule_context
    if bool(top_level_schedule_context.get("has_schedule_rows")) and (
        pair_threshold_evidence.get("schedule_context") is not top_level_schedule_context
    ):
        raise ValueError(
            "Schedule-context invariant violated: threshold_profile.evidence_snapshot.schedule_context "
            "must match top-level schedule_context when has_schedule_rows=true."
        )
    schedule_context_metadata = top_level_schedule_context

    compare_reason_code = {
        "passed_quality_gate": "EDGE_QUALITY_GATE_PASSED",
        "failed_quality_regression": "EDGE_QUALITY_GATE_FAILED",
        "blocked_insufficient_operational_sample": "EDGE_QUALITY_GATE_BLOCKED_INSUFFICIENT_OPERATIONAL_SAMPLE",
        "blocked_insufficient_sample": "EDGE_QUALITY_GATE_BLOCKED_INSUFFICIENT_SAMPLE",
        "blocked_low_volume_strict_sample": "EDGE_QUALITY_GATE_BLOCKED_LOW_VOLUME_STRICT_SAMPLE",
    }.get(gate_verdict, "EDGE_QUALITY_GATE_BLOCKED")

    return {
        "schema": "edge_quality_compare_report_v1",
        "comparison_scope": "strict_pair_with_optional_window_fallback",
        "threshold_profile": {
            "active_profile": str(pair_threshold_profile.get("active_profile") or threshold_profile.get("active_profile") or "strict_default"),
            "rationale": str(pair_threshold_profile.get("rationale") or threshold_profile.get("rationale") or "strict defaults enforced"),
            "fallback_mode_used": bool(pair_threshold_profile.get("fallback_mode_used", threshold_profile.get("fallback_mode_used"))),
            "activation_reasons": list(pair_threshold_profile.get("activation_reasons") or threshold_profile.get("activation_reasons") or []),
            "evidence_snapshot": pair_threshold_evidence,
            "schedule_context": top_level_schedule_context,
            "sample_floors": {
                "min_scored_signals_for_volatility": int(
                    profile_floors.get("min_scored_signals_for_volatility", config.min_scored_signals_for_volatility)
                ),
                "min_matched_events_for_volatility": int(
                    profile_floors.get("min_matched_events_for_volatility", config.min_matched_events_for_volatility)
                ),
            },
            "low_volume_mode": low_volume_mode,
        },
        "window_health_summary": window_health_summary,
        "schedule_context": top_level_schedule_context,
        "runbook_branch": runbook_branch,
        "runbook_path": runbook_path,
        "stake_policy_enabled": bool(config.stake_policy_enabled),
        "unit_size_mxn": float(canonical_stake_policy.unit_size_mxn),
        "min_bet_mxn": float(canonical_stake_policy.min_bet_mxn),
        "bucket_step_mxn": float(canonical_stake_policy.bucket_step_mxn),
        "rounding_mode": str(canonical_stake_policy.bucket_rounding),
        "compare_set_stake_policy_tags": compare_set_policy_tags,
        "labels": {
            "strict_pair_gate": "pair_level_result",
            "fallback_window_assessment": "windowed_fallback_result",
        },
        "evidence_bundle": {
            "stake_policy_enabled": bool(config.stake_policy_enabled),
            "stake_policy_min_stake_mxn": float(config.stake_policy_min_stake_mxn),
            "stake_policy_round_to_min": bool(config.stake_policy_round_to_min),
            "stake_policy": canonical_stake_policy.canonical_policy(),
            "unit_size_mxn": float(canonical_stake_policy.unit_size_mxn),
            "min_bet_mxn": float(canonical_stake_policy.min_bet_mxn),
            "bucket_step_mxn": float(canonical_stake_policy.bucket_step_mxn),
            "rounding_mode": str(canonical_stake_policy.bucket_rounding),
            "schedule_context": top_level_schedule_context,
        },
        "stake_policy_outcome_comparison": {
            "counts": policy_delta_metrics,
            "reason_code_shift": reason_code_shift,
            "stake_mode_used_shift": stake_mode_used_shift,
            "adjustment_reason_code_shift": adjustment_reason_code_shift,
            "final_risk_mxn_aggregate_shift": final_risk_mxn_aggregate_shift,
        },
        "pair_level_result": pair_level_result,
        "windowed_fallback_result": fallback_result,
        "final_operator_summary": {
            "strict_pair_status": strict_status,
            "strict_pair_status_reason": strict_status_reason,
            "windowed_decision_status": windowed_status,
            "strict_pair_sample_assessment": (pair_level_result.get("sample_assessment") or {}),
            "strict_pair_threshold_profile": pair_threshold_profile,
            "schedule_context": top_level_schedule_context,
            "fallback_effective_sample_counts": (
                (fallback_result or {}).get("effective_sample_counts")
                if isinstance(fallback_result, dict)
                else None
            ),
            "decision_authoritative_source": decision_authoritative_source,
            "decision_authoritative_status": decision_authoritative_status,
            "gate_verdict": gate_verdict,
            "strict_pair_preconditions": strict_pair_preconditions,
            "strict_pair_operational_pre_gate": strict_pair_operational_pre_gate,
            "blocked_by_strict_sample_in_low_volume": blocked_by_strict_sample_in_low_volume,
            "runbook_branch": runbook_branch,
            "stake_policy_enabled": bool(config.stake_policy_enabled),
        },
        "status": "success",
        "gate_verdict": gate_verdict,
        "reason_code": compare_reason_code,
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


def _window_threshold_value(mapping: dict[int, int] | None, window_days: int, fallback: int) -> int:
    if not mapping:
        return int(fallback)
    value = mapping.get(int(window_days))
    if value is None:
        return int(fallback)
    return max(0, int(value))


def _daily_readiness_sentinel(rows: list[dict[str, Any]]) -> dict[str, Any]:
    per_day: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not is_run_edgeboard_summary_row(row):
            continue
        summary = _normalize_legacy_summary_row(row)
        signal_summary = _extract_signal_summary(summary)
        ended_at = _parse_timestamp(summary.get("ended_at")) or _parse_timestamp(summary.get("started_at"))
        day = ended_at.date().isoformat() if ended_at else "unknown"
        bucket = per_day.setdefault(
            day,
            {
                "date": day,
                "run_count": 0,
                "runs_with_matched_events_gt0": 0,
                "runs_with_scored_signals_gt0": 0,
                "terminal_reason_counts": Counter(),
            },
        )
        bucket["run_count"] += 1

        matched_events = _extract_matched_events(summary)
        scored_signals = _extract_scored_signals(summary, signal_summary)
        if int(matched_events or 0) > 0:
            bucket["runs_with_matched_events_gt0"] += 1
        if int(scored_signals or 0) > 0:
            bucket["runs_with_scored_signals_gt0"] += 1

        terminal_reason = str(summary.get("no_hit_terminal_reason_code") or "none")
        bucket["terminal_reason_counts"][terminal_reason] += 1

    daily_rows: list[dict[str, Any]] = []
    for day in sorted(per_day):
        row = per_day[day]
        terminal_counts = row.pop("terminal_reason_counts")
        dominant_terminal_no_hit_reason = "none"
        if terminal_counts:
            dominant_terminal_no_hit_reason = sorted(
                terminal_counts.items(),
                key=lambda item: (-int(item[1]), str(item[0])),
            )[0][0]
        gate_recommendation = (
            "ready_for_strict_gate"
            if min(
                int(row.get("runs_with_matched_events_gt0") or 0),
                int(row.get("runs_with_scored_signals_gt0") or 0),
            ) >= 2
            else "insufficient_operational_sample"
        )
        daily_rows.append(
            {
                **row,
                "dominant_terminal_no_hit_reason": dominant_terminal_no_hit_reason,
                "gate_recommendation": gate_recommendation,
            }
        )

    latest_day = daily_rows[-1] if daily_rows else {}
    triage_decision_summary = {
        "date": latest_day.get("date"),
        "runs_with_matched_events_gt0": int(latest_day.get("runs_with_matched_events_gt0") or 0),
        "runs_with_scored_signals_gt0": int(latest_day.get("runs_with_scored_signals_gt0") or 0),
        "dominant_terminal_no_hit_reason": str(latest_day.get("dominant_terminal_no_hit_reason") or "none"),
        "gate_recommendation": str(latest_day.get("gate_recommendation") or "insufficient_operational_sample"),
    }
    return {
        "schema": "edge_quality_daily_readiness_sentinel_v1",
        "days": daily_rows,
        "triage_decision_summary": triage_decision_summary,
    }


def evaluate_daily_edge_quality_slo(
    rows: list[dict[str, Any]],
    gate_config: EdgeQualityGateConfig,
    slo_config: DailyEdgeQualitySLOConfig,
    as_of_utc: str = "",
) -> dict[str, Any]:
    as_of = _resolve_as_of_utc(as_of_utc)
    window_reports: list[dict[str, Any]] = []
    window_ratio_trends: list[dict[str, Any]] = []
    aggregate_status_counts = {"pass": 0, "true_fail": 0, "insufficient_sample": 0, "schema_missing": 0}
    aggregate_decisionable_status_counts = {"pass": 0, "true_fail": 0}
    excluded_pair_count = 0

    for window_days in sorted(set(int(item) for item in slo_config.window_days if int(item) > 0)):
        min_ended_at = _window_threshold_iso(as_of, window_days)
        run_ids = _rolling_run_ids(rows, min_ended_at=min_ended_at)
        run_snapshots = {run_id: _snapshot(rows=rows, run_id=run_id, config=gate_config) for run_id in run_ids}
        actionable_window_run_ids = [
            run_id for run_id in run_ids if bool((run_snapshots.get(run_id) or {}).get("actionable_window_run"))
        ]
        non_actionable_runs = [
            {
                "run_id": run_id,
                "actionable_window_run": False,
                "no_hit_terminal_reason_code": (run_snapshots.get(run_id) or {}).get("no_hit_terminal_reason_code"),
                "run_actionability_context": (run_snapshots.get(run_id) or {}).get("run_actionability_context") or {},
            }
            for run_id in run_ids
            if run_id not in actionable_window_run_ids
        ]
        pair_reports = [
            evaluate_edge_quality_gate(
                rows=rows,
                baseline_run_id=baseline_run_id,
                candidate_run_id=candidate_run_id,
                config=gate_config,
                ordered_run_ids=actionable_window_run_ids,
            )
            for baseline_run_id, candidate_run_id in _run_pairs_from_ids(actionable_window_run_ids)
        ]

        status_counts = {"pass": 0, "true_fail": 0, "insufficient_sample": 0, "schema_missing": 0}
        for report in pair_reports:
            status_counts[_status_bucket(report)] += 1

        decisionable_status_counts = {"pass": 0, "true_fail": 0}
        excluded_pairs: list[dict[str, Any]] = []
        min_scored = _window_threshold_value(
            slo_config.min_scored_signals_by_window,
            window_days,
            int(gate_config.min_scored_signals_for_volatility),
        )
        min_matched = _window_threshold_value(
            slo_config.min_matched_events_by_window,
            window_days,
            int(gate_config.min_matched_events_for_volatility),
        )
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
                        "volatility_diagnostic": report.get("volatility_diagnostic"),
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
            "actionable_window_run_count": len(actionable_window_run_ids),
            "non_actionable_run_count": len(non_actionable_runs),
            "non_actionable_runs": non_actionable_runs,
            "pair_count": len(pair_reports),
            "sample_floor": {
                "min_scored_signals": min_scored,
                "min_matched_events": min_matched,
                "min_pairs_per_window": int(slo_config.min_pairs_per_window),
            },
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
        pair_count = len(pair_reports)
        decisionable_ratio = (decisionable_pair_count / pair_count) if pair_count else None
        insufficient_ratio = (status_counts["insufficient_sample"] / pair_count) if pair_count else None
        window_ratio_trends.append(
            {
                "window_days": window_days,
                "pair_count": pair_count,
                "decisionable_pair_count": decisionable_pair_count,
                "insufficient_sample_pair_count": status_counts["insufficient_sample"],
                "decisionable_ratio": decisionable_ratio,
                "insufficient_sample_ratio": insufficient_ratio,
            }
        )
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
    insufficient_windows = [window for window in window_reports if not bool(window.get("decisionable"))]

    if not decisionable_windows:
        gate_verdict = "insufficient_sample"
        gate_reason = "no_decisionable_windows"
    elif insufficient_windows:
        gate_verdict = "insufficient_sample"
        gate_reason = "insufficient_sample_floor_not_met_for_all_windows"
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

    parity_contract_status = "not_evaluated"
    decisionability_status = "pass"
    if gate_reason in {"no_decisionable_windows", "insufficient_sample_floor_not_met_for_all_windows"}:
        decisionability_status = "insufficient_sample"
    quality_status = "pass"
    if gate_reason == "decisionable_window_fail_rate_exceeded_threshold":
        quality_status = "fail"
    elif not decisionable_windows:
        quality_status = "insufficient_sample"

    if parity_contract_status in {"fail", "insufficient_sample"}:
        operator_composite_reason = f"parity_contract_blocker:{parity_contract_status}"
    elif decisionability_status != "pass":
        operator_composite_reason = f"decisionability_blocker:{gate_reason}"
    elif quality_status != "pass":
        operator_composite_reason = f"quality_blocker:{gate_reason}"
    else:
        operator_composite_reason = "all_components_passing"

    readiness_sentinel = _daily_readiness_sentinel(rows)

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
        "window_ratio_trends": window_ratio_trends,
        "window_reports": window_reports,
        "gate_verdict": gate_verdict,
        "gate_reason": gate_reason,
        "parity_contract_status": parity_contract_status,
        "decisionability_status": decisionability_status,
        "quality_status": quality_status,
        "operator_composite_reason": operator_composite_reason,
        "decisionable_window_count": len(decisionable_windows),
        "failing_decisionable_window_count": len(failing_decisionable_windows),
        "stake_policy_summary_counts": stake_policy_aggregate,
        "triage_decision_summary": readiness_sentinel.get("triage_decision_summary") or {},
        "daily_readiness_sentinel": readiness_sentinel,
    }


def write_daily_slo_artifacts(
    report: dict[str, Any],
    output_dir: str,
    archive_dir: str = "",
) -> tuple[Path, Path, Path]:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    reports_path = Path(output_dir)
    reports_path.mkdir(parents=True, exist_ok=True)
    daily_output = reports_path / f"edge_quality_daily_slo_{stamp}.json"
    daily_output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    quality_summary = _build_daily_slo_quality_summary(report)
    quality_summary_output = reports_path / f"edge_quality_daily_slo_quality_summary_{stamp}.json"
    quality_summary_output.write_text(json.dumps(quality_summary, separators=(",", ":"), sort_keys=True) + "\n", encoding="utf-8")
    readiness_sentinel_output = reports_path / f"edge_quality_daily_readiness_sentinel_{stamp}.json"
    readiness_sentinel_output.write_text(
        json.dumps(report.get("daily_readiness_sentinel") or {}, separators=(",", ":"), sort_keys=True) + "\n",
        encoding="utf-8",
    )
    markdown_output = reports_path / f"edge_quality_daily_slo_summary_{stamp}.md"
    markdown_output.write_text(_daily_slo_markdown_summary(report, quality_summary), encoding="utf-8")

    archive_root = Path(archive_dir) if archive_dir else reports_path / "archive"
    archive_root.mkdir(parents=True, exist_ok=True)
    archive_summary = archive_root / "edge_quality_daily_slo_summary.jsonl"
    summary_row = {
        "generated_at_utc": report.get("generated_at_utc"),
        "as_of_utc": report.get("as_of_utc"),
        "gate_verdict": report.get("gate_verdict"),
        "gate_reason": report.get("gate_reason"),
        "parity_contract_status": report.get("parity_contract_status"),
        "decisionability_status": report.get("decisionability_status"),
        "quality_status": report.get("quality_status"),
        "operator_composite_reason": report.get("operator_composite_reason"),
        "decisionable_window_count": report.get("decisionable_window_count"),
        "failing_decisionable_window_count": report.get("failing_decisionable_window_count"),
        "aggregate_status_counts": report.get("aggregate_status_counts"),
        "aggregate_decisionable_status_counts": report.get("aggregate_decisionable_status_counts"),
        "excluded_pair_count": report.get("excluded_pair_count"),
        "triage_decision_summary": report.get("triage_decision_summary") or {},
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

    return daily_output, archive_summary, readiness_sentinel_output


def _extract_issue_code(item: Any) -> str:
    token = str(item or "").strip()
    if not token:
        return "unknown"
    return token.split()[0]


def _resolve_pair_timestamp(pair: dict[str, Any]) -> str:
    candidate = pair.get("candidate") or {}
    baseline = pair.get("baseline") or {}
    for key in ("ended_at", "endedAt"):
        value = candidate.get(key) or baseline.get(key)
        text = str(value or "").strip()
        if text:
            return text
    return "unknown"


def _build_daily_slo_quality_summary(report: dict[str, Any], top_n: int = 5) -> dict[str, Any]:
    status_counts = {"pass": 0, "fail": 0, "insufficient_sample": 0}
    failure_counts: dict[str, int] = {}
    warning_counts: dict[str, int] = {}
    failure_timestamps: dict[str, list[str]] = {}
    pair_count = 0

    for window in report.get("window_reports") or []:
        for pair in window.get("pairs") or []:
            pair_count += 1
            status = str(pair.get("status") or "").strip().lower()
            if status in status_counts:
                status_counts[status] += 1

            pair_timestamp = _resolve_pair_timestamp(pair)
            for failure in pair.get("failures") or []:
                code = _extract_issue_code(failure)
                failure_counts[code] = failure_counts.get(code, 0) + 1
                failure_timestamps.setdefault(code, []).append(pair_timestamp)

            for warning in pair.get("warnings") or []:
                code = _extract_issue_code(warning)
                warning_counts[code] = warning_counts.get(code, 0) + 1

    top_failure_codes = sorted(
        failure_counts.items(),
        key=lambda item: (-item[1], item[0]),
    )[: max(0, int(top_n))]
    top_warning_codes = sorted(
        warning_counts.items(),
        key=lambda item: (-item[1], item[0]),
    )[: max(0, int(top_n))]

    top_failures = []
    for code, count in top_failure_codes:
        timestamps = sorted(ts for ts in failure_timestamps.get(code, []) if ts and ts != "unknown")
        top_failures.append(
            {
                "code": code,
                "count": count,
                "first_seen_utc": timestamps[0] if timestamps else "unknown",
                "last_seen_utc": timestamps[-1] if timestamps else "unknown",
            }
        )

    top_warnings = [{"code": code, "count": count} for code, count in top_warning_codes]
    return {
        "schema": "edge_quality_daily_slo_quality_summary_v1",
        "generated_at_utc": report.get("generated_at_utc"),
        "as_of_utc": report.get("as_of_utc"),
        "pair_count": pair_count,
        "status_counts": status_counts,
        "top_failure_codes": top_failures,
        "top_warning_codes": top_warnings,
    }


def _daily_slo_markdown_summary(report: dict[str, Any], quality_summary: dict[str, Any] | None = None) -> str:
    summary = quality_summary or _build_daily_slo_quality_summary(report)
    window_lines: list[str] = []
    for item in report.get("window_reports") or []:
        window_days = item.get("window_days")
        pair_count = item.get("pair_count")
        decisionable_pairs = item.get("decisionable_pair_count")
        verdict = item.get("verdict")
        fail_rate = item.get("fail_rate")
        fail_rate_text = "n/a" if fail_rate is None else f"{float(fail_rate):.2%}"
        window_lines.append(
            f"| {window_days} | {pair_count} | {decisionable_pairs} | {fail_rate_text} | {verdict} |"
        )
    if not window_lines:
        window_lines.append("| n/a | 0 | 0 | n/a | insufficient_sample |")

    return "\n".join(
        [
            "# Daily Edge Quality SLO Summary",
            "",
            f"- **As-of UTC:** `{report.get('as_of_utc', 'n/a')}`",
            f"- **Overall verdict:** `{report.get('gate_verdict', 'unknown')}`",
            f"- **Gate reason:** `{report.get('gate_reason', 'unknown')}`",
            f"- **Operator composite reason:** `{report.get('operator_composite_reason', 'unknown')}`",
            "",
            "## Final gate components",
            "",
            "| Component | Status |",
            "|---|---|",
            f"| parity_contract_status | `{report.get('parity_contract_status', 'unknown')}` |",
            f"| decisionability_status | `{report.get('decisionability_status', 'unknown')}` |",
            f"| quality_status | `{report.get('quality_status', 'unknown')}` |",
            "",
            "## Window detail",
            "",
            "| window_days | pair_count | decisionable_pair_count | fail_rate | verdict |",
            "|---:|---:|---:|---:|---|",
            *window_lines,
            "",
            "## Quality summary (compact)",
            "",
            "```json",
            json.dumps(summary, separators=(",", ":"), sort_keys=True),
            "```",
            "",
            "### Top failure codes",
            "",
            "| code | count | first_seen_utc | last_seen_utc |",
            "|---|---:|---|---|",
            *(
                f"| `{item.get('code', 'unknown')}` | {int(item.get('count', 0) or 0)} | "
                f"`{item.get('first_seen_utc', 'unknown')}` | `{item.get('last_seen_utc', 'unknown')}` |"
                for item in (summary.get("top_failure_codes") or [])
            ),
            "" if summary.get("top_failure_codes") else "| `none` | 0 | `n/a` | `n/a` |",
            "",
            "### Top warning codes",
            "",
            "| code | count |",
            "|---|---:|",
            *(
                f"| `{item.get('code', 'unknown')}` | {int(item.get('count', 0) or 0)} |"
                for item in (summary.get("top_warning_codes") or [])
            ),
            "" if summary.get("top_warning_codes") else "| `none` | 0 |",
            "",
        ]
    )


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Evaluate edge-quality gate from Run_Log exports.")
    parser.add_argument("path", help="Run_Log file or export directory")
    parser.add_argument("--baseline-run-id", default="", help="Baseline run_id (defaults to second-latest)")
    parser.add_argument("--candidate-run-id", default="", help="Candidate run_id (defaults to latest)")
    parser.add_argument(
        "--out-json",
        default="",
        help=(
            "Optional output path for compare report JSON artifact. "
            "When omitted for compare mode, defaults to "
            "./artifacts/compare/<baseline_run_id>_vs_<candidate_run_id>.json."
        ),
    )
    parser.add_argument(
        "--auto-select-run-pair",
        action="store_true",
        help=(
            "Auto-select PREV/POSTV run IDs from near-latest runEdgeBoard summaries and produce "
            "a single pair compare report."
        ),
    )
    parser.add_argument(
        "--include-cancelled",
        action="store_true",
        help="Include runs marked as cancelled when auto-selecting PREV/POSTV run IDs.",
    )
    parser.add_argument(
        "--selection-diagnostics-limit",
        type=int,
        default=6,
        help="Maximum number of auto-selection acceptance/rejection diagnostics to include (default: 6).",
    )
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
    parser.add_argument("--stake-policy-min-stake-mxn", type=float, default=20.0, help="Minimum MXN stake floor (default: 20).")
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
    parser.add_argument(
        "--daily-slo-min-scored-by-window",
        default="",
        help="Optional comma-separated per-window minimum scored-signals floors (e.g. 3:10,7:20).",
    )
    parser.add_argument(
        "--daily-slo-min-matched-by-window",
        default="",
        help="Optional comma-separated per-window minimum matched-events floors (e.g. 3:5,7:12).",
    )
    parser.add_argument("--daily-slo-output-dir", default="reports")
    parser.add_argument("--daily-slo-archive-dir", default="docs/baselines/edge_quality_slo")
    parser.add_argument(
        "--fallback-recent-run-window-radius",
        type=int,
        default=3,
        help=(
            "When strict pair status is insufficient_sample, evaluate surrounding runs using this many runs "
            "before/after the candidate pair as decision-support context."
        ),
    )
    parser.add_argument(
        "--fallback-min-neighboring-pairs",
        type=int,
        default=4,
        help=(
            "Minimum neighboring run pairs required for windowed fallback decision support; "
            "window auto-expands when possible (default: 4)."
        ),
    )
    parser.add_argument(
        "--as-of-utc",
        default="",
        help="Optional ISO-8601 timestamp for deterministic daily-SLO window boundaries.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Emit traceback details for CLI failures.",
    )
    return parser


def _slugify_run_id(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value or "").strip())
    return cleaned.strip("._-") or "unknown"


def _resolve_compare_out_path(baseline_run_id: str, candidate_run_id: str, out_json: str) -> Path:
    if str(out_json or "").strip():
        return Path(str(out_json).strip()).expanduser()
    baseline_slug = _slugify_run_id(baseline_run_id)
    candidate_slug = _slugify_run_id(candidate_run_id)
    return Path("artifacts") / "compare" / f"{baseline_slug}_vs_{candidate_slug}.json"


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> Path:
    target = path.expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=str(target.parent),
        prefix=f".{target.name}.",
        suffix=".tmp",
        delete=False,
    ) as handle:
        handle.write(rendered)
        handle.flush()
        os.fsync(handle.fileno())
        tmp_name = handle.name
    os.replace(tmp_name, target)
    return target.resolve()




def _preflight_validate_run_ids(rows: list[dict[str, Any]], run_ids: list[str]) -> list[str]:
    errors: list[str] = []
    requested_ids: list[str] = []
    seen: set[str] = set()
    for item in run_ids:
        run_id = str(item or "").strip()
        if not run_id or run_id in seen:
            continue
        seen.add(run_id)
        requested_ids.append(run_id)

    if not requested_ids:
        return errors

    existing_run_ids = {str(row.get("run_id") or "").strip() for row in rows if str(row.get("run_id") or "").strip()}
    for run_id in requested_ids:
        if run_id not in existing_run_ids:
            errors.append(f"run_id `{run_id}` not found in loaded Run_Log rows")
            continue

        has_summary = any(
            str(row.get("run_id") or "").strip() == run_id and is_run_edgeboard_summary_row(row)
            for row in rows
        )
        if not has_summary:
            errors.append(
                f"run_id `{run_id}` is present but missing required row_type=summary + stage=runEdgeBoard row"
            )
    return errors


def _parse_window_thresholds(raw: str, field_name: str) -> dict[int, int]:
    parsed: dict[int, int] = {}
    if not str(raw or "").strip():
        return parsed
    for item in str(raw).split(","):
        token = item.strip()
        if not token:
            continue
        if ":" not in token:
            raise ValueError(f"{field_name} entry `{token}` must use day:value format.")
        day_raw, value_raw = token.split(":", 1)
        day = int(day_raw.strip())
        value = int(value_raw.strip())
        if day <= 0:
            raise ValueError(f"{field_name} day must be positive: `{token}`")
        if value < 0:
            raise ValueError(f"{field_name} value must be non-negative: `{token}`")
        parsed[day] = value
    return parsed


def _emit_compare_report(report: dict[str, Any], out_path: Path | None = None) -> int:
    resolved_artifact: Path | None = None
    if out_path is not None:
        resolved_artifact = _write_json_atomic(out_path, report)
    print(json.dumps(report, indent=2, sort_keys=True))
    if resolved_artifact is not None:
        print(f"Compare report artifact: {resolved_artifact}", file=sys.stderr)
    return 1 if str(report.get("gate_verdict") or "") != "passed_quality_gate" else 0


def _emit_strict_cardinality_failure(
    exc: ValueError,
    *,
    debug: bool,
    out_json_path: Path | None = None,
) -> int:
    message = str(exc)
    diagnostics: dict[str, Any] = {}
    run_id = ""
    match = re.search(
        r"Expected exactly one runEdgeBoard summary row for run_id=(?P<run_id>[^;]+);\s*selection_diagnostics=(?P<diag>\{.*\})$",
        message,
    )
    if match:
        run_id = str(match.group("run_id") or "").strip()
        diagnostics_raw = str(match.group("diag") or "").strip()
        if diagnostics_raw:
            try:
                parsed_diagnostics = json.loads(diagnostics_raw)
                if isinstance(parsed_diagnostics, dict):
                    diagnostics = parsed_diagnostics
            except json.JSONDecodeError:
                diagnostics = {"raw_selection_diagnostics": diagnostics_raw}

    qualifying_row_count = int(diagnostics.get("qualifying_row_count", -1))
    classification = "contract_missing" if qualifying_row_count == 0 else "duplicate_summary_rows"
    payload = {
        "error_type": "run_summary_cardinality_mismatch",
        "classification": classification,
        "run_id": run_id,
        "diagnostics": diagnostics,
    }
    if out_json_path is not None:
        _write_json_atomic(out_json_path, payload)
    print(json.dumps(payload, indent=2, sort_keys=True))
    print(classification, file=sys.stderr)
    print(json.dumps(payload, sort_keys=True), file=sys.stderr)
    if debug:
        traceback.print_exc()
    return 2


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
        try:
            min_scored_by_window = _parse_window_thresholds(
                str(args.daily_slo_min_scored_by_window or ""),
                "--daily-slo-min-scored-by-window",
            )
            min_matched_by_window = _parse_window_thresholds(
                str(args.daily_slo_min_matched_by_window or ""),
                "--daily-slo-min-matched-by-window",
            )
        except ValueError as exc:
            parser.error(str(exc))
        slo_config = DailyEdgeQualitySLOConfig(
            window_days=window_days,
            min_pairs_per_window=max(1, int(args.daily_slo_min_pairs)),
            fail_rate_threshold=max(0.0, float(args.daily_slo_fail_rate_threshold)),
            min_scored_signals_by_window=min_scored_by_window,
            min_matched_events_by_window=min_matched_by_window,
        )
        report = evaluate_daily_edge_quality_slo(
            rows=rows,
            gate_config=config,
            slo_config=slo_config,
            as_of_utc=str(args.as_of_utc or "").strip(),
        )
        daily_path, summary_path, sentinel_path = write_daily_slo_artifacts(
            report=report,
            output_dir=str(args.daily_slo_output_dir or "reports"),
            archive_dir=str(args.daily_slo_archive_dir or ""),
        )
        report["artifacts"] = {
            "daily_report_path": str(daily_path),
            "summary_archive_path": str(summary_path),
            "daily_readiness_sentinel_path": str(sentinel_path),
        }
        print(json.dumps(report, indent=2, sort_keys=True))
        return 1 if str(report.get("gate_verdict") or "") == "fail" else 0

    if args.baseline_run_id and args.candidate_run_id:
        run_pair = [args.baseline_run_id, args.candidate_run_id]
        try:
            enforce_preflight_guard(
                export_dir=str(args.path),
                run_a=run_pair[0],
                run_b=run_pair[1],
                emergency_override_incident_tag="",
            )
        except ValueError as exc:
            parser.error(
                "Compare preflight guard failed before evaluate step:\n"
                f"- {exc}\n"
                "Remediation: rerun `scripts/export_parity_precheck.sh` for this export directory and run pair, "
                "then retry evaluate_edge_quality.py."
            )
        preflight_errors = _preflight_validate_run_ids(rows, run_pair)
        if preflight_errors:
            parser.error(
                "Run-ID preflight failed:\n"
                + "\n".join(f"- {item}" for item in preflight_errors)
                + "\nRemediation: run `precheck_run_ids.py --require-gate-prereqs` for the target run IDs before re-running evaluate_edge_quality.py."
            )
        try:
            report = evaluate_edge_quality_compare_report(
                rows=rows,
                baseline_run_id=run_pair[0],
                candidate_run_id=run_pair[1],
                config=config,
                ordered_run_ids=_rolling_run_ids(rows),
                fallback_recent_run_window_radius=max(1, int(args.fallback_recent_run_window_radius)),
                fallback_min_neighboring_pairs=max(1, int(args.fallback_min_neighboring_pairs)),
                export_dir=str(args.path),
            )
        except ValueError as exc:
            if "Expected exactly one runEdgeBoard summary row for run_id=" not in str(exc):
                raise
            return _emit_strict_cardinality_failure(exc, debug=bool(args.debug), out_json_path=(_resolve_compare_out_path(run_pair[0], run_pair[1], str(args.out_json or "")) if str(args.out_json or "").strip() else None))
        compare_out_path = _resolve_compare_out_path(run_pair[0], run_pair[1], str(args.out_json or ""))
        return _emit_compare_report(report, compare_out_path)

    if args.baseline_run_id or args.candidate_run_id:
        parser.error("--baseline-run-id and --candidate-run-id must be provided together.")

    if args.auto_select_run_pair:
        run_pair, selection_diagnostics = _select_latest_run_ids(
            rows,
            include_cancelled=bool(args.include_cancelled),
            diagnostics_limit=max(1, int(args.selection_diagnostics_limit)),
        )
        preflight_errors = _preflight_validate_run_ids(rows, run_pair)
        if preflight_errors:
            parser.error(
                "Run-ID preflight failed:\n"
                + "\n".join(f"- {item}" for item in preflight_errors)
                + "\nRemediation: run `precheck_run_ids.py --require-gate-prereqs` for the target run IDs before re-running evaluate_edge_quality.py."
            )
        try:
            report = evaluate_edge_quality_compare_report(
                rows=rows,
                baseline_run_id=run_pair[0],
                candidate_run_id=run_pair[1],
                config=config,
                ordered_run_ids=_rolling_run_ids(rows),
                fallback_recent_run_window_radius=max(1, int(args.fallback_recent_run_window_radius)),
                fallback_min_neighboring_pairs=max(1, int(args.fallback_min_neighboring_pairs)),
                export_dir=str(args.path),
            )
        except ValueError as exc:
            if "Expected exactly one runEdgeBoard summary row for run_id=" not in str(exc):
                raise
            return _emit_strict_cardinality_failure(exc, debug=bool(args.debug), out_json_path=(_resolve_compare_out_path(run_pair[0], run_pair[1], str(args.out_json or "")) if str(args.out_json or "").strip() else None))
        report["selected_run_pair"] = {
            "baseline_run_id": run_pair[0],
            "candidate_run_id": run_pair[1],
            "include_cancelled": bool(args.include_cancelled),
        }
        report["selection_diagnostics"] = selection_diagnostics
        compare_out_path = _resolve_compare_out_path(run_pair[0], run_pair[1], str(args.out_json or ""))
        return _emit_compare_report(report, compare_out_path)

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
