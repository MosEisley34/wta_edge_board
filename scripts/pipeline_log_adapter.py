#!/usr/bin/env python3
"""Adapters for mixed run-log schemas (legacy + compact schema v2)."""

from __future__ import annotations

import json
from typing import Any

REASON_CODE_ALIAS_DICTIONARY: dict[str, str] = {
    "odds_refresh_skipped_outside_window": "or_out_win",
    "odds_refresh_cache_hit_within_window": "or_hit_win",
    "odds_refresh_cache_hit_outside_window": "or_hit_out",
    "odds_refresh_skipped_credits_soft_limit": "or_skip_soft",
    "odds_refresh_skipped_credits_hard_limit": "or_skip_hard",
    "odds_refresh_no_eligible_matches": "or_no_elig",
    "odds_refresh_fetched_success": "or_fetch_ok",
    "productive_output_empty_streak_detected": "po_empty_stk",
    "schedule_only_streak_detected": "sched_only_stk",
    "bootstrap_empty_cycle_detected": "boot_empty_stk",
    "opening_lag_within_limit": "open_lag_ok",
    "opening_lag_exceeded": "open_lag_hi",
    "missing_open_timestamp": "open_ts_miss",
    "run_health_no_matches_from_odds": "rh_no_match",
    "source_entity_domain_mismatch_non_tennis_sport_slug_football": "src_dm_foot",
    "source_entity_domain_mismatch": "src_dm",
    "match_map_diagnostic_records_written": "mm_diag_wr",
}

ALIAS_TO_REASON_CODE = {alias: code for code, alias in REASON_CODE_ALIAS_DICTIONARY.items()}


def _parse_json_like(value: Any, fallback: Any) -> Any:
    if value in (None, ""):
        return fallback
    if isinstance(value, (dict, list)):
        return value
    if not isinstance(value, str):
        return fallback
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return fallback


def _expand_reason_map(reason_map: dict[str, Any] | None) -> dict[str, float]:
    expanded: dict[str, float] = {}
    for alias_or_code, raw_value in (reason_map or {}).items():
        reason_code = ALIAS_TO_REASON_CODE.get(str(alias_or_code), str(alias_or_code))
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            continue
        expanded[reason_code] = value
    return expanded


def _expand_stage_summaries(stage_summaries: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    expanded = []
    for summary in stage_summaries or []:
        clone = dict(summary or {})
        clone["reason_codes"] = _expand_reason_map(clone.get("reason_codes") or {})
        expanded.append(clone)
    return expanded


def _legacy_row_type(event_type: str, stage: str) -> str:
    if event_type == "summary":
        return "summary"
    if event_type == "watchdog" or stage == "run_start_config_audit" or "watchdog" in stage or stage == "run_lifecycle":
        return "ops"
    return "stage"


def adapt_run_log_record_for_legacy(record: dict[str, Any]) -> dict[str, Any]:
    schema_version = int(record.get("schema_version") or 0)

    if schema_version != 2:
        adapted = dict(record)
        message = _parse_json_like(adapted.get("message"), None)
        if isinstance(message, dict) and message.get("reason_codes"):
            message["reason_codes"] = _expand_reason_map(message.get("reason_codes") or {})
            adapted["message"] = json.dumps(message)

        rejection_envelope = _parse_json_like(adapted.get("rejection_codes"), None)
        if isinstance(rejection_envelope, dict) and rejection_envelope.get("reason_codes"):
            adapted["rejection_codes"] = json.dumps(_expand_reason_map(rejection_envelope.get("reason_codes") or {}))

        stage_summary_envelope = _parse_json_like(adapted.get("stage_summaries"), None)
        if isinstance(stage_summary_envelope, dict) and stage_summary_envelope.get("stage_summaries"):
            adapted["stage_summaries"] = json.dumps(
                _expand_stage_summaries(stage_summary_envelope.get("stage_summaries") or [])
            )

        return adapted

    event_type = str(record.get("et") or "")
    stage = str(record.get("st") or event_type)
    row_type = _legacy_row_type(event_type, stage)

    expanded_stage_reason_codes = _expand_reason_map(record.get("rc") or {})
    expanded_rejections = _expand_reason_map(record.get("rj") or {})
    expanded_stage_summaries = _expand_stage_summaries(record.get("ssu") or [])

    msg = _parse_json_like(record.get("msg"), None)
    if isinstance(msg, dict):
        merged_message = dict(msg)
        if expanded_stage_reason_codes:
            merged_message["reason_codes"] = expanded_stage_reason_codes
        merged_message.setdefault("input_count", int(record.get("ic") or 0))
        merged_message.setdefault("output_count", int(record.get("oc") or 0))
        merged_message.setdefault("provider", str(record.get("pr") or ""))
        merged_message.setdefault("api_credit_usage", int(record.get("acu") or 0))
        if isinstance(record.get("rm"), dict) and record.get("rm"):
            merged_message.setdefault("reason_metadata", dict(record.get("rm") or {}))
        message = json.dumps(merged_message)
    else:
        message = str(record.get("msg") or "")

    return {
        "row_type": row_type,
        "run_id": str(record.get("rid") or ""),
        "stage": stage,
        "started_at": str(record.get("sa") or ""),
        "ended_at": str(record.get("ea") or ""),
        "status": str(record.get("ss") or ""),
        "reason_code": str(record.get("rcd") or ""),
        "message": message,
        "fetched_odds": int(record.get("fo") or 0),
        "fetched_schedule": int(record.get("fs") or 0),
        "allowed_tournaments": int(record.get("at") or 0),
        "matched": int(record.get("mt") or 0),
        "unmatched": int(record.get("um") or 0),
        "signals_found": int(record.get("sg") or 0),
        "rejection_codes": json.dumps(expanded_rejections),
        "cooldown_suppressed": int(record.get("cds") or 0),
        "duplicate_suppressed": int(record.get("dds") or 0),
        "lock_event": str(record.get("lk") or ""),
        "debounce_event": str(record.get("db") or ""),
        "trigger_event": str(record.get("tr") or ""),
        "exception": str(record.get("ex") or ""),
        "stack": str(record.get("stk") or ""),
        "stage_summaries": json.dumps(expanded_stage_summaries),
    }


def adapt_run_log_records_for_legacy(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [adapt_run_log_record_for_legacy(record) for record in records]
