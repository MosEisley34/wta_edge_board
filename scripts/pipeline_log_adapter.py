#!/usr/bin/env python3
"""Adapters for mixed run-log schemas (legacy + compact schema v2)."""

from __future__ import annotations

import json
from typing import Any

REASON_CODE_ALIAS_SCHEMA_ID = "reason_code_alias_v1"
REASON_CODE_ALIAS_DICTIONARIES: dict[str, dict[str, str]] = {
    REASON_CODE_ALIAS_SCHEMA_ID: {
        "odds_refresh_skipped_outside_window": "OR_OUT_WIN",
        "odds_refresh_cache_hit_within_window": "OR_HIT_WIN",
        "odds_refresh_cache_hit_outside_window": "OR_HIT_OUT",
        "odds_refresh_skipped_credits_soft_limit": "OR_SKIP_SOFT",
        "odds_refresh_skipped_credits_hard_limit": "OR_SKIP_HARD",
        "odds_refresh_no_eligible_matches": "OR_NO_ELIG",
        "odds_refresh_fetched_success": "OR_FETCH_OK",
        "productive_output_empty_streak_detected": "PO_EMPTY_STK",
        "schedule_only_streak_detected": "SCH_ONLY_STK",
        "bootstrap_empty_cycle_detected": "BOOT_EMPTY_STK",
        "opening_lag_within_limit": "OPEN_LAG_OK",
        "opening_lag_exceeded": "OPEN_LAG_HI",
        "missing_open_timestamp": "OPEN_TS_MISS",
        "run_health_no_matches_from_odds": "RH_NO_MATCH",
        "source_entity_domain_mismatch_non_tennis_sport_slug_football": "SRC_DM_FOOT",
        "source_entity_domain_mismatch": "SRC_DM",
        "match_map_diagnostic_records_written": "MM_DIAG_WR",
        "productive_output_mitigation_activated": "PO_MIT_ON",
        "odds_api_failure_no_stale_fallback": "ODDS_NO_STALE",
    }
}
ALIAS_TO_REASON_CODE_BY_SCHEMA = {
    schema_id: {alias: code for code, alias in aliases.items()}
    for schema_id, aliases in REASON_CODE_ALIAS_DICTIONARIES.items()
}

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


def _expand_reason_map(
    reason_map: dict[str, Any] | None,
    schema_id: str = REASON_CODE_ALIAS_SCHEMA_ID,
    fallback_aliases: dict[str, str] | None = None,
) -> dict[str, float]:
    expanded: dict[str, float] = {}
    alias_to_reason_code = ALIAS_TO_REASON_CODE_BY_SCHEMA.get(schema_id) or {}
    fallback_alias_map = {str(k): str(v) for k, v in (fallback_aliases or {}).items() if str(k) and str(v)}
    for alias_or_code, raw_value in (reason_map or {}).items():
        alias_or_code_text = str(alias_or_code)
        reason_code = fallback_alias_map.get(alias_or_code_text) or alias_to_reason_code.get(alias_or_code_text, alias_or_code_text)
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            continue
        expanded[reason_code] = value
    return expanded


def _expand_stage_summaries(
    stage_summaries: list[dict[str, Any]] | None, schema_id: str = REASON_CODE_ALIAS_SCHEMA_ID
) -> list[dict[str, Any]]:
    expanded = []
    for summary in stage_summaries or []:
        clone = dict(summary or {})
        clone["reason_codes"] = _expand_reason_map(
            clone.get("reason_codes") or {},
            schema_id=schema_id,
            fallback_aliases=clone.get("fallback_aliases") or {},
        )
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
            message_schema_id = str(message.get("schema_id") or REASON_CODE_ALIAS_SCHEMA_ID)
            message["reason_codes"] = _expand_reason_map(
                message.get("reason_codes") or {},
                schema_id=message_schema_id,
                fallback_aliases=message.get("fallback_aliases") or {},
            )
            adapted["message"] = json.dumps(message)

        rejection_envelope = _parse_json_like(adapted.get("rejection_codes"), None)
        if isinstance(rejection_envelope, dict) and rejection_envelope.get("reason_codes"):
            rejection_schema_id = str(rejection_envelope.get("schema_id") or REASON_CODE_ALIAS_SCHEMA_ID)
            adapted["rejection_codes"] = json.dumps(
                _expand_reason_map(
                    rejection_envelope.get("reason_codes") or {},
                    schema_id=rejection_schema_id,
                    fallback_aliases=rejection_envelope.get("fallback_aliases") or {},
                )
            )

        stage_summary_envelope = _parse_json_like(adapted.get("stage_summaries"), None)
        if isinstance(stage_summary_envelope, dict) and stage_summary_envelope.get("stage_summaries"):
            stage_schema_id = str(stage_summary_envelope.get("schema_id") or REASON_CODE_ALIAS_SCHEMA_ID)
            adapted["stage_summaries"] = json.dumps(
                _expand_stage_summaries(stage_summary_envelope.get("stage_summaries") or [], schema_id=stage_schema_id)
            )

        return adapted

    event_type = str(record.get("et") or "")
    stage = str(record.get("st") or event_type)
    row_type = _legacy_row_type(event_type, stage)

    compact_schema_id = str(record.get("ras") or REASON_CODE_ALIAS_SCHEMA_ID)
    rm = record.get("rm") if isinstance(record.get("rm"), dict) else {}
    expanded_stage_reason_codes = _expand_reason_map(
        record.get("rc") or {},
        schema_id=compact_schema_id,
        fallback_aliases=rm.get("fallback_aliases") if isinstance(rm.get("fallback_aliases"), dict) else {},
    )
    expanded_rejections = _expand_reason_map(
        record.get("rj") or {},
        schema_id=compact_schema_id,
        fallback_aliases=rm.get("rejection_fallback_aliases") if isinstance(rm.get("rejection_fallback_aliases"), dict) else {},
    )
    expanded_stage_summaries = _expand_stage_summaries(record.get("ssu") or [], schema_id=compact_schema_id)

    msg = _parse_json_like(record.get("msg"), None)
    if isinstance(msg, dict):
        merged_message = dict(msg)
        if merged_message.get("reason_codes"):
            message_schema_id = str(merged_message.get("schema_id") or compact_schema_id or REASON_CODE_ALIAS_SCHEMA_ID)
            merged_message["reason_codes"] = _expand_reason_map(
                merged_message.get("reason_codes") or {},
                schema_id=message_schema_id,
                fallback_aliases=merged_message.get("fallback_aliases") or {},
            )
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
