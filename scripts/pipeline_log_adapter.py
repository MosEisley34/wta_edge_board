#!/usr/bin/env python3
"""Adapters for mixed run-log schemas (legacy + compact schema v2)."""

from __future__ import annotations

import json
from typing import Any

REASON_CODE_ALIAS_SCHEMA_ID = "reason_code_alias_v1"
LEGACY_UNK_REASON_CODE_CANONICAL_MAP: dict[str, str] = {
    "UNK_OPEN_TS": "missing_open_timestamp",
    "UNK_OPEN_LAG": "opening_lag_exceeded",
    "UNK_7FLM53": "opening_lag_fallback_exemption_allowed",
    "UNK_X5ZLHC": "opening_lag_fallback_exemption_denied_source",
    "UNK_OYX5QE": "opening_lag_fallback_exemption_denied_age",
    "UNK_OYX4DV": "opening_lag_fallback_exemption_denied_cap",
}
REASON_CODE_ALIAS_DICTIONARIES: dict[str, dict[str, str]] = {
    REASON_CODE_ALIAS_SCHEMA_ID: {
        "competition_allowed": "CMP_ALLOW",
        "long_reason_code_key_for_schedule_enrichment_h2h_missing": "SCH_H2H_MISS",
        "market_h2h": "MKT_H2H",
        "matched_count": "MATCH_CT",
        "matched_exact": "MATCH_EXACT",
        "odds_actionable": "ODDS_ACT",
        "odds_api_success": "ODDS_API_OK",
        "odds_fetched": "ODDS_FETCH",
        "odds_non_actionable": "ODDS_NACT",
        "odds_no_active_wta_keys": "ODDS_NO_WTA",
        "opening_lag_fallback_exempted": "OPEN_EXEMPT",
        "provider_returned_null_features": "PROV_NULL",
        "rejected_count": "REJ_CT",
        "schedule_fetched": "SCH_FETCH",
        "schedule_api_success": "SCH_API_OK",
        "schedule_no_games_in_window": "SCH_NO_GAME",
        "schedule_no_active_wta_keys": "SCH_NO_WTA",
        "signal_edge_above_threshold": "SIG_EDGE",
        "signals_generated": "SIG_GEN",
        "stats_loaded": "STATS_LOAD",
        "very_long_reason_code_key_player_stats_incomplete_profile": "STATS_INCOMP",
        "odds_refresh_skipped_outside_window": "OR_OUT_WIN",
        "odds_refresh_cache_hit_within_window": "OR_HIT_WIN",
        "odds_refresh_cache_hit_outside_window": "OR_HIT_OUT",
        "odds_refresh_skipped_credits_soft_limit": "OR_SKIP_SOFT",
        "odds_refresh_skipped_credits_hard_limit": "OR_SKIP_HARD",
        "odds_refresh_skipped_no_games": "OR_SKIP_NOGAME",
        "odds_refresh_executed_in_window": "OR_EXEC_INWIN",
        "odds_refresh_bootstrap_fetch": "OR_BOOT_FETCH",
        "odds_refresh_bootstrap_inactive": "OR_BOOT_OFF",
        "odds_refresh_bootstrap_blocked_by_credit_limit": "OR_BOOT_CREDIT",
        "odds_refresh_no_eligible_matches": "OR_NO_ELIG",
        "odds_refresh_fetched_success": "OR_FETCH_OK",
        "schedule_fetch_skipped_outside_window_credit_saver": "SCH_SKIP_OUT_CRED",
        "schedule_fetch_skipped_outside_window_credit_saver_cache_expired": "SCH_SKIP_OUT_EXP",
        "productive_output_empty_streak_detected": "PO_EMPTY_STK",
        "schedule_only_streak_detected": "SCH_ONLY_STK",
        "bootstrap_empty_cycle_detected": "BOOT_EMPTY_STK",
        "opening_lag_within_limit": "OPEN_LAG_OK",
        "opening_lag_exceeded": "OPEN_LAG_HI",
        "opening_lag_blocked": "OPEN_LAG_BLOCK",
        "missing_open_timestamp": "OPEN_TS_MISS",
        "run_health_no_matches_from_odds": "RH_NO_MATCH",
        "run_health_no_matches_from_odds_consecutive": "RH_NO_MATCH_STK",
        "run_health_no_matches_from_odds_waiting": "RH_NO_MATCH_WAIT",
        "run_health_single_run_critical_triggered": "RH_CRIT_1RUN",
        "source_entity_domain_mismatch_non_tennis_sport_slug_football": "SRC_DM_FOOT",
        "source_entity_domain_mismatch": "SRC_DM",
        "player_stats_out_of_cohort_only": "PSTATS_OUT_COH",
        "player_stats_unknown_rank_only": "PSTATS_UNK_RNK",
        "match_map_diagnostic_records_written": "MM_DIAG_WR",
        "match_map_upserts": "MM_UPS",
        "match_map_upserts_matched": "MM_UPS_MT",
        "match_map_upserts_rejected": "MM_UPS_RJ",
        "productive_output_mitigation_activated": "PO_MIT_ON",
        "odds_api_failure_no_stale_fallback": "ODDS_NO_STALE",
        "odds_cache_hit": "ODDS_CACHE_HIT",
        "schedule_cache_hit": "SCH_CACHE_HIT",
        "stats_cache_hit": "STATS_CACHE_HIT",
        "odds_stale_fallback": "ODDS_STALE_FB",
        "schedule_stale_fallback": "SCH_STALE_FB",
        "stats_stale_fallback": "STATS_STALE_FB",
        "odds_cache_stale_refresh_throttled": "ODDS_CACHE_THR",
        "schedule_cache_stale_refresh_throttled": "SCH_CACHE_THR",
        "stats_cache_stale_refresh_throttled": "STATS_CACHE_THR",
        "raw_odds_upserts": "ODDS_UPS",
        "raw_player_stats_upserts": "PSTATS_UPS",
        "raw_schedule_upserts": "SCH_UPS",
        "signals_upserts": "SIG_UPS",
        "bookmakers_without_h2h_market": "ODDS_NO_H2H_BM",
        "cooldown_suppressed": "COOL_SUP",
        "duplicate_suppressed": "DUP_SUP",
        "edge_below_threshold": "EDGE_LOW",
        "events_missing_h2h_outcomes": "ODDS_NO_H2H_EVT",
        "fallback_short_circuit": "MATCH_FB_SC",
        "h2h_unavailable": "H2H_UNAV",
        "low_edge_suppressed": "EDGE_SUP",
        "missing_open_timestamp_fallback": "OPEN_TS_FB",
        "no_odds_candidates": "NO_ODDS_CAND",
        "no_player_match": "NO_P_MATCH",
        "no_schedule_candidates": "NO_SCH_CAND",
        "notify_disabled": "NOTIFY_OFF",
        "notify_missing_config": "NOTIFY_CFG",
        "null_features_fallback_scored": "NULL_FB_SCORE",
        "odds_rows_emitted": "ODDS_ROWS",
        "outside_window": "OUT_WIN",
        "outside_window_idle_skip": "OUT_WIN_IDLE",
        "missing_b": "MISS_B",
        "missing_repeat": "MISS_REPEAT",
        "provider_returned_empty": "PROV_EMPTY",
        "runtime_mode_soft_degraded": "RT_SOFT",
        "schedule_seed_no_odds": "SCH_SEED_NO_ODDS",
        "schedule_only_seed": "SCH_ONLY_SEED",
        "schedule_unavailable": "SCH_UNAV",
        "schedule_window_empty": "SCH_WIN_EMPTY",
        "schedule_enrichment_no_schedule_events": "SCH_ENR_NONE",
        "schedule_enrichment_no_upcoming_players": "SCH_ENR_NOPLY",
        "schedule_enrichment_ta_completed": "SCH_ENR_TA_OK",
        "schedule_enrichment_ta_failed_non_fatal": "SCH_ENR_TA_WARN",
        "schedule_api_success_sport_key_fallback": "SCH_API_SK_FB",
        "sent": "NOTIFY_SENT",
        "skipped_no_matched_events": "SKIP_NO_MATCHED",
        "skipped_no_player_keys": "SKIP_NO_KEYS",
        "skipped_schedule_only_no_odds": "SKIP_SCH_ONLY",
        "stale_fallback_bypassed": "STALE_BYPASS",
        "state_stale_payload_write_failed_non_fatal": "STALE_WRITE_WARN",
        "stale_odds_skip": "STALE_SKIP",
        "stats_enriched": "STATS_ENR",
        "stats_fallback_model_used": "STATS_FB_MODEL",
        "stats_model_fallback_used": "STATS_MODEL_FB",
        "stats_name_resolution_miss": "STATS_NAME_MISS",
        "stats_provider_no_record": "STATS_PROV_NONE",
        "stats_unresolved_after_fallback": "STATS_UNRES_FB",
        "stats_missing_player_a": "STATS_MISS_A",
        "stats_missing_player_b": "STATS_MISS_B",
        "stats_out_of_cohort": "STATS_OOC",
        "stats_rank_unknown": "STATS_RANK_UNK",
        "stats_top100_filter_excluded": "STATS_T100_EXCL",
        "stats_top100_fallback_applied": "STATS_T100_FB",
        "stats_zero_coverage": "STATS_ZERO",
        "bounded_stage_counter_invariant_exceeded": "INV_BOUNDED_CNT",
        "run_exception": "RUN_EXC",
        "run_success": "RUN_OK",
        "run_disabled_skip": "RUN_SKIP_OFF",
        "run_locked_skip": "RUN_SKIP_LOCK",
        "run_debounced_skip": "RUN_SKIP_DEB",
        "run_idempotency_overlap_skip": "RUN_SKIP_IDEMP",
        "run_rollup_emitted": "RUN_ROLLUP",
        "run_mode_gates": "RUN_MODE",
        "started": "RUN_START",
        "completed": "RUN_DONE",
        "trigger_noop": "TRIG_NOOP",
        "trigger_reinstalled": "TRIG_REINST",
        "trigger_removed": "TRIG_REM",
        "trigger_post_install_health": "TRIG_HEALTH",
        "reason_code_map_mutated_after_snapshot": "RC_MUTATE_SNAP",
        "reason_code_counter_exceeds_stage_max": "RC_GT_STAGE_MAX",
        "reason_code_alias_missing_fallback_emitted": "RC_ALIAS_FALLBACK",
        "run_health_expected_temporary_no_odds": "RH_TMP_NO_ODDS",
        "opening_lag_fallback_exemption_allowed": "OPEN_FB_ALLOW",
        "opening_lag_fallback_exemption_denied_source": "OPEN_FB_DENY_SRC",
        "opening_lag_fallback_exemption_denied_age": "OPEN_FB_DENY_AGE",
        "opening_lag_fallback_exemption_denied_cap": "OPEN_FB_DENY_CAP",
        "odds_api_success_sport_key_fallback": "ODDS_API_SK_FB",
        "ta_matchmx_ok": "TA_MX_OK",
        "ta_matchmx_parse_failed": "TA_MX_PARSE",
        "ta_matchmx_coverage_miss": "TA_MX_COVMISS",
        "ta_matchmx_coverage_ratio_low": "TA_MX_COVLOW",
        "ta_parse_coverage_mismatch": "TA_MX_COVMM",
        "ta_matchmx_overlap_low": "TA_MX_OVLP",
        "ta_matchmx_feature_coverage_low": "TA_MX_FEAT",
        "ta_matchmx_rows_low": "TA_MX_ROWS",
        "ta_matchmx_distinct_players_low": "TA_MX_DIST",
        "ta_matchmx_name_quality_low": "TA_MX_NAME",
        "ta_matchmx_unusable_payload": "TA_MX_UNUSE",
        "ta_matchmx_stale_fallback": "TA_MX_STALE_FB",
        "too_close_to_start_skip": "TOO_CLOSE_SKIP",
        "invalid_time_window": "TIME_WIN_BAD",
        "invalid_time_window_retry_failed": "TIME_WIN_RETRY",
        "invalid_time_window_recovered_relaxed_query": "TIME_WIN_RELAX",
        "credit_hard_limit_skip_odds": "CREDIT_HARD_ODDS",
        "source_credit_saver_skip": "SRC_CRED_SKIP",
        "schedule_window_fallback_no_odds": "SCH_WIN_FB_NO",
        "credit_header_missing": "CREDIT_HDR",
        "within_window": "IN_WIN",
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
    fallback_alias_map = dict(LEGACY_UNK_REASON_CODE_CANONICAL_MAP)
    fallback_alias_map.update({str(k): str(v) for k, v in (fallback_aliases or {}).items() if str(k) and str(v)})
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


def _apply_quality_contract_projection(adapted: dict[str, Any]) -> dict[str, Any]:
    metric_keys = ("feature_completeness", "matched_events", "scored_signals")

    def _extract_reason_aliases_from_legacy_feature_metric(value: Any) -> dict[str, Any] | None:
        parsed_value = _parse_json_like(value, value)
        if isinstance(parsed_value, dict):
            return dict(parsed_value)
        return None

    def _merge_reason_aliases(existing: Any, incoming: Any) -> dict[str, Any]:
        merged: dict[str, Any] = {}
        if isinstance(existing, dict):
            merged.update(existing)
        if isinstance(incoming, dict):
            merged.update(incoming)
        return merged

    def _coerce_numeric_metric(
        metric_name: str, current_value: Any, candidate_value: Any, row_ref: dict[str, Any], *, allow_existing: bool = False
    ) -> Any:
        if candidate_value in (None, ""):
            return current_value
        if isinstance(candidate_value, bool):
            return current_value
        if isinstance(candidate_value, (int, float)):
            if current_value not in (None, "") and not allow_existing:
                return current_value
            return candidate_value
        parsed_candidate = _parse_json_like(candidate_value, candidate_value)
        if isinstance(parsed_candidate, dict):
            row_ref["schema_violation"] = "quality_contract_metric_type_mismatch"
            row_ref["field_type_error"] = f"{metric_name}:expected_numeric_received_object"
            merged_reason_aliases = _merge_reason_aliases(
                row_ref.get("reason_aliases"), _extract_reason_aliases_from_legacy_feature_metric(parsed_candidate)
            )
            if merged_reason_aliases:
                row_ref["reason_aliases"] = merged_reason_aliases
            return current_value
        if isinstance(parsed_candidate, str):
            stripped = parsed_candidate.strip()
            if not stripped:
                return current_value
            if stripped.startswith("{") and stripped.endswith("}"):
                parsed_object = _parse_json_like(stripped, None)
                if isinstance(parsed_object, dict):
                    row_ref["schema_violation"] = "quality_contract_metric_type_mismatch"
                    row_ref["field_type_error"] = f"{metric_name}:expected_numeric_received_object"
                    merged_reason_aliases = _merge_reason_aliases(
                        row_ref.get("reason_aliases"),
                        _extract_reason_aliases_from_legacy_feature_metric(parsed_object),
                    )
                    if merged_reason_aliases:
                        row_ref["reason_aliases"] = merged_reason_aliases
                    return current_value
            try:
                numeric_value = int(stripped) if metric_name in ("matched_events", "scored_signals") else float(stripped)
                if current_value not in (None, "") and not allow_existing:
                    return current_value
                return numeric_value
            except ValueError:
                row_ref["schema_violation"] = "quality_contract_metric_type_mismatch"
                row_ref["field_type_error"] = f"{metric_name}:expected_numeric_received_string"
                return current_value
        row_ref["schema_violation"] = "quality_contract_metric_type_mismatch"
        row_ref["field_type_error"] = f"{metric_name}:expected_numeric_received_{type(parsed_candidate).__name__}"
        return current_value

    row = dict(adapted)
    migrated_aliases = _extract_reason_aliases_from_legacy_feature_metric(row.get("feature_completeness"))
    if migrated_aliases:
        row["reason_aliases"] = _merge_reason_aliases(row.get("reason_aliases"), migrated_aliases)
        row["feature_completeness"] = None

    signal_summary = _parse_json_like(row.get("signal_decision_summary"), {})
    if not isinstance(signal_summary, dict):
        return row
    quality_contract = signal_summary.get("quality_contract")
    if not isinstance(quality_contract, dict):
        return row
    quality_reason_aliases = quality_contract.get("reason_aliases")
    if isinstance(quality_reason_aliases, dict):
        row["reason_aliases"] = _merge_reason_aliases(row.get("reason_aliases"), quality_reason_aliases)

    row["feature_completeness"] = _coerce_numeric_metric(
        "feature_completeness",
        row.get("feature_completeness"),
        quality_contract.get("feature_completeness"),
        row,
    )
    if row.get("edge_volatility") in (None, "") and quality_contract.get("edge_volatility") not in (None, ""):
        row["edge_volatility"] = quality_contract.get("edge_volatility")
    row["matched_events"] = _coerce_numeric_metric(
        "matched_events",
        row.get("matched_events"),
        quality_contract.get("matched_events"),
        row,
    )
    row["scored_signals"] = _coerce_numeric_metric(
        "scored_signals",
        row.get("scored_signals"),
        quality_contract.get("scored_signals"),
        row,
    )
    if quality_contract.get("feature_completeness_reason_code") not in (None, ""):
        row["feature_completeness_reason_code"] = str(quality_contract.get("feature_completeness_reason_code"))
    if quality_contract.get("edge_volatility_reason_code") not in (None, ""):
        row["edge_volatility_reason_code"] = str(quality_contract.get("edge_volatility_reason_code"))
    if quality_contract.get("matched_events_reason_code") not in (None, ""):
        row["matched_events_reason_code"] = str(quality_contract.get("matched_events_reason_code"))
    if quality_contract.get("scored_signals_reason_code") not in (None, ""):
        row["scored_signals_reason_code"] = str(quality_contract.get("scored_signals_reason_code"))
    for metric_key in metric_keys:
        row[metric_key] = _coerce_numeric_metric(metric_key, row.get(metric_key), row.get(metric_key), row, allow_existing=True)
    return row


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

        return _apply_quality_contract_projection(adapted)

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

    adapted = {
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
        "signal_decision_summary": (
            json.dumps(record.get("sds"))
            if isinstance(record.get("sds"), (dict, list))
            else str(record.get("sds") or record.get("signal_decision_summary") or "")
        ),
    }
    return _apply_quality_contract_projection(adapted)


def adapt_run_log_records_for_legacy(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [adapt_run_log_record_for_legacy(record) for record in records]
