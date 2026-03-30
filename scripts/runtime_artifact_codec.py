#!/usr/bin/env python3
"""Normalization helpers for runtime Run_Log/State CSV/JSON artifacts."""

from __future__ import annotations

import json
from typing import Any

RUN_LOG_STRUCTURED_FIELD_KINDS: dict[str, str] = {
    "stage_summaries": "list_or_object",
    "summary": "object",
    "reason_codes": "object",
    "reason_metadata": "object",
    "signal_decision_summary": "object",
    "stageFetchPlayerStats": "object",
    "fallback_aliases": "object",
}


def _parse_json_text(text: str) -> Any:
    try:
        return json.loads(text)
    except Exception:
        return None


def normalize_structured_value(value: Any, expected_kind: str) -> Any:
    default = [] if expected_kind in {"list", "list_or_object"} else {}
    if value is None:
        return default
    if expected_kind == "list" and isinstance(value, list):
        return value
    if expected_kind == "object" and isinstance(value, dict):
        return value
    if expected_kind == "list_or_object" and isinstance(value, (list, dict)):
        return value

    if isinstance(value, str):
        text = value.strip()
        if text == "":
            return default
        parsed = _parse_json_text(text)
        if expected_kind == "list":
            return parsed if isinstance(parsed, list) else default
        if expected_kind == "object":
            return parsed if isinstance(parsed, dict) else default
        return parsed if isinstance(parsed, (list, dict)) else default

    return default


def normalize_run_log_row(row: dict[str, Any]) -> dict[str, Any]:
    """Normalize known structured run-log fields for CSV/JSON parity.

    Compatibility behavior for legacy CSV rows:
    - empty string for known list/object fields is normalized to [] / {}.
    """

    normalized = dict(row)
    for field, kind in RUN_LOG_STRUCTURED_FIELD_KINDS.items():
        if field in normalized:
            normalized[field] = normalize_structured_value(normalized.get(field), kind)
    return normalized
