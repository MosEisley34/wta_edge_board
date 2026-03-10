#!/usr/bin/env python3
"""Shared Tennis Abstract `matchmx` parsing helpers."""

from __future__ import annotations

import re
from dataclasses import dataclass

MATCHMX_ROW_IDX = {
    "DATE": 0,
    "EVENT": 1,
    "SURFACE": 2,
    "PLAYER_NAME": 3,
    "OPPONENT": 4,
    "SCORE": 5,
    "RANKING": 6,
    "RECENT_FORM": 7,
    "SURFACE_WIN_RATE": 8,
    "HOLD_PCT": 9,
    "BREAK_PCT": 10,
    "BP_SAVED_PCT": 11,
    "BP_CONV_PCT": 12,
    "FIRST_SERVE_IN_PCT": 13,
    "FIRST_SERVE_POINTS_WON_PCT": 14,
    "SECOND_SERVE_POINTS_WON_PCT": 15,
    "RETURN_POINTS_WON_PCT": 16,
    "DOMINANCE_RATIO": 17,
    "TOTAL_POINTS_WON_PCT": 18,
}

MATCHMX_MIN_FIELD_COUNT = max(MATCHMX_ROW_IDX.values()) + 1
DEFAULT_SHORT_NAME_WHITELIST = set()

ROW_REGEX = re.compile(r"matchmx\s*(?:\[\s*\d+\s*\])?\s*=\s*\[([\s\S]*?)\]\s*;")
TOKEN_REGEX = re.compile(r'"((?:\\.|[^"\\])*)"|\'((?:\\.|[^\'\\])*)\'|([^,]+)')


@dataclass
class MatchMxParseResult:
    tokens: list[str]
    row_shape_valid: bool
    reason_code: str | None


def parse_js_array_tokens(array_literal_body: str) -> list[str]:
    tokens: list[str] = []
    for part in TOKEN_REGEX.finditer(array_literal_body):
        raw = part.group(1) if part.group(1) is not None else (part.group(2) if part.group(2) is not None else part.group(3))
        normalized = str(raw or "").strip()
        if normalized in {"null", "undefined"}:
            tokens.append("")
            continue
        tokens.append(normalized.replace(r'\"', '"').replace(r"\\'", "'"))
    return tokens


def iter_matchmx_rows(payload: str):
    for match in ROW_REGEX.finditer(payload):
        tokens = parse_js_array_tokens(match.group(1))
        row_shape_valid = len(tokens) >= MATCHMX_MIN_FIELD_COUNT
        yield MatchMxParseResult(
            tokens=tokens,
            row_shape_valid=row_shape_valid,
            reason_code=None if row_shape_valid else "ta_matchmx_unusable_payload",
        )


def normalize_name(value: object) -> str | None:
    if value is None:
        return None
    normalized = re.sub(r"\s+", " ", str(value)).strip()
    return normalized or None


def is_accepted_name(name: str | None, whitelist: set[str] | None = None) -> bool:
    normalized = normalize_name(name)
    if not normalized:
        return False
    canonical = normalized.lower()
    allowed = whitelist if whitelist is not None else DEFAULT_SHORT_NAME_WHITELIST
    if len(canonical) < 3 and canonical not in allowed:
        return False
    return True
