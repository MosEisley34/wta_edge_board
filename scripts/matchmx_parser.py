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
NAME_LIKE_REGEX = re.compile(r"^[A-Za-z][A-Za-z .'-]*[A-Za-z]$")


@dataclass
class MatchMxParseResult:
    tokens: list[str]
    row_shape_valid: bool
    reason_code: str | None


def parse_js_array_tokens(array_literal_body: str) -> list[str]:
    tokens: list[str] = []
    current: list[str] = []
    quote: str | None = None
    escape = False
    in_token = False

    def flush_token() -> None:
        raw = "".join(current).strip()
        if raw in {"null", "undefined"}:
            tokens.append("")
        else:
            tokens.append(raw)

    for ch in array_literal_body:
        if escape:
            current.append(ch)
            escape = False
            in_token = True
            continue

        if ch == "\\":
            current.append(ch)
            escape = True
            in_token = True
            continue

        if quote:
            if ch == quote:
                quote = None
            else:
                current.append(ch)
            in_token = True
            continue

        if ch in {'"', "'"}:
            quote = ch
            in_token = True
            continue

        if ch == ",":
            flush_token()
            current = []
            in_token = False
            continue

        current.append(ch)
        if not ch.isspace():
            in_token = True

    if current or in_token or array_literal_body.rstrip().endswith(","):
        flush_token()

    normalized_tokens: list[str] = []
    for token in tokens:
        normalized_tokens.append(token.replace(r'\"', '"').replace(r"\\'", "'").replace(r"\\\\", "\\"))
    return normalized_tokens


def is_valid_row_shape(tokens: list[str]) -> bool:
    return len(tokens) == MATCHMX_MIN_FIELD_COUNT


def iter_matchmx_rows(payload: str):
    for match in ROW_REGEX.finditer(payload):
        tokens = parse_js_array_tokens(match.group(1))
        row_shape_valid = is_valid_row_shape(tokens)
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


def is_usable_canonical_name(name: str | None, whitelist: set[str] | None = None) -> bool:
    normalized = normalize_name(name)
    if not is_accepted_name(normalized, whitelist):
        return False
    assert normalized is not None
    if not NAME_LIKE_REGEX.match(normalized):
        return False
    if " " not in normalized and "-" not in normalized:
        return False
    return True
