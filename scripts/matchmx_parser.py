#!/usr/bin/env python3
"""Shared Tennis Abstract `matchmx` parsing helpers."""

from __future__ import annotations

import re
from dataclasses import dataclass

MATCHMX_OLD_ROW_IDX = {
    "DATE": 0,
    "EVENT": 1,
    "SURFACE": 2,
    "OPPONENT": 3,
    "PLAYER_NAME": 4,
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

MATCHMX_NEW_ROW_IDX = {
    "DATE": 0,
    "EVENT": 1,
    "SURFACE": 2,
    "OPPONENT": 3,
    "RESULT_FLAG": 4,
    "PLAYER_NAME": 5,
    "SCORE": 6,
    "RANKING": 7,
    "RECENT_FORM": 8,
    "SURFACE_WIN_RATE": 9,
    "HOLD_PCT": 10,
    "BREAK_PCT": 11,
    "BP_SAVED_PCT": 12,
    "BP_CONV_PCT": 13,
    "FIRST_SERVE_IN_PCT": 14,
    "FIRST_SERVE_POINTS_WON_PCT": 15,
    "SECOND_SERVE_POINTS_WON_PCT": 16,
    "RETURN_POINTS_WON_PCT": 17,
    "DOMINANCE_RATIO": 18,
    "TOTAL_POINTS_WON_PCT": 19,
}

MATCHMX_NEW_WITH_SEED_ROW_IDX = {
    "DATE": 0,
    "EVENT": 1,
    "SURFACE": 2,
    "OPPONENT": 3,
    "RESULT_FLAG": 4,
    "PLAYER_NAME": 5,
    "SCORE": 6,
    "RANKING": 7,
    "SEED": 8,
    "RECENT_FORM": 9,
    "SURFACE_WIN_RATE": 10,
    "HOLD_PCT": 11,
    "BREAK_PCT": 12,
    "BP_SAVED_PCT": 13,
    "BP_CONV_PCT": 14,
    "FIRST_SERVE_IN_PCT": 15,
    "FIRST_SERVE_POINTS_WON_PCT": 16,
    "SECOND_SERVE_POINTS_WON_PCT": 17,
    "RETURN_POINTS_WON_PCT": 18,
    "DOMINANCE_RATIO": 19,
    "TOTAL_POINTS_WON_PCT": 20,
}

MATCHMX_SCHEMA_INDEX_MAPS = {
    "old": MATCHMX_OLD_ROW_IDX,
    "new": MATCHMX_NEW_ROW_IDX,
    "new_with_seed": MATCHMX_NEW_WITH_SEED_ROW_IDX,
}

# Backwards-compatible alias used in existing callers/tests.
MATCHMX_ROW_IDX = MATCHMX_OLD_ROW_IDX

MATCHMX_MIN_FIELD_COUNT = max(MATCHMX_OLD_ROW_IDX.values()) + 1
MATCHMX_NEW_MIN_FIELD_COUNT = max(MATCHMX_NEW_ROW_IDX.values()) + 1
MATCHMX_REQUIRED_KEYS = ("DATE", "PLAYER_NAME", "SCORE")
MATCHMX_KEY_METRIC_KEYS = ("RANKING", "RECENT_FORM", "SURFACE_WIN_RATE", "HOLD_PCT", "BREAK_PCT")
DEFAULT_SHORT_NAME_WHITELIST = set()

ROW_START_REGEX = re.compile(r"matchmx\s*(?:\[\s*\d+\s*\])?\s*=\s*\[")
NAME_LIKE_REGEX = re.compile(r"^[A-Za-z][A-Za-z .'-]*[A-Za-z]$")
PLAYER_NAME_DRIFT_SENTINELS = {"i", "p"}


@dataclass
class MatchMxParseResult:
    tokens: list[str]
    row_shape_valid: bool
    reason_code: str | None
    row_index: int | None = None


def _is_array_literal(value: str) -> bool:
    stripped = value.strip()
    return len(stripped) >= 2 and stripped[0] == "[" and stripped[-1] == "]"


def _parse_row_collection(array_literal_body: str) -> list[list[str]] | None:
    """Return rows for `matchmx = [[...], [...]]` payloads.

    For standard `matchmx[0] = [...]` payloads we return None so callers can
    treat the body as a single row.
    """

    outer_tokens = _split_top_level_tokens(array_literal_body)
    if not outer_tokens:
        return []
    if not all(_is_array_literal(token) for token in outer_tokens):
        return None

    rows: list[list[str]] = []
    for token in outer_tokens:
        inner = token.strip()[1:-1]
        rows.append(parse_js_array_tokens(inner))
    return rows


@dataclass
class MatchMxPlayerRow:
    date: str
    player_name: str
    score: str
    ranking: float | None
    recent_form: float | None
    surface_win_rate: float | None
    hold_pct: float | None
    break_pct: float | None


def _unescape_js_string(value: str) -> str:
    out: list[str] = []
    i = 0
    while i < len(value):
        ch = value[i]
        if ch != "\\" or i + 1 >= len(value):
            out.append(ch)
            i += 1
            continue
        nxt = value[i + 1]
        mapping = {"n": "\n", "r": "\r", "t": "\t", '"': '"', "'": "'", "\\": "\\"}
        out.append(mapping.get(nxt, nxt))
        i += 2
    return "".join(out)


def _split_top_level_tokens(array_literal_body: str) -> list[str]:
    tokens: list[str] = []
    current: list[str] = []
    quote: str | None = None
    escape = False
    bracket_depth = 0
    brace_depth = 0
    paren_depth = 0

    for ch in array_literal_body:
        if escape:
            current.append(ch)
            escape = False
            continue

        if quote:
            if ch == "\\":
                current.append(ch)
                escape = True
                continue
            current.append(ch)
            if ch == quote:
                quote = None
            continue

        if ch in {'"', "'"}:
            current.append(ch)
            quote = ch
            continue

        if ch == "[":
            bracket_depth += 1
            current.append(ch)
            continue
        if ch == "]":
            bracket_depth = max(0, bracket_depth - 1)
            current.append(ch)
            continue
        if ch == "{":
            brace_depth += 1
            current.append(ch)
            continue
        if ch == "}":
            brace_depth = max(0, brace_depth - 1)
            current.append(ch)
            continue
        if ch == "(":
            paren_depth += 1
            current.append(ch)
            continue
        if ch == ")":
            paren_depth = max(0, paren_depth - 1)
            current.append(ch)
            continue

        if ch == "," and not quote and bracket_depth == 0 and brace_depth == 0 and paren_depth == 0:
            tokens.append("".join(current).strip())
            current = []
            continue

        current.append(ch)

    if current or array_literal_body.rstrip().endswith(","):
        tokens.append("".join(current).strip())
    return tokens


def parse_js_array_tokens(array_literal_body: str) -> list[str]:
    normalized_tokens: list[str] = []
    for token in _split_top_level_tokens(array_literal_body):
        stripped = token.strip()
        lowered = stripped.lower()
        if lowered in {"", "null", "undefined", "nan"}:
            normalized_tokens.append("")
            continue
        if len(stripped) >= 2 and stripped[0] == stripped[-1] and stripped[0] in {'"', "'"}:
            normalized_tokens.append(_unescape_js_string(stripped[1:-1]))
            continue
        normalized_tokens.append(stripped)
    return normalized_tokens


def is_valid_row_shape(tokens: list[str]) -> bool:
    return len(tokens) >= MATCHMX_MIN_FIELD_COUNT


def iter_matchmx_rows(payload: str):
    for match in ROW_START_REGEX.finditer(payload):
        start_idx = match.end()
        i = start_idx
        quote: str | None = None
        escape = False
        depth = 1
        while i < len(payload) and depth > 0:
            ch = payload[i]
            if escape:
                escape = False
                i += 1
                continue
            if quote:
                if ch == "\\":
                    escape = True
                elif ch == quote:
                    quote = None
                i += 1
                continue
            if ch in {'"', "'"}:
                quote = ch
            elif ch == "[":
                depth += 1
            elif ch == "]":
                depth -= 1
            i += 1

        if depth != 0:
            yield MatchMxParseResult(tokens=[], row_shape_valid=False, reason_code="ta_matchmx_unusable_payload")
            continue

        body = payload[start_idx : i - 1]
        collection_rows = _parse_row_collection(body)
        if collection_rows is not None:
            for row_idx, row_tokens in enumerate(collection_rows):
                row_shape_valid = is_valid_row_shape(row_tokens)
                yield MatchMxParseResult(
                    tokens=row_tokens,
                    row_shape_valid=row_shape_valid,
                    reason_code=None if row_shape_valid else "ta_matchmx_unusable_payload",
                    row_index=row_idx,
                )
            continue

        tokens = parse_js_array_tokens(body)
        row_shape_valid = is_valid_row_shape(tokens)
        yield MatchMxParseResult(
            tokens=tokens,
            row_shape_valid=row_shape_valid,
            reason_code=None if row_shape_valid else "ta_matchmx_unusable_payload",
            row_index=None,
        )


def normalize_name(value: object) -> str | None:
    if value is None:
        return None
    normalized = re.sub(r"\s+", " ", str(value)).strip()
    return normalized or None


def has_minimum_schema_columns(tokens: list[str]) -> bool:
    return len(tokens) >= MATCHMX_MIN_FIELD_COUNT


def has_consistent_metric_index_mapping(row_idx: dict[str, int] | None = None) -> bool:
    idx_map = row_idx or MATCHMX_ROW_IDX
    indices = sorted(idx_map.values())
    return indices == list(range(max(idx_map.values()) + 1))


def _is_result_flag(value: object) -> bool:
    return str(value).strip().upper() in {"W", "L"}


def _is_full_name_like(value: object) -> bool:
    candidate = normalize_name(value)
    if not candidate:
        return False
    if not NAME_LIKE_REGEX.match(candidate):
        return False
    return " " in candidate or "-" in candidate


def get_matchmx_row_idx(tokens: list[str]) -> dict[str, int]:
    def _plausible_pct(value: float | None, low: float, high: float) -> bool:
        return value is not None and low <= value <= high

    def _schema_quality(idx_map: dict[str, int]) -> tuple[int, int]:
        hold = _to_number(tokens, idx_map["HOLD_PCT"])
        brk = _to_number(tokens, idx_map["BREAK_PCT"])
        ranking = _to_number(tokens, idx_map["RANKING"])
        quality = 0
        if _plausible_pct(hold, 35.0, 95.0):
            quality += 1
        if _plausible_pct(brk, 5.0, 70.0):
            quality += 1
        if ranking is not None and 0.0 <= ranking <= 2000.0:
            quality += 1
        return quality, -max(idx_map.values())

    if len(tokens) > MATCHMX_OLD_ROW_IDX["PLAYER_NAME"] and _is_full_name_like(tokens[MATCHMX_OLD_ROW_IDX["PLAYER_NAME"]]):
        return MATCHMX_OLD_ROW_IDX

    if len(tokens) > MATCHMX_NEW_ROW_IDX["PLAYER_NAME"] and _is_result_flag(tokens[MATCHMX_OLD_ROW_IDX["PLAYER_NAME"]]):
        new_candidates = []
        if len(tokens) > max(MATCHMX_NEW_ROW_IDX.values()) and _is_full_name_like(tokens[MATCHMX_NEW_ROW_IDX["PLAYER_NAME"]]):
            new_candidates.append(MATCHMX_NEW_ROW_IDX)
        if (
            len(tokens) > max(MATCHMX_NEW_WITH_SEED_ROW_IDX.values())
            and _is_full_name_like(tokens[MATCHMX_NEW_WITH_SEED_ROW_IDX["PLAYER_NAME"]])
        ):
            new_candidates.append(MATCHMX_NEW_WITH_SEED_ROW_IDX)
        if new_candidates:
            return max(new_candidates, key=_schema_quality)

    return MATCHMX_OLD_ROW_IDX


def required_indices_present(tokens: list[str], row_idx: dict[str, int]) -> bool:
    return all(row_idx[key] < len(tokens) for key in MATCHMX_REQUIRED_KEYS)


def has_any_key_metrics(tokens: list[str], row_idx: dict[str, int]) -> bool:
    for key in MATCHMX_KEY_METRIC_KEYS:
        idx = row_idx[key]
        if idx >= len(tokens):
            continue
        token = str(tokens[idx]).strip().lower()
        if token and token not in {"null", "undefined", "nan"}:
            return True
    return False


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


def _to_number(tokens: list[str], idx: int) -> float | None:
    if idx >= len(tokens):
        return None
    raw = str(tokens[idx]).strip()
    if not raw or raw.lower() in {"null", "undefined", "nan"}:
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    if value != value:
        return None
    return value


def _select_player_name(tokens: list[str], row_idx: dict[str, int]) -> str | None:
    candidate_indices = [row_idx["PLAYER_NAME"], row_idx["OPPONENT"]]
    fallback: str | None = None
    for idx in candidate_indices:
        if idx >= len(tokens):
            continue
        candidate = normalize_name(tokens[idx])
        if not candidate:
            continue
        if fallback is None:
            fallback = candidate
        if is_usable_canonical_name(candidate):
            return candidate
    return fallback


def parse_matchmx_player_row(tokens: list[str]) -> tuple[MatchMxPlayerRow | None, str | None]:
    row_idx = get_matchmx_row_idx(tokens)
    if not has_consistent_metric_index_mapping(row_idx):
        return None, "metric_index_mapping_invalid"
    min_field_count = max(row_idx.values()) + 1
    if len(tokens) < min_field_count:
        return None, "row_shape_invalid_for_matchmx_schema"
    if not required_indices_present(tokens, row_idx):
        return None, "row_indexes_out_of_bounds"

    score = str(tokens[row_idx["SCORE"]]).strip()
    player_name = _select_player_name(tokens, row_idx) or ""
    if not player_name or not score:
        return None, "required_fields_missing"
    if not is_usable_canonical_name(player_name):
        if player_name.lower() in PLAYER_NAME_DRIFT_SENTINELS:
            return None, "player_name_mapping_drift"
        return None, "canonical_name_rejected"
    if not has_any_key_metrics(tokens, row_idx):
        return None, "all_key_metrics_null"

    has_walkover_or_ret = bool(re.search(r"\b(?:ret|wo)\b", score, flags=re.IGNORECASE))
    retired_metric_keys = {
        "RECENT_FORM",
        "SURFACE_WIN_RATE",
        "HOLD_PCT",
        "BREAK_PCT",
        "BP_SAVED_PCT",
        "BP_CONV_PCT",
        "FIRST_SERVE_IN_PCT",
        "FIRST_SERVE_POINTS_WON_PCT",
        "SECOND_SERVE_POINTS_WON_PCT",
        "RETURN_POINTS_WON_PCT",
        "DOMINANCE_RATIO",
        "TOTAL_POINTS_WON_PCT",
    }

    def take(key: str) -> float | None:
        if has_walkover_or_ret and key in retired_metric_keys:
            return None
        return _to_number(tokens, row_idx[key])

    row = MatchMxPlayerRow(
        date=str(tokens[row_idx["DATE"]]),
        player_name=player_name,
        score=score,
        ranking=take("RANKING"),
        recent_form=take("RECENT_FORM"),
        surface_win_rate=take("SURFACE_WIN_RATE"),
        hold_pct=take("HOLD_PCT"),
        break_pct=take("BREAK_PCT"),
    )

    if row.ranking is None and row.hold_pct is None and row.break_pct is None:
        return None, "ranking_hold_break_all_null"
    return row, None
