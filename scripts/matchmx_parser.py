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

MATCHMX_LONG_ROW_IDX = {
    "DATE": 0,
    "EVENT": 1,
    "SURFACE": 2,
    "OPPONENT": 3,
    "RESULT_FLAG": 4,
    "PLAYER_NAME": 5,
    "ROUND": 6,
    "DRAW_SIZE": 7,
    "COURT": 8,
    "BEST_OF": 9,
    "MATCH_MINUTES": 10,
    "MATCH_ID": 11,
    "COUNTRY": 12,
    "LEVEL": 13,
    "TOUR": 14,
    "SEASON": 15,
    "SEED": 16,
    "ENTRY": 17,
    "AGE": 18,
    "HAND": 19,
    "ACES": 20,
    "DOUBLE_FAULTS": 21,
    "FIRST_SERVE_IN_RAW": 22,
    "SCORE": 23,
    "RANKING": 24,
    "RECENT_FORM": 25,
    "SURFACE_WIN_RATE": 26,
    "HOLD_PCT": 27,
    "BREAK_PCT": 28,
    "BP_SAVED_PCT": 29,
    "BP_CONV_PCT": 30,
    "FIRST_SERVE_IN_PCT": 31,
    "FIRST_SERVE_POINTS_WON_PCT": 32,
    "SECOND_SERVE_POINTS_WON_PCT": 33,
    "RETURN_POINTS_WON_PCT": 34,
    "DOMINANCE_RATIO": 35,
    "TOTAL_POINTS_WON_PCT": 36,
    "SERVICE_GAMES": 37,
    "RETURN_GAMES": 38,
    "POINTS_PLAYED": 39,
    "TB_RECORD": 40,
    "OPENER_ODDS": 41,
    "CLOSING_ODDS": 42,
    "NOTES": 43,
}

MATCHMX_LONG_LIVE_ROW_IDX = {
    "DATE": 0,
    "EVENT": 1,
    "SURFACE": 2,
    "TOURNAMENT_PHASE": 3,
    "RESULT_FLAG": 4,
    "PLAYER_NAME": 5,
    "RANKING": 6,
    "SEED": 7,
    "ENTRY": 8,
    "ROUND": 9,
    "SCORE": 10,
    "BEST_OF": 11,
    "OPPONENT": 12,
    "MATCH_MINUTES": 13,
    "MATCH_ID": 14,
    "LEVEL": 15,
    "HAND": 16,
    "TOUR": 17,
    "SEASON": 18,
    "AGE": 19,
    "COUNTRY": 20,
    "COURT": 21,
    "ACES": 22,
    "DOUBLE_FAULTS": 23,
    "FIRST_SERVE_IN_RAW": 24,
    "DRAW_SIZE": 25,
    "RECENT_FORM": 26,
    "SURFACE_WIN_RATE": 27,
    "BP_SAVED_PCT": 28,
    "BP_CONV_PCT": 29,
    "HOLD_PCT": 30,
    "BREAK_PCT": 31,
    "FIRST_SERVE_IN_PCT": 32,
    "FIRST_SERVE_POINTS_WON_PCT": 33,
    "SECOND_SERVE_POINTS_WON_PCT": 34,
    "RETURN_POINTS_WON_PCT": 35,
    "DOMINANCE_RATIO": 36,
    "TOTAL_POINTS_WON_PCT": 37,
    "SERVICE_GAMES": 38,
    "RETURN_GAMES": 39,
    "POINTS_PLAYED": 40,
    "TB_RECORD": 41,
    "OPENER_ODDS": 42,
    "CLOSING_ODDS": 43,
    "NOTES": 44,
}

MATCHMX_SCHEMA_INDEX_MAPS = {
    "old": MATCHMX_OLD_ROW_IDX,
    "new": MATCHMX_NEW_ROW_IDX,
    "new_with_seed": MATCHMX_NEW_WITH_SEED_ROW_IDX,
    "long": MATCHMX_LONG_ROW_IDX,
    "long_live": MATCHMX_LONG_LIVE_ROW_IDX,
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
    candidate_maps = [row_idx] if row_idx is not None else list(MATCHMX_SCHEMA_INDEX_MAPS.values())
    for idx_map in candidate_maps:
        if idx_map is None:
            return False
        indices = sorted(idx_map.values())
        if indices != list(range(max(idx_map.values()) + 1)):
            return False
    return True


def _is_result_flag(value: object) -> bool:
    return str(value).strip().upper() in {"W", "L"}


def _is_full_name_like(value: object) -> bool:
    candidate = normalize_name(value)
    if not candidate:
        return False
    if not NAME_LIKE_REGEX.match(candidate):
        return False
    return " " in candidate or "-" in candidate


def _is_score_like(value: object) -> bool:
    candidate = str(value).strip()
    if not candidate:
        return False
    if re.search(r"\b(?:ret|wo)\b", candidate, flags=re.IGNORECASE):
        return True
    return bool(re.search(r"\d\s*-\s*\d", candidate))


def _is_round_label(value: object) -> bool:
    return str(value).strip().upper() in {"R16", "QF", "SF", "F"}


def get_matchmx_row_idx(tokens: list[str], sample_rows: list[list[str]] | None = None) -> dict[str, int]:
    def _plausible_pct(value: float | None, low: float, high: float) -> bool:
        return value is not None and low <= value <= high

    def _plausible_ratio_or_pct(value: float | None) -> bool:
        return value is not None and (0.0 <= value <= 1.0 or 0.0 <= value <= 100.0)

    def _is_numeric_token(value: object) -> bool:
        try:
            float(str(value).strip())
            return True
        except (TypeError, ValueError):
            return False

    def _schema_quality(idx_map: dict[str, int], rows: list[list[str]]) -> tuple[int, int, int, int, int]:
        quality = 0
        valid_rows = 0
        hold_valid_count = 0
        break_valid_count = 0
        hard_reject = False
        hold_values: list[float] = []
        break_values: list[float] = []

        def _is_missing_metric_token(raw: str) -> bool:
            return not raw or raw.lower() in {"null", "undefined", "nan"}

        for row_tokens in rows:
            recent_form = _to_number(row_tokens, idx_map["RECENT_FORM"])
            surface = _to_number(row_tokens, idx_map["SURFACE_WIN_RATE"])
            hold = _to_number(row_tokens, idx_map["HOLD_PCT"])
            brk = _to_number(row_tokens, idx_map["BREAK_PCT"])
            ranking = _to_number(row_tokens, idx_map["RANKING"])
            hold_token = str(row_tokens[idx_map["HOLD_PCT"]]).strip() if idx_map["HOLD_PCT"] < len(row_tokens) else ""
            break_token = str(row_tokens[idx_map["BREAK_PCT"]]).strip() if idx_map["BREAK_PCT"] < len(row_tokens) else ""

            row_score = 0
            numeric_metrics = [
                recent_form,
                surface,
                hold,
                brk,
                _to_number(row_tokens, idx_map["BP_SAVED_PCT"]),
                _to_number(row_tokens, idx_map["BP_CONV_PCT"]),
                _to_number(row_tokens, idx_map["FIRST_SERVE_IN_PCT"]),
                _to_number(row_tokens, idx_map["FIRST_SERVE_POINTS_WON_PCT"]),
                _to_number(row_tokens, idx_map["SECOND_SERVE_POINTS_WON_PCT"]),
                _to_number(row_tokens, idx_map["RETURN_POINTS_WON_PCT"]),
                _to_number(row_tokens, idx_map["TOTAL_POINTS_WON_PCT"]),
            ]

            if _plausible_ratio_or_pct(recent_form):
                row_score += 1
            if _plausible_ratio_or_pct(surface):
                row_score += 1
            if _plausible_pct(hold, 35.0, 95.0):
                row_score += 2
                hold_valid_count += 1
            if _plausible_pct(brk, 5.0, 70.0):
                row_score += 2
                break_valid_count += 1
            if ranking is not None and 0.0 <= ranking <= 2000.0:
                row_score += 1

            # Hard rejects: HOLD/BREAK should be numeric when populated, and BREAK
            # should not resolve into identity/name text.
            if not _is_missing_metric_token(hold_token) and not _is_numeric_token(hold_token):
                hard_reject = True
            if not _is_missing_metric_token(break_token) and not _is_numeric_token(break_token):
                hard_reject = True
            if not _is_missing_metric_token(break_token) and _is_full_name_like(break_token):
                hard_reject = True

            # Penalize drift where HOLD is null while the surrounding metric window
            # is populated, which usually indicates a shifted schema map.
            if hold is None and any(metric is not None for metric in numeric_metrics):
                row_score -= 3

            # Metric window distribution guards: hold should generally exceed break,
            # and both should sit within realistic ranges when available.
            if hold is not None and brk is not None:
                if hold <= brk:
                    row_score -= 2
                if hold - brk < 8.0:
                    row_score -= 1

            if row_score > 0:
                valid_rows += 1
            quality += row_score

            if brk is not None:
                break_values.append(brk)
            if hold is not None:
                hold_values.append(hold)

        def _constant_tiny_integer(values: list[float]) -> bool:
            if len(values) < 3:
                return False
            int_values = [value for value in values if abs(value - round(value)) < 1e-9]
            if len(int_values) < 3:
                return False
            if len(set(int_values)) != 1:
                return False
            return abs(int_values[0]) <= 10

        # Hard reject implausibly constant tiny integer HOLD/BREAK values across
        # sampled rows (e.g., always "3" due to shifted columns).
        if _constant_tiny_integer(hold_values) or _constant_tiny_integer(break_values):
            hard_reject = True

        if hard_reject:
            quality -= 10_000

        return quality, valid_rows, hold_valid_count, break_valid_count, -max(idx_map.values())

    def _matches_long_variant(idx_map: dict[str, int], require_phase: bool = False) -> bool:
        required = ("RESULT_FLAG", "PLAYER_NAME", "SCORE", "RANKING", "HOLD_PCT", "BREAK_PCT")
        if any(idx_map[key] >= len(tokens) for key in required):
            return False
        if not _is_result_flag(tokens[idx_map["RESULT_FLAG"]]):
            return False
        if not _is_full_name_like(tokens[idx_map["PLAYER_NAME"]]):
            return False
        if not _is_score_like(tokens[idx_map["SCORE"]]):
            return False

        ranking = _to_number(tokens, idx_map["RANKING"])
        hold = _to_number(tokens, idx_map["HOLD_PCT"])
        brk = _to_number(tokens, idx_map["BREAK_PCT"])
        if ranking is None or not (0.0 <= ranking <= 2000.0):
            return False
        if not _plausible_pct(hold, 35.0, 95.0):
            return False
        if not _plausible_pct(brk, 5.0, 70.0):
            return False

        if require_phase:
            phase = str(tokens[idx_map["TOURNAMENT_PHASE"]]).strip()
            if not phase or _is_result_flag(phase) or _is_score_like(phase):
                return False

        return True

    def _matches_live_45_shape() -> bool:
        idx_map = MATCHMX_LONG_LIVE_ROW_IDX
        required = ("TOURNAMENT_PHASE", "RESULT_FLAG", "PLAYER_NAME", "ROUND", "SCORE", "RANKING", "HOLD_PCT", "BREAK_PCT")
        if any(idx_map[key] >= len(tokens) for key in required):
            return False
        if len(tokens) != 45:
            return False

        phase = str(tokens[idx_map["TOURNAMENT_PHASE"]]).strip()
        if not phase or _is_result_flag(phase) or _is_score_like(phase):
            return False
        if not _is_result_flag(tokens[idx_map["RESULT_FLAG"]]):
            return False
        if not _is_full_name_like(tokens[idx_map["PLAYER_NAME"]]):
            return False
        if not str(tokens[idx_map["ROUND"]]).strip():
            return False
        if not _is_score_like(tokens[idx_map["SCORE"]]):
            return False

        ranking = _to_number(tokens, idx_map["RANKING"])
        hold = _to_number(tokens, idx_map["HOLD_PCT"])
        brk = _to_number(tokens, idx_map["BREAK_PCT"])
        if ranking is None or not (0.0 <= ranking <= 2000.0):
            return False
        if not _plausible_pct(hold, 35.0, 95.0):
            return False
        if not _plausible_pct(brk, 5.0, 70.0):
            return False

        # Guard against mapping into text/identity columns.
        hold_token = str(tokens[idx_map["HOLD_PCT"]]).strip()
        break_token = str(tokens[idx_map["BREAK_PCT"]]).strip()
        if not _is_numeric_token(hold_token) or not _is_numeric_token(break_token):
            return False
        if _is_full_name_like(hold_token) or _is_full_name_like(break_token):
            return False

        return True

    def _old_map_hard_rejected() -> bool:
        if len(tokens) <= MATCHMX_OLD_ROW_IDX["BREAK_PCT"]:
            return False

        old_player_token = str(tokens[MATCHMX_OLD_ROW_IDX["PLAYER_NAME"]]).strip()
        old_hold_token = str(tokens[MATCHMX_OLD_ROW_IDX["HOLD_PCT"]]).strip()
        old_break_token = str(tokens[MATCHMX_OLD_ROW_IDX["BREAK_PCT"]]).strip()

        if _is_result_flag(old_player_token):
            return True
        if _is_round_label(old_hold_token):
            return True
        if _is_score_like(old_break_token):
            return True

        return False

    old_map_rejected = _old_map_hard_rejected()

    if _matches_live_45_shape():
        return MATCHMX_LONG_LIVE_ROW_IDX

    if len(tokens) >= 45 and _matches_long_variant(MATCHMX_LONG_LIVE_ROW_IDX, require_phase=True):
        return MATCHMX_LONG_LIVE_ROW_IDX

    if (
        len(tokens) >= 40
        and _matches_long_variant(MATCHMX_LONG_ROW_IDX)
    ):
        return MATCHMX_LONG_ROW_IDX

    if (
        not old_map_rejected
        and len(tokens) > MATCHMX_OLD_ROW_IDX["SCORE"]
        and _is_full_name_like(tokens[MATCHMX_OLD_ROW_IDX["PLAYER_NAME"]])
        and _is_score_like(tokens[MATCHMX_OLD_ROW_IDX["SCORE"]])
    ):
        return MATCHMX_OLD_ROW_IDX

    if len(tokens) > MATCHMX_NEW_ROW_IDX["PLAYER_NAME"] and _is_result_flag(tokens[MATCHMX_OLD_ROW_IDX["PLAYER_NAME"]]):
        new_candidates = []
        if len(tokens) > max(MATCHMX_NEW_ROW_IDX.values()) and _is_full_name_like(tokens[MATCHMX_NEW_ROW_IDX["PLAYER_NAME"]]):
            new_candidates.append(MATCHMX_NEW_ROW_IDX)
        if (
            len(tokens) > max(MATCHMX_NEW_WITH_SEED_ROW_IDX.values())
            and _is_full_name_like(tokens[MATCHMX_NEW_WITH_SEED_ROW_IDX["PLAYER_NAME"]])
        ):
            hold_token = str(tokens[MATCHMX_NEW_WITH_SEED_ROW_IDX["HOLD_PCT"]]).strip()
            break_token = str(tokens[MATCHMX_NEW_WITH_SEED_ROW_IDX["BREAK_PCT"]]).strip()
            hold_numeric = _to_number(tokens, MATCHMX_NEW_WITH_SEED_ROW_IDX["HOLD_PCT"]) is not None
            break_numeric = _to_number(tokens, MATCHMX_NEW_WITH_SEED_ROW_IDX["BREAK_PCT"]) is not None
            if (
                hold_numeric
                and break_numeric
                and not _is_full_name_like(hold_token)
                and not _is_full_name_like(break_token)
            ):
                new_candidates.append(MATCHMX_NEW_WITH_SEED_ROW_IDX)
        if new_candidates:
            scored_rows = [tokens]
            if sample_rows:
                min_required_len = min(max(candidate.values()) + 1 for candidate in new_candidates)
                for row_tokens in sample_rows:
                    if row_tokens is tokens:
                        continue
                    if row_tokens and len(row_tokens) >= min_required_len:
                        scored_rows.append(row_tokens)
                    if len(scored_rows) >= 5:
                        break

            minimum_valid_metrics = 2 if len(scored_rows) >= 2 else 1
            ranked_candidates: list[tuple[tuple[int, int, int, int, int], dict[str, int]]] = []
            for idx_map in new_candidates:
                score = _schema_quality(idx_map, scored_rows)
                hold_valid = score[2]
                break_valid = score[3]
                if hold_valid < minimum_valid_metrics or break_valid < minimum_valid_metrics:
                    continue
                ranked_candidates.append((score, idx_map))

            if ranked_candidates:
                return max(ranked_candidates, key=lambda pair: pair[0])[1]

    if old_map_rejected and len(tokens) in {45, 65}:
        return MATCHMX_LONG_LIVE_ROW_IDX

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


def parse_matchmx_player_row(
    tokens: list[str], sample_rows: list[list[str]] | None = None
) -> tuple[MatchMxPlayerRow | None, str | None]:
    row_idx = get_matchmx_row_idx(tokens, sample_rows=sample_rows)
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
