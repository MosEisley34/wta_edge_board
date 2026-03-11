#!/usr/bin/env python3
"""Extract normalized player features into JSONL and CSV artifacts.

Reads source payloads from OUT_DIR/raw (or --out-dir) and writes:
- OUT_DIR/normalized/player_features.jsonl
- OUT_DIR/normalized/player_features.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from fnmatch import fnmatch

from matchmx_parser import (
    has_consistent_metric_index_mapping,
    iter_matchmx_rows,
    is_accepted_name,
    normalize_name,
    parse_matchmx_player_row,
)
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

SCHEMA_COLUMNS = [
    "player_canonical_name",
    "source",
    "as_of",
    "ranking",
    "recent_form",
    "surface_win_rate",
    "hold_pct",
    "break_pct",
    "h2h_wins",
    "h2h_losses",
    "has_stats",
    "reason_code",
    "reason_code_detail",
]

DIAGNOSTIC_COLUMNS = [
    "source",
    "as_of",
    "payload_file",
    "payload_mode",
    "issue_code",
    "issue_detail",
]


CATALOG_PATH = Path(__file__).resolve().parents[1] / "config" / "probe_sources.tsv"

SOFASCORE_MIN_PARTICIPANTS_BY_SOURCE: dict[str, int] = {
    "sofascore_events_live": 2,
    "sofascore_scheduled_events": 2,
}


@dataclass(frozen=True)
class ParserContract:
    endpoint: str
    table_markers: tuple[str, ...]
    expected_headers: tuple[str, ...]
    player_column_aliases: tuple[str, ...]
    stat_column_map: dict[str, tuple[str, ...]]
    percent_stats: tuple[str, ...] = ()


TA_SOURCE_MATRIX: dict[str, ParserContract] = {
    "ta_leaders_top50_serve": ParserContract(
        endpoint="https://www.tennisabstract.com/cgi-bin/leaders_wta.cgi?players=top50&view=serve",
        table_markers=("leaders_wta", "Serve"),
        expected_headers=("Player", "Hold%", "Break%", "Ace%", "DF%"),
        player_column_aliases=("Player",),
        stat_column_map={"hold_pct": ("Hold%", "Hold %"), "break_pct": ("Break%", "Break %")},
        percent_stats=("hold_pct", "break_pct"),
    ),
    "ta_leaders_top50_return": ParserContract(
        endpoint="https://www.tennisabstract.com/cgi-bin/leaders_wta.cgi?players=top50&view=return",
        table_markers=("leaders_wta", "Return"),
        expected_headers=("Player", "Break%", "RPW%"),
        player_column_aliases=("Player",),
        stat_column_map={"break_pct": ("Break%", "Break %")},
        percent_stats=("break_pct",),
    ),
    "ta_leaders_top50_breaks": ParserContract(
        endpoint="https://www.tennisabstract.com/cgi-bin/leaders_wta.cgi?players=top50&view=breaks",
        table_markers=("leaders_wta", "Breaks"),
        expected_headers=("Player", "Break%", "BPs Created"),
        player_column_aliases=("Player",),
        stat_column_map={"break_pct": ("Break%", "Break %")},
        percent_stats=("break_pct",),
    ),
    "ta_leaders_top50_more": ParserContract(
        endpoint="https://www.tennisabstract.com/cgi-bin/leaders_wta.cgi?players=top50&view=more",
        table_markers=("leaders_wta", "More"),
        expected_headers=("Player", "Rank", "Win%"),
        player_column_aliases=("Player",),
        stat_column_map={"ranking": ("Rank", "Rk")},
    ),
    "ta_leaders_51_100_serve": ParserContract(
        endpoint="https://www.tennisabstract.com/cgi-bin/leaders_wta.cgi?players=51-100&view=serve",
        table_markers=("leaders_wta", "Serve"),
        expected_headers=("Player", "Hold%"),
        player_column_aliases=("Player",),
        stat_column_map={"hold_pct": ("Hold%", "Hold %")},
        percent_stats=("hold_pct",),
    ),
    "ta_leaders_51_100_return": ParserContract(
        endpoint="https://www.tennisabstract.com/cgi-bin/leaders_wta.cgi?players=51-100&view=return",
        table_markers=("leaders_wta", "Return"),
        expected_headers=("Player", "Break%"),
        player_column_aliases=("Player",),
        stat_column_map={"break_pct": ("Break%", "Break %")},
        percent_stats=("break_pct",),
    ),
    "ta_leaders_51_100_breaks": ParserContract(
        endpoint="https://www.tennisabstract.com/cgi-bin/leaders_wta.cgi?players=51-100&view=breaks",
        table_markers=("leaders_wta", "Breaks"),
        expected_headers=("Player", "Break%"),
        player_column_aliases=("Player",),
        stat_column_map={"break_pct": ("Break%", "Break %")},
        percent_stats=("break_pct",),
    ),
    "ta_leaders_51_100_more": ParserContract(
        endpoint="https://www.tennisabstract.com/cgi-bin/leaders_wta.cgi?players=51-100&view=more",
        table_markers=("leaders_wta", "More"),
        expected_headers=("Player", "Rank"),
        player_column_aliases=("Player",),
        stat_column_map={"ranking": ("Rank", "Rk")},
    ),
    "ta_winners_errors": ParserContract(
        endpoint="https://www.tennisabstract.com/reports/winners_errors_leaders_women_last52.html",
        table_markers=("winners_errors_leaders_women_last52", "winners", "errors"),
        expected_headers=("Player", "Winners", "Unforced Errors"),
        player_column_aliases=("Player",),
        stat_column_map={"recent_form": ("W/UE", "Winners/UE")},
    ),
    "mcp_report_serve": ParserContract(
        endpoint="https://www.tennisabstract.com/cgi-bin/mcp_leaders_wta.cgi?tab=serve",
        table_markers=("mcp", "serve"),
        expected_headers=("Player", "Hold%"),
        player_column_aliases=("Player", "Server"),
        stat_column_map={"hold_pct": ("Hold%", "Service Hold%")},
        percent_stats=("hold_pct",),
    ),
    "mcp_report_return": ParserContract(
        endpoint="https://www.tennisabstract.com/cgi-bin/mcp_leaders_wta.cgi?tab=return",
        table_markers=("mcp", "return"),
        expected_headers=("Player", "Break%"),
        player_column_aliases=("Player", "Returner"),
        stat_column_map={"break_pct": ("Break%", "Return Break%")},
        percent_stats=("break_pct",),
    ),
    "mcp_report_rally": ParserContract(
        endpoint="https://www.tennisabstract.com/cgi-bin/mcp_leaders_wta.cgi?tab=rally",
        table_markers=("mcp", "rally"),
        expected_headers=("Player", "Rally Win%"),
        player_column_aliases=("Player",),
        stat_column_map={"recent_form": ("Rally Win%",)},
        percent_stats=("recent_form",),
    ),
    "mcp_report_tactics": ParserContract(
        endpoint="https://www.tennisabstract.com/cgi-bin/mcp_leaders_wta.cgi?tab=tactics",
        table_markers=("mcp", "tactics"),
        expected_headers=("Player", "Approach Win%"),
        player_column_aliases=("Player",),
        stat_column_map={"recent_form": ("Approach Win%", "Tactic Win%")},
        percent_stats=("recent_form",),
    ),
}

SOURCE_HEALTH_THRESHOLDS = {
    "min_rows": 5,
    "min_unique_players": 5,
}


def _load_selected_sources(catalog_path: Path) -> set[str]:
    if not catalog_path.exists():
        return set()

    selected: set[str] = set()
    for line in catalog_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        row = line.strip()
        if not row or row.startswith("#"):
            continue
        source_key = row.split("\t", 1)[0].strip()
        if source_key:
            selected.add(source_key)
    return selected

def _parse_allowlist(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _is_allowed_source(source: str, allowlist: list[str]) -> bool:
    if not allowlist:
        return True
    return any(fnmatch(source, pattern) for pattern in allowlist)


def _is_html_payload(path: Path, text: str) -> bool:
    suffix = path.suffix.lower()
    if suffix in {".html", ".htm"}:
        return True
    if suffix == ".body":
        marker = text[:4096].lower()
        if "<html" in marker or "<!doctype html" in marker or "<head" in marker or "<body" in marker:
            return True
    return False


def _detect_payload_mode(source: str, path: Path, text: str) -> str:
    if "matchmx" in text:
        return "matchmx"

    json_sources = {
        "itf",
        "sofascore_events_live",
        "sofascore_scheduled_events",
        "sofascore_player_detail",
        "sofascore_player_recent",
        "sofascore_player_stats_overall",
        "sofascore_player_stats_last52",
    }
    if source in json_sources:
        return "json"

    if source in {"tennisabstract_leaders", "tennisabstract_leadersource_wta"}:
        return "matchmx"

    if _is_html_payload(path, text):
        return "html"

    stripped = text.lstrip()
    if stripped.startswith("{") or stripped.startswith("["):
        return "json"

    if path.suffix.lower() == ".csv":
        return "csv"

    if source in {"tennisexplorer", "ta_h2h"}:
        return "html"

    return "unknown"


@dataclass
class PlayerFeature:
    player_canonical_name: str | None
    source: str
    as_of: str
    ranking: int | None
    recent_form: float | None
    surface_win_rate: float | None
    hold_pct: float | None
    break_pct: float | None
    h2h_wins: int | None
    h2h_losses: int | None
    has_stats: bool
    reason_code: str
    reason_code_detail: str | None


@dataclass
class SourceDiagnostic:
    source: str
    as_of: str
    payload_file: str
    payload_mode: str
    issue_code: str
    issue_detail: str | None


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text or text.lower() in {"null", "none", "na", "n/a", "-"}:
        return None
    if text.endswith("%"):
        text = text[:-1].strip()
    try:
        return float(text)
    except ValueError:
        return None


def _to_percent(value: object) -> float | None:
    num = _to_float(value)
    if num is None:
        return None
    if num <= 1:
        return num * 100
    return num


def _strip_html(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _extract_tables(html: str) -> list[tuple[list[str], list[list[str]]]]:
    tables: list[tuple[list[str], list[list[str]]]] = []
    for table in re.findall(r"<table\b.*?</table>", html, flags=re.IGNORECASE | re.DOTALL):
        rows: list[list[str]] = []
        for tr in re.findall(r"<tr\b.*?</tr>", table, flags=re.IGNORECASE | re.DOTALL):
            cells = re.findall(r"<t[hd]\b[^>]*>(.*?)</t[hd]>", tr, flags=re.IGNORECASE | re.DOTALL)
            cleaned = [_strip_html(cell) for cell in cells]
            if cleaned:
                rows.append(cleaned)
        if not rows:
            continue
        headers = rows[0]
        tables.append((headers, rows[1:]))
    return tables


def _find_column_index(headers: list[str], aliases: tuple[str, ...]) -> int | None:
    for idx, header in enumerate(headers):
        normalized = header.lower().strip()
        for alias in aliases:
            if normalized == alias.lower().strip():
                return idx
    return None


def _parse_contract_html_rows(source: str, text: str, as_of: str) -> list[PlayerFeature]:
    contract = TA_SOURCE_MATRIX.get(source)
    if not contract:
        return []
    haystack = text.lower()
    if not all(marker.lower() in haystack for marker in contract.table_markers):
        return []

    rows: list[PlayerFeature] = []
    for headers, data_rows in _extract_tables(text):
        if not all(any(h.lower() == expected.lower() for h in headers) for expected in contract.expected_headers[:2]):
            continue

        player_col = _find_column_index(headers, contract.player_column_aliases)
        if player_col is None:
            continue

        stat_indices: dict[str, int] = {}
        for stat, aliases in contract.stat_column_map.items():
            idx = _find_column_index(headers, aliases)
            if idx is not None:
                stat_indices[stat] = idx

        for raw in data_rows:
            if player_col >= len(raw):
                continue
            player_name = normalize_name(raw[player_col])
            stat_values: dict[str, object] = {}
            for stat, idx in stat_indices.items():
                stat_values[stat] = raw[idx] if idx < len(raw) else None
            feature = PlayerFeature(
                player_canonical_name=player_name,
                source=source,
                as_of=as_of,
                ranking=_to_int(stat_values.get("ranking")),
                recent_form=(
                    _to_percent(stat_values.get("recent_form"))
                    if "recent_form" in contract.percent_stats
                    else _to_float(stat_values.get("recent_form"))
                ),
                surface_win_rate=_to_percent(stat_values.get("surface_win_rate")) if "surface_win_rate" in contract.percent_stats else _to_float(stat_values.get("surface_win_rate")),
                hold_pct=_to_percent(stat_values.get("hold_pct")) if "hold_pct" in contract.percent_stats else _to_float(stat_values.get("hold_pct")),
                break_pct=_to_percent(stat_values.get("break_pct")) if "break_pct" in contract.percent_stats else _to_float(stat_values.get("break_pct")),
                h2h_wins=None,
                h2h_losses=None,
                has_stats=False,
                reason_code="ok",
                reason_code_detail=f"normalized_from_contract:{source}",
            )
            feature.has_stats = _has_stats(feature)
            if not is_accepted_name(feature.player_canonical_name):
                feature.reason_code = "missing_player_name"
                feature.player_canonical_name = None
            elif not feature.has_stats:
                feature.reason_code = "provider_returned_null_features"
            rows.append(feature)
        if rows:
            return rows
    return []


def _to_int(value: object) -> int | None:
    number = _to_float(value)
    if number is None:
        return None
    try:
        return int(round(number))
    except (TypeError, ValueError):
        return None


def _pick(obj: dict[str, object], *keys: str) -> object | None:
    for key in keys:
        if key in obj and obj[key] not in (None, ""):
            return obj[key]
    return None


def _has_stats(row: PlayerFeature) -> bool:
    return any(
        value is not None
        for value in [
            row.ranking,
            row.recent_form,
            row.surface_win_rate,
            row.hold_pct,
            row.break_pct,
            row.h2h_wins,
            row.h2h_losses,
        ]
    )


def _parse_matchmx_rows(source: str, text: str, as_of: str) -> list[PlayerFeature]:
    rows: list[PlayerFeature] = []
    if not has_consistent_metric_index_mapping():
        rows.append(
            PlayerFeature(
                player_canonical_name=None,
                source=source,
                as_of=as_of,
                ranking=None,
                recent_form=None,
                surface_win_rate=None,
                hold_pct=None,
                break_pct=None,
                h2h_wins=None,
                h2h_losses=None,
                has_stats=False,
                reason_code="ta_matchmx_unusable_payload",
                reason_code_detail="metric_index_mapping_invalid",
            )
        )
        return rows

    for logical_row_number, parsed in enumerate(iter_matchmx_rows(text), start=1):
        values = parsed.tokens
        if parsed.reason_code:
            rows.append(
                PlayerFeature(
                    player_canonical_name=None,
                    source=source,
                    as_of=as_of,
                    ranking=None,
                    recent_form=None,
                    surface_win_rate=None,
                    hold_pct=None,
                    break_pct=None,
                    h2h_wins=None,
                    h2h_losses=None,
                    has_stats=False,
                    reason_code="ta_matchmx_unusable_payload",
                    reason_code_detail=json.dumps(
                        {
                            "reason": parsed.reason_code,
                            "row_number": logical_row_number,
                            "row_index": parsed.row_index,
                            "token_count": len(values),
                            "token_sample": values[:8],
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                )
            )
            continue

        parsed_row, reason = parse_matchmx_player_row(values)
        if reason:
            rows.append(
                PlayerFeature(
                    player_canonical_name=None,
                    source=source,
                    as_of=as_of,
                    ranking=None,
                    recent_form=None,
                    surface_win_rate=None,
                    hold_pct=None,
                    break_pct=None,
                    h2h_wins=None,
                    h2h_losses=None,
                    has_stats=False,
                    reason_code="ta_matchmx_unusable_payload",
                    reason_code_detail=json.dumps(
                        {
                            "reason": reason,
                            "row_number": logical_row_number,
                            "row_index": parsed.row_index,
                            "token_count": len(values),
                            "token_sample": values[:8],
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                )
            )
            continue

        feature = PlayerFeature(
            player_canonical_name=parsed_row.player_name,
            source=source,
            as_of=as_of,
            ranking=_to_int(parsed_row.ranking),
            recent_form=_to_float(parsed_row.recent_form),
            surface_win_rate=_to_float(parsed_row.surface_win_rate),
            hold_pct=_to_float(parsed_row.hold_pct),
            break_pct=_to_float(parsed_row.break_pct),
            h2h_wins=None,
            h2h_losses=None,
            has_stats=False,
            reason_code="ok",
            reason_code_detail="normalized_from_matchmx",
        )
        feature.has_stats = _has_stats(feature)
        if not feature.has_stats:
            feature.reason_code = "provider_returned_null_features"
        rows.append(feature)

    valid_rows = [r for r in rows if r.reason_code == "ok" and r.player_canonical_name]
    metric_non_null = {
        "ranking": sum(1 for r in valid_rows if r.ranking is not None),
        "hold_pct": sum(1 for r in valid_rows if r.hold_pct is not None),
        "break_pct": sum(1 for r in valid_rows if r.break_pct is not None),
    }
    unique_players = len({r.player_canonical_name for r in valid_rows if r.player_canonical_name})
    threshold_failures: list[str] = []
    if unique_players < SOURCE_HEALTH_THRESHOLDS["min_unique_players"]:
        threshold_failures.append("min_unique_players")
    for metric, count in metric_non_null.items():
        if count <= 0:
            threshold_failures.append(f"{metric}_non_null_coverage")

    full_name_pattern = re.compile(r"^[A-Za-z][A-Za-z .'-]*\s[A-Za-z .'-]*[A-Za-z]$")
    first_n = 20
    name_quality_rows = [r for r in valid_rows if r.player_canonical_name][:first_n]
    bad_name_rows = [
        {"row": idx, "player_name": r.player_canonical_name}
        for idx, r in enumerate(name_quality_rows, start=1)
        if not full_name_pattern.match((r.player_canonical_name or "").strip())
    ]
    if bad_name_rows:
        threshold_failures.append("first_n_rows_non_alphabetic_full_name")

    if threshold_failures:
        rows.append(
            PlayerFeature(
                player_canonical_name=None,
                source=source,
                as_of=as_of,
                ranking=None,
                recent_form=None,
                surface_win_rate=None,
                hold_pct=None,
                break_pct=None,
                h2h_wins=None,
                h2h_losses=None,
                has_stats=False,
                reason_code="ta_matchmx_unusable_payload",
                reason_code_detail=json.dumps(
                    {
                        "reason": "ta_matchmx_guardrail_failed",
                        "failed_checks": threshold_failures,
                        "unique_players": unique_players,
                        "metric_non_null": metric_non_null,
                        "name_quality": {
                            "rows_checked": len(name_quality_rows),
                            "bad_rows": bad_name_rows[:10],
                        },
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            )
        )
    return rows


def _records_from_json(data: object) -> Iterable[dict[str, object]]:
    if isinstance(data, list):
        for row in data:
            if isinstance(row, dict):
                yield row
        return
    if isinstance(data, dict):
        if "players" in data and isinstance(data["players"], list):
            for row in data["players"]:
                if isinstance(row, dict):
                    yield row
            return
        if "data" in data and isinstance(data["data"], list):
            for row in data["data"]:
                if isinstance(row, dict):
                    yield row
            return
        if all(not isinstance(v, (dict, list)) for v in data.values()):
            yield data


def _as_dict(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return value
    return {}


def _contains_any_token(value: str, tokens: tuple[str, ...]) -> bool:
    normalized = value.casefold()
    return any(token in normalized for token in tokens)


def _is_sofascore_tennis_event(event: dict[str, object]) -> bool:
    sport = _as_dict(event.get("sport"))
    if _contains_any_token(str(sport.get("slug") or ""), ("tennis",)):
        return True
    if _contains_any_token(str(sport.get("name") or ""), ("tennis",)):
        return True
    return False


def _is_sofascore_wta_event(event: dict[str, object]) -> bool:
    searchable_segments: list[str] = []
    for node in (
        _as_dict(event.get("category")),
        _as_dict(event.get("tournament")),
        _as_dict(event.get("uniqueTournament")),
        _as_dict(_as_dict(event.get("tournament")).get("category")),
        _as_dict(_as_dict(event.get("tournament")).get("uniqueTournament")),
    ):
        searchable_segments.extend([
            str(node.get("name") or ""),
            str(node.get("slug") or ""),
            str(node.get("gender") or ""),
        ])

    return any(_contains_any_token(segment, ("wta",)) for segment in searchable_segments)


def _split_sofascore_team_name(value: str) -> list[str]:
    text = value.strip()
    if not text:
        return []
    parts = re.split(r"\s*(?:/|&| and )\s*", text, flags=re.IGNORECASE)
    if len(parts) <= 1:
        return [text]
    return [part for part in (segment.strip() for segment in parts) if part]


def _extract_sofascore_team_player_names(team: dict[str, object]) -> tuple[list[str], bool]:
    has_supported_name_paths = False

    def _normalize_string(value: object) -> str | None:
        if not isinstance(value, str):
            return None
        return normalize_name(value)

    def _dedupe_non_generic(values: list[str]) -> list[str]:
        normalized_unique: list[str] = []
        seen: set[str] = set()
        for candidate in values:
            normalized = _normalize_string(candidate)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            normalized_unique.append(normalized)

        non_generic = [name for name in normalized_unique if not name.casefold().endswith(" team")]
        if non_generic:
            return non_generic
        return normalized_unique

    def _collect_from_path(node: dict[str, object], path: tuple[str, ...]) -> list[str]:
        cursor: object = node
        for token in path:
            if not isinstance(cursor, dict):
                return []
            cursor = cursor.get(token)
        normalized = _normalize_string(cursor)
        if not normalized:
            return []
        return _split_sofascore_team_name(normalized)

    direct_field_fallback_order: tuple[tuple[str, ...], ...] = (
        ("name",),
        ("teamName",),
        ("displayName",),
        ("fullName",),
        ("participantName",),
        ("shortName",),
        ("slug",),
    )
    deferred_generic_direct_names: list[str] = []
    for path in direct_field_fallback_order:
        if path[0] in team:
            has_supported_name_paths = True
        names = _collect_from_path(team, path)
        if not names:
            continue
        cleaned = _dedupe_non_generic(names)
        if cleaned and all(name.casefold().endswith(" team") for name in cleaned):
            deferred_generic_direct_names.extend(cleaned)
            continue
        return cleaned, has_supported_name_paths

    nested_field_fallback_order: tuple[tuple[str, ...], ...] = (
        ("player", "name"),
        ("player", "displayName"),
        ("player", "fullName"),
        ("player1", "name"),
        ("player1", "displayName"),
        ("player2", "name"),
        ("player2", "displayName"),
        ("team", "name"),
        ("team", "displayName"),
        ("team", "fullName"),
        ("homeTeam", "name"),
        ("awayTeam", "name"),
    )
    for path in nested_field_fallback_order:
        if path[0] in team:
            has_supported_name_paths = True
        names = _collect_from_path(team, path)
        if names:
            return _dedupe_non_generic(names), has_supported_name_paths

    candidates: list[str] = []

    def _collect_entity(value: object) -> None:
        nonlocal has_supported_name_paths
        if isinstance(value, dict):
            for key in ("name", "displayName", "fullName", "participantName", "shortName", "slug"):
                if key in value:
                    has_supported_name_paths = True
                normalized = _normalize_string(value.get(key))
                if normalized:
                    candidates.extend(_split_sofascore_team_name(normalized))
                    break
            for nested_key in (
                "player",
                "players",
                "player1",
                "player2",
                "participants",
                "participant",
                "members",
                "athletes",
                "team",
                "teams",
                "homeTeam",
                "awayTeam",
                "nameTranslations",
            ):
                if nested_key in value:
                    has_supported_name_paths = True
                _collect_entity(value.get(nested_key))
            return
        if isinstance(value, list):
            if value:
                has_supported_name_paths = True
            for item in value:
                _collect_entity(item)

    for key in (
        "players",
        "player1",
        "player2",
        "participants",
        "participant",
        "members",
        "athletes",
        "team",
        "teams",
        "homeTeam",
        "awayTeam",
        "nameTranslations",
        "player",
    ):
        if key in team:
            has_supported_name_paths = True
        _collect_entity(team.get(key))

    deduped_candidates = _dedupe_non_generic(candidates)
    if deduped_candidates:
        return deduped_candidates, has_supported_name_paths
    if deferred_generic_direct_names:
        return _dedupe_non_generic(deferred_generic_direct_names), has_supported_name_paths
    return [], has_supported_name_paths


def _normalize_sofascore_team_player_name(team: dict[str, object]) -> str | None:
    names, _ = _extract_sofascore_team_player_names(team)
    return names[0] if names else None


def _extract_sofascore_event_team_names(event: dict[str, object], side: str) -> tuple[list[str], bool]:
    side_key = "home" if side == "homeTeam" else "away"
    candidates: list[str] = []
    has_supported_name_paths = False
    ordered_keys = (
        f"{side_key}TeamName",
        f"{side_key}Team",
        f"{side_key}Name",
        f"{side_key}ParticipantName",
    )
    for key in ordered_keys:
        value = event.get(key)
        if value is None:
            continue
        has_supported_name_paths = True
        if isinstance(value, str):
            normalized = normalize_name(value)
            if normalized:
                candidates.extend(_split_sofascore_team_name(normalized))
    return candidates, has_supported_name_paths


def _parse_sofascore_events_records(data: dict[str, object], source: str, as_of: str) -> list[PlayerFeature]:
    events = data.get("events")
    if not isinstance(events, list):
        return []

    rows: list[PlayerFeature] = []
    participant_fields_seen = False
    for event in events:
        if not isinstance(event, dict):
            continue
        if not _is_sofascore_tennis_event(event):
            continue
        if not _is_sofascore_wta_event(event):
            continue
        for side in ("homeTeam", "awayTeam"):
            team = _as_dict(event.get(side))
            player_names, has_participant_fields = _extract_sofascore_team_player_names(team)
            if not player_names:
                event_level_names, event_has_fields = _extract_sofascore_event_team_names(event, side)
                if event_level_names:
                    player_names = event_level_names
                has_participant_fields = has_participant_fields or event_has_fields
            participant_fields_seen = participant_fields_seen or has_participant_fields
            for player_name in player_names:
                rows.append(
                    PlayerFeature(
                        player_canonical_name=player_name,
                        source=source,
                        as_of=as_of,
                        ranking=_to_int(_pick(team, "ranking", "seed")),
                        recent_form=None,
                        surface_win_rate=None,
                        hold_pct=None,
                        break_pct=None,
                        h2h_wins=None,
                        h2h_losses=None,
                        has_stats=False,
                        reason_code="ok",
                        reason_code_detail="normalized_from_sofascore_events",
                    )
                )

    min_participants = SOFASCORE_MIN_PARTICIPANTS_BY_SOURCE.get(source, 1)
    if len(rows) >= min_participants:
        for row in rows:
            row.has_stats = _has_stats(row)
        return rows

    if len(rows) < min_participants:
        return [
            PlayerFeature(
                player_canonical_name=None,
                source=source,
                as_of=as_of,
                ranking=None,
                recent_form=None,
                surface_win_rate=None,
                hold_pct=None,
                break_pct=None,
                h2h_wins=None,
                h2h_losses=None,
                has_stats=False,
                reason_code="sofascore_events_participant_floor_unmet",
                reason_code_detail=f"participants_extracted:{len(rows)}_min_required:{min_participants}",
            )
        ]

    if participant_fields_seen:
        return []
    return [
        PlayerFeature(
            player_canonical_name=None,
            source=source,
            as_of=as_of,
            ranking=None,
            recent_form=None,
            surface_win_rate=None,
            hold_pct=None,
            break_pct=None,
            h2h_wins=None,
            h2h_losses=None,
            has_stats=False,
            reason_code="sofascore_events_payload_without_players",
            reason_code_detail="events_present_but_no_home_away_team_names",
        )
    ]


def _parse_sofascore_player_record(data: dict[str, object], source: str, as_of: str) -> list[PlayerFeature]:
    player = data.get("player")
    if not isinstance(player, dict):
        return []
    feature = PlayerFeature(
        player_canonical_name=normalize_name(_pick(player, "name", "shortName", "slug")),
        source=source,
        as_of=as_of,
        ranking=_to_int(_pick(player, "ranking", "position", "seed")),
        recent_form=None,
        surface_win_rate=None,
        hold_pct=None,
        break_pct=None,
        h2h_wins=None,
        h2h_losses=None,
        has_stats=False,
        reason_code="ok",
        reason_code_detail="normalized_from_sofascore_player",
    )
    feature.has_stats = _has_stats(feature)
    if feature.player_canonical_name is None:
        feature.reason_code = "sofascore_player_missing_name"
    return [feature]


def _parse_sofascore_statistics_record(data: dict[str, object], source: str, as_of: str) -> list[PlayerFeature]:
    statistics = data.get("statistics")
    if not isinstance(statistics, dict):
        return []
    stat_keys = sorted([str(key) for key in statistics.keys()])
    detail = ",".join(stat_keys[:5]) if stat_keys else "no_statistics_keys"
    return [
        PlayerFeature(
            player_canonical_name=None,
            source=source,
            as_of=as_of,
            ranking=None,
            recent_form=None,
            surface_win_rate=None,
            hold_pct=None,
            break_pct=None,
            h2h_wins=None,
            h2h_losses=None,
            has_stats=False,
            reason_code="sofascore_statistics_payload",
            reason_code_detail=f"statistics_keys:{detail}",
        )
    ]


def _parse_json_rows(source: str, text: str, as_of: str) -> list[PlayerFeature]:
    try:
        data = json.loads(text)
    except Exception:
        return []

    if isinstance(data, dict):
        sofascore_events = _parse_sofascore_events_records(data, source, as_of)
        if sofascore_events:
            return sofascore_events
        sofascore_player = _parse_sofascore_player_record(data, source, as_of)
        if sofascore_player:
            return sofascore_player
        sofascore_statistics = _parse_sofascore_statistics_record(data, source, as_of)
        if sofascore_statistics:
            return sofascore_statistics

    rows: list[PlayerFeature] = []
    for obj in _records_from_json(data):
        player_name = normalize_name(_pick(obj, "player_canonical_name", "player", "player_name", "name", "athlete"))
        feature = PlayerFeature(
            player_canonical_name=player_name,
            source=source,
            as_of=as_of,
            ranking=_to_int(_pick(obj, "ranking", "rank", "wta_rank")),
            recent_form=_to_float(_pick(obj, "recent_form", "form", "form_score")),
            surface_win_rate=_to_float(_pick(obj, "surface_win_rate", "surface_wr", "surface_win_pct")),
            hold_pct=_to_float(_pick(obj, "hold_pct", "hold", "service_hold_pct")),
            break_pct=_to_float(_pick(obj, "break_pct", "break", "return_break_pct")),
            h2h_wins=_to_int(_pick(obj, "h2h_wins", "head_to_head_wins")),
            h2h_losses=_to_int(_pick(obj, "h2h_losses", "head_to_head_losses")),
            has_stats=False,
            reason_code=str(_pick(obj, "reason_code") or "ok"),
            reason_code_detail=str(_pick(obj, "reason_code_detail", "diagnostic") or "normalized_from_json"),
        )
        feature.has_stats = _has_stats(feature)
        if feature.player_canonical_name is None:
            feature.reason_code = "missing_player_name"
        elif not feature.has_stats and feature.reason_code == "ok":
            feature.reason_code = "provider_returned_null_features"
        rows.append(feature)
    return rows


def _parse_csv_rows(source: str, text: str, as_of: str) -> list[PlayerFeature]:
    reader = csv.DictReader(text.splitlines())
    rows: list[PlayerFeature] = []
    for obj in reader:
        player_name = normalize_name(_pick(obj, "player_canonical_name", "player", "player_name", "name"))
        feature = PlayerFeature(
            player_canonical_name=player_name,
            source=source,
            as_of=as_of,
            ranking=_to_int(_pick(obj, "ranking", "rank", "wta_rank")),
            recent_form=_to_float(_pick(obj, "recent_form", "form", "form_score")),
            surface_win_rate=_to_float(_pick(obj, "surface_win_rate", "surface_win_pct")),
            hold_pct=_to_float(_pick(obj, "hold_pct", "service_hold_pct")),
            break_pct=_to_float(_pick(obj, "break_pct", "return_break_pct")),
            h2h_wins=_to_int(_pick(obj, "h2h_wins")),
            h2h_losses=_to_int(_pick(obj, "h2h_losses")),
            has_stats=False,
            reason_code=str(_pick(obj, "reason_code") or "ok"),
            reason_code_detail=str(_pick(obj, "reason_code_detail") or "normalized_from_csv"),
        )
        feature.has_stats = _has_stats(feature)
        if feature.player_canonical_name is None:
            feature.reason_code = "missing_player_name"
        elif not feature.has_stats and feature.reason_code == "ok":
            feature.reason_code = "provider_returned_null_features"
        rows.append(feature)
    return rows


def _source_role(source: str) -> str:
    if source in {"tennisabstract_leaders"}:
        return "pointer"
    return "features"


def _detect_hard_api_error(text: str, payload_mode: str) -> tuple[str, str] | None:
    if payload_mode != "json":
        return None

    try:
        data = json.loads(text)
    except Exception:
        return None
    if not isinstance(data, dict):
        return None

    status = data.get("status")
    if isinstance(status, int) and status >= 400:
        return "api_hard_error", f"status:{status}"

    status_code = data.get("statusCode")
    if isinstance(status_code, int) and status_code >= 400:
        return "api_hard_error", f"statusCode:{status_code}"

    error_value = data.get("error")
    if isinstance(error_value, (str, dict, list)):
        detail = error_value if isinstance(error_value, str) else json.dumps(error_value, ensure_ascii=False)[:300]
        return "api_hard_error", f"error:{detail}"

    success = data.get("success")
    if success is False and data.get("data") in (None, [], {}):
        message = str(data.get("message") or "success_false_empty_data")
        return "api_hard_error", message

    return None


def _extract_from_file(path: Path, selected_sources: set[str], diagnostics: list[SourceDiagnostic] | None = None) -> list[PlayerFeature]:
    source = path.stem.split(".")[0]
    as_of = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat()

    if selected_sources and source not in selected_sources:
        return []

    text = path.read_text(encoding="utf-8", errors="ignore")
    payload_mode = _detect_payload_mode(source, path, text)

    if _source_role(source) == "pointer":
        if diagnostics is not None:
            diagnostics.append(
                SourceDiagnostic(
                    source=source,
                    as_of=as_of,
                    payload_file=path.name,
                    payload_mode=payload_mode,
                    issue_code="source_role_pointer_skipped",
                    issue_detail="pointer_or_metadata_payload_not_emitted_to_player_features",
                )
            )
        return []

    hard_error = _detect_hard_api_error(text, payload_mode)
    if hard_error is not None:
        issue_code, issue_detail = hard_error
        if diagnostics is not None:
            diagnostics.append(
                SourceDiagnostic(
                    source=source,
                    as_of=as_of,
                    payload_file=path.name,
                    payload_mode=payload_mode,
                    issue_code=issue_code,
                    issue_detail=issue_detail,
                )
            )
        return []

    if payload_mode == "matchmx":
        rows = _parse_matchmx_rows(source, text, as_of)
        if rows:
            return rows

    contract_rows = _parse_contract_html_rows(source, text, as_of)
    if contract_rows:
        return contract_rows

    if payload_mode == "json":
        json_rows = _parse_json_rows(source, text, as_of)
        if json_rows:
            return json_rows

    if payload_mode == "csv":
        csv_rows = _parse_csv_rows(source, text, as_of)
        if csv_rows:
            return csv_rows

    if diagnostics is not None:
        diagnostics.append(
            SourceDiagnostic(
                source=source,
                as_of=as_of,
                payload_file=path.name,
                payload_mode=payload_mode,
                issue_code="source_parse_error",
                issue_detail=f"unsupported_or_empty_payload:{path.name}",
            )
        )
    return []


def _quality_score(row: PlayerFeature) -> int:
    score = 0
    for value in [
        row.ranking,
        row.recent_form,
        row.surface_win_rate,
        row.hold_pct,
        row.break_pct,
        row.h2h_wins,
        row.h2h_losses,
    ]:
        if value is not None:
            score += 1
    return score


def _dedupe_rows(rows: list[PlayerFeature]) -> list[PlayerFeature]:
    best: dict[tuple[str, str], PlayerFeature] = {}
    passthrough: list[PlayerFeature] = []

    for row in rows:
        if row.player_canonical_name is None:
            passthrough.append(row)
            continue
        key = (row.player_canonical_name.lower(), row.source)
        existing = best.get(key)
        if existing is None or _quality_score(row) > _quality_score(existing):
            best[key] = row

    ordered = sorted(best.values(), key=lambda item: (item.source, item.player_canonical_name or ""))
    return passthrough + ordered


def _write_jsonl(path: Path, rows: list[PlayerFeature]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(asdict(row), ensure_ascii=False) + "\n")


def _write_csv(path: Path, rows: list[PlayerFeature]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=SCHEMA_COLUMNS)
        writer.writeheader()
        for row in rows:
            data = asdict(row)
            writer.writerow({key: data.get(key) for key in SCHEMA_COLUMNS})


def _write_diagnostics_csv(path: Path, rows: list[SourceDiagnostic]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=DIAGNOSTIC_COLUMNS)
        writer.writeheader()
        for row in rows:
            data = asdict(row)
            writer.writerow({key: data.get(key) for key in DIAGNOSTIC_COLUMNS})


def _compute_parse_health(rows: list[PlayerFeature]) -> dict[str, dict[str, object]]:
    by_source: dict[str, list[PlayerFeature]] = {}
    for row in rows:
        by_source.setdefault(row.source, []).append(row)

    health: dict[str, dict[str, object]] = {}
    for source, source_rows in sorted(by_source.items()):
        unique_players = {r.player_canonical_name for r in source_rows if r.player_canonical_name}
        tracked = [r for r in source_rows if r.player_canonical_name]
        metric_non_null = {
            "ranking": sum(1 for r in tracked if r.ranking is not None),
            "hold_pct": sum(1 for r in tracked if r.hold_pct is not None),
            "break_pct": sum(1 for r in tracked if r.break_pct is not None),
        }
        invalid_samples = [
            {
                "reason_code": r.reason_code,
                "reason_code_detail": r.reason_code_detail,
                "player": r.player_canonical_name,
            }
            for r in source_rows
            if r.reason_code != "ok"
        ][:5]
        health[source] = {
            "rows_parsed": len(source_rows),
            "unique_players": len(unique_players),
            "metric_non_null": metric_non_null,
            "first_invalid_rows": invalid_samples,
        }
    return health


def _assert_parse_health(health: dict[str, dict[str, object]]) -> None:
    errors: list[str] = []
    for source, metrics in health.items():
        if source not in TA_SOURCE_MATRIX and not source.startswith("tennisabstract_"):
            continue
        rows_parsed = int(metrics.get("rows_parsed", 0))
        unique_players = int(metrics.get("unique_players", 0))
        metric_non_null = metrics.get("metric_non_null", {}) if isinstance(metrics.get("metric_non_null"), dict) else {}
        ranking_non_null = int(metric_non_null.get("ranking", 0))
        hold_non_null = int(metric_non_null.get("hold_pct", 0))
        break_non_null = int(metric_non_null.get("break_pct", 0))
        invalid_rows = metrics.get("first_invalid_rows", [])
        if rows_parsed < SOURCE_HEALTH_THRESHOLDS["min_rows"]:
            errors.append(f"{source}: rows_parsed={rows_parsed} < {SOURCE_HEALTH_THRESHOLDS['min_rows']}; invalid_samples={invalid_rows}")
        if unique_players < SOURCE_HEALTH_THRESHOLDS["min_unique_players"]:
            errors.append(f"{source}: unique_players={unique_players} < {SOURCE_HEALTH_THRESHOLDS['min_unique_players']}; invalid_samples={invalid_rows}")
        if ranking_non_null <= 0:
            errors.append(f"{source}: ranking_non_null_coverage=0; invalid_samples={invalid_rows}")
        if hold_non_null <= 0:
            errors.append(f"{source}: hold_pct_non_null_coverage=0; invalid_samples={invalid_rows}")
        if break_non_null <= 0:
            errors.append(f"{source}: break_pct_non_null_coverage=0; invalid_samples={invalid_rows}")
    if errors:
        raise RuntimeError("Parse health thresholds not met:\n- " + "\n- ".join(errors))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract normalized player features from source payloads")
    parser.add_argument(
        "--out-dir",
        default=None,
        help="Output directory containing raw/ payloads (default: $OUT_DIR or ./tmp/source_probes)",
    )
    parser.add_argument(
        "--source-catalog",
        default=str(CATALOG_PATH),
        help="Path to source catalog used to determine selected providers",
    )
    parser.add_argument(
        "--provider-allowlist",
        default=os.environ.get("PROVIDER_ALLOWLIST", "tennisabstract_*,ta_*,mcp_report_*,sofascore_*"),
        help="Comma-separated wildcard patterns for providers to include during extraction",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    out_dir = Path(args.out_dir or Path.cwd() / "tmp" / "source_probes")
    if not args.out_dir and "OUT_DIR" in os.environ:
        out_dir = Path(os.environ["OUT_DIR"])

    raw_dir = out_dir / "raw"
    normalized_dir = out_dir / "normalized"
    normalized_dir.mkdir(parents=True, exist_ok=True)

    if not raw_dir.exists():
        print(f"ERROR: raw payload directory not found: {raw_dir}", file=sys.stderr)
        return 1

    payload_files = sorted(path for path in raw_dir.iterdir() if path.is_file())
    if not payload_files:
        print(f"ERROR: no payload files found in {raw_dir}", file=sys.stderr)
        return 1

    selected_sources = _load_selected_sources(Path(args.source_catalog))
    provider_allowlist = _parse_allowlist(args.provider_allowlist)

    extracted: list[PlayerFeature] = []
    diagnostics: list[SourceDiagnostic] = []
    for path in payload_files:
        source = path.stem.split(".")[0]
        if not _is_allowed_source(source, provider_allowlist):
            print(f"INFO: skipping provider '{source}' (not in provider allowlist: {args.provider_allowlist})")
            continue
        extracted.extend(_extract_from_file(path, selected_sources, diagnostics))

    normalized = _dedupe_rows(extracted)
    jsonl_path = normalized_dir / "player_features.jsonl"
    csv_path = normalized_dir / "player_features.csv"
    diagnostics_path = normalized_dir / "source_diagnostics.csv"
    _write_jsonl(jsonl_path, normalized)
    _write_csv(csv_path, normalized)
    _write_diagnostics_csv(diagnostics_path, diagnostics)
    health = _compute_parse_health(normalized)
    health_path = normalized_dir / "parse_health.json"
    health_path.write_text(json.dumps(health, indent=2, sort_keys=True), encoding="utf-8")
    _assert_parse_health(health)

    print(f"Wrote {len(normalized)} rows to {jsonl_path}")
    print(f"Wrote {len(normalized)} rows to {csv_path}")
    print(f"Wrote {len(diagnostics)} rows to {diagnostics_path}")
    print(f"Wrote parse health to {health_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
