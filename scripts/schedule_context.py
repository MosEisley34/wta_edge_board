#!/usr/bin/env python3
"""Utilities for extracting deterministic schedule context from Raw_Schedule rows."""

from __future__ import annotations

import csv
import glob
import json
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import Any

_STAGE_KEY_CANDIDATES: tuple[str, ...] = (
    "competition_stage",
    "tournament_stage",
    "draw_stage",
    "round",
    "round_name",
    "stage",
)

_MATCH_LABEL_KEY_CANDIDATES: tuple[str, ...] = (
    "match_label",
    "match_name",
    "event_name",
    "label",
    "name",
    "description",
)

_TOURNAMENT_TIER_KEY_CANDIDATES: tuple[str, ...] = (
    "tournament_tier",
    "tier",
    "tour_level",
    "event_level",
    "competition_level",
    "series",
    "category",
)

_TOURNAMENT_CONTEXT_KEY_CANDIDATES: tuple[str, ...] = (
    "tournament_id",
    "tournament_key",
    "tournament_slug",
    "tournament",
    "tournament_name",
    "competition_id",
    "competition_key",
    "competition_name",
    "event_group_id",
    "event_group_name",
    "run_context_id",
    "context_id",
)

_EVENT_ID_KEY_CANDIDATES: tuple[str, ...] = (
    "event_id",
    "match_event_id",
    "match_id",
    "schedule_event_id",
    "candidate_event_id",
    "baseline_event_id",
    "id",
)


def _normalize_scope_token(value: Any) -> str:
    token = str(value or "").strip().lower().replace("-", " ").replace("_", " ")
    token = " ".join(token.split())
    return token


def _pick_primary_scope(scope_counts: dict[str, int], scope_order: list[str]) -> tuple[str | None, int | None]:
    if not scope_counts:
        return None, None
    ranked = sorted(scope_counts.items(), key=lambda item: (-item[1], scope_order.index(item[0])))
    winner_key, winner_count = ranked[0]
    return winner_key, int(winner_count)


def _normalize_stage_token(value: Any) -> str:
    token = str(value or "").strip().lower().replace("-", " ").replace("_", " ")
    token = " ".join(token.split())
    return token


def _normalize_label_token(value: Any) -> str:
    token = str(value or "").strip().lower().replace("-", " ").replace("_", " ")
    token = " ".join(token.split())
    return token


def _infer_stage_from_token(token: str) -> str | None:
    if not token:
        return None
    if token in {"f", "finals"}:
        return "final"
    if token in {"sf", "semi", "semis"}:
        return "semifinal"
    if token in {"qf", "quarters"}:
        return "quarterfinal"
    if "final" in token and "semi" not in token and "quarter" not in token:
        return "final"
    if "semi" in token:
        return "semifinal"
    if "quarter" in token:
        return "quarterfinal"
    if " sf" in f" {token}" or token.startswith("sf "):
        return "semifinal"
    if " qf" in f" {token}" or token.startswith("qf "):
        return "quarterfinal"
    if "round of 16" in token or "r16" in token:
        return "round_of_16"
    if "round of 32" in token or "r32" in token:
        return "round_of_32"
    if "round of 64" in token or "r64" in token:
        return "round_of_64"
    if "1st round" in token or token == "r1" or "first round" in token:
        return "round_1"
    if "2nd round" in token or token == "r2" or "second round" in token:
        return "round_2"
    if "3rd round" in token or token == "r3" or "third round" in token:
        return "round_3"
    return None


def _parse_start_time(value: Any) -> datetime | None:
    token = str(value or "").strip()
    if not token:
        return None
    if token.endswith("Z"):
        token = token[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(token)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _row_scope_token(row: dict[str, Any]) -> str:
    for key in _TOURNAMENT_CONTEXT_KEY_CANDIDATES:
        value = row.get(key)
        if value in (None, ""):
            continue
        token = _normalize_scope_token(value)
        if token:
            return token
    return ""


def _row_hint_stages(row: dict[str, Any]) -> list[str]:
    hints: list[str] = []
    for key in _STAGE_KEY_CANDIDATES + _MATCH_LABEL_KEY_CANDIDATES:
        value = row.get(key)
        if value in (None, ""):
            continue
        inferred = _infer_stage_from_token(_normalize_label_token(value))
        if inferred is not None:
            hints.append(inferred)
    return hints


def _row_event_id_token(row: dict[str, Any]) -> str:
    for key in _EVENT_ID_KEY_CANDIDATES:
        value = row.get(key)
        if value in (None, ""):
            continue
        token = str(value).strip()
        if token:
            return token
    return ""


def _extract_event_id_tokens(candidate: Any) -> list[str]:
    if not isinstance(candidate, dict):
        return []
    tokens: list[str] = []
    for key in _EVENT_ID_KEY_CANDIDATES:
        value = candidate.get(key)
        if value in (None, ""):
            continue
        token = str(value).strip()
        if token:
            tokens.append(token)
    for key, value in candidate.items():
        if not isinstance(key, str):
            continue
        lowered = key.strip().lower()
        if "event_ids" in lowered or lowered.endswith("_event_id_list"):
            if isinstance(value, list):
                for item in value:
                    token = str(item).strip()
                    if token:
                        tokens.append(token)
            elif isinstance(value, str):
                for item in value.split(","):
                    token = item.strip()
                    if token:
                        tokens.append(token)
    return list(dict.fromkeys(tokens))


def _collect_scope_hints(
    payloads: list[Any],
    *,
    scoped_event_ids: set[str],
) -> dict[str, Any]:
    tier_by_event: dict[str, dict[str, int]] = {}
    context_by_event: dict[str, dict[str, int]] = {}
    tier_global_counts: dict[str, int] = {}
    context_global_counts: dict[str, int] = {}
    tier_order: list[str] = []
    context_order: list[str] = []

    def _bump(counter: dict[str, int], order: list[str], token: str) -> None:
        if token not in counter:
            order.append(token)
            counter[token] = 0
        counter[token] += 1

    def _consume(candidate: Any) -> None:
        if isinstance(candidate, list):
            for item in candidate:
                _consume(item)
            return
        if not isinstance(candidate, dict):
            return

        event_ids = _extract_event_id_tokens(candidate)
        tier_token = ""
        context_token = ""
        for key in _TOURNAMENT_TIER_KEY_CANDIDATES:
            value = candidate.get(key)
            if value in (None, ""):
                continue
            tier_token = _normalize_scope_token(value)
            if tier_token:
                _bump(tier_global_counts, tier_order, tier_token)
                break
        for key in _TOURNAMENT_CONTEXT_KEY_CANDIDATES:
            value = candidate.get(key)
            if value in (None, ""):
                continue
            context_token = _normalize_scope_token(value)
            if context_token:
                _bump(context_global_counts, context_order, context_token)
                break

        if event_ids and tier_token:
            for event_id in event_ids:
                tier_by_event.setdefault(event_id, {})
                tier_by_event[event_id][tier_token] = tier_by_event[event_id].get(tier_token, 0) + 1
        if event_ids and context_token:
            for event_id in event_ids:
                context_by_event.setdefault(event_id, {})
                context_by_event[event_id][context_token] = context_by_event[event_id].get(context_token, 0) + 1

        for value in candidate.values():
            _consume(value)

    for payload in payloads:
        _consume(payload)

    scoped_tier_counts: dict[str, int] = {}
    scoped_context_counts: dict[str, int] = {}
    for event_id in scoped_event_ids:
        for token, count in tier_by_event.get(event_id, {}).items():
            scoped_tier_counts[token] = scoped_tier_counts.get(token, 0) + int(count)
        for token, count in context_by_event.get(event_id, {}).items():
            scoped_context_counts[token] = scoped_context_counts.get(token, 0) + int(count)

    scoped_tier_order = [token for token in tier_order if token in scoped_tier_counts]
    scoped_context_order = [token for token in context_order if token in scoped_context_counts]
    primary_scoped_tier, _ = _pick_primary_scope(scoped_tier_counts, scoped_tier_order)
    primary_scoped_context, _ = _pick_primary_scope(scoped_context_counts, scoped_context_order)
    primary_global_tier, _ = _pick_primary_scope(tier_global_counts, tier_order)
    primary_global_context, _ = _pick_primary_scope(context_global_counts, context_order)

    return {
        "primary_scoped_tier_token": primary_scoped_tier,
        "primary_scoped_context_token": primary_scoped_context,
        "primary_global_tier_token": primary_global_tier,
        "primary_global_context_token": primary_global_context,
        "has_event_level_scope_hints": bool(scoped_tier_counts or scoped_context_counts),
    }


def _infer_stage_from_rows_without_stage_tokens(rows: list[dict[str, Any]]) -> tuple[str | None, str, str, str]:
    stage_votes: dict[str, int] = {}
    stage_order: list[str] = []
    for row in rows:
        for inferred in _row_hint_stages(row):
            if inferred not in stage_votes:
                stage_votes[inferred] = 0
                stage_order.append(inferred)
            stage_votes[inferred] += 1
    if stage_votes:
        winner = sorted(stage_votes.items(), key=lambda item: (-item[1], stage_order.index(item[0])))[0]
        stage, votes = winner
        confidence = "high" if votes >= 2 else "medium"
        return stage, "fallback_match_label_token", confidence, "label_or_round_hint"

    scoped_rows: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        scope = _row_scope_token(row) or "__unknown__"
        scoped_rows.setdefault(scope, []).append(row)
    ranked_scopes = sorted(scoped_rows.items(), key=lambda item: -len(item[1]))
    primary_scope, scope_rows = ranked_scopes[0]
    secondary_count = len(ranked_scopes[1][1]) if len(ranked_scopes) > 1 else 0
    dominant_ratio = float(len(scope_rows)) / float(max(1, len(rows)))
    if len(scoped_rows) > 1 and (len(scope_rows) < 2 or len(scope_rows) <= secondary_count or dominant_ratio < 0.5):
        return None, "schedule_rows_present_but_stage_unknown", "low", "mixed_tournament_distribution"

    scoped_count = len(scope_rows)
    parsed_times = [_parse_start_time(row.get("start_time")) for row in scope_rows]
    parsed_times = [item for item in parsed_times if item is not None]
    if scoped_count == 2:
        cluster_hours = None
        if len(parsed_times) == 2:
            delta = abs((max(parsed_times) - min(parsed_times)).total_seconds()) / 3600.0
            cluster_hours = float(delta)
        if cluster_hours is None or cluster_hours <= 8.0:
            confidence = "high" if (cluster_hours is not None and cluster_hours <= 4.0 and primary_scope != "__unknown__") else "medium"
            return "semifinal", "fallback_two_matches_remaining", confidence, "scope_row_count_with_time_cluster"
    if scoped_count == 1 and primary_scope != "__unknown__":
        return "final", "fallback_single_match_remaining", "medium", "scope_row_count"
    return None, "schedule_rows_present_but_stage_unknown", "low", "insufficient_progression_hints"


def _extract_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [dict(item) for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        rows = payload.get("rows")
        if isinstance(rows, list):
            return [dict(item) for item in rows if isinstance(item, dict)]
    return []


def _infer_stage_from_row(row: dict[str, Any]) -> str | None:
    for key in _STAGE_KEY_CANDIDATES:
        value = row.get(key)
        if value in (None, ""):
            continue
        inferred = _infer_stage_from_token(_normalize_stage_token(value))
        if inferred is not None:
            return inferred
    for key in _MATCH_LABEL_KEY_CANDIDATES:
        value = row.get(key)
        if value in (None, ""):
            continue
        inferred = _infer_stage_from_token(_normalize_label_token(value))
        if inferred is not None:
            return inferred
    return None


def compute_schedule_context(
    raw_schedule_payload: Any,
    *,
    run_scoped_event_ids: set[str] | None = None,
    scope_fallback_payloads: list[Any] | None = None,
) -> dict[str, Any]:
    rows = _extract_rows(raw_schedule_payload)
    global_upcoming_match_count = len(rows)
    normalized_scoped_event_ids = {str(token).strip() for token in (run_scoped_event_ids or set()) if str(token).strip()}
    scoped_rows = (
        [row for row in rows if _row_event_id_token(row) in normalized_scoped_event_ids]
        if normalized_scoped_event_ids
        else []
    )
    scoped_upcoming_match_count = len(scoped_rows)

    stage_tokens: list[str] = []
    tournament_tier: str | None = None
    tournament_tier_counts: dict[str, int] = {}
    tournament_tier_order: list[str] = []
    tournament_context_counts: dict[str, int] = {}
    tournament_context_order: list[str] = []
    stage_source_rows = scoped_rows if scoped_rows else rows
    for row in stage_source_rows:
        for key in _STAGE_KEY_CANDIDATES:
            value = row.get(key)
            if value not in (None, ""):
                token = _normalize_stage_token(value)
                if token:
                    stage_tokens.append(token)
        if tournament_tier is None:
            for key in _TOURNAMENT_TIER_KEY_CANDIDATES:
                value = row.get(key)
                if value not in (None, ""):
                    tournament_tier = str(value).strip()
                    break
        for key in _TOURNAMENT_TIER_KEY_CANDIDATES:
            value = row.get(key)
            if value in (None, ""):
                continue
            token = _normalize_scope_token(value)
            if not token:
                continue
            if token not in tournament_tier_counts:
                tournament_tier_order.append(token)
                tournament_tier_counts[token] = 0
            tournament_tier_counts[token] += 1
            break
        for key in _TOURNAMENT_CONTEXT_KEY_CANDIDATES:
            value = row.get(key)
            if value in (None, ""):
                continue
            token = _normalize_scope_token(value)
            if not token:
                continue
            if token not in tournament_context_counts:
                tournament_context_order.append(token)
                tournament_context_counts[token] = 0
            tournament_context_counts[token] += 1
            break

    inferred_stage = None
    stage_inference_fallback = "none"
    stage_inference_confidence = "none"
    stage_inference_source = "none"
    for candidate in stage_tokens:
        inferred = _infer_stage_from_token(candidate)
        if inferred is not None:
            inferred_stage = inferred
            stage_inference_confidence = "high"
            stage_inference_source = "direct_stage_token"
            break
    if inferred_stage is None and len(stage_source_rows) > 0:
        (
            inferred_stage,
            stage_inference_fallback,
            stage_inference_confidence,
            stage_inference_source,
        ) = _infer_stage_from_rows_without_stage_tokens(stage_source_rows)
        if inferred_stage is not None:
            stage_tokens.append(inferred_stage)
    elif len(stage_source_rows) == 0:
        stage_inference_fallback = "none"
        stage_inference_confidence = "none"
        stage_inference_source = "none"

    stage_scoped_rows = list(stage_source_rows)
    if inferred_stage is not None:
        stage_filtered_rows = [row for row in stage_source_rows if _infer_stage_from_row(row) == inferred_stage]
        if stage_filtered_rows:
            stage_scoped_rows = stage_filtered_rows

    scoped_tier_counts: dict[str, int] = {}
    scoped_tier_order: list[str] = []
    scoped_context_counts: dict[str, int] = {}
    scoped_context_order: list[str] = []
    for row in stage_scoped_rows:
        for key in _TOURNAMENT_TIER_KEY_CANDIDATES:
            value = row.get(key)
            if value in (None, ""):
                continue
            token = _normalize_scope_token(value)
            if not token:
                continue
            if token not in scoped_tier_counts:
                scoped_tier_order.append(token)
                scoped_tier_counts[token] = 0
            scoped_tier_counts[token] += 1
            break
        for key in _TOURNAMENT_CONTEXT_KEY_CANDIDATES:
            value = row.get(key)
            if value in (None, ""):
                continue
            token = _normalize_scope_token(value)
            if not token:
                continue
            if token not in scoped_context_counts:
                scoped_context_order.append(token)
                scoped_context_counts[token] = 0
            scoped_context_counts[token] += 1
            break

    primary_tier_token, tournament_tier_upcoming_match_count = _pick_primary_scope(scoped_tier_counts, scoped_tier_order)
    primary_context_token, same_tournament_context_upcoming_match_count = _pick_primary_scope(
        scoped_context_counts,
        scoped_context_order,
    )
    scope_token_source = "schedule_rows"
    scope_fallback_used = "none"
    if primary_tier_token is None or primary_context_token is None:
        hints = _collect_scope_hints(scope_fallback_payloads or [], scoped_event_ids=normalized_scoped_event_ids)
        if primary_tier_token is None:
            if hints.get("primary_scoped_tier_token"):
                primary_tier_token = str(hints["primary_scoped_tier_token"])
                scope_token_source = "fallback_event_ids"
                scope_fallback_used = "matched_event_ids"
            elif hints.get("primary_global_tier_token"):
                primary_tier_token = str(hints["primary_global_tier_token"])
                scope_token_source = "fallback_global_context"
                scope_fallback_used = "run_summary_or_stage_reason_metadata"
        if primary_context_token is None:
            if hints.get("primary_scoped_context_token"):
                primary_context_token = str(hints["primary_scoped_context_token"])
                scope_token_source = "fallback_event_ids"
                scope_fallback_used = "matched_event_ids"
            elif hints.get("primary_global_context_token"):
                primary_context_token = str(hints["primary_global_context_token"])
                scope_token_source = "fallback_global_context"
                scope_fallback_used = "run_summary_or_stage_reason_metadata"

    return {
        "has_schedule_rows": global_upcoming_match_count > 0,
        "upcoming_match_count": global_upcoming_match_count,
        "global_upcoming_match_count": global_upcoming_match_count,
        "upcoming_match_count_global": global_upcoming_match_count,
        "upcoming_match_count_scoped": scoped_upcoming_match_count if normalized_scoped_event_ids else None,
        "scoped_upcoming_match_count": scoped_upcoming_match_count if normalized_scoped_event_ids else None,
        "has_scoped_schedule_rows": scoped_upcoming_match_count > 0 if normalized_scoped_event_ids else False,
        "tournament_tier_upcoming_match_count": tournament_tier_upcoming_match_count,
        "same_tournament_context_upcoming_match_count": same_tournament_context_upcoming_match_count,
        "primary_tournament_tier_scope_token": primary_tier_token,
        "primary_tournament_context_scope_token": primary_context_token,
        "scope_token_source": scope_token_source,
        "scope_token_fallback_used": scope_fallback_used,
        "inferred_stage": inferred_stage,
        "stage_inference_available": inferred_stage is not None,
        "stage_inference_fallback": stage_inference_fallback,
        "stage_inference_confidence": stage_inference_confidence,
        "stage_inference_source": stage_inference_source,
        "tournament_tier": tournament_tier,
        "stage_tokens": sorted(set(stage_tokens)),
    }


def fallback_schedule_context(reason: str = "schedule_artifacts_unavailable") -> dict[str, Any]:
    context = compute_schedule_context([])
    context["schedule_artifacts_available"] = False
    context["context_source_reason"] = str(reason)
    return context


def schedule_context_from_export_dir(export_dir: str) -> dict[str, Any]:
    candidate = Path(str(export_dir or "")).expanduser()
    if not str(export_dir or "").strip():
        return fallback_schedule_context("export_dir_missing")
    if candidate.is_file():
        return fallback_schedule_context("export_dir_is_file")
    if not candidate.exists():
        return fallback_schedule_context("export_dir_not_found")
    if not candidate.is_dir():
        return fallback_schedule_context("export_dir_not_directory")

    artifact_paths: list[Path] = []
    for pattern in ("**/*Raw_Schedule*.json", "**/*Raw_Schedule*.csv"):
        for path in glob.glob(os.path.join(str(candidate), pattern), recursive=True):
            artifact_paths.append(Path(path))
    if not artifact_paths:
        return fallback_schedule_context("raw_schedule_artifact_missing")

    artifact = max(artifact_paths, key=lambda path: path.stat().st_mtime)
    rows: list[dict[str, Any]] = []
    if artifact.suffix.lower() == ".json":
        payload = json.loads(artifact.read_text(encoding="utf-8"))
        rows = _extract_rows(payload)
    elif artifact.suffix.lower() == ".csv":
        with artifact.open("r", encoding="utf-8", newline="") as handle:
            rows = [dict(row) for row in csv.DictReader(handle)]

    context = compute_schedule_context(rows)
    context["schedule_artifacts_available"] = True
    context["context_source_reason"] = "raw_schedule_artifact_loaded"
    context["schedule_artifact_path"] = str(artifact)
    return context
