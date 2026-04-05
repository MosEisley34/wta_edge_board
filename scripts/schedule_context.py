#!/usr/bin/env python3
"""Utilities for extracting deterministic schedule context from Raw_Schedule rows."""

from __future__ import annotations

import csv
import glob
import json
import os
from pathlib import Path
from typing import Any

_STAGE_KEY_CANDIDATES: tuple[str, ...] = (
    "competition_stage",
    "tournament_stage",
    "draw_stage",
    "round",
    "round_name",
    "stage",
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


def _normalize_stage_token(value: Any) -> str:
    token = str(value or "").strip().lower().replace("-", " ").replace("_", " ")
    token = " ".join(token.split())
    return token


def _infer_stage_from_token(token: str) -> str | None:
    if not token:
        return None
    if "final" in token and "semi" not in token and "quarter" not in token:
        return "final"
    if "semi" in token:
        return "semifinal"
    if "quarter" in token:
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


def _extract_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [dict(item) for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        rows = payload.get("rows")
        if isinstance(rows, list):
            return [dict(item) for item in rows if isinstance(item, dict)]
    return []


def compute_schedule_context(raw_schedule_payload: Any) -> dict[str, Any]:
    rows = _extract_rows(raw_schedule_payload)
    upcoming_match_count = len(rows)

    stage_tokens: list[str] = []
    tournament_tier: str | None = None
    for row in rows:
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

    inferred_stage = None
    for candidate in stage_tokens:
        inferred = _infer_stage_from_token(candidate)
        if inferred is not None:
            inferred_stage = inferred
            break

    return {
        "has_schedule_rows": upcoming_match_count > 0,
        "upcoming_match_count": upcoming_match_count,
        "inferred_stage": inferred_stage,
        "stage_inference_available": inferred_stage is not None,
        "stage_inference_fallback": (
            "schedule_rows_present_but_stage_unknown"
            if upcoming_match_count > 0 and inferred_stage is None
            else "none"
        ),
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
