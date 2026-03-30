#!/usr/bin/env python3
"""Utilities for enforcing export preflight guardrails before compare workflows."""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

INCIDENT_TAG_PATTERN = re.compile(r"^[A-Za-z]+-[0-9]{3,}$")
PREFLIGHT_SIDECAR_NAME = "run_compare_preflight.json"
MANIFEST_NAME = "runtime_export_manifest.json"
REQUIRED_EXPORT_FILES = ("Run_Log.csv", "Run_Log.json", "State.csv", "State.json")
REQUIRED_COMPARE_STAGES = (
    "stageFetchOdds",
    "stageFetchSchedule",
    "stageMatchEvents",
    "stageFetchPlayerStats",
    "stageGenerateSignals",
    "stagePersist",
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def validate_incident_tag(tag: str) -> str:
    value = str(tag or "").strip()
    if not INCIDENT_TAG_PATTERN.match(value):
        raise ValueError(
            "Incident tag is required and must match <LETTERS>-<NNN> (for example INC-1234)."
        )
    return value


def load_manifest(export_dir: str) -> dict[str, Any]:
    manifest_path = Path(export_dir) / MANIFEST_NAME
    if not manifest_path.exists():
        raise FileNotFoundError(
            f"Missing {MANIFEST_NAME} in {export_dir}. Run scripts/export_parity_precheck.sh first."
        )
    with manifest_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Invalid {MANIFEST_NAME}: expected JSON object.")
    _validate_manifest_completeness(payload)
    return payload


def _validate_manifest_completeness(payload: dict[str, Any]) -> None:
    files = payload.get("files")
    if not isinstance(files, list):
        raise ValueError(f"Invalid {MANIFEST_NAME}: missing files list.")

    basenames = {
        Path(str(item.get("path") or "")).name
        for item in files
        if isinstance(item, dict)
    }
    missing = [name for name in REQUIRED_EXPORT_FILES if name not in basenames]
    if missing:
        raise ValueError(
            "Preflight guard failed: export manifest is incomplete for the runtime batch. "
            f"Missing required artifact(s): {missing}. "
            "Remediation: re-export artifacts to regenerate a complete batch with Run_Log.csv, "
            "Run_Log.json, State.csv, and State.json."
        )


def _canonical_run_pair(run_a: str, run_b: str) -> list[str]:
    return sorted([str(run_a).strip(), str(run_b).strip()])


def _parse_json_like(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        return None


def _summary_stage_entries(summary_row: dict[str, Any]) -> list[dict[str, Any]]:
    stage_summaries = _parse_json_like(summary_row.get("stage_summaries"))
    if isinstance(stage_summaries, list):
        return [item for item in stage_summaries if isinstance(item, dict)]
    if isinstance(stage_summaries, dict):
        nested = stage_summaries.get("stage_summaries")
        if isinstance(nested, list):
            return [item for item in nested if isinstance(item, dict)]
    return []


def _load_run_log_rows(export_dir: str) -> list[dict[str, Any]]:
    run_log_path = Path(export_dir) / "Run_Log.json"
    if not run_log_path.exists():
        return []
    with run_log_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, list):
        return []
    return [row for row in payload if isinstance(row, dict)]


def _run_checklist(rows: list[dict[str, Any]], run_id: str) -> dict[str, bool]:
    summary_row = next(
        (
            row
            for row in reversed(rows)
            if str(row.get("run_id") or "") == run_id
            and str(row.get("row_type") or "") == "summary"
            and str(row.get("stage") or "") == "runEdgeBoard"
        ),
        None,
    )
    stage_set: set[str] = set()
    for row in rows:
        if str(row.get("run_id") or "") != run_id:
            continue
        stage = str(row.get("stage") or "").strip()
        if stage and stage != "runEdgeBoard":
            stage_set.add(stage)
    stage_entries = _summary_stage_entries(summary_row or {})
    for stage_entry in stage_entries:
        stage = str(stage_entry.get("stage") or "").strip()
        if stage:
            stage_set.add(stage)

    checklist: dict[str, bool] = {
        "has_runEdgeBoard_summary": summary_row is not None,
        "has_runEdgeBoard_stage_summaries_shape": bool(stage_entries),
        "has_stageFetchPlayerStats_summary": any(
            str(entry.get("stage") or "") == "stageFetchPlayerStats" for entry in stage_entries
        ),
    }
    for stage in REQUIRED_COMPARE_STAGES:
        checklist[f"has_{stage}"] = stage in stage_set
    checklist["has_required_stages"] = all(checklist[f"has_{stage}"] for stage in REQUIRED_COMPARE_STAGES)
    checklist["compare_ready"] = all(
        (
            checklist["has_runEdgeBoard_summary"],
            checklist["has_runEdgeBoard_stage_summaries_shape"],
            checklist["has_stageFetchPlayerStats_summary"],
            checklist["has_required_stages"],
        )
    )
    return checklist


def write_preflight_sidecar(
    export_dir: str,
    run_a: str,
    run_b: str,
    allow_csv_only_triage: bool,
    incident_tag: str,
) -> str:
    manifest = load_manifest(export_dir)
    manifest_generated_at = str(manifest.get("generated_at_utc") or "").strip()
    if not manifest_generated_at:
        raise ValueError(f"Invalid {MANIFEST_NAME}: missing generated_at_utc.")

    run_rows = _load_run_log_rows(export_dir)
    sidecar = {
        "schema": "wta_edge_board.preflight.v1",
        "recorded_at_utc": _utc_now_iso(),
        "export_dir": os.path.normpath(export_dir),
        "manifest_generated_at_utc": manifest_generated_at,
        "run_pair": _canonical_run_pair(run_a, run_b),
        "allow_csv_only_triage": bool(allow_csv_only_triage),
        "incident_tag": str(incident_tag or "").strip(),
        "preflight_evidence": {
            "run_checklist_by_run_id": {
                run_id: _run_checklist(run_rows, run_id)
                for run_id in _canonical_run_pair(run_a, run_b)
            }
        },
    }
    output_path = Path(export_dir) / PREFLIGHT_SIDECAR_NAME
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(sidecar, handle, indent=2)
        handle.write("\n")
    return str(output_path)


def enforce_preflight_guard(
    export_dir: str,
    run_a: str,
    run_b: str,
    emergency_override_incident_tag: str,
) -> dict[str, Any]:
    """Raise ValueError when preflight sidecar/manifest do not authorize comparison."""

    manifest = load_manifest(export_dir)
    manifest_generated_at = str(manifest.get("generated_at_utc") or "").strip()
    if not manifest_generated_at:
        raise ValueError(f"Invalid {MANIFEST_NAME}: missing generated_at_utc.")

    sidecar_path = Path(export_dir) / PREFLIGHT_SIDECAR_NAME
    if not sidecar_path.exists():
        override_tag = str(emergency_override_incident_tag or "").strip()
        if override_tag:
            validate_incident_tag(override_tag)
            return {
                "status": "emergency_override",
                "incident_tag": override_tag,
                "reason": "missing_preflight_sidecar",
            }
        raise ValueError(
            "Preflight guard failed: missing run_compare_preflight.json for current export batch. "
            "Run scripts/export_parity_precheck.sh before compare scripts, or provide "
            "--emergency-preflight-override-tag <INCIDENT-TAG>."
        )

    with sidecar_path.open("r", encoding="utf-8") as handle:
        sidecar = json.load(handle)
    if not isinstance(sidecar, dict):
        raise ValueError(f"Invalid {PREFLIGHT_SIDECAR_NAME}: expected JSON object.")

    sidecar_manifest = str(sidecar.get("manifest_generated_at_utc") or "").strip()
    if sidecar_manifest != manifest_generated_at:
        raise ValueError(
            "Preflight guard failed: sidecar manifest stamp does not match current export batch. "
            "Re-run scripts/export_parity_precheck.sh for this export directory."
        )

    sidecar_pair = sidecar.get("run_pair")
    if not isinstance(sidecar_pair, list) or sorted(str(item).strip() for item in sidecar_pair) != _canonical_run_pair(run_a, run_b):
        raise ValueError(
            "Preflight guard failed: sidecar run pair does not match requested compare run IDs. "
            "Re-run scripts/export_parity_precheck.sh with the same run ID pair."
        )

    return {"status": "ok", "sidecar_path": str(sidecar_path)}
