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
    return payload


def _canonical_run_pair(run_a: str, run_b: str) -> list[str]:
    return sorted([str(run_a).strip(), str(run_b).strip()])


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

    sidecar = {
        "schema": "wta_edge_board.preflight.v1",
        "recorded_at_utc": _utc_now_iso(),
        "export_dir": os.path.normpath(export_dir),
        "manifest_generated_at_utc": manifest_generated_at,
        "run_pair": _canonical_run_pair(run_a, run_b),
        "allow_csv_only_triage": bool(allow_csv_only_triage),
        "incident_tag": str(incident_tag or "").strip(),
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
