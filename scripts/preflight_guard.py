#!/usr/bin/env python3
"""Utilities for enforcing export preflight guardrails before compare workflows."""

from __future__ import annotations

import json
import os
import re
import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from run_summary_cardinality import merge_run_summary_rows_for_cardinality
INCIDENT_TAG_PATTERN = re.compile(r"^[A-Za-z]+-[0-9]{3,}$")
PREFLIGHT_SIDECAR_NAME = "run_compare_preflight.json"
MANIFEST_NAME = "runtime_export_manifest.json"
REQUIRED_EXPORT_FILES = ("Run_Log.csv", "Run_Log.json", "State.csv", "State.json")
CANONICAL_RUNTIME_TABS = (
    "Config",
    "Run_Log",
    "Raw_Odds",
    "Raw_Schedule",
    "Raw_Player_Stats",
    "Match_Map",
    "Signals",
    "State",
    "ProviderHealth",
)
REQUIRED_COMPARE_STAGES = (
    "stageFetchOdds",
    "stageFetchSchedule",
    "stageMatchEvents",
    "stageFetchPlayerStats",
    "stageGenerateSignals",
    "stagePersist",
)
RUN_ID_TIMESTAMP_PATTERNS = (
    re.compile(r"(?<!\d)(20\d{2})(\d{2})(\d{2})[-_]?(\d{2})(\d{2})(\d{2})?(?!\d)"),
    re.compile(
        r"(?<!\d)"
        r"(20\d{2}-\d{2}-\d{2}[Tt_ -]?\d{2}:?\d{2}(?::?\d{2})?(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)"
        r"(?!\d)"
    ),
)
RUN_LOG_FRESHNESS_TIMESTAMP_KEYS = (
    "started_at",
    "ended_at",
    "timestamp",
    "created_at",
    "updated_at",
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


def _parse_timestamp_utc(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_run_id_timestamp(run_id: str) -> datetime | None:
    value = str(run_id or "").strip()
    if not value:
        return None
    compact_match = RUN_ID_TIMESTAMP_PATTERNS[0].search(value)
    if compact_match:
        year, month, day, hour, minute, second = compact_match.groups()
        second = second or "00"
        try:
            return datetime(
                int(year),
                int(month),
                int(day),
                int(hour),
                int(minute),
                int(second),
                tzinfo=timezone.utc,
            )
        except ValueError:
            pass

    iso_match = RUN_ID_TIMESTAMP_PATTERNS[1].search(value)
    if iso_match:
        return _parse_timestamp_utc(iso_match.group(1))
    return None


def _collect_export_run_ids(export_dir: str) -> set[str]:
    run_ids: set[str] = set()
    run_log_json = Path(export_dir) / "Run_Log.json"
    run_log_csv = Path(export_dir) / "Run_Log.csv"
    if run_log_json.is_file():
        with run_log_json.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        rows = payload if isinstance(payload, list) else []
        for row in rows:
            if isinstance(row, dict):
                run_id = str(row.get("run_id") or "").strip()
                if run_id:
                    run_ids.add(run_id)
    if run_log_csv.is_file():
        with run_log_csv.open("r", encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                run_id = str(row.get("run_id") or "").strip()
                if run_id:
                    run_ids.add(run_id)
    return run_ids


def _collect_export_run_log_rows(export_dir: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    run_log_json = Path(export_dir) / "Run_Log.json"
    run_log_csv = Path(export_dir) / "Run_Log.csv"
    if run_log_json.is_file():
        with run_log_json.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if isinstance(payload, list):
            rows.extend(item for item in payload if isinstance(item, dict))
    if run_log_csv.is_file():
        with run_log_csv.open("r", encoding="utf-8-sig", newline="") as handle:
            rows.extend(dict(row) for row in csv.DictReader(handle))
    return rows


def _parse_row_freshness_timestamp_utc(row: dict[str, Any]) -> datetime | None:
    for key in RUN_LOG_FRESHNESS_TIMESTAMP_KEYS:
        parsed = _parse_timestamp_utc(row.get(key))
        if parsed is not None:
            return parsed
    return None


def evaluate_export_freshness(export_dir: str, requested_run_ids: list[str]) -> dict[str, Any]:
    requested = [str(run_id or "").strip() for run_id in requested_run_ids if str(run_id or "").strip()]
    requested_from_run_id = {
        run_id: parse_run_id_timestamp(run_id)
        for run_id in requested
    }
    requested_resolved: dict[str, datetime] = {
        run_id: ts for run_id, ts in requested_from_run_id.items() if ts is not None
    }
    export_run_ids = _collect_export_run_ids(export_dir)
    export_parsed: dict[str, datetime] = {}
    for run_id in export_run_ids:
        parsed = parse_run_id_timestamp(run_id)
        if parsed is not None:
            export_parsed[run_id] = parsed
    export_rows = _collect_export_run_log_rows(export_dir)
    export_row_timestamps: list[tuple[str, datetime]] = []
    for row in export_rows:
        run_id = str(row.get("run_id") or "").strip()
        if not run_id:
            continue
        parsed = _parse_row_freshness_timestamp_utc(row)
        if parsed is None:
            continue
        export_row_timestamps.append((run_id, parsed))
        if run_id in requested_resolved:
            continue
        if run_id in requested:
            requested_resolved[run_id] = parsed

    latest_export = None
    if export_parsed:
        latest_export = max(export_parsed.items(), key=lambda item: item[1])
    if export_row_timestamps:
        latest_export_row = max(export_row_timestamps, key=lambda item: item[1])
        if latest_export is None or latest_export_row[1] > latest_export[1]:
            latest_export = latest_export_row

    latest_requested = None
    if requested_resolved:
        latest_requested = max(requested_resolved.items(), key=lambda item: item[1])

    command = (
        f"scripts/export_parity_precheck.sh --out-dir {os.path.normpath(export_dir)} "
        f"{' '.join(requested)} <fresh-runtime-export-path>"
    )
    result: dict[str, Any] = {
        "status": "ok",
        "reason_code": "fresh_export_dir",
        "requested_run_ids": requested,
        "requested_run_id_timestamps_utc": {run_id: ts.isoformat() for run_id, ts in requested_resolved.items()},
        "latest_requested_run_id": latest_requested[0] if latest_requested else "",
        "latest_requested_run_id_timestamp_utc": latest_requested[1].isoformat() if latest_requested else "",
        "max_export_run_id": latest_export[0] if latest_export else "",
        "max_export_run_id_timestamp_utc": latest_export[1].isoformat() if latest_export else "",
        "suggested_export_command": command,
    }

    if not latest_requested:
        result["status"] = "warning"
        result["reason_code"] = "requested_run_id_timestamp_unparseable"
        return result
    if not latest_export:
        result["status"] = "warning"
        result["reason_code"] = "export_run_id_timestamp_unparseable"
        return result
    if latest_requested[1] > latest_export[1]:
        result["status"] = "error"
        result["reason_code"] = "stale_export_dir"
    return result


def _count_csv_rows(path: Path) -> int:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return sum(1 for _ in csv.DictReader(handle))


def _count_json_rows(path: Path) -> int:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, list):
        return -1
    return len(payload)


def evaluate_raw_tab_completeness(export_dir: str) -> dict[str, Any]:
    export_path = Path(export_dir)
    per_tab: dict[str, dict[str, Any]] = {}
    missing_files: list[str] = []
    mismatched_tabs: list[dict[str, Any]] = []
    missing_tabs: list[str] = []

    for tab in CANONICAL_RUNTIME_TABS:
        csv_path = export_path / f"{tab}.csv"
        json_path = export_path / f"{tab}.json"
        csv_exists = csv_path.is_file()
        json_exists = json_path.is_file()

        if not csv_exists:
            missing_files.append(str(csv_path))
        if not json_exists:
            missing_files.append(str(json_path))

        csv_rows = _count_csv_rows(csv_path) if csv_exists else None
        json_rows = _count_json_rows(json_path) if json_exists else None

        status = "ok"
        if not csv_exists or not json_exists:
            status = "missing_file"
            missing_tabs.append(tab)
        elif csv_rows != json_rows:
            status = "row_count_mismatch"
            mismatch = {
                "tab": tab,
                "csv_path": str(csv_path),
                "json_path": str(json_path),
                "csv_rows": csv_rows,
                "json_rows": json_rows,
            }
            mismatched_tabs.append(mismatch)

        per_tab[tab] = {
            "csv_path": str(csv_path),
            "json_path": str(json_path),
            "csv_exists": csv_exists,
            "json_exists": json_exists,
            "csv_rows": csv_rows,
            "json_rows": json_rows,
            "status": status,
        }

    is_complete = not missing_tabs and not mismatched_tabs
    return {
        "status": "ok" if is_complete else "error",
        "is_complete": is_complete,
        "canonical_tab_count": len(CANONICAL_RUNTIME_TABS),
        "expected_file_count": len(CANONICAL_RUNTIME_TABS) * 2,
        "missing_tabs": sorted(set(missing_tabs)),
        "missing_files": sorted(set(missing_files)),
        "mismatched_tabs": mismatched_tabs,
        "per_tab": per_tab,
    }


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
    _, duplicate_diagnostics_by_run_id = merge_run_summary_rows_for_cardinality(run_rows)
    raw_tab_completeness = evaluate_raw_tab_completeness(export_dir)
    canonical_pair = _canonical_run_pair(run_a, run_b)
    export_freshness = evaluate_export_freshness(export_dir, canonical_pair)
    sidecar = {
        "schema": "wta_edge_board.preflight.v1",
        "recorded_at_utc": _utc_now_iso(),
        "export_dir": os.path.normpath(export_dir),
        "manifest_generated_at_utc": manifest_generated_at,
        "run_pair": canonical_pair,
        "allow_csv_only_triage": bool(allow_csv_only_triage),
        "incident_tag": str(incident_tag or "").strip(),
        "preflight_evidence": {
            "raw_tab_completeness": raw_tab_completeness,
            "export_freshness": export_freshness,
            "run_checklist_by_run_id": {
                run_id: _run_checklist(run_rows, run_id)
                for run_id in canonical_pair
            },
            "duplicate_summary_diagnostics_by_run_id": {
                run_id: duplicate_diagnostics_by_run_id.get(
                    run_id,
                    {
                        "raw_summary_rows": 0,
                        "unique_summary_rows": 0,
                        "duplicate_instances": 0,
                        "identical_duplicate_groups": 0,
                        "has_duplicate_summary_rows": False,
                        "has_non_identical_duplicate_summary_rows": False,
                        "compare_will_fail_due_to_duplicate_summary_rows": False,
                    },
                )
                for run_id in canonical_pair
            },
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

    preflight_evidence = sidecar.get("preflight_evidence")
    raw_tab_completeness = (
        preflight_evidence.get("raw_tab_completeness")
        if isinstance(preflight_evidence, dict)
        else None
    )
    if not isinstance(raw_tab_completeness, dict) or not bool(raw_tab_completeness.get("is_complete")):
        missing_tabs = []
        mismatched_tabs: list[str] = []
        if isinstance(raw_tab_completeness, dict):
            missing_tabs = [str(tab) for tab in raw_tab_completeness.get("missing_tabs") or []]
            mismatched_tabs = [
                str(item.get("tab"))
                for item in raw_tab_completeness.get("mismatched_tabs") or []
                if isinstance(item, dict) and item.get("tab")
            ]
        raise ValueError(
            "Preflight guard failed: raw runtime tab completeness check is not satisfied. "
            f"missing_tabs={missing_tabs}, mismatched_tabs={mismatched_tabs}. "
            "Re-run scripts/export_parity_precheck.sh to regenerate complete CSV/JSON tab exports."
        )

    run_checklist_by_run_id = (
        preflight_evidence.get("run_checklist_by_run_id")
        if isinstance(preflight_evidence, dict)
        else None
    )
    if not isinstance(run_checklist_by_run_id, dict):
        raise ValueError(
            "Preflight guard failed: sidecar missing run_checklist_by_run_id evidence. "
            "Re-run scripts/export_parity_precheck.sh for this export batch and run pair."
        )

    compare_ready_failures: list[str] = []
    for run_id in _canonical_run_pair(run_a, run_b):
        checklist = run_checklist_by_run_id.get(run_id)
        if not isinstance(checklist, dict) or not bool(checklist.get("compare_ready")):
            compare_ready_failures.append(run_id)
    if compare_ready_failures:
        raise ValueError(
            "Preflight guard failed: compare_ready evidence is missing/false for run ID(s): "
            f"{compare_ready_failures}. "
            "Block compare/evaluate commands until scripts/export_parity_precheck.sh passes with compare_ready=true for both runs."
        )

    duplicate_summary_diagnostics = (
        preflight_evidence.get("duplicate_summary_diagnostics_by_run_id")
        if isinstance(preflight_evidence, dict)
        else None
    )
    if isinstance(duplicate_summary_diagnostics, dict):
        duplicate_failures = [
            run_id
            for run_id in _canonical_run_pair(run_a, run_b)
            if bool(
                isinstance(duplicate_summary_diagnostics.get(run_id), dict)
                and duplicate_summary_diagnostics.get(run_id, {}).get(
                    "compare_will_fail_due_to_duplicate_summary_rows"
                )
            )
        ]
        if duplicate_failures:
            raise ValueError(
                "Preflight guard failed: duplicate summary diagnostics indicate compare failure for run ID(s): "
                f"{duplicate_failures}. "
                "Block compare/evaluate commands until replacement run IDs pass precheck."
            )

    return {"status": "ok", "sidecar_path": str(sidecar_path)}
