#!/usr/bin/env python3
"""Canonical sync for stageFetchPlayerStats fields across Run_Log CSV/JSON."""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

KEY_COUNTERS = ("STATS_ENR", "STATS_MISS_A", "STATS_MISS_B")
REQUIRED_COMPARE_STAGES = (
    "stageFetchOdds",
    "stageFetchSchedule",
    "stageMatchEvents",
    "stageFetchPlayerStats",
    "stageGenerateSignals",
    "stagePersist",
)


def _default_reason_metadata() -> dict[str, Any]:
    return {
        "coverage": {"requested": 0, "resolved": 0, "resolved_rate": 0, "unresolved": 0},
        "requested_player_count": 0,
        "resolved_player_count": 0,
        "unresolved_player_count": 0,
        "overlap_ratio": 0,
        "top_unresolved_player_samples": [],
        "players_total": 0,
        "players_found_ta": 0,
        "players_fallback_provider": 0,
        "players_fallback_model": 0,
        "players_unresolved": 0,
        "player_a_source": "none",
        "player_b_source": "none",
        "player_resolution_source_by_player": {},
        "reason_code_partitioning": {
            "upstream_payload_empty_or_changed_shape": [],
            "parser_contract_mismatch": [],
            "no_demand_cases": [],
        },
    }


def _default_stage_contract_payload(stage: str) -> dict[str, Any]:
    return {
        "stage": stage,
        "summary": {},
        "reason_metadata": _default_reason_metadata(),
        "player_stats_counters": {
            "summary_reason_codes": {},
            "stage_reason_codes": {},
        },
    }


def _parse_json_like(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        return None


def _json_string(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _load_csv(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = [dict(row) for row in reader]
        return rows, list(reader.fieldnames or [])


def _write_csv(path: Path, rows: list[dict[str, str]], headers: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def _extract_stage_entry(stage_row: dict[str, str], stage: str) -> dict[str, Any]:
    payload: dict[str, Any] = _default_stage_contract_payload(stage)
    message = _parse_json_like(stage_row.get("message"))
    if isinstance(message, dict):
        if isinstance(message.get("summary"), (dict, list, str, int, float, bool)):
            payload["summary"] = message.get("summary")
        if isinstance(message.get("reason_metadata"), dict):
            payload["reason_metadata"] = message.get("reason_metadata")

    summary = _parse_json_like(stage_row.get("summary"))
    if summary is not None:
        payload["summary"] = summary

    reason_metadata = _parse_json_like(stage_row.get("reason_metadata"))
    if isinstance(reason_metadata, dict):
        payload["reason_metadata"] = reason_metadata

    reason_codes = _parse_json_like(stage_row.get("reason_codes"))
    if isinstance(reason_codes, dict):
        counters = {}
        for key in KEY_COUNTERS:
            if key in reason_codes:
                counters[key] = int(float(reason_codes.get(key) or 0))
        payload["player_stats_counters"]["stage_reason_codes"] = counters

    return _normalize_stage_contract_payload(payload, stage)


def _normalize_stage_contract_payload(payload: dict[str, Any], stage: str) -> dict[str, Any]:
    normalized = _default_stage_contract_payload(stage)

    summary = payload.get("summary")
    if isinstance(summary, dict):
        normalized["summary"] = dict(summary)

    reason_metadata = payload.get("reason_metadata")
    if isinstance(reason_metadata, dict):
        merged_reason_metadata = _default_reason_metadata()
        merged_reason_metadata.update(reason_metadata)
        merged_coverage = _parse_json_like(reason_metadata.get("coverage"))
        if not isinstance(merged_coverage, dict):
            merged_coverage = {}
        coverage = dict(_default_reason_metadata()["coverage"])
        coverage.update(merged_coverage)
        merged_reason_metadata["coverage"] = coverage
        merged_partitions = _parse_json_like(reason_metadata.get("reason_code_partitioning"))
        if not isinstance(merged_partitions, dict):
            merged_partitions = {}
        partitioning = dict(_default_reason_metadata()["reason_code_partitioning"])
        for key, default_values in partitioning.items():
            candidate_values = merged_partitions.get(key)
            if isinstance(candidate_values, list):
                partitioning[key] = list(candidate_values)
            elif isinstance(candidate_values, str) and candidate_values.strip():
                partitioning[key] = [candidate_values.strip()]
            else:
                partitioning[key] = list(default_values)
        merged_reason_metadata["reason_code_partitioning"] = partitioning
        normalized["reason_metadata"] = merged_reason_metadata

    counters = payload.get("player_stats_counters")
    if isinstance(counters, dict):
        stage_reason_codes = counters.get("stage_reason_codes")
        summary_reason_codes = counters.get("summary_reason_codes")
        normalized["player_stats_counters"] = {
            "stage_reason_codes": dict(stage_reason_codes) if isinstance(stage_reason_codes, dict) else {},
            "summary_reason_codes": dict(summary_reason_codes) if isinstance(summary_reason_codes, dict) else {},
        }

    return normalized


def _stage_summaries_from_summary_row(summary_row: dict[str, str]) -> tuple[Any, list[dict[str, Any]]]:
    parsed = _parse_json_like(summary_row.get("stage_summaries"))
    if isinstance(parsed, list):
        return parsed, [item for item in parsed if isinstance(item, dict)]
    if isinstance(parsed, dict):
        nested = parsed.get("stage_summaries")
        if isinstance(nested, list):
            return parsed, [item for item in nested if isinstance(item, dict)]
    return None, []


def _stage_snapshot(rows: list[dict[str, Any]], run_id: str, stage: str) -> dict[str, Any]:
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
    if not summary_row:
        return _default_stage_contract_payload(stage)

    reason_codes = _parse_json_like(summary_row.get("reason_codes"))
    summary_counters = {}
    if isinstance(reason_codes, dict):
        for key in KEY_COUNTERS:
            if key in reason_codes:
                summary_counters[key] = int(float(reason_codes.get(key) or 0))

    _, stage_entries = _stage_summaries_from_summary_row(summary_row)
    stage_entry = next((entry for entry in stage_entries if str(entry.get("stage") or "") == stage), {})
    entry_counters = _parse_json_like(stage_entry.get("reason_codes"))
    entry_reason_metadata = _parse_json_like(stage_entry.get("reason_metadata"))
    entry_summary = _parse_json_like(stage_entry.get("summary"))

    return _normalize_stage_contract_payload(
        {
            "stage": stage,
            "summary": entry_summary if isinstance(entry_summary, dict) else {},
            "reason_metadata": entry_reason_metadata if isinstance(entry_reason_metadata, dict) else {},
            "player_stats_counters": {
                "summary_reason_codes": summary_counters,
                "stage_reason_codes": entry_counters if isinstance(entry_counters, dict) else {},
            },
        },
        stage,
    )


def _find_summary_row(rows: list[dict[str, Any]], run_id: str) -> dict[str, Any] | None:
    return next(
        (
            row
            for row in reversed(rows)
            if str(row.get("run_id") or "") == run_id
            and str(row.get("row_type") or "") == "summary"
            and str(row.get("stage") or "") == "runEdgeBoard"
        ),
        None,
    )


def _stage_set_for_run(rows: list[dict[str, Any]], run_id: str) -> set[str]:
    stages: set[str] = set()
    for row in rows:
        if str(row.get("run_id") or "") != run_id:
            continue
        stage_name = str(row.get("stage") or "").strip()
        if stage_name and stage_name != "runEdgeBoard":
            stages.add(stage_name)
    summary_row = _find_summary_row(rows, run_id)
    if not summary_row:
        return stages
    _, stage_entries = _stage_summaries_from_summary_row(summary_row)
    for entry in stage_entries:
        stage_name = str(entry.get("stage") or "").strip()
        if stage_name:
            stages.add(stage_name)
    return stages


def _summary_shape_valid(summary_row: dict[str, Any] | None) -> bool:
    if not summary_row:
        return False
    stage_summaries = _parse_json_like(summary_row.get("stage_summaries"))
    if isinstance(stage_summaries, list):
        return True
    if isinstance(stage_summaries, dict):
        return isinstance(stage_summaries.get("stage_summaries"), list)
    return False


def _has_stage_summary_entry(summary_row: dict[str, Any] | None, stage: str) -> bool:
    if not summary_row:
        return False
    _, stage_entries = _stage_summaries_from_summary_row(summary_row)
    return any(str(entry.get("stage") or "") == stage for entry in stage_entries)


def _run_checklist(rows: list[dict[str, Any]], run_id: str, stage: str) -> dict[str, bool]:
    summary_row = _find_summary_row(rows, run_id)
    stage_set = _stage_set_for_run(rows, run_id)
    checklist: dict[str, bool] = {
        "has_runEdgeBoard_summary": bool(summary_row),
        "runEdgeBoard_summary_shape_valid": _summary_shape_valid(summary_row),
        f"has_{stage}_summary": _has_stage_summary_entry(summary_row, stage),
    }
    for required_stage in REQUIRED_COMPARE_STAGES:
        checklist[f"has_{required_stage}"] = required_stage in stage_set
    checklist["has_required_compare_stages"] = all(
        checklist[f"has_{required_stage}"] for required_stage in REQUIRED_COMPARE_STAGES
    )
    checklist["compare_critical_summary_ready"] = all(
        (
            checklist["has_runEdgeBoard_summary"],
            checklist["runEdgeBoard_summary_shape_valid"],
            checklist[f"has_{stage}_summary"],
        )
    )
    checklist["compare_ready"] = all(
        (
            checklist["compare_critical_summary_ready"],
            checklist["has_required_compare_stages"],
        )
    )
    return checklist


def _apply_sync(rows: list[dict[str, str]], headers: list[str], run_ids: list[str], stage: str) -> int:
    updates = 0
    for run_id in run_ids:
        stage_row_idx = next(
            (idx for idx in range(len(rows) - 1, -1, -1) if rows[idx].get("run_id") == run_id and rows[idx].get("stage") == stage),
            None,
        )
        summary_row_idx = next(
            (
                idx
                for idx in range(len(rows) - 1, -1, -1)
                if rows[idx].get("run_id") == run_id
                and rows[idx].get("row_type") == "summary"
                and rows[idx].get("stage") == "runEdgeBoard"
            ),
            None,
        )
        if stage_row_idx is None or summary_row_idx is None:
            continue

        stage_row = rows[stage_row_idx]
        summary_row = rows[summary_row_idx]
        try:
            canonical_entry = _extract_stage_entry(stage_row, stage)
        except Exception:
            canonical_entry = _default_stage_contract_payload(stage)

        if "summary" in headers:
            stage_row["summary"] = _json_string(canonical_entry["summary"])
        if "reason_metadata" in headers:
            stage_row["reason_metadata"] = _json_string(canonical_entry["reason_metadata"])
        if "reason_codes" in headers:
            stage_row["reason_codes"] = _json_string(canonical_entry["player_stats_counters"]["stage_reason_codes"])

        envelope, stage_entries = _stage_summaries_from_summary_row(summary_row)
        replaced = False
        for idx, entry in enumerate(stage_entries):
            if str(entry.get("stage") or "") != stage:
                continue
            stage_entries[idx] = {
                **entry,
                "stage": canonical_entry["stage"],
                "summary": canonical_entry["summary"],
                "reason_metadata": canonical_entry["reason_metadata"],
                "reason_codes": canonical_entry["player_stats_counters"]["stage_reason_codes"],
            }
            replaced = True
            break
        if not replaced:
            stage_entries.append(
                {
                    "stage": canonical_entry["stage"],
                    "summary": canonical_entry["summary"],
                    "reason_metadata": canonical_entry["reason_metadata"],
                    "reason_codes": canonical_entry["player_stats_counters"]["stage_reason_codes"],
                }
            )

        if isinstance(envelope, list):
            summary_row["stage_summaries"] = _json_string(stage_entries)
        elif isinstance(envelope, dict):
            merged = dict(envelope)
            merged["stage_summaries"] = stage_entries
            summary_row["stage_summaries"] = _json_string(merged)

        reason_codes = _parse_json_like(summary_row.get("reason_codes"))
        if not isinstance(reason_codes, dict):
            reason_codes = {}
        stage_counters = canonical_entry["player_stats_counters"]["stage_reason_codes"]
        for key in KEY_COUNTERS:
            if key in stage_counters:
                reason_codes[key] = int(float(stage_counters[key]))
        if "reason_codes" in headers:
            summary_row["reason_codes"] = _json_string(reason_codes)
        updates += 1
    return updates


def _run_mirror(export_dir: Path) -> None:
    subprocess.run(
        ["python3", "scripts/mirror_runtime_csv_to_json.py", "--input-dir", str(export_dir), "--out-dir", str(export_dir)],
        check=True,
    )


def _print_validation(export_dir: Path, run_ids: list[str], stage: str) -> bool:
    csv_rows, _ = _load_csv(export_dir / "Run_Log.csv")
    json_rows = json.loads((export_dir / "Run_Log.json").read_text(encoding="utf-8"))
    if not isinstance(json_rows, list):
        raise RuntimeError("Run_Log.json must contain a list of rows.")

    mismatch = False
    contract_failure = False
    json_dict_rows = [r for r in json_rows if isinstance(r, dict)]
    print(f"CSV-vs-JSON validation for stage={stage}")
    for run_id in run_ids:
        csv_checklist = _run_checklist(csv_rows, run_id, stage)
        json_checklist = _run_checklist(json_dict_rows, run_id, stage)
        csv_snapshot = _stage_snapshot(csv_rows, run_id, stage)
        json_snapshot = _stage_snapshot(json_dict_rows, run_id, stage)
        print(f"- run_id={run_id}")
        compact_checklist = {
            "csv": csv_checklist,
            "json": json_checklist,
        }
        print(f"  checklist: {json.dumps(compact_checklist, sort_keys=True)}")
        print(f"  csv:  {json.dumps(csv_snapshot, sort_keys=True)}")
        print(f"  json: {json.dumps(json_snapshot, sort_keys=True)}")
        if csv_snapshot != json_snapshot:
            mismatch = True
        if (not csv_checklist["compare_critical_summary_ready"]) or (not json_checklist["compare_critical_summary_ready"]):
            contract_failure = True
    if contract_failure:
        print(
            "Error: compare-critical contract rows missing or malformed "
            "(runEdgeBoard summary + required stages + stageFetchPlayerStats summary).",
            file=sys.stderr,
        )
    return (not mismatch) and (not contract_failure)


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync stageFetchPlayerStats contract from CSV into JSON mirror.")
    parser.add_argument("--export-dir", required=True)
    parser.add_argument("--run-id", action="append", dest="run_ids", required=True)
    parser.add_argument("--stage", default="stageFetchPlayerStats")
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()

    export_dir = Path(args.export_dir)
    csv_path = export_dir / "Run_Log.csv"
    json_path = export_dir / "Run_Log.json"
    if not csv_path.is_file() or not json_path.is_file():
        raise SystemExit("Error: expected Run_Log.csv and Run_Log.json in export-dir.")

    if not args.validate_only:
        rows, headers = _load_csv(csv_path)
        updated = _apply_sync(rows, headers, args.run_ids, args.stage)
        _write_csv(csv_path, rows, headers)
        _run_mirror(export_dir)
        print(f"Synced stage contract rows in CSV: {updated}")

    ok = _print_validation(export_dir, args.run_ids, args.stage)
    if not ok:
        print("Error: CSV/JSON sync mismatch detected on key stage fields.", file=sys.stderr)
        return 1
    print("Stage contract sync validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
