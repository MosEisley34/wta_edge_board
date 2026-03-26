#!/usr/bin/env python3
"""Verify CSV/JSON Run_Log parity for the latest export batch."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import glob
import json
import os
import sys
from dataclasses import dataclass
from typing import Any


TIMESTAMP_KEYS = ("ts", "timestamp", "started_at", "time", "created_at")
REQUIRED_STAGE_SUMMARIES = ("stageFetchPlayerStats",)
GS_PARITY_METADATA_KEYS = (
    "gs_export_parity_contract",
    "export_parity_contract",
    "parity_contract",
    "gs_native_parity",
)


@dataclass(frozen=True)
class BatchView:
    source: str
    latest_run_ids: set[str]
    max_timestamp_iso: str
    window_start_iso: str
    window_end_iso: str
    summary_presence: dict[str, bool]
    required_stage_presence: dict[str, dict[str, bool]]
    gs_parity_metadata_by_run_id: dict[str, dict[str, Any]]


@dataclass(frozen=True)
class ParityResult:
    export_dir: str
    latest_run_ids: list[str]
    max_timestamp_iso: str
    window_start_iso: str
    window_end_iso: str
    json_files: list[str]
    csv_files: list[str]


class ParityError(RuntimeError):
    pass


REMEDIATION_HINT = "Remediation: re-export source batch."


def _parse_timestamp(raw: Any) -> dt.datetime | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def _row_timestamp(row: dict[str, Any]) -> dt.datetime | None:
    for key in TIMESTAMP_KEYS:
        parsed = _parse_timestamp(row.get(key))
        if parsed is not None:
            return parsed
    return None


def _iter_run_log_files(export_dir: str) -> tuple[list[str], list[str]]:
    json_files = sorted(glob.glob(os.path.join(export_dir, "**", "*Run_Log*.json"), recursive=True))
    csv_files = sorted(glob.glob(os.path.join(export_dir, "**", "*Run_Log*.csv"), recursive=True))
    return json_files, csv_files


def _load_json_rows(path: str) -> list[dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if isinstance(payload, dict):
        rows = payload.get("rows")
        if isinstance(rows, list):
            return [r for r in rows if isinstance(r, dict)]
    return []


def _load_csv_rows(path: str) -> list[dict[str, Any]]:
    with open(path, "r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _parse_json_like(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        return None


def _extract_stage_summaries(row: dict[str, Any]) -> list[dict[str, Any]]:
    payload = _parse_json_like(row.get("stage_summaries"))
    if isinstance(payload, list):
        return [entry for entry in payload if isinstance(entry, dict)]
    if isinstance(payload, dict):
        nested = payload.get("stage_summaries")
        if isinstance(nested, list):
            return [entry for entry in nested if isinstance(entry, dict)]
    message_payload = _parse_json_like(row.get("message"))
    if isinstance(message_payload, dict):
        nested = message_payload.get("stage_summaries")
        if isinstance(nested, list):
            return [entry for entry in nested if isinstance(entry, dict)]
    return []


def _extract_gs_native_parity_metadata(row: dict[str, Any]) -> dict[str, Any] | None:
    stage_payload = _parse_json_like(row.get("stage_summaries"))
    if isinstance(stage_payload, dict):
        for key in GS_PARITY_METADATA_KEYS:
            candidate = stage_payload.get(key)
            if isinstance(candidate, dict):
                return candidate
    message_payload = _parse_json_like(row.get("message"))
    if isinstance(message_payload, dict):
        for key in GS_PARITY_METADATA_KEYS:
            candidate = message_payload.get(key)
            if isinstance(candidate, dict):
                return candidate
    return None


def _metadata_claims_pass(metadata: dict[str, Any]) -> bool:
    parity_status = str(metadata.get("parity_status") or "").strip().lower()
    reason_code = str(metadata.get("reason_code") or "").strip().lower()
    if parity_status == "pass":
        return True
    if metadata.get("pass") is True:
        return True
    return reason_code in {"export_parity_contract_pass", "export_parity_contract_pass_precheck"}


def _metadata_latest_run_ids(metadata: dict[str, Any]) -> set[str]:
    values = metadata.get("latest_run_ids")
    if not isinstance(values, list):
        return set()
    return {str(v).strip() for v in values if str(v).strip()}


def _build_batch_view(source: str, rows: list[dict[str, Any]]) -> BatchView:
    timestamped_rows: list[tuple[dt.datetime, dict[str, Any]]] = []
    for row in rows:
        run_id = str(row.get("run_id") or "").strip()
        if not run_id:
            continue
        ts = _row_timestamp(row)
        if ts is None:
            continue
        timestamped_rows.append((ts, row))

    if not timestamped_rows:
        raise ParityError(
            f"Run log parity check failed: {source} has no rows with both run_id and timestamp. "
            f"Ensure Run_Log.{source} includes one of timestamp keys: {', '.join(TIMESTAMP_KEYS)}."
        )

    max_ts = max(ts for ts, _ in timestamped_rows)
    latest_run_ids = {
        str(row.get("run_id") or "").strip()
        for ts, row in timestamped_rows
        if ts == max_ts and str(row.get("run_id") or "").strip()
    }

    if not latest_run_ids:
        raise ParityError(f"Run log parity check failed: could not determine latest run_id set from {source}.")

    batch_rows = [(ts, row) for ts, row in timestamped_rows if str(row.get("run_id") or "").strip() in latest_run_ids]
    start_ts = min(ts for ts, _ in batch_rows)
    end_ts = max(ts for ts, _ in batch_rows)

    summary_presence = {run_id: False for run_id in sorted(latest_run_ids)}
    required_stage_presence = {
        run_id: {stage: False for stage in REQUIRED_STAGE_SUMMARIES}
        for run_id in sorted(latest_run_ids)
    }
    gs_parity_metadata_by_run_id: dict[str, dict[str, Any]] = {}

    for _, row in batch_rows:
        run_id = str(row.get("run_id") or "").strip()
        stage = str(row.get("stage") or "").strip()
        row_type = str(row.get("row_type") or "").strip()
        if row_type == "summary" and stage == "runEdgeBoard":
            summary_presence[run_id] = True
            for stage_summary in _extract_stage_summaries(row):
                stage_name = str(stage_summary.get("stage") or "").strip()
                if stage_name in required_stage_presence[run_id]:
                    required_stage_presence[run_id][stage_name] = True
            metadata = _extract_gs_native_parity_metadata(row)
            if isinstance(metadata, dict):
                gs_parity_metadata_by_run_id[run_id] = metadata

    return BatchView(
        source=source,
        latest_run_ids=latest_run_ids,
        max_timestamp_iso=max_ts.isoformat(),
        window_start_iso=start_ts.isoformat(),
        window_end_iso=end_ts.isoformat(),
        summary_presence=summary_presence,
        required_stage_presence=required_stage_presence,
        gs_parity_metadata_by_run_id=gs_parity_metadata_by_run_id,
    )


def _validate_metadata_contract(batch: BatchView, errors: list[str]) -> None:
    for run_id, metadata in batch.gs_parity_metadata_by_run_id.items():
        if not isinstance(metadata.get("summary_presence_by_run_id"), dict):
            errors.append(
                f"{batch.source} GS parity metadata missing summary_presence_by_run_id for run_id={run_id}"
            )
            continue
        if not isinstance(metadata.get("required_stage_summary_presence_by_run_id"), dict):
            errors.append(
                f"{batch.source} GS parity metadata missing required_stage_summary_presence_by_run_id for run_id={run_id}"
            )
            continue

        if not _metadata_claims_pass(metadata):
            continue

        metadata_run_ids = _metadata_latest_run_ids(metadata)
        if metadata_run_ids and run_id not in metadata_run_ids:
            errors.append(
                f"{batch.source} GS parity metadata claims pass but excludes run_id={run_id} in latest_run_ids={sorted(metadata_run_ids)}"
            )

        summary_map = metadata.get("summary_presence_by_run_id") or {}
        stage_map_root = metadata.get("required_stage_summary_presence_by_run_id") or {}
        if bool(summary_map.get(run_id)) is not True:
            errors.append(
                f"{batch.source} GS parity metadata claims pass but summary_presence_by_run_id[{run_id}] is not true"
            )
        if bool(batch.summary_presence.get(run_id)) is not True:
            errors.append(
                f"{batch.source} GS parity metadata claims pass but run log summary row is missing for run_id={run_id}"
            )

        metadata_stage_map = stage_map_root.get(run_id)
        if not isinstance(metadata_stage_map, dict):
            errors.append(
                f"{batch.source} GS parity metadata claims pass but stage presence map missing for run_id={run_id}"
            )
            continue

        for stage_name in REQUIRED_STAGE_SUMMARIES:
            if bool(metadata_stage_map.get(stage_name)) is not True:
                errors.append(
                    f"{batch.source} GS parity metadata claims pass but required stage {stage_name} is false for run_id={run_id}"
                )
            if bool((batch.required_stage_presence.get(run_id) or {}).get(stage_name)) is not True:
                errors.append(
                    f"{batch.source} GS parity metadata claims pass but file rows missing required stage {stage_name} for run_id={run_id}"
                )


def _write_latest_batch_sidecar(export_dir: str, result: ParityResult) -> str:
    sidecar_path = os.path.join(export_dir, "run_log_latest_batch_note.json")
    payload = {
        "recorded_at_utc": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
        "export_dir": result.export_dir,
        "latest_batch_run_ids": result.latest_run_ids,
        "latest_batch_max_timestamp_utc": result.max_timestamp_iso,
        "latest_batch_window_start_utc": result.window_start_iso,
        "latest_batch_window_end_utc": result.window_end_iso,
        "verified_sources": {
            "run_log_json_files": result.json_files,
            "run_log_csv_files": result.csv_files,
        },
        "operator_check": (
            "Confirm both Run_Log.json and Run_Log.csv include every run_id listed in latest_batch_run_ids."
        ),
    }
    with open(sidecar_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")
    return sidecar_path


def verify_run_log_parity(export_dir: str) -> ParityResult:
    json_files, csv_files = _iter_run_log_files(export_dir)

    if not json_files or not csv_files:
        raise ParityError(
            "Run log parity check failed: expected both Run_Log JSON and CSV artifacts in the export batch. "
            f"Found json={len(json_files)} csv={len(csv_files)} under {export_dir}."
        )

    json_rows: list[dict[str, Any]] = []
    for path in json_files:
        json_rows.extend(_load_json_rows(path))

    csv_rows: list[dict[str, Any]] = []
    for path in csv_files:
        csv_rows.extend(_load_csv_rows(path))

    json_batch = _build_batch_view("json", json_rows)
    csv_batch = _build_batch_view("csv", csv_rows)

    errors: list[str] = []

    if json_batch.window_start_iso != csv_batch.window_start_iso or json_batch.window_end_iso != csv_batch.window_end_iso:
        errors.append(
            "Latest batch timestamp window mismatch: "
            f"json=[{json_batch.window_start_iso} .. {json_batch.window_end_iso}] "
            f"csv=[{csv_batch.window_start_iso} .. {csv_batch.window_end_iso}]"
        )

    if json_batch.max_timestamp_iso != csv_batch.max_timestamp_iso:
        errors.append(
            "Latest max timestamp mismatch: "
            f"json={json_batch.max_timestamp_iso} csv={csv_batch.max_timestamp_iso}"
        )

    if json_batch.latest_run_ids != csv_batch.latest_run_ids:
        errors.append(
            "Latest run_id set mismatch: "
            f"json={sorted(json_batch.latest_run_ids)} csv={sorted(csv_batch.latest_run_ids)}"
        )

    if json_batch.summary_presence != csv_batch.summary_presence:
        errors.append(
            "runEdgeBoard summary presence mismatch for latest batch run_id(s): "
            f"json={json_batch.summary_presence} csv={csv_batch.summary_presence}"
        )
    if json_batch.required_stage_presence != csv_batch.required_stage_presence:
        errors.append(
            "stage summary availability mismatch for latest batch run_id(s): "
            f"json={json_batch.required_stage_presence} csv={csv_batch.required_stage_presence}"
        )

    missing_stage_summaries_json = {
        run_id: stage_map
        for run_id, stage_map in json_batch.required_stage_presence.items()
        if not all(stage_map.values())
    }
    if missing_stage_summaries_json:
        errors.append(
            "missing required stage summaries in Run_Log.json latest batch: "
            f"{missing_stage_summaries_json}"
        )
    missing_stage_summaries_csv = {
        run_id: stage_map
        for run_id, stage_map in csv_batch.required_stage_presence.items()
        if not all(stage_map.values())
    }
    if missing_stage_summaries_csv:
        errors.append(
            "missing required stage summaries in Run_Log.csv latest batch: "
            f"{missing_stage_summaries_csv}"
        )

    metadata_run_ids_json = set(json_batch.gs_parity_metadata_by_run_id.keys())
    metadata_run_ids_csv = set(csv_batch.gs_parity_metadata_by_run_id.keys())
    missing_metadata_json = sorted(json_batch.latest_run_ids - metadata_run_ids_json)
    missing_metadata_csv = sorted(csv_batch.latest_run_ids - metadata_run_ids_csv)
    if missing_metadata_json:
        errors.append(
            "GS parity metadata missing for latest batch run_id(s) in Run_Log.json: "
            f"{missing_metadata_json}"
        )
    if missing_metadata_csv:
        errors.append(
            "GS parity metadata missing for latest batch run_id(s) in Run_Log.csv: "
            f"{missing_metadata_csv}"
        )
    if metadata_run_ids_json != metadata_run_ids_csv:
        errors.append(
            "GS parity metadata presence mismatch for latest batch run_id(s): "
            f"json={sorted(metadata_run_ids_json)} csv={sorted(metadata_run_ids_csv)}"
        )

    _validate_metadata_contract(json_batch, errors)
    _validate_metadata_contract(csv_batch, errors)

    if errors:
        joined = "\n - ".join(errors)
        raise ParityError(
            "Run log parity check failed for export batch; aborting publish to avoid partial artifacts.\n"
            f" - {joined}"
        )
    return ParityResult(
        export_dir=os.path.abspath(export_dir),
        latest_run_ids=sorted(json_batch.latest_run_ids),
        max_timestamp_iso=json_batch.max_timestamp_iso,
        window_start_iso=json_batch.window_start_iso,
        window_end_iso=json_batch.window_end_iso,
        json_files=[os.path.abspath(path) for path in json_files],
        csv_files=[os.path.abspath(path) for path in csv_files],
    )


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Verify Run_Log.csv and Run_Log.json parity for the latest export batch before publishing artifacts."
        )
    )
    parser.add_argument("--export-dir", required=True, help="Directory containing exported Run_Log artifacts.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    try:
        result = verify_run_log_parity(args.export_dir)
        sidecar_path = _write_latest_batch_sidecar(args.export_dir, result)
    except ParityError as exc:
        print(str(exc), file=sys.stderr)
        print(REMEDIATION_HINT, file=sys.stderr)
        return 1
    print(f"Run log parity check passed for {args.export_dir}.")
    print(f"Latest batch sidecar note: {sidecar_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
