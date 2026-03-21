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


@dataclass(frozen=True)
class BatchView:
    source: str
    latest_run_ids: set[str]
    max_timestamp_iso: str
    window_start_iso: str
    window_end_iso: str
    summary_presence: dict[str, bool]


class ParityError(RuntimeError):
    pass


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
    for _, row in batch_rows:
        run_id = str(row.get("run_id") or "").strip()
        stage = str(row.get("stage") or "").strip()
        row_type = str(row.get("row_type") or "").strip()
        if row_type == "summary" and stage == "runEdgeBoard":
            summary_presence[run_id] = True

    return BatchView(
        source=source,
        latest_run_ids=latest_run_ids,
        max_timestamp_iso=max_ts.isoformat(),
        window_start_iso=start_ts.isoformat(),
        window_end_iso=end_ts.isoformat(),
        summary_presence=summary_presence,
    )


def verify_run_log_parity(export_dir: str) -> None:
    json_files, csv_files = _iter_run_log_files(export_dir)

    if not json_files or not csv_files:
        raise ParityError(
            "Run log parity check failed: expected both Run_Log JSON and CSV artifacts in the export batch. "
            f"Found json={len(json_files)} csv={len(csv_files)} under {export_dir}. "
            "Remediation: re-export both live_runtime/Run_Log.json and live_runtime/Run_Log.csv, then rerun export."
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

    if errors:
        joined = "\n - ".join(errors)
        raise ParityError(
            "Run log parity check failed for export batch; aborting publish to avoid partial artifacts.\n"
            f" - {joined}\n"
            "Remediation: regenerate Run_Log.csv and Run_Log.json from the same run window and rerun export."
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
        verify_run_log_parity(args.export_dir)
    except ParityError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(f"Run log parity check passed for {args.export_dir}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
