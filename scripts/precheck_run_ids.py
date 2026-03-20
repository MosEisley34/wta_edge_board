#!/usr/bin/env python3
"""Precheck target run IDs exist in exported Run_Log artifacts before triage comparisons."""

from __future__ import annotations

import argparse
import csv
import glob
import json
import os
from typing import Iterable, Set


def _iter_run_log_files(export_dir: str) -> list[str]:
    patterns = ("*Run_Log*.json", "*Run_Log*.csv")
    files: list[str] = []
    for pattern in patterns:
        files.extend(glob.glob(os.path.join(export_dir, "**", pattern), recursive=True))
    # Stable de-dup preserving sorted order.
    return sorted(dict.fromkeys(files))


def _load_run_ids(path: str) -> Set[str]:
    run_ids: Set[str] = set()
    lower = path.lower()
    if lower.endswith(".json"):
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if isinstance(payload, list):
            for row in payload:
                if isinstance(row, dict):
                    run_id = str(row.get("run_id") or "").strip()
                    if run_id:
                        run_ids.add(run_id)
        return run_ids

    with open(path, "r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            run_id = str(row.get("run_id") or "").strip()
            if run_id:
                run_ids.add(run_id)
    return run_ids


def _format_missing(run_ids: Iterable[str], present_ids: Set[str]) -> list[str]:
    return [run_id for run_id in run_ids if run_id not in present_ids]


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Verify that both target run IDs exist in exported Run_Log artifacts before triage/comparison."
        )
    )
    parser.add_argument("run_id_a", help="First target run ID.")
    parser.add_argument("run_id_b", help="Second target run ID.")
    parser.add_argument(
        "--export-dir",
        default="./exports_live",
        help="Directory containing exported runtime artifacts (default: ./exports_live).",
    )
    args = parser.parse_args()

    files = _iter_run_log_files(args.export_dir)
    if not files:
        print(f"Precheck failed: no Run_Log artifacts found under {args.export_dir}.")
        print("Expected at least one file matching ./exports_live/*Run_Log*.{json,csv}.")
        print("Stop triage and re-export from the sheet before further analysis.")
        return 2

    present_ids: Set[str] = set()
    for path in files:
        try:
            present_ids.update(_load_run_ids(path))
        except Exception as exc:  # noqa: BLE001 - continue scanning other artifacts
            print(f"Warning: failed to parse {path}: {exc}")

    targets = (args.run_id_a, args.run_id_b)
    missing = _format_missing(targets, present_ids)

    print(f"Precheck scanned {len(files)} Run_Log artifact(s) under {args.export_dir}.")
    for run_id in targets:
        status = "FOUND" if run_id in present_ids else "MISSING"
        print(f"- {run_id}: {status}")

    if missing:
        print("Precheck failed: one or more target run IDs are missing.")
        print("Stop triage and re-export from the sheet before further analysis.")
        return 2

    print("Precheck passed: both target run IDs are present. Safe to proceed to comparison scripts.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
