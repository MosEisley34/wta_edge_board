#!/usr/bin/env python3
"""Precheck target run IDs exist in exported Run_Log artifacts before triage comparisons."""

from __future__ import annotations

import argparse
import csv
import glob
import json
import os
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable


@dataclass(frozen=True)
class RunLogSource:
    path: str
    kind: str
    mtime: float


def _iter_run_log_sources(export_dir: str) -> list[RunLogSource]:
    """Find Run_Log.json / Run_Log.csv artifacts under the export directory."""
    patterns = (("json", "**/Run_Log.json"), ("csv", "**/Run_Log.csv"))
    sources: list[RunLogSource] = []
    seen: set[str] = set()

    for kind, pattern in patterns:
        for path in glob.glob(os.path.join(export_dir, pattern), recursive=True):
            norm_path = os.path.normpath(path)
            if norm_path in seen:
                continue
            seen.add(norm_path)
            try:
                mtime = os.path.getmtime(norm_path)
            except OSError:
                continue
            sources.append(RunLogSource(path=norm_path, kind=kind, mtime=mtime))

    return sorted(sources, key=lambda source: source.mtime, reverse=True)


def _scan_run_ids(path: str) -> Counter[str]:
    """Return run_id occurrence counts from a Run_Log artifact."""
    run_id_counts: Counter[str] = Counter()
    lower = path.lower()

    if lower.endswith(".json"):
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if isinstance(payload, list):
            for row in payload:
                if isinstance(row, dict):
                    run_id = str(row.get("run_id") or "").strip()
                    if run_id:
                        run_id_counts[run_id] += 1
        return run_id_counts

    with open(path, "r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            run_id = str(row.get("run_id") or "").strip()
            if run_id:
                run_id_counts[run_id] += 1
    return run_id_counts


def _format_missing(run_ids: Iterable[str], present_ids: set[str]) -> list[str]:
    return [run_id for run_id in run_ids if run_id not in present_ids]


def _format_mtime(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


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

    sources = _iter_run_log_sources(args.export_dir)
    if not sources:
        print(f"Precheck failed: no Run_Log artifacts found under {args.export_dir}.")
        print("Expected at least one file matching **/Run_Log.{json,csv}.")
        print("Stop triage and re-export from the sheet before further analysis.")
        return 2

    merged_counts: Counter[str] = Counter()
    source_counts: dict[str, Counter[str]] = {}
    kind_counts = Counter(source.kind for source in sources)

    print(f"Precheck source files under {args.export_dir} (newest first):")
    for source in sources:
        print(f"- {source.path} [{source.kind}, mtime={_format_mtime(source.mtime)}]")

    for source in sources:
        try:
            source_counter = _scan_run_ids(source.path)
        except Exception as exc:  # noqa: BLE001 - continue scanning other artifacts
            print(f"Warning: failed to parse {source.path}: {exc}")
            continue

        source_counts[source.path] = source_counter
        merged_counts.update(source_counter)

    targets = (args.run_id_a, args.run_id_b)
    present_ids = set(merged_counts)
    missing = _format_missing(targets, present_ids)

    print(
        "Precheck merged run IDs across "
        f"{len(sources)} source file(s): {kind_counts['json']} JSON, {kind_counts['csv']} CSV."
    )
    for run_id in targets:
        total_matches = merged_counts.get(run_id, 0)
        matched_files = sum(
            1 for counter in source_counts.values() if counter.get(run_id, 0) > 0
        )
        status = "FOUND" if total_matches > 0 else "MISSING"
        print(
            f"- {run_id}: {status} (matches={total_matches}, source_files_with_match={matched_files})"
        )

    if missing:
        print("Precheck failed: one or more target run IDs are missing from merged JSON/CSV sources.")
        print("Stop triage and re-export from the sheet before further analysis.")
        return 2

    print("Precheck passed: both target run IDs are present in merged Run_Log JSON/CSV data.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
