#!/usr/bin/env python3
"""Read latest daily edge-quality SLO report status with schema checks."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

INCLUDE_GLOB = "edge_quality_daily_slo_*.json"
EXCLUDE_PATTERNS = ("*cli_day*.json", "*summary*.json", "*helper*.json")
EXPECTED_KEYS = ("status", "windows")


def _should_skip(path: Path) -> bool:
    return any(path.match(pattern) for pattern in EXCLUDE_PATTERNS)


def _candidate_reports(reports_dir: Path) -> list[Path]:
    candidates: list[Path] = []
    for path in reports_dir.rglob(INCLUDE_GLOB):
        if _should_skip(path):
            continue
        candidates.append(path)
    return sorted(candidates, key=lambda item: item.stat().st_mtime, reverse=True)


def _format_schema_warning(path: Path, payload: dict[str, Any]) -> str:
    missing = [key for key in EXPECTED_KEYS if key not in payload]
    return f"{path}: schema warning missing keys={','.join(missing)}"


def read_status_lines(reports_dir: Path, limit: int = 1) -> list[str]:
    lines: list[str] = []
    for path in _candidate_reports(reports_dir)[: max(1, limit)]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            lines.append(f"{path}: schema warning invalid_json error={exc}")
            continue

        if not isinstance(payload, dict):
            lines.append(f"{path}: schema warning payload_not_object")
            continue

        missing = [key for key in EXPECTED_KEYS if key not in payload]
        if missing:
            lines.append(_format_schema_warning(path, payload))
            continue

        status = payload.get("status")
        windows = payload.get("windows")
        window_count = len(windows) if isinstance(windows, list) else "n/a"
        lines.append(f"{path}: status={status} windows={window_count}")

    if not lines:
        lines.append(f"{reports_dir}: no reports found matching {INCLUDE_GLOB}")
    return lines


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Read latest edge-quality daily SLO report status with schema validation.",
    )
    parser.add_argument(
        "--reports-dir",
        default="reports",
        help="Directory containing edge_quality_daily_slo_*.json artifacts (default: reports).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1,
        help="Number of latest matching reports to print (default: 1).",
    )
    args = parser.parse_args()

    for line in read_status_lines(Path(args.reports_dir), limit=args.limit):
        print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
