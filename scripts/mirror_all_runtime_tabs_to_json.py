#!/usr/bin/env python3
"""Mirror canonical runtime CSV tabs into JSON with parity enforcement."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any

CANONICAL_TABS = (
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

SUMMARY_FILE = "runtime_tab_json_mirror_summary.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Mirror the 9 canonical runtime CSV tabs into matching JSON files and "
            "emit a machine-readable parity summary."
        )
    )
    parser.add_argument("--export-dir", required=True, help="Directory containing canonical runtime CSV tabs.")
    parser.add_argument(
        "--summary-path",
        default="",
        help=f"Optional summary output path. Defaults to <export-dir>/{SUMMARY_FILE}",
    )
    return parser.parse_args()


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_json(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        json.dump(rows, handle, ensure_ascii=False, indent=2)
        handle.write("\n")


def run(export_dir: Path, summary_path: Path) -> int:
    summary: dict[str, Any] = {
        "schema": "wta_edge_board.runtime_tab_json_mirror.v1",
        "export_dir": str(export_dir),
        "canonical_tabs": list(CANONICAL_TABS),
        "tab_counts": {},
        "missing_files": [],
        "mismatches": [],
    }

    if not export_dir.is_dir():
        summary["error"] = f"export_dir_not_found: {export_dir}"
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 1

    for tab in CANONICAL_TABS:
        csv_path = export_dir / f"{tab}.csv"
        json_path = export_dir / f"{tab}.json"
        if not csv_path.is_file():
            summary["missing_files"].append(str(csv_path))
            summary["tab_counts"][tab] = {
                "csv_rows": 0,
                "json_rows": 0,
                "json_path": str(json_path),
                "status": "missing_csv",
            }
            continue

        csv_rows = _read_csv_rows(csv_path)
        if not json_path.is_file():
            _write_json(json_path, csv_rows)

        json_rows = json.loads(json_path.read_text(encoding="utf-8"))

        csv_count = len(csv_rows)
        json_count = len(json_rows) if isinstance(json_rows, list) else -1
        status = "ok"
        if csv_count != json_count:
            status = "row_count_mismatch"
            summary["mismatches"].append(
                {
                    "tab": tab,
                    "csv_path": str(csv_path),
                    "json_path": str(json_path),
                    "csv_rows": csv_count,
                    "json_rows": json_count,
                }
            )

        summary["tab_counts"][tab] = {
            "csv_rows": csv_count,
            "json_rows": json_count,
            "json_path": str(json_path),
            "status": status,
        }

    if summary["mismatches"]:
        summary["status"] = "error"
    elif summary["missing_files"]:
        summary["status"] = "missing_tabs"
    else:
        summary["status"] = "ok"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if not summary["mismatches"] else 1


def main() -> int:
    args = parse_args()
    export_dir = Path(args.export_dir).expanduser().resolve()
    summary_path = (
        Path(args.summary_path).expanduser().resolve()
        if args.summary_path
        else (export_dir / SUMMARY_FILE)
    )
    return run(export_dir, summary_path)


if __name__ == "__main__":
    raise SystemExit(main())
