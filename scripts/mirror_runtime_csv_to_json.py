#!/usr/bin/env python3
"""Mirror runtime CSV artifacts into JSON artifacts.

This utility converts canonical runtime CSV artifacts:
- Run_Log.csv -> Run_Log.json
- State.csv   -> State.json

Behavior contract:
- If CSV exists, read and mirror it to JSON.
- If CSV is missing but JSON already exists, log a skip message and continue.
- If both CSV and JSON are missing for an artifact, exit with a remediation error.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path


ARTIFACTS = (
    ("Run_Log", "Run_Log.csv", "Run_Log.json"),
    ("State", "State.csv", "State.json"),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Mirror Run_Log.csv/State.csv into Run_Log.json/State.json using a directory contract."
        )
    )
    parser.add_argument(
        "--input-dir",
        required=True,
        help="Directory containing runtime artifacts (Run_Log.csv / State.csv and/or JSON counterparts).",
    )
    parser.add_argument(
        "--out-dir",
        default="",
        help="Optional output directory for mirrored JSON files. Defaults to --input-dir.",
    )
    return parser.parse_args()


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_json_rows(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(rows, handle, indent=2)
        handle.write("\n")


def main() -> int:
    args = parse_args()

    input_dir = Path(args.input_dir).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve() if args.out_dir else input_dir

    if not input_dir.is_dir():
        print(f"Error: --input-dir is not a directory: {input_dir}", file=sys.stderr)
        return 1

    missing_both: list[str] = []

    for label, csv_name, json_name in ARTIFACTS:
        csv_path = input_dir / csv_name
        out_json_path = out_dir / json_name
        input_json_path = input_dir / json_name

        if csv_path.is_file():
            rows = _read_csv_rows(csv_path)
            _write_json_rows(out_json_path, rows)
            print(f"Mirrored {csv_path} -> {out_json_path} ({len(rows)} rows)")
            continue

        if out_json_path.is_file() or input_json_path.is_file():
            existing = out_json_path if out_json_path.is_file() else input_json_path
            if existing != out_json_path:
                out_json_path.parent.mkdir(parents=True, exist_ok=True)
                out_json_path.write_text(existing.read_text(encoding="utf-8"), encoding="utf-8")
                print(
                    f"Skip: {csv_path} is missing; reused existing JSON {existing} -> {out_json_path}"
                )
            else:
                print(f"Skip: {csv_path} is missing; keeping existing JSON {existing}")
            continue

        missing_both.append(label)

    if missing_both:
        missing_list = ", ".join(missing_both)
        print(
            "Error: missing both CSV and JSON runtime artifacts for: "
            f"{missing_list}.\n"
            f"Remediation: provide {input_dir}/Run_Log.csv and/or {input_dir}/State.csv, "
            "or place existing Run_Log.json/State.json under the input directory "
            "(or --out-dir when reusing an already-mirrored batch) and rerun.",
            file=sys.stderr,
        )
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
