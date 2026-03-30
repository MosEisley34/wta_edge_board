#!/usr/bin/env python3
"""Print schema_missing subtype frequencies from triage_last20_next.csv snapshots."""

from __future__ import annotations

import argparse
import csv
from collections import Counter
from pathlib import Path


def _read_counts(csv_path: Path) -> Counter[str]:
    counts: Counter[str] = Counter()
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            subtype = str(row.get("schema_missing_subreason") or "").strip()
            if not subtype:
                continue
            try:
                count = int(float(row.get("count") or 0))
            except Exception:
                count = 0
            if count > 0:
                counts[subtype] += count
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description="Show schema_missing subtype frequencies for triage tracking.")
    parser.add_argument(
        "inputs",
        nargs="*",
        help="Explicit triage_last20_next.csv files. If omitted, scans --root recursively.",
    )
    parser.add_argument(
        "--root",
        default=".",
        help="Root directory for recursive triage_last20_next.csv discovery when no explicit inputs are provided.",
    )
    args = parser.parse_args()

    paths = [Path(item) for item in args.inputs] if args.inputs else sorted(Path(args.root).rglob("triage_last20_next.csv"))
    if not paths:
        print("No triage_last20_next.csv files found.")
        return 0

    aggregate: Counter[str] = Counter()
    for path in paths:
        counts = _read_counts(path)
        aggregate.update(counts)
        summary = ", ".join(f"{k}:{v}" for k, v in sorted(counts.items(), key=lambda item: (-item[1], item[0])))
        print(f"{path}: {summary or 'none'}")

    print("\nAggregate subtype frequencies")
    for subtype, count in sorted(aggregate.items(), key=lambda item: (-item[1], item[0])):
        print(f"- {subtype}: {count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
