#!/usr/bin/env python3
"""Extract schema_missing subreasons from compare outputs into triage_last20_next.csv."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any


def _iter_json_objects(raw_text: str) -> list[dict[str, Any]]:
    decoder = json.JSONDecoder()
    out: list[dict[str, Any]] = []
    idx = 0
    length = len(raw_text)
    while idx < length:
        if raw_text[idx] not in "{[":
            idx += 1
            continue
        try:
            parsed, next_idx = decoder.raw_decode(raw_text, idx)
        except Exception:
            idx += 1
            continue
        idx = next_idx
        if isinstance(parsed, dict):
            out.append(parsed)
    return out


def _schema_missing_details(payload: dict[str, Any]) -> list[str]:
    if str(payload.get("reason_code") or "").strip() != "schema_missing":
        return []
    details = payload.get("schema_missing_details")
    if not isinstance(details, list):
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for item in details:
        code = str(item or "").strip()
        if not code or code in seen:
            continue
        seen.add(code)
        normalized.append(code)
    return normalized


def _extract_events(paths: list[Path]) -> list[list[str]]:
    events: list[list[str]] = []
    for path in paths:
        text = path.read_text(encoding="utf-8", errors="replace")
        for payload in _iter_json_objects(text):
            details = _schema_missing_details(payload)
            if details:
                events.append(details)
    return events


def main() -> int:
    parser = argparse.ArgumentParser(description="Aggregate schema_missing subreasons into a triage CSV.")
    parser.add_argument("inputs", nargs="+", help="Compare output files (JSON or plain logs containing JSON blocks).")
    parser.add_argument("--limit", type=int, default=20, help="Include only the most recent N schema_missing events (default: 20).")
    parser.add_argument("--out", default="triage_last20_next.csv", help="Output CSV path (default: triage_last20_next.csv).")
    args = parser.parse_args()

    input_paths = [Path(p) for p in args.inputs if Path(p).exists()]
    events = _extract_events(input_paths)
    recent_events = events[-max(1, int(args.limit)) :]

    subtype_counts: Counter[str] = Counter()
    for detail_list in recent_events:
        for detail in detail_list:
            subtype_counts[detail] += 1

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["reason_code", "schema_missing_subreason", "count", "events_considered"],
        )
        writer.writeheader()
        if subtype_counts:
            for subtype, count in sorted(subtype_counts.items(), key=lambda item: (-item[1], item[0])):
                writer.writerow(
                    {
                        "reason_code": "schema_missing",
                        "schema_missing_subreason": subtype,
                        "count": count,
                        "events_considered": len(recent_events),
                    }
                )
        else:
            writer.writerow(
                {
                    "reason_code": "schema_missing",
                    "schema_missing_subreason": "none_detected",
                    "count": 0,
                    "events_considered": len(recent_events),
                }
            )
    print(str(out_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
