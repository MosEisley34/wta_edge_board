#!/usr/bin/env python3
import json
import sys
from collections import Counter

from pipeline_log_adapter import adapt_run_log_record_for_legacy


def usage():
    print("Usage: scripts/analyze_pipeline_log_bytes.py <sample.json>")


def json_bytes(value):
    return len(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8"))


def walk(obj, prefix=""):
    if isinstance(obj, dict):
        for key, value in obj.items():
            path = f"{prefix}.{key}" if prefix else key
            yield path, key, value
            yield from walk(value, path)
    elif isinstance(obj, list):
        for idx, value in enumerate(obj):
            yield from walk(value, f"{prefix}[{idx}]")


def main(path):
    rows = json.load(open(path, "r", encoding="utf-8"))

    total_bytes = 0
    bytes_by_event_type = Counter()
    bytes_by_family = Counter()
    field_bytes = Counter()
    long_reason_rows = Counter()

    long_key_threshold = 24
    static_fields = {"row_type", "stage", "status", "provider", "reason_code"}

    for raw_row in rows:
        row = adapt_run_log_record_for_legacy(raw_row)
        row_size = json_bytes(row)
        total_bytes += row_size
        bytes_by_event_type[row.get("row_type", "summary")] += row_size

        for key, value in row.items():
            size = json_bytes(value)
            field_bytes[key] += size
            key_l = key.lower()
            if "reason" in key_l:
                bytes_by_family["reason_codes"] += size
            elif "time" in key_l or key.endswith("_at") or "timestamp" in key_l:
                bytes_by_family["timestamps"] += size
            elif key in static_fields:
                bytes_by_family["repeated_static_fields"] += size
            else:
                bytes_by_family["metadata"] += size

        for key in ("message", "rejection_codes", "stage_summaries"):
            value = row.get(key)
            if not value:
                continue
            parsed = None
            if isinstance(value, str):
                try:
                    parsed = json.loads(value)
                except Exception:
                    continue
            else:
                parsed = value

            for path_name, nested_key, nested_value in walk(parsed):
                size = json_bytes(nested_value)
                field_bytes[f"{key}.{path_name}"] += size
                key_l = nested_key.lower()
                if "reason" in key_l:
                    bytes_by_family["reason_codes"] += size
                    if isinstance(nested_value, dict):
                        for reason_key in nested_value.keys():
                            if len(reason_key) >= long_key_threshold:
                                long_reason_rows[reason_key] += 1
                elif "time" in key_l or nested_key.endswith("_at") or "timestamp" in key_l:
                    bytes_by_family["timestamps"] += size
                elif nested_key in static_fields:
                    bytes_by_family["repeated_static_fields"] += size
                else:
                    bytes_by_family["metadata"] += size

    print(json.dumps({
        "rows": len(rows),
        "total_bytes": total_bytes,
        "bytes_by_event_type": bytes_by_event_type,
        "bytes_by_family": bytes_by_family,
        "top_10_largest_fields": field_bytes.most_common(10),
        "long_reason_key_repetition_rows": [
            {
                "reason_key": key,
                "rows_with_key": count,
                "row_rate": round(count / len(rows), 4) if rows else 0,
            }
            for key, count in long_reason_rows.most_common()
        ],
    }, indent=2, default=dict))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        usage()
        raise SystemExit(1)
    main(sys.argv[1])
