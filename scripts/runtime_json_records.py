#!/usr/bin/env python3
"""Helpers for iterating runtime JSON artifacts across supported schemas."""

from __future__ import annotations

import json
from typing import Any, Iterator


def _extract_rows(payload: Any) -> list[Any] | None:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        rows = payload.get("rows")
        if isinstance(rows, list):
            return rows
    return None


def iter_json_records(data: str) -> Iterator[tuple[int, Any]]:
    """Yield ``(row_number, row_payload)`` from JSON or NDJSON text.

    Supports both top-level list payloads and top-level objects with a ``rows``
    collection.
    """
    text = data.strip()
    if not text:
        return

    ndjson_records: list[tuple[int, Any]] = []
    ndjson_ok = True
    for idx, line in enumerate(text.splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        try:
            ndjson_records.append((idx, json.loads(line)))
        except Exception:
            ndjson_ok = False
            break

    if ndjson_ok and ndjson_records:
        if len(ndjson_records) == 1:
            _, first_payload = ndjson_records[0]
            rows = _extract_rows(first_payload)
            if rows is not None:
                for row_idx, row in enumerate(rows, start=1):
                    yield row_idx, row
                return
        for row_idx, row in ndjson_records:
            yield row_idx, row
        return

    parsed = json.loads(text)
    rows = _extract_rows(parsed)
    if rows is not None:
        for row_idx, row in enumerate(rows, start=1):
            yield row_idx, row
        return
    yield 1, parsed
