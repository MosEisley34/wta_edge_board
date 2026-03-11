#!/usr/bin/env python3
"""Diagnostic helper to inspect parsed Tennis Abstract `matchmx` rows."""

from __future__ import annotations

import argparse
from pathlib import Path

from matchmx_parser import MATCHMX_SCHEMA_INDEX_MAPS, get_matchmx_row_idx, iter_matchmx_rows

DEFAULT_INPUT = "scripts/fixtures/tennisabstract_leadersource_wta.body"


def _schema_name(row_idx: dict[str, int]) -> str:
    for name, idx_map in MATCHMX_SCHEMA_INDEX_MAPS.items():
        if row_idx is idx_map:
            return name
    for name, idx_map in MATCHMX_SCHEMA_INDEX_MAPS.items():
        if row_idx == idx_map:
            return name
    return "unknown"


def _fmt_token(idx: int, token: str) -> str:
    compact = " ".join(str(token).split())
    return f"[{idx:02d}] {compact}"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inspect parsed matchmx row tokens and detected schema mapping."
    )
    parser.add_argument("--input", default=DEFAULT_INPUT, help="Path to payload file")
    parser.add_argument("--rows", type=int, default=5, help="Number of parsed rows to print")
    parser.add_argument(
        "--max-cols",
        type=int,
        default=25,
        help="Max token columns to print per row",
    )
    args = parser.parse_args()

    path = Path(args.input)
    if not path.exists():
        print(f"error: input file not found: {path}")
        return 1

    payload = path.read_text(encoding="utf-8", errors="ignore")
    max_rows = max(1, int(args.rows))
    max_cols = max(1, int(args.max_cols))

    printed = 0
    for logical_row_number, parsed in enumerate(iter_matchmx_rows(payload), start=1):
        if printed >= max_rows:
            break

        tokens = parsed.tokens
        row_idx = get_matchmx_row_idx(tokens) if tokens else MATCHMX_SCHEMA_INDEX_MAPS["old"]
        schema = _schema_name(row_idx)
        hold_idx = row_idx.get("HOLD_PCT")
        break_idx = row_idx.get("BREAK_PCT")

        hold_token = tokens[hold_idx] if hold_idx is not None and hold_idx < len(tokens) else ""
        break_token = tokens[break_idx] if break_idx is not None and break_idx < len(tokens) else ""

        print(
            " ".join(
                [
                    f"row={logical_row_number}",
                    f"row_index={parsed.row_index}",
                    f"token_count={len(tokens)}",
                    f"row_shape_valid={parsed.row_shape_valid}",
                    f"reason={parsed.reason_code}",
                    f"schema={schema}",
                    f"hold_idx={hold_idx}",
                    f"hold={hold_token}",
                    f"break_idx={break_idx}",
                    f"break={break_token}",
                ]
            )
        )

        col_limit = min(len(tokens), max_cols)
        for i in range(col_limit):
            print(f"  {_fmt_token(i, tokens[i])}")
        if len(tokens) > col_limit:
            print(f"  ... ({len(tokens) - col_limit} more columns)")
        print()
        printed += 1

    if printed == 0:
        print("warning: no matchmx rows were parsed")
        return 2

    print(f"printed_rows={printed} input={path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
