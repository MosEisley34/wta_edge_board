#!/usr/bin/env python3
"""Pre-deploy parity check for Tennis Abstract `matchmx` parsing.

Reads a leadersource payload, parses `matchmx[...]` rows using the same index mapping
as Apps Script `MATCHMX_ROW_IDX`, emits coverage diagnostics, and fails fast when
raw CLI coverage looks healthy but Apps Script-like normalized coverage is poor.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

from matchmx_parser import (
    MATCHMX_ROW_IDX,
    has_any_key_metrics,
    has_minimum_schema_columns,
    is_usable_canonical_name,
    iter_matchmx_rows,
    required_indices_present,
)

DEFAULT_INPUT_PATH = "tmp/source_probe_latest/raw/tennisabstract_leadersource_wta.body"
FALLBACK_INPUT_PATH = "tmp/source_probes/raw/tennisabstract_leadersource_wta.body"


def _to_number(tokens: list[str], idx: int) -> float | None:
    if idx >= len(tokens):
        return None
    try:
        value = float(tokens[idx])
    except (TypeError, ValueError):
        return None
    if value != value:
        return None
    return value


def _canonicalize_name(name: str) -> str:
    return re.sub(r"\s+", " ", str(name or "")).strip().lower()


def _build_structured_row(tokens: list[str]) -> dict[str, object]:
    score = str(tokens[MATCHMX_ROW_IDX["SCORE"]] if len(tokens) > MATCHMX_ROW_IDX["SCORE"] else "").strip()
    has_walkover_or_ret = bool(re.search(r"\b(?:ret|wo)\b", score, flags=re.IGNORECASE))

    def take(key: str) -> float | None:
        value = _to_number(tokens, MATCHMX_ROW_IDX[key])
        if has_walkover_or_ret and key in {
            "RECENT_FORM",
            "SURFACE_WIN_RATE",
            "HOLD_PCT",
            "BREAK_PCT",
            "BP_SAVED_PCT",
            "BP_CONV_PCT",
            "FIRST_SERVE_IN_PCT",
            "FIRST_SERVE_POINTS_WON_PCT",
            "SECOND_SERVE_POINTS_WON_PCT",
            "RETURN_POINTS_WON_PCT",
            "DOMINANCE_RATIO",
            "TOTAL_POINTS_WON_PCT",
        }:
            return None
        return value

    return {
        "date": str(tokens[MATCHMX_ROW_IDX["DATE"]] if len(tokens) > MATCHMX_ROW_IDX["DATE"] else ""),
        "player_name": str(tokens[MATCHMX_ROW_IDX["PLAYER_NAME"]] if len(tokens) > MATCHMX_ROW_IDX["PLAYER_NAME"] else ""),
        "score": score,
        "ranking": take("RANKING"),
        "hold_pct": take("HOLD_PCT"),
        "break_pct": take("BREAK_PCT"),
    }


def _extract_rows(payload: str) -> tuple[list[dict[str, object]], dict[str, int]]:
    rows: list[dict[str, object]] = []
    metrics = {"ta_matchmx_unusable_payload": 0}
    for parsed in iter_matchmx_rows(payload):
        tokens = parsed.tokens
        if not has_minimum_schema_columns(tokens):
            metrics["ta_matchmx_unusable_payload"] += 1
            continue
        if not required_indices_present(tokens):
            metrics["ta_matchmx_unusable_payload"] += 1
            continue
        row = _build_structured_row(tokens)
        if not row["player_name"] or not row["score"]:
            continue
        if not is_usable_canonical_name(str(row["player_name"])):
            metrics["ta_matchmx_unusable_payload"] += 1
            continue
        if not has_any_key_metrics(tokens):
            metrics["ta_matchmx_unusable_payload"] += 1
            continue
        if row["ranking"] is None and row["hold_pct"] is None and row["break_pct"] is None:
            metrics["ta_matchmx_unusable_payload"] += 1
            continue
        rows.append(row)
    return rows, metrics


def _coverage(items: list[dict[str, object]], key: str) -> tuple[int, float]:
    non_null = sum(1 for row in items if row.get(key) is not None)
    denom = len(items)
    return non_null, (non_null / denom) if denom else 0.0


def _normalize_records(rows: list[dict[str, object]]) -> dict[str, dict[str, object]]:
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        canonical = _canonicalize_name(str(row.get("player_name") or ""))
        if not is_usable_canonical_name(canonical):
            continue
        grouped[canonical].append(row)

    normalized: dict[str, dict[str, object]] = {}
    for canonical, player_rows in grouped.items():
        sorted_rows = sorted(player_rows, key=lambda r: str(r.get("date") or ""), reverse=True)
        ranking = sorted_rows[0].get("ranking")

        def avg(metric: str) -> float | None:
            vals = [float(v) for v in (r.get(metric) for r in sorted_rows) if v is not None]
            if not vals:
                return None
            return round(sum(vals) / len(vals), 3)

        normalized[canonical] = {
            "player": canonical,
            "ranking": int(round(float(ranking))) if ranking is not None else None,
            "hold_pct": avg("hold_pct"),
            "break_pct": avg("break_pct"),
            "row_count": len(sorted_rows),
        }
    return normalized


def main() -> int:
    parser = argparse.ArgumentParser(description="Check TA parser parity from leadersource_wta.js payload.")
    parser.add_argument("--input", default=DEFAULT_INPUT_PATH, help="Path to leadersource_wta.js payload")
    parser.add_argument("--sample-size", type=int, default=5, help="Number of normalized records to print")
    parser.add_argument("--min-cli-coverage", type=float, default=0.60, help="Min row-level coverage threshold to consider CLI healthy")
    parser.add_argument("--max-apps-coverage", type=float, default=0.20, help="Max normalized coverage threshold to consider Apps Script poor")
    args = parser.parse_args()

    path = Path(args.input)
    if not path.exists():
        print(json.dumps({"status": "fail", "reason_code": "input_missing", "path": str(path)}))
        return 1

    payload = path.read_text(encoding="utf-8", errors="ignore")
    matchmx_markers = len(re.findall(r"\bmatchmx\s*(?:\[\s*\d+\s*\])?\s*=\s*\[", payload))
    if matchmx_markers == 0:
        print(
            json.dumps(
                {
                    "status": "fail",
                    "reason_code": "ta_matchmx_markers_missing",
                    "path": str(path),
                    "reason": "Input does not include any matchmx rows. TA parity expects leadersource_wta.js content.",
                    "suggested_input": DEFAULT_INPUT_PATH,
                    "alternate_input": FALLBACK_INPUT_PATH,
                    "matchmx_marker_count": 0,
                }
            )
        )
        return 1

    rows, parser_metrics = _extract_rows(payload)
    unusable_payload_rows = parser_metrics["ta_matchmx_unusable_payload"]
    if unusable_payload_rows > 0 and not rows:
        print(
            json.dumps(
                {
                    "status": "fail",
                    "reason_code": "ta_matchmx_unusable_payload",
                    "path": str(path),
                    "matchmx_unusable_rows": unusable_payload_rows,
        "parser_metrics": parser_metrics,
                }
            )
        )
        return 1
    normalized = _normalize_records(rows)
    normalized_rows = list(normalized.values())

    row_rank_count, row_rank_ratio = _coverage(rows, "ranking")
    row_hold_count, row_hold_ratio = _coverage(rows, "hold_pct")
    row_break_count, row_break_ratio = _coverage(rows, "break_pct")

    norm_rank_count, norm_rank_ratio = _coverage(normalized_rows, "ranking")
    norm_hold_count, norm_hold_ratio = _coverage(normalized_rows, "hold_pct")
    norm_break_count, norm_break_ratio = _coverage(normalized_rows, "break_pct")

    summary = {
        "input": str(path),
        "total_rows": len(rows),
        "matchmx_unusable_rows": unusable_payload_rows,
        "parser_metrics": parser_metrics,
        "unique_players": len(normalized_rows),
        "row_non_null_coverage": {
            "ranking": {"non_null": row_rank_count, "ratio": round(row_rank_ratio, 4)},
            "hold_pct": {"non_null": row_hold_count, "ratio": round(row_hold_ratio, 4)},
            "break_pct": {"non_null": row_break_count, "ratio": round(row_break_ratio, 4)},
        },
        "normalized_non_null_coverage": {
            "ranking": {"non_null": norm_rank_count, "ratio": round(norm_rank_ratio, 4)},
            "hold_pct": {"non_null": norm_hold_count, "ratio": round(norm_hold_ratio, 4)},
            "break_pct": {"non_null": norm_break_count, "ratio": round(norm_break_ratio, 4)},
        },
        "sample_normalized_records": sorted(normalized_rows, key=lambda r: str(r["player"]))[: max(0, args.sample_size)],
    }

    print(json.dumps(summary, indent=2, sort_keys=True))

    cli_good = min(row_rank_ratio, row_hold_ratio, row_break_ratio) >= args.min_cli_coverage
    apps_poor = max(norm_rank_ratio, norm_hold_ratio, norm_break_ratio) <= args.max_apps_coverage
    if cli_good and apps_poor:
        print("parser_parity_regression", file=sys.stderr)
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
