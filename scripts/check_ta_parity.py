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
    has_consistent_metric_index_mapping,
    is_usable_canonical_name,
    iter_matchmx_rows,
    parse_matchmx_player_row,
)

DEFAULT_INPUT_PATH = "tmp/source_probe_latest/raw/tennisabstract_leadersource_wta.body"
FALLBACK_INPUT_PATH = "tmp/source_probes/raw/tennisabstract_leadersource_wta.body"


def _canonicalize_name(name: str) -> str:
    return re.sub(r"\s+", " ", str(name or "")).strip().lower()


def _extract_rows(payload: str) -> tuple[list[dict[str, object]], dict[str, object]]:
    rows: list[dict[str, object]] = []
    metrics: dict[str, object] = {"ta_matchmx_unusable_payload": 0, "first_invalid_rows": []}
    for logical_row_number, parsed in enumerate(iter_matchmx_rows(payload), start=1):
        tokens = parsed.tokens

        def note_invalid(reason: str) -> None:
            metrics["ta_matchmx_unusable_payload"] = int(metrics["ta_matchmx_unusable_payload"]) + 1
            samples = metrics["first_invalid_rows"]
            if isinstance(samples, list) and len(samples) < 5:
                samples.append(
                    {
                        "reason": reason,
                        "row_number": logical_row_number,
                        "row_index": parsed.row_index,
                        "token_count": len(tokens),
                        "token_sample": tokens[:8],
                    }
                )

        if parsed.reason_code:
            note_invalid(parsed.reason_code)
            continue

        parsed_row, reason = parse_matchmx_player_row(tokens)
        if reason:
            note_invalid(reason)
            continue
        assert parsed_row is not None
        rows.append(
            {
                "date": parsed_row.date,
                "player_name": parsed_row.player_name,
                "score": parsed_row.score,
                "ranking": parsed_row.ranking,
                "hold_pct": parsed_row.hold_pct,
                "break_pct": parsed_row.break_pct,
            }
        )
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
    parser.add_argument("--min-rows", type=int, default=5, help="Minimum parsed rows required")
    parser.add_argument("--min-unique-players", type=int, default=5, help="Minimum unique players required")
    args = parser.parse_args()

    path = Path(args.input)
    if not path.exists():
        print(json.dumps({"status": "fail", "reason_code": "input_missing", "path": str(path)}))
        return 1

    payload = path.read_text(encoding="utf-8", errors="ignore")
    if not has_consistent_metric_index_mapping():
        print(json.dumps({"status": "fail", "reason_code": "metric_index_mapping_invalid"}))
        return 1

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


    thresholds = {
        "min_rows": max(1, int(args.min_rows)),
        "min_unique_players": max(1, int(args.min_unique_players)),
    }
    threshold_errors: list[str] = []
    if len(rows) < thresholds["min_rows"]:
        threshold_errors.append(f"rows={len(rows)} < {thresholds['min_rows']}")
    if len(normalized_rows) < thresholds["min_unique_players"]:
        threshold_errors.append(f"unique_players={len(normalized_rows)} < {thresholds['min_unique_players']}")
    if row_rank_count <= 0:
        threshold_errors.append("ranking_non_null_coverage=0")
    if row_hold_count <= 0:
        threshold_errors.append("hold_pct_non_null_coverage=0")
    if row_break_count <= 0:
        threshold_errors.append("break_pct_non_null_coverage=0")

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
        "thresholds": thresholds,
        "threshold_errors": threshold_errors,
        "first_invalid_rows": parser_metrics.get("first_invalid_rows", []),
    }

    if threshold_errors:
        summary["status"] = "fail"
        summary["reason_code"] = "ta_matchmx_unusable_payload"
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 1

    cli_good = min(row_rank_ratio, row_hold_ratio, row_break_ratio) >= args.min_cli_coverage
    apps_poor = max(norm_rank_ratio, norm_hold_ratio, norm_break_ratio) <= args.max_apps_coverage
    print(json.dumps(summary, indent=2, sort_keys=True))
    if cli_good and apps_poor:
        print("parser_parity_regression", file=sys.stderr)
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
