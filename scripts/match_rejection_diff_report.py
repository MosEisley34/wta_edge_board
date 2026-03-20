#!/usr/bin/env python3
import argparse
import json
from collections import Counter
from pathlib import Path


def _load_rows(path: Path):
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, list) else []


def _parse_state_row_value(raw_value):
    if isinstance(raw_value, dict):
        return raw_value
    if not isinstance(raw_value, str):
        return {}
    try:
        parsed = json.loads(raw_value)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _extract_rejected_rows(state_rows, limit):
    rejected = []
    for row in state_rows:
        key = str(row.get("key", ""))
        if key not in ("LAST_RUN_SUMMARY", "LAST_RUN_VERBOSE_JSON"):
            continue
        summary = _parse_state_row_value(row.get("value"))
        for sample in summary.get("sample_unmatched_cases", []):
            if str(sample.get("rejection_code", "")).strip():
                rejected.append(sample)
    return rejected[:limit]


def build_markdown(rows):
    out = []
    out.append("# stageMatchEvents rejected event diff")
    out.append("")
    out.append("| odds_event_id | normalized_odds_players | nearest_schedule_candidate | rejection_reason | primary_time_delta_min | fallback_time_delta_min |")
    out.append("|---|---|---|---|---:|---:|")
    for row in rows:
        nearest = row.get("nearest_schedule_candidate") or {}
        nearest_short = f"{nearest.get('event_id','')} [{', '.join(nearest.get('normalized_players', []))}]"
        out.append(
            f"| {row.get('odds_event_id','')} | {', '.join(row.get('normalized_odds_players', []))} | "
            f"{nearest_short.strip()} | {row.get('rejection_code','')} | "
            f"{row.get('primary_time_delta_min','')} | {row.get('fallback_time_delta_min','')} |"
        )
    if not rows:
        out.append("| - | - | - | - | - | - |")

    reasons = Counter(str(row.get("rejection_code", "")) for row in rows if row.get("rejection_code"))
    initials_near_match = sum(
        1 for row in rows if bool((row.get("nearest_schedule_candidate") or {}).get("initial_key_match"))
    )
    out.append("")
    out.append("## Pattern summary")
    out.append("")
    out.append(f"- rejected_rows: **{len(rows)}**")
    out.append(f"- rejection_breakdown: **{dict(reasons)}**")
    out.append(f"- nearest_candidate_initial_key_match: **{initials_near_match}**")
    return "\n".join(out) + "\n"


def main():
    parser = argparse.ArgumentParser(description="Build per-event diff for stageMatchEvents rejections.")
    parser.add_argument("--state-json", default="runtime_exports/source/State.json")
    parser.add_argument("--limit", type=int, default=18)
    parser.add_argument("--out", default="docs/baselines/stage_match_events_rejection_diff.md")
    args = parser.parse_args()

    rows = _load_rows(Path(args.state_json))
    rejected = _extract_rejected_rows(rows, args.limit)
    report = build_markdown(rejected)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(report, encoding="utf-8")
    print(f"wrote {out_path} with {len(rejected)} rejected rows")


if __name__ == "__main__":
    main()
