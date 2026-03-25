#!/usr/bin/env python3
"""Generate GS-focused suppression quality report from Run_Log summaries.

GS here refers to stageGenerateSignals behavior in unattended runtime.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


SUPPRESSION_BUCKET_REASON_MAP = {
    "edge": {"edge_below_threshold"},
    "timing": {"too_close_to_start_skip"},
    "stale": {"stale_odds_skip"},
    "cooldown": {"cooldown_suppressed"},
}


@dataclass
class RunSnapshot:
    run_id: str
    ended_at: datetime
    matched_events: int
    scored_signals: int
    sent_notifications: int
    suppressions: dict[str, int]
    player_stats_requested: int
    player_stats_resolved: int
    player_stats_ta_resolved: int



def _parse_json(value: Any, fallback: Any) -> Any:
    if value in (None, ""):
        return fallback
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return fallback
    return fallback


def _parse_ts(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _load_rows(path: Path) -> list[dict[str, Any]]:
    if path.suffix.lower() == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, list) else []

    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _extract_stage_summaries(summary: dict[str, Any]) -> list[dict[str, Any]]:
    stage_summaries = _parse_json(summary.get("stage_summaries"), None)
    if isinstance(stage_summaries, list):
        return [row for row in stage_summaries if isinstance(row, dict)]
    if isinstance(stage_summaries, dict):
        nested = stage_summaries.get("stage_summaries")
        if isinstance(nested, list):
            return [row for row in nested if isinstance(row, dict)]
    message = _parse_json(summary.get("message"), {})
    nested = message.get("stage_summaries") if isinstance(message, dict) else None
    if isinstance(nested, list):
        return [row for row in nested if isinstance(row, dict)]
    return []


def _extract_signal_summary(summary: dict[str, Any]) -> dict[str, Any]:
    fallback: dict[str, Any] | None = None
    for candidate in _signal_summary_candidates(summary):
        if not isinstance(candidate, dict):
            continue
        if "suppression_counts" in candidate or "by_reason" in candidate:
            if fallback is None:
                fallback = candidate
            if _flatten_suppressions(candidate):
                return candidate
    if fallback is not None:
        return fallback
    for candidate in _signal_summary_candidates(summary):
        if isinstance(candidate, dict):
            return candidate
    return {}


def _signal_summary_candidates(summary: dict[str, Any]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []

    direct = _parse_json(summary.get("signal_decision_summary"), {})
    if isinstance(direct, dict):
        candidates.append(direct)

    message = _parse_json(summary.get("message"), {})
    if isinstance(message, dict):
        nested = _parse_json(message.get("signal_decision_summary"), {})
        if isinstance(nested, dict):
            candidates.append(nested)
        message_suppression = _parse_json(message.get("suppression_counts"), {})
        if isinstance(message_suppression, dict):
            candidates.append({"suppression_counts": message_suppression})

    for stage in _extract_stage_summaries(summary):
        if str(stage.get("stage") or "").strip() != "stageGenerateSignals":
            continue
        stage_summary = _parse_json(stage.get("signal_decision_summary"), {})
        if isinstance(stage_summary, dict):
            candidates.append(stage_summary)
        reason_metadata = _parse_json(stage.get("reason_metadata"), {})
        if isinstance(reason_metadata, dict):
            nested = _parse_json(reason_metadata.get("signal_decision_summary"), {})
            if isinstance(nested, dict):
                candidates.append(nested)
            suppression = _parse_json(reason_metadata.get("suppression_counts"), {})
            if isinstance(suppression, dict):
                candidates.append({"suppression_counts": suppression})

    return candidates


def _flatten_suppressions(signal_summary: dict[str, Any]) -> dict[str, int]:
    if not isinstance(signal_summary, dict):
        return {}
    suppression = signal_summary.get("suppression_counts", signal_summary)
    if not isinstance(suppression, dict):
        return {}
    counts: dict[str, int] = {}

    for bucket_name, bucket in suppression.items():
        if not isinstance(bucket, dict):
            try:
                counts[str(bucket_name)] = counts.get(str(bucket_name), 0) + int(float(bucket))
            except (TypeError, ValueError):
                continue
            continue
        by_reason = bucket.get("by_reason")
        if isinstance(by_reason, dict):
            for reason, raw in by_reason.items():
                try:
                    counts[str(reason)] = counts.get(str(reason), 0) + int(float(raw))
                except (TypeError, ValueError):
                    continue
            continue
    return counts


def _extract_scored_signals(summary: dict[str, Any], signal_summary: dict[str, Any]) -> int:
    for key in ("scored_signals", "scored_signals_count", "signals_scored", "signal_count"):
        for source in (summary, signal_summary):
            try:
                return max(0, int(float(source.get(key))))
            except (TypeError, ValueError, AttributeError):
                continue
    for stage in _extract_stage_summaries(summary):
        if str(stage.get("stage") or "").strip() != "stageGenerateSignals":
            continue
        try:
            return max(0, int(float(stage.get("output_count"))))
        except (TypeError, ValueError):
            continue
    return 0


def _extract_matched(summary: dict[str, Any]) -> int:
    for key in ("matched",):
        try:
            return max(0, int(float(summary.get(key))))
        except (TypeError, ValueError):
            continue
    for stage in _extract_stage_summaries(summary):
        if str(stage.get("stage") or "") != "stageMatchEvents":
            continue
        reason_codes = _parse_json(stage.get("reason_codes"), {})
        if isinstance(reason_codes, dict):
            for key in ("matched_count", "MATCH_CT"):
                try:
                    return max(0, int(float(reason_codes.get(key))))
                except (TypeError, ValueError):
                    continue
        try:
            return max(0, int(float(stage.get("output_count"))))
        except (TypeError, ValueError):
            continue
    return 0


def _extract_sent_notifications(summary: dict[str, Any], suppressions: dict[str, int]) -> int:
    rc = _parse_json(summary.get("reason_codes"), {})
    if isinstance(rc, dict):
        for key in ("sent", "SIG_EDGE"):
            try:
                if key == "SIG_EDGE":
                    # alias fallback for newer schema where signal-edge approximates actionable sent count
                    return max(0, int(float(rc.get(key))))
                return max(0, int(float(rc.get(key))))
            except (TypeError, ValueError):
                continue
    try:
        return max(0, int(float(summary.get("signals_found"))))
    except (TypeError, ValueError):
        pass
    scored = _extract_scored_signals(summary, _extract_signal_summary(summary))
    suppressed = sum(suppressions.values())
    return max(0, scored - suppressed)


def _extract_player_stats(summary: dict[str, Any]) -> tuple[int, int, int]:
    requested = resolved = ta_resolved = 0
    for stage in _extract_stage_summaries(summary):
        if str(stage.get("stage") or "").strip() != "stageFetchPlayerStats":
            continue
        metadata = _parse_json(stage.get("reason_metadata"), {})
        if not isinstance(metadata, dict):
            continue
        for key in ("requested_player_count", "total_player_count", "players_total"):
            try:
                requested = max(requested, int(float(metadata.get(key))))
            except (TypeError, ValueError):
                continue
        for key in ("resolved_player_count", "resolved_players_count"):
            try:
                resolved = max(resolved, int(float(metadata.get(key))))
            except (TypeError, ValueError):
                continue
        for key in ("players_found_ta", "resolved_via_ta_count", "ta_resolved_count"):
            try:
                ta_resolved = max(ta_resolved, int(float(metadata.get(key))))
            except (TypeError, ValueError):
                continue
    return requested, resolved, ta_resolved


def build_snapshots(rows: list[dict[str, Any]]) -> list[RunSnapshot]:
    snapshots: list[RunSnapshot] = []
    for row in rows:
        if str(row.get("row_type") or "") != "summary" or str(row.get("stage") or "") != "runEdgeBoard":
            continue
        run_id = str(row.get("run_id") or "").strip()
        ended = _parse_ts(row.get("ended_at") or row.get("started_at"))
        if not run_id or not ended:
            continue
        signal_summary = _extract_signal_summary(row)
        suppressions = _flatten_suppressions(signal_summary)
        requested, resolved, ta_resolved = _extract_player_stats(row)
        snapshots.append(
            RunSnapshot(
                run_id=run_id,
                ended_at=ended,
                matched_events=_extract_matched(row),
                scored_signals=_extract_scored_signals(row, signal_summary),
                sent_notifications=_extract_sent_notifications(row, suppressions),
                suppressions=suppressions,
                player_stats_requested=requested,
                player_stats_resolved=resolved,
                player_stats_ta_resolved=ta_resolved,
            )
        )
    return sorted(snapshots, key=lambda item: item.ended_at)


def _window(snapshots: list[RunSnapshot], start: datetime, end: datetime) -> list[RunSnapshot]:
    return [s for s in snapshots if start <= s.ended_at < end]


def _aggregate(snapshots: list[RunSnapshot]) -> dict[str, Any]:
    suppressions: defaultdict[str, int] = defaultdict(int)
    requested = resolved = ta_resolved = 0
    for snap in snapshots:
        requested += snap.player_stats_requested
        resolved += snap.player_stats_resolved
        ta_resolved += snap.player_stats_ta_resolved
        for reason, count in snap.suppressions.items():
            suppressions[reason] += count

    total_suppressions = sum(suppressions.values())
    suppression_mix = [
        {
            "reason": reason,
            "count": count,
            "share": round((count / total_suppressions), 4) if total_suppressions else 0,
        }
        for reason, count in sorted(suppressions.items(), key=lambda item: (-item[1], item[0]))
    ]

    coverage_rate = round((resolved / requested), 4) if requested else None
    ta_parity_rate = round((ta_resolved / resolved), 4) if resolved else None

    return {
        "run_count": len(snapshots),
        "run_ids": [s.run_id for s in snapshots],
        "matched_events": sum(s.matched_events for s in snapshots),
        "scored_signals": sum(s.scored_signals for s in snapshots),
        "sent_notifications": sum(s.sent_notifications for s in snapshots),
        "suppression_total": total_suppressions,
        "suppression_reason_mix": suppression_mix,
        "suppression_bucket_mix": _bucket_mix_from_suppressions(dict(suppressions)),
        "player_stats": {
            "requested": requested,
            "resolved": resolved,
            "ta_resolved": ta_resolved,
            "coverage_rate": coverage_rate,
            "ta_parity_rate": ta_parity_rate,
        },
    }


def _bucket_mix_from_suppressions(suppressions: dict[str, int]) -> list[dict[str, Any]]:
    bucket_counts = {bucket: 0 for bucket in SUPPRESSION_BUCKET_REASON_MAP}
    for reason, count in suppressions.items():
        for bucket, reasons in SUPPRESSION_BUCKET_REASON_MAP.items():
            if reason in reasons:
                bucket_counts[bucket] += int(count)

    total = sum(bucket_counts.values())
    ordered = sorted(bucket_counts.items(), key=lambda item: (-item[1], item[0]))
    return [
        {
            "bucket": bucket,
            "count": count,
            "share": round((count / total), 4) if total else 0,
        }
        for bucket, count in ordered
    ]


def _top_suppression_buckets(summary: dict[str, Any], top_n: int = 2) -> list[dict[str, Any]]:
    buckets = summary.get("suppression_bucket_mix", [])
    out = []
    for entry in buckets[:top_n]:
        bucket = str(entry["bucket"])
        count = int(entry["count"])
        # cooldown is expected noise; edge/timing/stale are tuneable avoidable suppression churn.
        classification = "expected" if bucket == "cooldown" else "avoidable"
        out.append(
            {
                "bucket": bucket,
                "count": count,
                "share": entry.get("share", 0),
                "expected_count": count if classification == "expected" else 0,
                "avoidable_count": count if classification == "avoidable" else 0,
                "classification": classification,
            }
        )
    return out


def _compare_windows(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    def delta(key: str) -> int:
        return int(after.get(key, 0)) - int(before.get(key, 0))

    before_cov = before.get("player_stats", {}).get("coverage_rate")
    after_cov = after.get("player_stats", {}).get("coverage_rate")
    before_parity = before.get("player_stats", {}).get("ta_parity_rate")
    after_parity = after.get("player_stats", {}).get("ta_parity_rate")

    no_regression_coverage = True if before_cov is None or after_cov is None else after_cov >= before_cov
    no_regression_parity = True if before_parity is None or after_parity is None else after_parity >= before_parity

    return {
        "matched_events_delta": delta("matched_events"),
        "scored_signals_delta": delta("scored_signals"),
        "sent_notifications_delta": delta("sent_notifications"),
        "suppression_total_delta": delta("suppression_total"),
        "player_stats_coverage_delta": None if before_cov is None or after_cov is None else round(after_cov - before_cov, 4),
        "player_stats_ta_parity_delta": None if before_parity is None or after_parity is None else round(after_parity - before_parity, 4),
        "no_regression": {
            "player_stats_coverage": no_regression_coverage,
            "player_stats_ta_parity": no_regression_parity,
            "overall": no_regression_coverage and no_regression_parity,
        },
    }


def _markdown_changelog(change_label: str, change_run_id: str, before: dict[str, Any], after: dict[str, Any], diff: dict[str, Any]) -> str:
    return "\n".join(
        [
            f"### {change_label}",
            f"- Change pivot run_id: `{change_run_id}`",
            f"- Before window run_ids ({before.get('run_count', 0)}): {', '.join(before.get('run_ids', [])) or 'none'}",
            f"- After window run_ids ({after.get('run_count', 0)}): {', '.join(after.get('run_ids', [])) or 'none'}",
            f"- Suppression total Δ: {diff.get('suppression_total_delta', 0):+d}",
            f"- Scored signals Δ: {diff.get('scored_signals_delta', 0):+d}",
            f"- Sent notifications Δ: {diff.get('sent_notifications_delta', 0):+d}",
            "- Player-stats no-regression gate: "
            + ("PASS" if diff.get("no_regression", {}).get("overall") else "FAIL"),
        ]
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build weekly GS-focused signal/suppression quality report")
    parser.add_argument("--input", default="exports_live/Run_Log.csv", help="Run_Log CSV/JSON path")
    parser.add_argument("--as-of", default="", help="UTC timestamp (ISO-8601) used as report end; default=now")
    parser.add_argument("--change-run-id", default="", help="Run ID where a suppression/control tuning change started")
    parser.add_argument("--weekly-window-days", type=int, default=7)
    parser.add_argument("--rolling-window-days", type=int, default=3)
    parser.add_argument("--change-label", default="Suppression tuning change")
    parser.add_argument("--json-out", default="", help="Optional JSON output path")
    parser.add_argument("--markdown-out", default="", help="Optional markdown changelog output path")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    path = Path(args.input)
    rows = _load_rows(path)
    snapshots = build_snapshots(rows)
    if not snapshots:
        raise SystemExit("No runEdgeBoard summary snapshots found in input.")

    as_of = _parse_ts(args.as_of) if args.as_of else snapshots[-1].ended_at
    if as_of is None:
        raise SystemExit("Invalid --as-of timestamp.")

    weekly_start = as_of - timedelta(days=max(1, args.weekly_window_days))
    weekly = _window(snapshots, weekly_start, as_of)
    weekly_summary = _aggregate(weekly)
    top_two = _top_suppression_buckets(weekly_summary, top_n=2)

    report: dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "input": str(path),
        "weekly_window": {
            "start_utc": weekly_start.isoformat(),
            "end_utc": as_of.isoformat(),
            "summary": weekly_summary,
            "top_suppression_buckets": top_two,
        },
    }

    if args.change_run_id:
        pivot = next((s for s in snapshots if s.run_id == args.change_run_id), None)
        if pivot is None:
            raise SystemExit(f"change_run_id not found: {args.change_run_id}")

        rolling_days = max(1, args.rolling_window_days)
        before = _aggregate(_window(snapshots, pivot.ended_at - timedelta(days=rolling_days), pivot.ended_at))
        after = _aggregate(_window(snapshots, pivot.ended_at, pivot.ended_at + timedelta(days=rolling_days)))
        diff = _compare_windows(before, after)
        report["tuning_effect"] = {
            "change_label": args.change_label,
            "change_run_id": args.change_run_id,
            "window_days": rolling_days,
            "before": before,
            "after": after,
            "diff": diff,
        }
        report["runbook_changelog_markdown"] = _markdown_changelog(
            args.change_label,
            args.change_run_id,
            before,
            after,
            diff,
        )

    rendered = json.dumps(report, indent=2, sort_keys=True)
    print(rendered)

    if args.json_out:
        out_path = Path(args.json_out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(rendered + "\n", encoding="utf-8")

    if args.markdown_out and report.get("runbook_changelog_markdown"):
        md_path = Path(args.markdown_out)
        md_path.parent.mkdir(parents=True, exist_ok=True)
        md_path.write_text(str(report["runbook_changelog_markdown"]) + "\n", encoding="utf-8")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
