import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from gs_signal_quality_report import build_snapshots, _aggregate, _top_suppression_buckets, _compare_windows


def _summary(run_id, ended_at, scored, sent, matched, suppression_by_reason, requested=10, resolved=8, ta=6):
    return {
        "row_type": "summary",
        "stage": "runEdgeBoard",
        "run_id": run_id,
        "ended_at": ended_at,
        "matched": matched,
        "signals_found": sent,
        "signal_decision_summary": json.dumps({
            "scored_signals": scored,
            "suppression_counts": {
                "timing": {"total": suppression_by_reason.get("too_close_to_start_skip", 0), "by_reason": {"too_close_to_start_skip": suppression_by_reason.get("too_close_to_start_skip", 0)}},
                "stale": {"total": suppression_by_reason.get("stale_odds_skip", 0), "by_reason": {"stale_odds_skip": suppression_by_reason.get("stale_odds_skip", 0)}},
                "cooldown": {"total": suppression_by_reason.get("cooldown_suppressed", 0), "by_reason": {"cooldown_suppressed": suppression_by_reason.get("cooldown_suppressed", 0)}},
            },
        }),
        "stage_summaries": json.dumps([
            {
                "stage": "stageFetchPlayerStats",
                "reason_metadata": {
                    "requested_player_count": requested,
                    "resolved_player_count": resolved,
                    "players_found_ta": ta,
                },
            }
        ]),
    }


def test_weekly_aggregate_and_top_buckets():
    rows = [
        _summary("r1", "2026-03-20T00:00:00Z", 20, 5, 15, {"too_close_to_start_skip": 8, "stale_odds_skip": 4}),
        _summary("r2", "2026-03-21T00:00:00Z", 22, 7, 16, {"too_close_to_start_skip": 7, "cooldown_suppressed": 2}),
    ]
    snapshots = build_snapshots(rows)
    summary = _aggregate(snapshots)

    assert summary["matched_events"] == 31
    assert summary["scored_signals"] == 42
    assert summary["sent_notifications"] == 12
    assert summary["suppression_total"] == 21
    assert summary["player_stats"]["coverage_rate"] == 0.8

    top = _top_suppression_buckets(summary)
    assert top[0]["reason"] == "too_close_to_start_skip"
    assert top[0]["classification"] == "avoidable"


def test_compare_windows_no_regression_gate():
    before = {
        "matched_events": 40,
        "scored_signals": 30,
        "sent_notifications": 8,
        "suppression_total": 22,
        "player_stats": {"coverage_rate": 0.8, "ta_parity_rate": 0.75},
    }
    after = {
        "matched_events": 42,
        "scored_signals": 25,
        "sent_notifications": 9,
        "suppression_total": 15,
        "player_stats": {"coverage_rate": 0.82, "ta_parity_rate": 0.78},
    }
    diff = _compare_windows(before, after)
    assert diff["suppression_total_delta"] == -7
    assert diff["no_regression"]["overall"] is True
