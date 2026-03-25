import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from gs_signal_quality_report import build_snapshots, _aggregate, _top_suppression_buckets, _compare_windows
from stake_policy import StakePolicyConfig


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
    assert summary["suppression_bucket_mix"][0]["bucket"] == "timing"
    assert summary["player_stats"]["coverage_rate"] == 0.8

    top = _top_suppression_buckets(summary)
    assert top[0]["bucket"] == "timing"
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


def test_top_bucket_defaults_when_all_zero():
    summary = {
        "suppression_bucket_mix": [
            {"bucket": "cooldown", "count": 0, "share": 0},
            {"bucket": "edge", "count": 0, "share": 0},
            {"bucket": "stale", "count": 0, "share": 0},
            {"bucket": "timing", "count": 0, "share": 0},
        ]
    }
    top = _top_suppression_buckets(summary, top_n=1)
    assert top[0]["bucket"] == "cooldown"


def test_suppression_extraction_from_message_and_stage_variants():
    rows = json.loads((ROOT / "scripts" / "fixtures" / "gs_signal_quality_recent_suppressions.json").read_text(encoding="utf-8"))

    snapshots = build_snapshots(rows)
    summary = _aggregate(snapshots)

    assert summary["suppression_total"] == 17
    reason_counts = {entry["reason"]: entry["count"] for entry in summary["suppression_reason_mix"]}
    assert reason_counts["too_close_to_start_skip"] == 8
    assert reason_counts["stale_odds_skip"] == 4
    assert reason_counts["edge_below_threshold"] == 4
    assert reason_counts["cooldown_suppressed"] == 1


def test_aggregate_includes_stake_policy_counts():
    rows = [
        _summary("r1", "2026-03-20T00:00:00Z", 20, 5, 15, {"too_close_to_start_skip": 1}),
        {"row_type": "diag", "stage": "stageGenerateSignals", "run_id": "r1", "stake_mxn": 2},
        _summary("r2", "2026-03-21T00:00:00Z", 20, 5, 15, {"too_close_to_start_skip": 1}),
        {"row_type": "diag", "stage": "stageGenerateSignals", "run_id": "r2", "stake_mxn": 12},
    ]
    snapshots = build_snapshots(rows, stake_policy_config=StakePolicyConfig(enabled=True))
    summary = _aggregate(snapshots)
    assert summary["stake_policy"]["suppressed_count"] >= 1


def test_stake_policy_summary_regression_uses_fixture_based_inputs():
    fixture = json.loads((ROOT / "scripts" / "fixtures" / "stake_policy_mixed_signal_rows.json").read_text(encoding="utf-8"))
    rows = [
        _summary(fixture["run_id"], "2026-03-22T00:00:00Z", 10, 5, 8, {"too_close_to_start_skip": 1}),
        *fixture["rows"],
    ]
    snapshots = build_snapshots(
        rows,
        stake_policy_config=StakePolicyConfig(enabled=True, minimum_stake_mxn=20.0, round_to_min=False),
    )

    summary = _aggregate(snapshots)
    assert summary["stake_policy"]["suppressed_count"] == 3
    assert summary["stake_policy"]["passed_count"] == 2
    assert summary["stake_policy"]["missing_stake_count"] == 1


def test_cli_invalid_stake_policy_min_stake_has_deterministic_error():
    fixture_input = ROOT / "scripts" / "fixtures" / "gs_signal_quality_recent_suppressions.json"
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "gs_signal_quality_report.py"),
            "--input",
            str(fixture_input),
            "--stake-policy-enabled",
            "--stake-policy-min-stake-mxn",
            "not-a-number",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert "invalid float value: 'not-a-number'" in result.stderr


def test_cli_missing_stake_policy_min_stake_value_has_deterministic_error():
    fixture_input = ROOT / "scripts" / "fixtures" / "gs_signal_quality_recent_suppressions.json"
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "gs_signal_quality_report.py"),
            "--input",
            str(fixture_input),
            "--stake-policy-enabled",
            "--stake-policy-min-stake-mxn",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert "expected one argument" in result.stderr
