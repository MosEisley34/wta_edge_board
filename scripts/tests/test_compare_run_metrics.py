import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from compare_run_metrics import build_report  # noqa: E402


class CompareRunMetricsTests(unittest.TestCase):
    def test_build_report_emits_deterministic_sections(self):
        rows = [
            {
                "row_type": "summary",
                "stage": "runEdgeBoard",
                "run_id": "run-a",
                "reason_codes": {
                    "MATCH_CT": 3,
                    "NO_P_MATCH": 1,
                    "REJ_CT": 1,
                    "STATS_ENR": 2,
                    "STATS_MISS_A": 1,
                    "STATS_MISS_B": 0,
                },
                "signal_decision_summary": {
                    "suppression_counts": {
                        "edge": {"by_reason": {"edge_below_threshold": 2}},
                        "cooldown": {"by_reason": {"cooldown_suppressed": 1}},
                    }
                },
                "stage_summaries": [
                    {"stage": "stageFetchOdds", "duration_ms": 50},
                    {"stage": "stageFetchSchedule", "duration_ms": 75},
                    {"stage": "stageMatchEvents", "duration_ms": 100},
                    {"stage": "stageFetchPlayerStats", "duration_ms": 200},
                    {"stage": "stageGenerateSignals", "duration_ms": 30},
                    {"stage": "stagePersist", "duration_ms": 40},
                ],
            },
            {
                "row_type": "summary",
                "stage": "runEdgeBoard",
                "run_id": "run-b",
                "reason_codes": {
                    "MATCH_CT": 4,
                    "NO_P_MATCH": 2,
                    "REJ_CT": 2,
                    "STATS_ENR": 1,
                    "STATS_MISS_A": 0,
                    "STATS_MISS_B": 1,
                },
                "signal_decision_summary": {
                    "suppression_counts": {
                        "edge": {"by_reason": {"edge_below_threshold": 3}},
                        "stale": {"by_reason": {"stale_odds_skip": 2}},
                    }
                },
                "stage_summaries": [
                    {"stage": "stageFetchOdds", "duration_ms": 45},
                    {"stage": "stageFetchSchedule", "duration_ms": 70},
                    {"stage": "stageMatchEvents", "duration_ms": 90},
                    {"stage": "stageFetchPlayerStats", "duration_ms": 230},
                    {"stage": "stageGenerateSignals", "duration_ms": 35},
                    {"stage": "stagePersist", "duration_ms": 42},
                ],
            },
        ]

        report = build_report(rows, "run-a", "run-b")

        self.assertIn("run_comparator left=run-a right=run-b", report)
        self.assertIn("[core_metrics]", report)
        self.assertIn("MATCH_CT", report)
        self.assertIn("STATS_MISS_B", report)
        self.assertIn("[signal_suppression_reasons]", report)
        self.assertIn("cooldown_suppressed", report)
        self.assertIn("stale_odds_skip", report)
        self.assertIn("[per_stage_duration_ms]", report)
        self.assertIn("stageFetchPlayerStats", report)

    def test_build_report_fails_when_missing_run(self):
        with self.assertRaises(ValueError):
            build_report([], "run-a", "run-b")

    def test_build_report_fails_when_run_has_disallowed_skip_reason(self):
        rows = [
            {
                "row_type": "summary",
                "stage": "runEdgeBoard",
                "run_id": "run-a",
                "reason_codes": {},
                "signal_decision_summary": {},
                "stage_summaries": [
                    {"stage": "stageFetchOdds", "duration_ms": 10},
                    {"stage": "stageFetchSchedule", "duration_ms": 10},
                    {"stage": "stageMatchEvents", "duration_ms": 10},
                    {"stage": "stageFetchPlayerStats", "duration_ms": 10},
                    {"stage": "stageGenerateSignals", "duration_ms": 10},
                    {"stage": "stagePersist", "duration_ms": 10},
                ],
            },
            {
                "row_type": "diag",
                "stage": "runEdgeBoard",
                "run_id": "run-a",
                "reason_code": "run_locked_skip",
            },
            {
                "row_type": "summary",
                "stage": "runEdgeBoard",
                "run_id": "run-b",
                "reason_codes": {},
                "signal_decision_summary": {},
                "stage_summaries": [
                    {"stage": "stageFetchOdds", "duration_ms": 10},
                    {"stage": "stageFetchSchedule", "duration_ms": 10},
                    {"stage": "stageMatchEvents", "duration_ms": 10},
                    {"stage": "stageFetchPlayerStats", "duration_ms": 10},
                    {"stage": "stageGenerateSignals", "duration_ms": 10},
                    {"stage": "stagePersist", "duration_ms": 10},
                ],
            },
        ]

        with self.assertRaisesRegex(ValueError, "replacement run IDs required"):
            build_report(rows, "run-a", "run-b")

    def test_build_report_fails_when_run_is_missing_required_stage_chain(self):
        rows = [
            {
                "row_type": "summary",
                "stage": "runEdgeBoard",
                "run_id": "run-a",
                "reason_codes": {},
                "signal_decision_summary": {},
                "stage_summaries": [
                    {"stage": "stageFetchOdds", "duration_ms": 10},
                    {"stage": "stageFetchSchedule", "duration_ms": 10},
                    {"stage": "stageMatchEvents", "duration_ms": 10},
                    {"stage": "stageFetchPlayerStats", "duration_ms": 10},
                    {"stage": "stageGenerateSignals", "duration_ms": 10},
                    {"stage": "stagePersist", "duration_ms": 10},
                ],
            },
            {
                "row_type": "summary",
                "stage": "runEdgeBoard",
                "run_id": "run-b",
                "reason_codes": {},
                "signal_decision_summary": {},
                "stage_summaries": [
                    {"stage": "stageFetchOdds", "duration_ms": 10},
                    {"stage": "stageFetchSchedule", "duration_ms": 10},
                    {"stage": "stageMatchEvents", "duration_ms": 10},
                    {"stage": "stageFetchPlayerStats", "duration_ms": 10},
                    {"stage": "stagePersist", "duration_ms": 10},
                ],
            },
        ]

        with self.assertRaisesRegex(ValueError, "missing stage chain entries"):
            build_report(rows, "run-a", "run-b")


if __name__ == "__main__":
    unittest.main()
