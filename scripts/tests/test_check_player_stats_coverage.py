import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from check_player_stats_coverage import GateConfig, evaluate_player_stats_gate  # noqa: E402


class PlayerStatsCoverageGateTests(unittest.TestCase):
    def _rows(self, baseline_reason_codes=None, candidate_reason_codes=None, baseline_meta=None, candidate_meta=None):
        baseline_reason_codes = baseline_reason_codes if baseline_reason_codes is not None else {"STATS_MISS_A": 1, "STATS_MISS_B": 1}
        candidate_reason_codes = candidate_reason_codes if candidate_reason_codes is not None else {"STATS_MISS_A": 1, "STATS_MISS_B": 1}
        baseline_meta = baseline_meta if baseline_meta is not None else {
            "requested_player_count": 10,
            "resolved_player_count": 8,
            "unresolved_player_count": 2,
        }
        candidate_meta = candidate_meta if candidate_meta is not None else {
            "requested_player_count": 10,
            "resolved_player_count": 8,
            "unresolved_player_count": 2,
        }
        return [
            {
                "run_id": "run-a",
                "row_type": "summary",
                "stage": "runEdgeBoard",
                "reason_codes": baseline_reason_codes,
                "stage_summaries": [{"stage": "stageFetchPlayerStats", "reason_metadata": baseline_meta}],
            },
            {
                "run_id": "run-b",
                "row_type": "summary",
                "stage": "runEdgeBoard",
                "reason_codes": candidate_reason_codes,
                "stage_summaries": [{"stage": "stageFetchPlayerStats", "reason_metadata": candidate_meta}],
            },
        ]

    def test_gate_passes_when_thresholds_hold(self):
        rows = self._rows()
        config = GateConfig(min_resolved_rate=0.6, max_unresolved_players=3, max_missing_side_increase=0)
        report = evaluate_player_stats_gate(rows, "run-a", "run-b", config)
        self.assertEqual("pass", report["status"])
        self.assertEqual([], report["failures"])

    def test_gate_fails_on_resolved_rate_and_missing_deltas(self):
        rows = self._rows(
            baseline_reason_codes={"STATS_MISS_A": 1, "STATS_MISS_B": 1},
            candidate_reason_codes={"STATS_MISS_A": 3, "STATS_MISS_B": 2},
            candidate_meta={"requested_player_count": 10, "resolved_player_count": 5, "unresolved_player_count": 5},
        )
        config = GateConfig(min_resolved_rate=0.6, max_unresolved_players=4, max_missing_side_increase=0)
        report = evaluate_player_stats_gate(rows, "run-a", "run-b", config)
        self.assertEqual("fail", report["status"])
        self.assertTrue(any("player_stats_resolved_rate_below_min" in failure for failure in report["failures"]))
        self.assertTrue(any("stats_missing_player_a_increase_exceeded" in failure for failure in report["failures"]))
        self.assertTrue(any("stats_missing_player_b_increase_exceeded" in failure for failure in report["failures"]))

    def test_gate_override_allows_exit(self):
        rows = self._rows(candidate_meta={"requested_player_count": 10, "resolved_player_count": 3, "unresolved_player_count": 7})
        config = GateConfig(min_resolved_rate=0.6, max_unresolved_players=2, max_missing_side_increase=0, override_reason="INC-123")
        report = evaluate_player_stats_gate(rows, "run-a", "run-b", config)
        self.assertEqual("override", report["status"])
        self.assertTrue(report["override_used"])

    def test_override_does_not_bypass_schema_missing(self):
        rows = self._rows(candidate_meta={})
        config = GateConfig(
            min_resolved_rate=0.9,
            max_unresolved_players=0,
            max_missing_side_increase=0,
            override_reason="INC-999",
        )
        report = evaluate_player_stats_gate(rows, "run-a", "run-b", config)
        self.assertEqual("schema_missing", report["status"])
        self.assertEqual("schema_missing", report["reason_code"])
        self.assertEqual("override", report["coverage_gate"])
        self.assertEqual("fail", report["schema_integrity"])
        self.assertTrue(report["override_used"])
        self.assertTrue(any(failure.startswith("candidate_") for failure in report["schema_failures"]))


if __name__ == "__main__":
    unittest.main()
