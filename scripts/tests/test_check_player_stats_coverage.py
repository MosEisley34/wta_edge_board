import unittest
from pathlib import Path
import sys
import json

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
        rows[1]["stage_summaries"][0]["input_count"] = 3
        rows[1]["stage_summaries"][0]["output_count"] = 1
        config = GateConfig(
            min_resolved_rate=0.9,
            max_unresolved_players=0,
            max_missing_side_increase=0,
            override_reason="INC-999",
        )
        report = evaluate_player_stats_gate(rows, "run-a", "run-b", config)
        self.assertEqual("schema_missing", report["status"])
        self.assertEqual("schema_missing", report["reason_code"])
        self.assertIn("missing_coverage_counters", report["schema_missing_details"])
        self.assertIn("parser_contract_mismatch", report["schema_missing_details"])
        self.assertEqual("pass", report["coverage_gate"])
        self.assertEqual("fail", report["schema_integrity"])
        self.assertFalse(report["override_used"])
        self.assertTrue(any(failure.startswith("candidate_") for failure in report["schema_failures"]))

    def test_schema_missing_details_include_missing_summary(self):
        rows = self._rows()
        rows[1]["stage_summaries"] = []
        report = evaluate_player_stats_gate(rows, "run-a", "run-b", GateConfig())
        self.assertEqual("schema_missing", report["status"])
        self.assertIn("missing_summary", report["schema_missing_details"])
        self.assertIn("upstream_payload_empty_or_changed_shape", report["schema_missing_details"])

    def test_legacy_stage_message_coverage_aliases_are_accepted(self):
        rows = self._rows(
            baseline_meta={},
            candidate_meta={},
        )
        rows[0]["stage_summaries"][0] = {
            "stage": "stageFetchPlayerStats",
            "input_count": 4,
            "output_count": 4,
            "message": {
                "coverage": {
                    "requested_players": 4,
                    "resolved_players": 3,
                    "unresolved_players": 1,
                    "coverage_rate": 0.75,
                }
            },
        }
        rows[1]["stage_summaries"][0] = {
            "stage": "stageFetchPlayerStats",
            "input_count": 4,
            "output_count": 4,
            "message": {
                "coverage": {
                    "requested_players": 4,
                    "resolved_players": 3,
                    "unresolved_players": 1,
                    "coverage_rate": 0.75,
                }
            },
        }
        config = GateConfig(min_resolved_rate=0.7, max_unresolved_players=2, max_missing_side_increase=0)
        report = evaluate_player_stats_gate(rows, "run-a", "run-b", config)
        self.assertEqual("pass", report["status"])
        self.assertEqual([], report["schema_failures"])

    def test_requested_zero_is_non_fatal_no_demand(self):
        fixture_path = ROOT / "scripts" / "fixtures" / "player_stats_legacy_window_2026-03-26.json"
        rows = json.loads(fixture_path.read_text(encoding="utf-8"))
        config = GateConfig(min_resolved_rate=0.9, max_unresolved_players=0, max_missing_side_increase=0)
        report = evaluate_player_stats_gate(rows, "legacy-base", "legacy-candidate", config)
        self.assertEqual("pass", report["status"])
        self.assertEqual([], report["schema_failures"])
        self.assertIn("candidate_no_demand_not_applicable", report["coverage_notes"])
        self.assertIn("no_demand_not_applicable", report["schema_missing_details"])

    def test_requested_zero_missing_counters_only_is_warn(self):
        rows = self._rows(
            baseline_meta={"requested_player_count": 5, "resolved_player_count": 5, "unresolved_player_count": 0},
            candidate_meta={"notes": "no players requested in this run"},
        )
        config = GateConfig(min_resolved_rate=0.9, max_unresolved_players=0, max_missing_side_increase=0)
        report = evaluate_player_stats_gate(rows, "run-a", "run-b", config)
        self.assertEqual("warn", report["status"])
        self.assertEqual([], report["schema_failures"])
        self.assertIn(
            "player-stats coverage counters missing_without_demand_evidence",
            report["schema_warnings"]["candidate"],
        )
        self.assertIn("candidate_no_demand_not_applicable", report["coverage_notes"])
        self.assertIn("no_demand_not_applicable", report["schema_missing_details"])


if __name__ == "__main__":
    unittest.main()
