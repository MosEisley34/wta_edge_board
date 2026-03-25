import unittest
from pathlib import Path
import sys
import json

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from compare_run_metrics import build_report, _metric_counts, _has_stage_summary_zero_core_metrics  # noqa: E402


class CompareRunMetricsTests(unittest.TestCase):
    @staticmethod
    def _stage_chain():
        return [
            {"stage": "stageFetchOdds", "duration_ms": 10},
            {"stage": "stageFetchSchedule", "duration_ms": 10},
            {"stage": "stageMatchEvents", "duration_ms": 10},
            {"stage": "stageFetchPlayerStats", "duration_ms": 10},
            {"stage": "stageGenerateSignals", "duration_ms": 10},
            {"stage": "stagePersist", "duration_ms": 10},
        ]

    @staticmethod
    def _load_live_shape_fixtures():
        fixture_path = ROOT / "scripts" / "fixtures" / "compare_run_metrics_live_runtime_rows.json"
        payload = json.loads(fixture_path.read_text(encoding="utf-8"))
        return payload

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
                    {
                        "stage": "stageFetchPlayerStats",
                        "duration_ms": 200,
                        "reason_metadata": {
                            "requested_player_count": 10,
                            "resolved_player_count": 8,
                            "resolved_via_ta_count": 6,
                            "resolved_via_provider_fallback_count": 1,
                            "resolved_via_model_fallback_count": 1,
                            "unresolved_player_a_count": 1,
                            "unresolved_player_b_count": 1,
                            "fallback_reason_counts": {"ta_h2h_empty_table": 2},
                        },
                    },
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
                    {
                        "stage": "stageFetchPlayerStats",
                        "duration_ms": 230,
                        "reason_metadata": {
                            "requested_player_count": 10,
                            "resolved_player_count": 7,
                            "resolved_via_ta_count": 4,
                            "resolved_via_provider_fallback_count": 2,
                            "resolved_via_model_fallback_count": 1,
                            "unresolved_player_a_count": 2,
                            "unresolved_player_b_count": 1,
                            "fallback_reason_counts": {"ta_h2h_empty_table": 3, "provider_timeout": 1},
                        },
                    },
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
        self.assertIn("[player_stats_coverage_pct]", report)
        self.assertIn("[player_stats_source_mix_counts]", report)
        self.assertIn("[player_stats_unresolved_by_side]", report)
        self.assertIn("[player_stats_top_fallback_reason_deltas]", report)

    def test_build_report_fails_when_missing_run(self):
        with self.assertRaises(ValueError):
            build_report([], "run-a", "run-b")

    def test_build_report_includes_stake_policy_reason_codes(self):
        rows = [
            {
                "row_type": "summary",
                "stage": "runEdgeBoard",
                "run_id": "run-a",
                "reason_codes": {},
                "signal_decision_summary": {},
                "stage_summaries": self._stage_chain(),
            },
            {"row_type": "signal", "run_id": "run-a", "stake_mxn": 5},
            {
                "row_type": "summary",
                "stage": "runEdgeBoard",
                "run_id": "run-b",
                "reason_codes": {},
                "signal_decision_summary": {},
                "stage_summaries": self._stage_chain(),
            },
            {"row_type": "signal", "run_id": "run-b", "stake_mxn": 3},
        ]
        from stake_policy import StakePolicyConfig

        report = build_report(rows, "run-a", "run-b", StakePolicyConfig(enabled=True))
        self.assertIn("[stake_policy_counts]", report)
        self.assertIn("[stake_policy_reason_codes]", report)
        self.assertIn("stake_below_min_suppressed", report)

    def test_build_report_fails_when_run_has_disallowed_skip_reason(self):
        rows = [
            {
                "row_type": "summary",
                "stage": "runEdgeBoard",
                "run_id": "run-a",
                "reason_codes": {},
                "signal_decision_summary": {},
                "stage_summaries": self._stage_chain(),
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
                "stage_summaries": self._stage_chain(),
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
                "stage_summaries": self._stage_chain(),
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

    def test_build_report_parses_alias_envelope_core_metrics(self):
        rows = [
            {
                "row_type": "summary",
                "stage": "runEdgeBoard",
                "run_id": "run-a",
                "reason_codes": json.dumps(
                    {
                        "schema_id": "reason_code_alias_v1",
                        "reason_codes": {
                            "MATCH_CT": 4,
                            "NO_P_MATCH": 1,
                            "REJ_CT": 1,
                            "STATS_ENR": 3,
                            "STATS_MISS_A": 1,
                            "STATS_MISS_B": 0,
                        },
                    }
                ),
                "signal_decision_summary": {},
                "stage_summaries": self._stage_chain(),
            },
            {
                "row_type": "summary",
                "stage": "runEdgeBoard",
                "run_id": "run-b",
                "reason_codes": {
                    "MATCH_CT": 2,
                    "NO_P_MATCH": 0,
                    "REJ_CT": 1,
                    "STATS_ENR": 1,
                    "STATS_MISS_A": 0,
                    "STATS_MISS_B": 1,
                },
                "signal_decision_summary": {},
                "stage_summaries": self._stage_chain(),
            },
        ]

        report = build_report(rows, "run-a", "run-b")
        self.assertIn("MATCH_CT               4", report)
        self.assertIn("STATS_ENR              3", report)
        self.assertNotIn("WARNING run=run-a", report)

    def test_build_report_maps_fallback_aliases_back_to_canonical_metrics(self):
        rows = [
            {
                "row_type": "summary",
                "stage": "runEdgeBoard",
                "run_id": "run-a",
                "reason_codes": {
                    "schema_id": "reason_code_alias_v1",
                    "reason_codes": {"UNK_MATCH": 3, "UNK_STATS": 2, "STATS_MISS_A": 1, "STATS_MISS_B": 1},
                    "fallback_aliases": {"UNK_MATCH": "MATCH_CT", "UNK_STATS": "STATS_ENR"},
                },
                "signal_decision_summary": {},
                "stage_summaries": self._stage_chain(),
            },
            {
                "row_type": "summary",
                "stage": "runEdgeBoard",
                "run_id": "run-b",
                "reason_codes": {
                    "MATCH_CT": 1,
                    "NO_P_MATCH": 0,
                    "REJ_CT": 0,
                    "STATS_ENR": 1,
                    "STATS_MISS_A": 0,
                    "STATS_MISS_B": 0,
                },
                "signal_decision_summary": {},
                "stage_summaries": self._stage_chain(),
            },
        ]

        report = build_report(rows, "run-a", "run-b")
        # Regression: diagnostics may show non-zero alias counters while old metric parser showed zero.
        self.assertIn("MATCH_CT               3", report)
        self.assertIn("STATS_ENR              2", report)
        self.assertNotIn("WARNING run=run-a", report)

    def test_build_report_warns_on_stage_summaries_with_zero_core_metrics(self):
        rows = [
            {
                "row_type": "summary",
                "stage": "runEdgeBoard",
                "run_id": "run-a",
                "reason_codes": {},
                "signal_decision_summary": {},
                "stage_summaries": self._stage_chain(),
            },
            {
                "row_type": "summary",
                "stage": "runEdgeBoard",
                "run_id": "run-b",
                "reason_codes": {"MATCH_CT": 1},
                "signal_decision_summary": {},
                "stage_summaries": self._stage_chain(),
            },
        ]

        report = build_report(rows, "run-a", "run-b")
        self.assertIn("WARNING run=run-a: stage summaries detected but all core metrics parsed as zero", report)
        self.assertNotIn("WARNING run=run-b", report)

    def test_live_runtime_summary_row_shapes_parse_non_zero_core_metrics(self):
        fixture_rows = self._load_live_shape_fixtures()

        for fixture in fixture_rows:
            with self.subTest(run_id=fixture["run_id"]):
                metric_counts = _metric_counts(fixture)
                self.assertGreater(metric_counts["MATCH_CT"], 0)
                self.assertGreater(metric_counts["STATS_ENR"], 0)
                self.assertFalse(_has_stage_summary_zero_core_metrics(fixture, metric_counts))



if __name__ == "__main__":
    unittest.main()
