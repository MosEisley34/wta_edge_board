import unittest
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from compare_run_diagnostics import compare_rows  # noqa: E402
from stake_policy import StakePolicyConfig  # noqa: E402


def _full_stage_rows(run_id: str):
    return [
        {"run_id": run_id, "stage": "stageFetchOdds", "row_type": "summary"},
        {"run_id": run_id, "stage": "stageFetchSchedule", "row_type": "summary"},
        {"run_id": run_id, "stage": "stageMatchEvents", "row_type": "summary"},
        {"run_id": run_id, "stage": "stageFetchPlayerStats", "row_type": "summary"},
        {"run_id": run_id, "stage": "stageGenerateSignals", "row_type": "summary"},
        {"run_id": run_id, "stage": "stagePersist", "row_type": "summary"},
    ]


class CompareRunDiagnosticsValidationTests(unittest.TestCase):
    def test_compare_rows_fails_on_disallowed_skip_reason(self):
        rows = _full_stage_rows("run-a") + _full_stage_rows("run-b")
        rows.append({"run_id": "run-b", "stage": "runEdgeBoard", "reason_code": "run_debounced_skip"})

        with self.assertRaisesRegex(ValueError, "replacement run IDs required"):
            compare_rows(rows, "run-a", "run-b")

    def test_compare_rows_fails_on_missing_stage_chain(self):
        rows = _full_stage_rows("run-a") + [
            {"run_id": "run-b", "stage": "stageFetchOdds", "row_type": "summary"},
            {"run_id": "run-b", "stage": "stageFetchSchedule", "row_type": "summary"},
            {"run_id": "run-b", "stage": "stageMatchEvents", "row_type": "summary"},
        ]

        with self.assertRaisesRegex(ValueError, "missing stage chain entries"):
            compare_rows(rows, "run-a", "run-b")

    def test_compare_rows_emits_player_stats_metadata_sections(self):
        rows = _full_stage_rows("run-a") + _full_stage_rows("run-b")
        rows.extend(
            [
                {
                    "run_id": "run-a",
                    "stage": "runEdgeBoard",
                    "row_type": "summary",
                    "message": {
                        "stage_summaries": [
                            {
                                "stage": "stageFetchPlayerStats",
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
                            }
                        ]
                    },
                },
                {
                    "run_id": "run-b",
                    "stage": "runEdgeBoard",
                    "row_type": "summary",
                    "message": {
                        "stage_summaries": [
                            {
                                "stage": "stageFetchPlayerStats",
                                "reason_metadata": {
                                    "requested_player_count": 10,
                                    "resolved_player_count": 7,
                                    "resolved_via_ta_count": 5,
                                    "resolved_via_provider_fallback_count": 1,
                                    "resolved_via_model_fallback_count": 1,
                                    "unresolved_player_a_count": 2,
                                    "unresolved_player_b_count": 1,
                                    "fallback_reason_counts": {"ta_h2h_empty_table": 3},
                                },
                            }
                        ]
                    },
                },
            ]
        )
        report = compare_rows(rows, "run-a", "run-b")
        self.assertIn("stageFetchPlayerStats coverage", report)
        self.assertIn("source mix deltas", report)
        self.assertIn("unresolved players by side", report)
        self.assertIn("top fallback reason deltas", report)

    def test_compare_rows_emits_stake_policy_summary_sections_from_fixture_rows(self):
        fixture_path = ROOT / "scripts" / "fixtures" / "stake_policy_mixed_signal_rows.json"
        fixture = json.loads(fixture_path.read_text(encoding="utf-8"))

        run_a_fixture_rows = []
        for row in fixture["rows"]:
            if row.get("case_id") in {"wrong_stage_ignored", "wrong_run_ignored"}:
                continue
            updated = dict(row)
            updated["run_id"] = "run-a"
            run_a_fixture_rows.append(updated)

        rows = _full_stage_rows("run-a") + _full_stage_rows("run-b")
        rows.extend(
            [
                {"run_id": "run-a", "stage": "runEdgeBoard", "row_type": "summary", "message": {}},
                {"run_id": "run-b", "stage": "runEdgeBoard", "row_type": "summary", "message": {}},
                *run_a_fixture_rows,
                {"row_type": "diag", "stage": "stageGenerateSignals", "run_id": "run-b", "stake_mxn": 25},
            ]
        )

        report = compare_rows(
            rows,
            "run-a",
            "run-b",
            stake_policy_config=StakePolicyConfig(enabled=True, minimum_stake_mxn=20.0, round_to_min=False),
        )
        self.assertIn("stake_policy_enabled=true", report)
        self.assertIn("## stake-policy outcomes", report)
        self.assertIn("## stake-policy reason codes", report)
        self.assertIn("stake_below_min_suppressed", report)

    def test_compare_rows_fails_when_pair_mixes_policy_enabled_tags(self):
        rows = _full_stage_rows("run-a") + _full_stage_rows("run-b")
        rows.extend(
            [
                {
                    "run_id": "run-a",
                    "stage": "runEdgeBoard",
                    "row_type": "summary",
                    "signal_decision_summary": {"stake_policy_summary": {"enabled": True}},
                },
                {
                    "run_id": "run-b",
                    "stage": "runEdgeBoard",
                    "row_type": "summary",
                    "signal_decision_summary": {"stake_policy_summary": {"enabled": False}},
                },
            ]
        )
        with self.assertRaisesRegex(ValueError, "Mixed stake_policy_enabled states"):
            compare_rows(rows, "run-a", "run-b", stake_policy_config=StakePolicyConfig(enabled=True))


if __name__ == "__main__":
    unittest.main()
