import unittest
import sys
import tempfile
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

from schedule_context import compute_schedule_context, schedule_context_from_export_dir


class ScheduleContextTests(unittest.TestCase):
    def test_extracts_early_round_context(self):
        context = compute_schedule_context(
            [
                {"event_id": "m1", "round": "Round of 32", "tournament_tier": "WTA 500"},
                {"event_id": "m2", "round": "Round of 32", "tournament_tier": "WTA 500"},
            ]
        )
        self.assertTrue(context["has_schedule_rows"])
        self.assertEqual(2, context["upcoming_match_count"])
        self.assertEqual("round_of_32", context["inferred_stage"])
        self.assertEqual("WTA 500", context["tournament_tier"])

    def test_extracts_quarterfinal_context(self):
        context = compute_schedule_context(
            [{"event_id": "m1", "round_name": "Quarterfinal", "tour_level": "WTA 1000"}]
        )
        self.assertEqual("quarterfinal", context["inferred_stage"])
        self.assertTrue(context["stage_inference_available"])
        self.assertEqual("none", context["stage_inference_fallback"])

    def test_extracts_semifinal_and_final_context(self):
        semifinal = compute_schedule_context([{"event_id": "m1", "competition_stage": "Semifinal"}])
        final = compute_schedule_context([{"event_id": "m2", "competition_stage": "Final"}])
        self.assertEqual("semifinal", semifinal["inferred_stage"])
        self.assertEqual("final", final["inferred_stage"])

    def test_sparse_or_missing_rows(self):
        sparse = compute_schedule_context([{"event_id": "m1"}])
        empty = compute_schedule_context([])
        self.assertTrue(sparse["has_schedule_rows"])
        self.assertEqual("schedule_rows_present_but_stage_unknown", sparse["stage_inference_fallback"])
        self.assertFalse(empty["has_schedule_rows"])
        self.assertEqual(0, empty["upcoming_match_count"])
        self.assertEqual("none", empty["stage_inference_fallback"])

    def test_schedule_context_from_export_dir_reads_raw_schedule_artifact(self):
        with tempfile.TemporaryDirectory() as tmp:
            export_dir = Path(tmp)
            (export_dir / "Raw_Schedule.json").write_text(
                json.dumps(
                    [
                        {"event_id": "m1", "round": "Semifinal", "tournament_tier": "WTA 1000"},
                        {"event_id": "m2", "round": "Semifinal", "tournament_tier": "WTA 1000"},
                    ]
                ),
                encoding="utf-8",
            )
            context = schedule_context_from_export_dir(str(export_dir))
        self.assertTrue(context["schedule_artifacts_available"])
        self.assertEqual(2, context["upcoming_match_count"])
        self.assertEqual("semifinal", context["inferred_stage"])
        self.assertEqual("raw_schedule_artifact_loaded", context["context_source_reason"])

    def test_schedule_context_from_export_dir_fallback_is_explicit_non_null_object(self):
        with tempfile.TemporaryDirectory() as tmp:
            context = schedule_context_from_export_dir(str(Path(tmp)))
        self.assertFalse(context["schedule_artifacts_available"])
        self.assertEqual("raw_schedule_artifact_missing", context["context_source_reason"])
        self.assertEqual(0, context["upcoming_match_count"])


if __name__ == "__main__":
    unittest.main()
