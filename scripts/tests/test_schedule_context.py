import unittest
import sys
import tempfile
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

from schedule_context import compute_schedule_context, schedule_context_from_export_dir


class ScheduleContextTests(unittest.TestCase):
    def test_stage_inference_token_present_rows(self):
        context = compute_schedule_context(
            [
                {"event_id": "m1", "round": "Round of 32", "tournament_tier": "WTA 500"},
                {"event_id": "m2", "round": "Round of 32", "tournament_tier": "WTA 500"},
            ]
        )
        self.assertTrue(context["has_schedule_rows"])
        self.assertEqual(2, context["upcoming_match_count"])
        self.assertEqual("round_of_32", context["inferred_stage"])
        self.assertEqual("none", context["stage_inference_fallback"])
        self.assertEqual("high", context["stage_inference_confidence"])
        self.assertEqual("direct_stage_token", context["stage_inference_source"])
        self.assertEqual("WTA 500", context["tournament_tier"])
        self.assertEqual(2, context["global_upcoming_match_count"])
        self.assertEqual(2, context["tournament_tier_upcoming_match_count"])

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

    def test_stage_inference_token_missing_but_inferable_from_match_labels(self):
        semifinal = compute_schedule_context(
            [
                {"event_id": "m1", "match_label": "Semi-Final 1", "tournament_tier": "WTA 1000"},
                {"event_id": "m2", "match_name": "Semi Final 2", "tournament_tier": "WTA 1000"},
            ]
        )
        final = compute_schedule_context(
            [{"event_id": "m3", "event_name": "Championship Final", "tournament_tier": "WTA 500"}]
        )
        self.assertEqual("semifinal", semifinal["inferred_stage"])
        self.assertEqual("fallback_match_label_token", semifinal["stage_inference_fallback"])
        self.assertEqual("high", semifinal["stage_inference_confidence"])
        self.assertEqual("label_or_round_hint", semifinal["stage_inference_source"])
        self.assertEqual("final", final["inferred_stage"])
        self.assertEqual("fallback_match_label_token", final["stage_inference_fallback"])

    def test_stage_inference_token_missing_but_inferable_from_row_count(self):
        semifinal = compute_schedule_context(
            [
                {"event_id": "m1", "start_time": "2026-03-02T12:00:00Z"},
                {"event_id": "m2", "start_time": "2026-03-02T14:00:00Z"},
            ]
        )
        final = compute_schedule_context([{"event_id": "m3", "start_time": "2026-03-02T16:00:00Z"}])
        self.assertEqual("semifinal", semifinal["inferred_stage"])
        self.assertEqual("fallback_two_matches_remaining", semifinal["stage_inference_fallback"])
        self.assertEqual("medium", semifinal["stage_inference_confidence"])
        self.assertEqual("scope_row_count_with_time_cluster", semifinal["stage_inference_source"])
        self.assertIsNone(final["inferred_stage"])
        self.assertEqual("schedule_rows_present_but_stage_unknown", final["stage_inference_fallback"])

    def test_stage_inference_non_inferable_rows(self):
        sparse = compute_schedule_context([{"event_id": "m1"}])
        empty = compute_schedule_context([])
        self.assertTrue(sparse["has_schedule_rows"])
        self.assertIsNone(sparse["inferred_stage"])
        self.assertEqual("schedule_rows_present_but_stage_unknown", sparse["stage_inference_fallback"])
        non_inferable = compute_schedule_context(
            [
                {"event_id": "m2", "start_time": "2026-03-02T12:00:00Z"},
                {"event_id": "m3", "start_time": "2026-03-02T14:00:00Z"},
                {"event_id": "m4", "start_time": "2026-03-02T16:00:00Z"},
            ]
        )
        self.assertTrue(non_inferable["has_schedule_rows"])
        self.assertIsNone(non_inferable["inferred_stage"])
        self.assertEqual("schedule_rows_present_but_stage_unknown", non_inferable["stage_inference_fallback"])
        self.assertEqual("low", non_inferable["stage_inference_confidence"])
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

    def test_multi_tournament_schedule_exposes_scoped_counts(self):
        context = compute_schedule_context(
            [
                {"event_id": "m1", "round": "Quarterfinal", "tournament_tier": "WTA 1000", "tournament_id": "miami"},
                {"event_id": "m2", "round": "Quarterfinal", "tournament_tier": "WTA 1000", "tournament_id": "miami"},
                {"event_id": "m3", "round": "Quarterfinal", "tournament_tier": "WTA 250", "tournament_id": "bogota"},
                {"event_id": "m4", "round": "Quarterfinal", "tournament_tier": "WTA 250", "tournament_id": "bogota"},
                {"event_id": "m5", "round": "Quarterfinal", "tournament_tier": "WTA 250", "tournament_id": "bogota"},
            ]
        )
        self.assertEqual(5, context["global_upcoming_match_count"])
        self.assertEqual(3, context["tournament_tier_upcoming_match_count"])
        self.assertEqual(3, context["same_tournament_context_upcoming_match_count"])
        self.assertEqual("wta 250", context["primary_tournament_tier_scope_token"])
        self.assertEqual("bogota", context["primary_tournament_context_scope_token"])

    def test_infers_semifinal_from_distribution_and_tournament_context(self):
        context = compute_schedule_context(
            [
                {"event_id": "m1", "tournament_id": "miami", "start_time": "2026-03-02T12:00:00Z"},
                {"event_id": "m2", "tournament_id": "miami", "start_time": "2026-03-02T14:00:00Z"},
                {"event_id": "m3", "tournament_id": "doha", "start_time": "2026-03-03T10:00:00Z"},
            ]
        )
        self.assertEqual("semifinal", context["inferred_stage"])
        self.assertEqual("fallback_two_matches_remaining", context["stage_inference_fallback"])
        self.assertEqual("high", context["stage_inference_confidence"])
        self.assertEqual("scope_row_count_with_time_cluster", context["stage_inference_source"])

    def test_rows_present_but_ambiguous_stage_reports_low_confidence(self):
        context = compute_schedule_context(
            [
                {"event_id": "m1", "tournament_id": "miami", "start_time": "2026-03-02T12:00:00Z"},
                {"event_id": "m2", "tournament_id": "doha", "start_time": "2026-03-02T14:00:00Z"},
                {"event_id": "m3", "tournament_id": "bogota", "start_time": "2026-03-02T16:00:00Z"},
            ]
        )
        self.assertIsNone(context["inferred_stage"])
        self.assertEqual("low", context["stage_inference_confidence"])
        self.assertEqual("mixed_tournament_distribution", context["stage_inference_source"])

    def test_mixed_tournament_rows_keep_stage_unknown(self):
        context = compute_schedule_context(
            [
                {"event_id": "m1", "tournament_id": "miami", "start_time": "2026-03-02T12:00:00Z"},
                {"event_id": "m2", "tournament_id": "miami", "start_time": "2026-03-02T13:00:00Z"},
                {"event_id": "m3", "tournament_id": "doha", "start_time": "2026-03-02T12:00:00Z"},
                {"event_id": "m4", "tournament_id": "doha", "start_time": "2026-03-02T13:00:00Z"},
            ]
        )
        self.assertIsNone(context["inferred_stage"])
        self.assertEqual("schedule_rows_present_but_stage_unknown", context["stage_inference_fallback"])
        self.assertEqual("mixed_tournament_distribution", context["stage_inference_source"])


if __name__ == "__main__":
    unittest.main()
