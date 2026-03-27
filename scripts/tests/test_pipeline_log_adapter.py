import json
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from pipeline_log_adapter import adapt_run_log_record_for_legacy  # noqa: E402


class PipelineLogAdapterTests(unittest.TestCase):
    MIXED_QUALITY_CONTRACT_FIXTURE = {
        "feature_completeness": 0.81,
        "matched_events": 22,
        "scored_signals": 14,
        "reason_aliases": {"UNK_OPEN_TS": "missing_open_timestamp"},
    }

    LEGACY_ALIAS_IN_METRIC_FIXTURE = {
        "feature_completeness": json.dumps({"UNK_OPEN_TS": "missing_open_timestamp", "UNK_OPEN_LAG": "opening_lag_exceeded"}),
        "matched_events": 7,
        "scored_signals": 3,
    }

    def test_legacy_row_expands_alias_reason_maps(self):
        adapted = adapt_run_log_record_for_legacy(
            {
                "row_type": "summary",
                "run_id": "run-1",
                "stage": "runEdgeBoard",
                "message": json.dumps({"reason_codes": {"OR_OUT_WIN": 2}}),
                "rejection_codes": json.dumps(
                    {"schema_id": "reason_code_alias_v1", "reason_codes": {"OPEN_LAG_HI": 1}}
                ),
                "stage_summaries": json.dumps(
                    {
                        "schema_id": "reason_code_alias_v1",
                        "stage_summaries": [
                            {"stage": "stageFetchOdds", "reason_codes": {"MM_DIAG_WR": 3}},
                        ],
                    }
                ),
            }
        )

        message = json.loads(adapted["message"])
        rejection_codes = json.loads(adapted["rejection_codes"])
        stage_summaries = json.loads(adapted["stage_summaries"])

        self.assertEqual(2, message["reason_codes"]["odds_refresh_skipped_outside_window"])
        self.assertEqual(1, rejection_codes["opening_lag_exceeded"])
        self.assertEqual(3, stage_summaries[0]["reason_codes"]["match_map_diagnostic_records_written"])

    def test_compact_v2_alias_only_message_reason_codes_expand_to_canonical(self):
        adapted = adapt_run_log_record_for_legacy(
            {
                "schema_version": 2,
                "et": "summary",
                "rid": "run-v2-msg",
                "st": "runEdgeBoard",
                "ss": "success",
                "msg": {"schema_id": "reason_code_alias_v1", "reason_codes": {"OR_OUT_WIN": 4}},
                "rc": {},
            }
        )

        message = json.loads(adapted["message"])
        self.assertEqual(4, message["reason_codes"]["odds_refresh_skipped_outside_window"])

    def test_compact_v2_row_reconstructed_to_legacy_shape(self):
        adapted = adapt_run_log_record_for_legacy(
            {
                "schema_version": 2,
                "et": "stageFetchOdds",
                "rid": "run-v2",
                "st": "stageFetchOdds",
                "sa": "2026-03-12T12:00:00Z",
                "ea": "2026-03-12T12:00:02Z",
                "ss": "success",
                "rcd": "stage_completed",
                "ic": 10,
                "oc": 5,
                "pr": "odds_api",
                "acu": 1,
                "rc": {"OR_OUT_WIN": 2},
                "rm": {"resolver": "canonical"},
                "msg": {"context": "compact"},
                "rj": {"OPEN_LAG_HI": 1},
                "ssu": [{"stage": "stageFetchOdds", "reason_codes": {"MM_DIAG_WR": 2}}],
            }
        )

        message = json.loads(adapted["message"])
        rejection_codes = json.loads(adapted["rejection_codes"])
        stage_summaries = json.loads(adapted["stage_summaries"])

        self.assertEqual("stage", adapted["row_type"])
        self.assertEqual("run-v2", adapted["run_id"])
        self.assertEqual(2, message["reason_codes"]["odds_refresh_skipped_outside_window"])
        self.assertEqual("odds_api", message["provider"])
        self.assertEqual(1, rejection_codes["opening_lag_exceeded"])
        self.assertEqual(2, stage_summaries[0]["reason_codes"]["match_map_diagnostic_records_written"])

    def test_compact_v2_fallback_alias_map_round_trips_to_canonical(self):
        adapted = adapt_run_log_record_for_legacy(
            {
                "schema_version": 2,
                "et": "stageFetchOdds",
                "rid": "run-v2-fallback",
                "st": "stageFetchOdds",
                "rm": {"fallback_aliases": {"UNK_1ABC": "long_unknown_reason_code"}},
                "rc": {"UNK_1ABC": 3},
                "msg": {
                    "schema_id": "reason_code_alias_v1",
                    "reason_codes": {"UNK_1ABC": 3},
                    "fallback_aliases": {"UNK_1ABC": "long_unknown_reason_code"},
                },
            }
        )

        message = json.loads(adapted["message"])
        self.assertEqual(3, message["reason_codes"]["long_unknown_reason_code"])

    def test_compact_v2_supports_new_alias_entries(self):
        adapted = adapt_run_log_record_for_legacy(
            {
                "schema_version": 2,
                "et": "summary",
                "rid": "run-v2-new-alias",
                "st": "runEdgeBoard",
                "msg": {"schema_id": "reason_code_alias_v1", "reason_codes": {"PO_MIT_ON": 1}},
            }
        )
        message = json.loads(adapted["message"])
        self.assertEqual(1, message["reason_codes"]["productive_output_mitigation_activated"])

    def test_compact_v2_supports_stage_persist_alias_entries(self):
        adapted = adapt_run_log_record_for_legacy(
            {
                "schema_version": 2,
                "et": "stagePersist",
                "rid": "run-v2-persist-aliases",
                "st": "stagePersist",
                "msg": {
                    "schema_id": "reason_code_alias_v1",
                    "reason_codes": {"ODDS_UPS": 2, "SCH_UPS": 1, "PSTATS_UPS": 1, "MM_UPS": 1, "SIG_UPS": 1},
                },
            }
        )
        message = json.loads(adapted["message"])
        self.assertEqual(2, message["reason_codes"]["raw_odds_upserts"])
        self.assertEqual(1, message["reason_codes"]["raw_schedule_upserts"])
        self.assertEqual(1, message["reason_codes"]["raw_player_stats_upserts"])
        self.assertEqual(1, message["reason_codes"]["match_map_upserts"])
        self.assertEqual(1, message["reason_codes"]["signals_upserts"])

    def test_compact_v2_supports_recurring_alias_entries(self):
        adapted = adapt_run_log_record_for_legacy(
            {
                "schema_version": 2,
                "et": "summary",
                "rid": "run-v2-recurring-aliases",
                "st": "runEdgeBoard",
                "msg": {
                    "schema_id": "reason_code_alias_v1",
                    "reason_codes": {
                        "RUN_START": 1,
                        "RUN_DONE": 1,
                        "SRC_CRED_SKIP": 1,
                        "SCH_WIN_FB_NO": 1,
                        "CREDIT_HDR": 1,
                    },
                },
            }
        )
        message = json.loads(adapted["message"])
        self.assertEqual(1, message["reason_codes"]["started"])
        self.assertEqual(1, message["reason_codes"]["completed"])
        self.assertEqual(1, message["reason_codes"]["source_credit_saver_skip"])
        self.assertEqual(1, message["reason_codes"]["schedule_window_fallback_no_odds"])
        self.assertEqual(1, message["reason_codes"]["credit_header_missing"])

    def test_compact_v2_normalizes_recurring_legacy_unk_aliases_without_fallback_metadata(self):
        adapted = adapt_run_log_record_for_legacy(
            {
                "schema_version": 2,
                "et": "summary",
                "rid": "run-v2-legacy-unk",
                "st": "runEdgeBoard",
                "msg": {
                    "schema_id": "reason_code_alias_v1",
                    "reason_codes": {
                        "UNK_OPEN_TS": 2,
                        "UNK_OPEN_LAG": 1,
                    },
                },
            }
        )
        message = json.loads(adapted["message"])
        self.assertEqual(2, message["reason_codes"]["missing_open_timestamp"])
        self.assertEqual(1, message["reason_codes"]["opening_lag_exceeded"])

    def test_projects_quality_contract_fields_from_signal_summary(self):
        adapted = adapt_run_log_record_for_legacy(
            {
                "row_type": "summary",
                "run_id": "run-qc",
                "stage": "runEdgeBoard",
                "signal_decision_summary": json.dumps(
                    {
                        "quality_contract": {
                            "feature_completeness": 0.75,
                            "feature_completeness_reason_code": "resolved_rate_from_player_stats_coverage",
                            "edge_volatility": 0.01,
                            "edge_volatility_reason_code": "edge_volatility_abs_delta_p95",
                            "matched_events": 13,
                            "matched_events_reason_code": "stage_generate_signals_input_count",
                            "scored_signals": 9,
                            "scored_signals_reason_code": "signal_decision_summary_scored_count",
                        }
                    }
                ),
            }
        )
        self.assertEqual(0.75, adapted["feature_completeness"])
        self.assertEqual(0.01, adapted["edge_volatility"])
        self.assertEqual(13, adapted["matched_events"])
        self.assertEqual(9, adapted["scored_signals"])
        self.assertEqual(
            "resolved_rate_from_player_stats_coverage", adapted["feature_completeness_reason_code"]
        )
        self.assertEqual("edge_volatility_abs_delta_p95", adapted["edge_volatility_reason_code"])
        self.assertEqual("stage_generate_signals_input_count", adapted["matched_events_reason_code"])
        self.assertEqual("signal_decision_summary_scored_count", adapted["scored_signals_reason_code"])

    def test_quality_contract_keeps_alias_payload_in_reason_aliases_field(self):
        adapted = adapt_run_log_record_for_legacy(
            {
                "row_type": "summary",
                "run_id": "run-qc-mixed",
                "stage": "runEdgeBoard",
                "signal_decision_summary": json.dumps({"quality_contract": dict(self.MIXED_QUALITY_CONTRACT_FIXTURE)}),
            }
        )
        self.assertEqual(0.81, adapted["feature_completeness"])
        self.assertEqual(22, adapted["matched_events"])
        self.assertEqual(14, adapted["scored_signals"])
        self.assertEqual({"UNK_OPEN_TS": "missing_open_timestamp"}, adapted["reason_aliases"])

    def test_quality_contract_rejects_object_or_string_overwrite_for_numeric_metrics(self):
        adapted = adapt_run_log_record_for_legacy(
            {
                "row_type": "summary",
                "run_id": "run-qc-guard",
                "stage": "runEdgeBoard",
                "feature_completeness": 0.64,
                "matched_events": 11,
                "scored_signals": 5,
                "signal_decision_summary": json.dumps(
                    {
                        "quality_contract": {
                            "feature_completeness": {"UNK_OPEN_TS": "missing_open_timestamp"},
                            "matched_events": "not_a_number",
                            "scored_signals": {"UNK_OPEN_LAG": "opening_lag_exceeded"},
                        }
                    }
                ),
            }
        )
        self.assertEqual(0.64, adapted["feature_completeness"])
        self.assertEqual(11, adapted["matched_events"])
        self.assertEqual(5, adapted["scored_signals"])
        self.assertEqual("quality_contract_metric_type_mismatch", adapted["schema_violation"])
        self.assertIn("expected_numeric_received", adapted["field_type_error"])
        self.assertEqual(
            {"UNK_OPEN_TS": "missing_open_timestamp", "UNK_OPEN_LAG": "opening_lag_exceeded"},
            adapted["reason_aliases"],
        )

    def test_migrates_historical_alias_payload_from_feature_completeness(self):
        adapted = adapt_run_log_record_for_legacy(
            {
                "row_type": "summary",
                "run_id": "run-qc-migrate",
                "stage": "runEdgeBoard",
                "feature_completeness": self.LEGACY_ALIAS_IN_METRIC_FIXTURE["feature_completeness"],
                "signal_decision_summary": json.dumps({"quality_contract": dict(self.MIXED_QUALITY_CONTRACT_FIXTURE)}),
            }
        )
        self.assertEqual(0.81, adapted["feature_completeness"])
        self.assertEqual(
            {
                "UNK_OPEN_TS": "missing_open_timestamp",
                "UNK_OPEN_LAG": "opening_lag_exceeded",
            },
            {
                "UNK_OPEN_TS": adapted["reason_aliases"]["UNK_OPEN_TS"],
                "UNK_OPEN_LAG": adapted["reason_aliases"]["UNK_OPEN_LAG"],
            },
        )



if __name__ == "__main__":
    unittest.main()
