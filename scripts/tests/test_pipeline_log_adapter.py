import json
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from pipeline_log_adapter import adapt_run_log_record_for_legacy  # noqa: E402


class PipelineLogAdapterTests(unittest.TestCase):
    def test_legacy_row_expands_alias_reason_maps(self):
        adapted = adapt_run_log_record_for_legacy(
            {
                "row_type": "summary",
                "run_id": "run-1",
                "stage": "runEdgeBoard",
                "message": json.dumps({"reason_codes": {"or_out_win": 2}}),
                "rejection_codes": json.dumps(
                    {"schema_id": "reason_code_alias_v1", "reason_codes": {"open_lag_hi": 1}}
                ),
                "stage_summaries": json.dumps(
                    {
                        "schema_id": "reason_code_alias_v1",
                        "stage_summaries": [
                            {"stage": "stageFetchOdds", "reason_codes": {"mm_diag_wr": 3}},
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
                "rc": {"or_out_win": 2},
                "rm": {"resolver": "canonical"},
                "msg": {"context": "compact"},
                "rj": {"open_lag_hi": 1},
                "ssu": [{"stage": "stageFetchOdds", "reason_codes": {"mm_diag_wr": 2}}],
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


if __name__ == "__main__":
    unittest.main()
