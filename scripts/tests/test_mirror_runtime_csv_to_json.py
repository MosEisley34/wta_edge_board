import csv
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path



ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
SCRIPT = ROOT / "scripts" / "mirror_runtime_csv_to_json.py"
from runtime_artifact_codec import normalize_run_log_row
FIXTURES = ROOT / "scripts" / "fixtures"


class MirrorRuntimeCsvToJsonTypingTests(unittest.TestCase):
    def test_run_log_quality_metrics_are_typed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with (root / "Run_Log.csv").open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=["run_id", "stage", "feature_completeness", "matched_events", "scored_signals"],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "run_id": "run-1",
                        "stage": "runEdgeBoard",
                        "feature_completeness": "0.75",
                        "matched_events": "4",
                        "scored_signals": "",
                    }
                )
            (root / "State.csv").write_text("run_id,state_key,state_value\nrun-1,k,v\n", encoding="utf-8")

            proc = subprocess.run(
                ["python3", str(SCRIPT), "--input-dir", str(root)],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(0, proc.returncode, msg=proc.stderr)

            payload = json.loads((root / "Run_Log.json").read_text(encoding="utf-8"))
            self.assertIsInstance(payload[0]["feature_completeness"], float)
            self.assertEqual(0.75, payload[0]["feature_completeness"])
            self.assertIsInstance(payload[0]["matched_events"], int)
            self.assertEqual(4, payload[0]["matched_events"])
            self.assertIsNone(payload[0]["scored_signals"])

    def test_type_violation_logs_run_id_stage_and_field(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with (root / "Run_Log.csv").open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=["run_id", "stage", "feature_completeness", "matched_events", "scored_signals"],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "run_id": "run-2",
                        "stage": "runEdgeBoard",
                        "feature_completeness": "0.90",
                        "matched_events": "bad-int",
                        "scored_signals": "3",
                    }
                )
            (root / "State.csv").write_text("run_id,state_key,state_value\nrun-2,k,v\n", encoding="utf-8")

            proc = subprocess.run(
                ["python3", str(SCRIPT), "--input-dir", str(root)],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(0, proc.returncode, msg=proc.stderr)
            self.assertIn("Summary (deduped by run_id/stage/field", proc.stderr)
            self.assertIn("run-2 | runEdgeBoard | matched_events | 1", proc.stderr)

    def test_compact_warning_summary_caps_per_run_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dynamic_fields = [f"metric_{idx}_count" for idx in range(12)]
            fieldnames = ["run_id", "stage", *dynamic_fields]
            with (root / "Run_Log.csv").open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerow(
                    {
                        "run_id": "run-cap",
                        "stage": "runEdgeBoard",
                        **{field: "not-a-number" for field in dynamic_fields},
                    }
                )
            (root / "State.csv").write_text("run_id,state_key,state_value\nrun-cap,k,v\n", encoding="utf-8")

            proc = subprocess.run(
                ["python3", str(SCRIPT), "--input-dir", str(root)],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(0, proc.returncode, msg=proc.stderr)
            self.assertIn("run-cap | <suppressed> | <suppressed> | 2", proc.stderr)
            self.assertIn("- run-cap: 12 violation(s)", proc.stderr)

    def test_schema_violation_annotation_is_structured(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with (root / "Run_Log.csv").open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "run_id",
                        "stage",
                        "feature_completeness",
                        "matched_events",
                        "scored_signals",
                        "no_hit_no_events_from_source_count",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "run_id": "run-3",
                        "stage": "runEdgeBoard",
                        "feature_completeness": "{\"alias\":\"legacy_payload\"}",
                        "matched_events": "bad-int",
                        "scored_signals": "1.2",
                        "no_hit_no_events_from_source_count": "n/a",
                    }
                )
            (root / "State.csv").write_text("run_id,state_key,state_value\nrun-3,k,v\n", encoding="utf-8")

            proc = subprocess.run(
                ["python3", str(SCRIPT), "--input-dir", str(root)],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(0, proc.returncode, msg=proc.stderr)
            payload = json.loads((root / "Run_Log.json").read_text(encoding="utf-8"))
            self.assertIsNone(payload[0]["feature_completeness"])
            self.assertIsNone(payload[0]["matched_events"])
            self.assertIsNone(payload[0]["scored_signals"])
            self.assertIsNone(payload[0]["no_hit_no_events_from_source_count"])
            self.assertIn("schema_violation", payload[0])
            self.assertEqual("Run_Log", payload[0]["schema_violation"]["artifact"])
            self.assertEqual("run_log_row_schema_violation", payload[0]["schema_violation"]["code"])
            self.assertEqual(4, len(payload[0]["schema_violation"]["errors"]))

    def test_coerces_generic_numeric_fields_to_json_numbers(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with (root / "Run_Log.csv").open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "run_id",
                        "stage",
                        "duration_ms",
                        "opening_lag_minutes",
                        "cooldown_rate",
                        "status",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "run_id": "run-5",
                        "stage": "runEdgeBoard",
                        "duration_ms": "123",
                        "opening_lag_minutes": "15",
                        "cooldown_rate": "0.25",
                        "status": "ok",
                    }
                )
            (root / "State.csv").write_text("run_id,state_key,state_value\nrun-5,k,v\n", encoding="utf-8")

            proc = subprocess.run(
                ["python3", str(SCRIPT), "--input-dir", str(root)],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(0, proc.returncode, msg=proc.stderr)
            payload = json.loads((root / "Run_Log.json").read_text(encoding="utf-8"))
            row = payload[0]
            self.assertEqual(123, row["duration_ms"])
            self.assertEqual(15, row["opening_lag_minutes"])
            self.assertEqual(0.25, row["cooldown_rate"])
            self.assertEqual("ok", row["status"])

    def test_no_string_leakage_for_numeric_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with (root / "Run_Log.csv").open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "run_id",
                        "stage",
                        "feature_completeness",
                        "matched_events",
                        "scored_signals",
                        "no_hit_no_events_from_source_count",
                        "no_hit_events_outside_time_window_count",
                        "no_hit_tournament_filter_excluded_count",
                        "no_hit_odds_present_but_match_failed_count",
                        "no_hit_schema_invalid_metrics_count",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "run_id": "run-4",
                        "stage": "runEdgeBoard",
                        "feature_completeness": "0.92",
                        "matched_events": "7",
                        "scored_signals": "",
                        "no_hit_no_events_from_source_count": "3",
                        "no_hit_events_outside_time_window_count": "2",
                        "no_hit_tournament_filter_excluded_count": "bad",
                        "no_hit_odds_present_but_match_failed_count": "1.0",
                        "no_hit_schema_invalid_metrics_count": "5",
                    }
                )
            (root / "State.csv").write_text("run_id,state_key,state_value\nrun-4,k,v\n", encoding="utf-8")

            proc = subprocess.run(
                ["python3", str(SCRIPT), "--input-dir", str(root)],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(0, proc.returncode, msg=proc.stderr)
            payload = json.loads((root / "Run_Log.json").read_text(encoding="utf-8"))
            row = payload[0]
            self.assertEqual(0.92, row["feature_completeness"])
            self.assertEqual(7, row["matched_events"])
            self.assertIsNone(row["scored_signals"])
            self.assertEqual(3, row["no_hit_no_events_from_source_count"])
            self.assertEqual(2, row["no_hit_events_outside_time_window_count"])
            self.assertIsNone(row["no_hit_tournament_filter_excluded_count"])
            self.assertEqual(1, row["no_hit_odds_present_but_match_failed_count"])
            self.assertEqual(5, row["no_hit_schema_invalid_metrics_count"])

            numeric_fields = [
                "feature_completeness",
                "matched_events",
                "scored_signals",
                "no_hit_no_events_from_source_count",
                "no_hit_events_outside_time_window_count",
                "no_hit_tournament_filter_excluded_count",
                "no_hit_odds_present_but_match_failed_count",
                "no_hit_schema_invalid_metrics_count",
            ]
            for field in numeric_fields:
                self.assertFalse(isinstance(row[field], str), msg=f"{field} leaked as string")


    def test_json_csv_round_trip_parity_for_stage_payloads(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fieldnames = [
                "run_id",
                "row_type",
                "stage",
                "stage_summaries",
                "reason_codes",
                "summary",
                "reason_metadata",
            ]
            with (root / "Run_Log.csv").open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerow(
                    {
                        "run_id": "run-empty",
                        "row_type": "summary",
                        "stage": "runEdgeBoard",
                        "stage_summaries": "",
                        "reason_codes": "",
                        "summary": "",
                        "reason_metadata": "",
                    }
                )
                writer.writerow(
                    {
                        "run_id": "run-full",
                        "row_type": "summary",
                        "stage": "runEdgeBoard",
                        "stage_summaries": json.dumps([{"stage": "stageFetchPlayerStats", "reason_codes": {"STATS_ENR": 2}}]),
                        "reason_codes": json.dumps({"MATCH_CT": 1}),
                        "summary": json.dumps({"status": "success"}),
                        "reason_metadata": json.dumps({"requested_player_count": 2}),
                    }
                )
            (root / "State.csv").write_text("run_id,state_key,state_value\nrun-empty,k,v\n", encoding="utf-8")

            proc = subprocess.run(
                ["python3", str(SCRIPT), "--input-dir", str(root)],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(0, proc.returncode, msg=proc.stderr)

            with (root / "Run_Log.csv").open("r", encoding="utf-8", newline="") as handle:
                csv_rows = [normalize_run_log_row(dict(row)) for row in csv.DictReader(handle)]
            json_rows = json.loads((root / "Run_Log.json").read_text(encoding="utf-8"))
            self.assertEqual(csv_rows, [normalize_run_log_row(dict(row)) for row in json_rows])

            empty_row = next(row for row in json_rows if row["run_id"] == "run-empty")
            self.assertIsNone(empty_row["stage_summaries"])
            self.assertIsNone(empty_row["reason_codes"])
            self.assertIsNone(empty_row["summary"])
            self.assertIsNone(empty_row["reason_metadata"])

    def test_object_string_sentinel_is_flagged_and_nullified(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with (root / "Run_Log.csv").open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=["run_id", "stage", "reason_codes", "stage_summaries"],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "run_id": "run-sentinel",
                        "stage": "runEdgeBoard",
                        "reason_codes": "[object Object]",
                        "stage_summaries": "[object Object]",
                    }
                )
            (root / "State.csv").write_text("run_id,state_key,state_value\nrun-sentinel,k,v\n", encoding="utf-8")

            proc = subprocess.run(
                ["python3", str(SCRIPT), "--input-dir", str(root)],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(0, proc.returncode, msg=proc.stderr)
            payload = json.loads((root / "Run_Log.json").read_text(encoding="utf-8"))
            row = payload[0]
            self.assertIsNone(row["reason_codes"])
            self.assertIsNone(row["stage_summaries"])
            self.assertIn("schema_violation", row)
            self.assertEqual(2, len(row["schema_violation"]["errors"]))

    def test_state_object_values_are_preserved_and_absent_is_null(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "Run_Log.csv").write_text("run_id,stage\nrun-state,runEdgeBoard\n", encoding="utf-8")
            with (root / "State.csv").open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=["run_id", "state_key", "state_value"])
                writer.writeheader()
                writer.writerow({"run_id": "run-state", "state_key": "a", "state_value": "{\"x\":1}"})
                writer.writerow({"run_id": "run-state", "state_key": "b", "state_value": ""})
                writer.writerow({"run_id": "run-state", "state_key": "c", "state_value": "[object Object]"})

            proc = subprocess.run(
                ["python3", str(SCRIPT), "--input-dir", str(root)],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(0, proc.returncode, msg=proc.stderr)
            state_payload = json.loads((root / "State.json").read_text(encoding="utf-8"))
            self.assertEqual({"x": 1}, state_payload[0]["state_value"])
            self.assertIsNone(state_payload[1]["state_value"])
            self.assertIsNone(state_payload[2]["state_value"])

    def test_reuses_existing_run_log_json_list_and_object_schema_variants(self):
        for fixture_name in ("run_log_rows_list.json", "run_log_rows_object.json"):
            with self.subTest(fixture=fixture_name):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    (root / "Run_Log.json").write_text(
                        (FIXTURES / fixture_name).read_text(encoding="utf-8"),
                        encoding="utf-8",
                    )
                    (root / "State.json").write_text("[]\n", encoding="utf-8")

                    proc = subprocess.run(
                        ["python3", str(SCRIPT), "--input-dir", str(root)],
                        cwd=ROOT,
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    self.assertEqual(0, proc.returncode, msg=proc.stderr)
                    self.assertEqual(
                        json.loads((FIXTURES / fixture_name).read_text(encoding="utf-8")),
                        json.loads((root / "Run_Log.json").read_text(encoding="utf-8")),
                    )

    def test_reuses_existing_state_json_list_and_object_schema_variants(self):
        for fixture_name in ("state_rows_list.json", "state_rows_object.json"):
            with self.subTest(fixture=fixture_name):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    (root / "Run_Log.json").write_text("[]\n", encoding="utf-8")
                    (root / "State.json").write_text(
                        (FIXTURES / fixture_name).read_text(encoding="utf-8"),
                        encoding="utf-8",
                    )

                    proc = subprocess.run(
                        ["python3", str(SCRIPT), "--input-dir", str(root)],
                        cwd=ROOT,
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    self.assertEqual(0, proc.returncode, msg=proc.stderr)
                    self.assertEqual(
                        json.loads((FIXTURES / fixture_name).read_text(encoding="utf-8")),
                        json.loads((root / "State.json").read_text(encoding="utf-8")),
                    )


if __name__ == "__main__":
    unittest.main()
