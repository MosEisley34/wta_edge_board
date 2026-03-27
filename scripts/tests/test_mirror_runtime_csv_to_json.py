import csv
import json
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "mirror_runtime_csv_to_json.py"
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
            self.assertIn("run_id=run-2", proc.stderr)
            self.assertIn("stage=runEdgeBoard", proc.stderr)
            self.assertIn("field=matched_events", proc.stderr)

    def test_feature_completeness_schema_violation_is_sanitized(self):
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
                        "run_id": "run-3",
                        "stage": "runEdgeBoard",
                        "feature_completeness": "{\"alias\":\"legacy_payload\"}",
                        "matched_events": "2",
                        "scored_signals": "1",
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
            self.assertEqual("run_log_row_schema_violation", payload[0]["schema_violation"])
            self.assertIn("feature_completeness_expected_numeric_or_null", payload[0]["field_type_error"])
            self.assertIn("schema_error", payload[0])
            self.assertEqual("Run_Log", payload[0]["schema_error"]["artifact"])
            self.assertEqual("run_log_row_schema_violation", payload[0]["schema_error"]["code"])

    def test_quality_contract_numeric_fields_move_non_scalar_payloads_to_detail_fields(self):
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
                        "run_id": "run-4",
                        "stage": "runEdgeBoard",
                        "feature_completeness": "0.92",
                        "matched_events": "{\"unexpected\": true}",
                        "scored_signals": "[1,2,3]",
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
            self.assertIsNone(row["matched_events"])
            self.assertIsNone(row["scored_signals"])
            self.assertEqual({"unexpected": True}, row["matched_events_detail"])
            self.assertEqual([1, 2, 3], row["scored_signals_detail"])
            self.assertIn("matched_events_detail_json", row)
            self.assertIn("scored_signals_detail_json", row)
            self.assertIn("schema_error", row)
            self.assertEqual(2, len(row["schema_error"]["errors"]))

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
