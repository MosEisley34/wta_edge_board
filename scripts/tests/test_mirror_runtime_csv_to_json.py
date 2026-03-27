import csv
import json
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "mirror_runtime_csv_to_json.py"


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


if __name__ == "__main__":
    unittest.main()
