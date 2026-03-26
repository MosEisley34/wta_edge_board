import csv
import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import precheck_run_ids  # noqa: E402


class PrecheckRunIdsSourceContractTests(unittest.TestCase):
    def _write_json(self, path: Path, run_ids):
        rows = [{"run_id": run_id, "stage": "stageFetchOdds"} for run_id in run_ids]
        path.write_text(json.dumps(rows), encoding="utf-8")

    def _write_csv(self, path: Path, run_ids):
        headers = ["run_id", "stage"]
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=headers)
            writer.writeheader()
            for run_id in run_ids:
                writer.writerow({"run_id": run_id, "stage": "stageFetchOdds"})

    def _run_main(self, argv):
        with patch.object(sys, "argv", argv):
            output = io.StringIO()
            with redirect_stdout(output):
                code = precheck_run_ids.main()
        return code, output.getvalue()

    def test_fails_when_target_run_exists_only_in_csv(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_csv(root / "Run_Log.csv", ["run-a", "run-b"])
            self._write_json(root / "Run_Log.json", ["run-a"])

            code, output = self._run_main(
                ["precheck_run_ids.py", "run-a", "run-b", "--export-dir", str(root)]
            )

            self.assertEqual(code, 2)
            self.assertIn("run_id_source_mismatch", output)
            self.assertIn("run-b: csv_present=true json_present=false", output)

    def test_fails_when_target_run_exists_only_in_json_even_with_override(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_csv(root / "Run_Log.csv", ["run-a"])
            self._write_json(root / "Run_Log.json", ["run-a", "run-b"])

            code, output = self._run_main(
                [
                    "precheck_run_ids.py",
                    "run-a",
                    "run-b",
                    "--export-dir",
                    str(root),
                    "--allow-csv-only-triage",
                    "--allow-csv-only-triage-incident-tag",
                    "INC-1234",
                ]
            )

            self.assertEqual(code, 2)
            self.assertIn("run_id_source_mismatch", output)
            self.assertIn("run-b: csv_present=false json_present=true", output)

    def test_passes_when_target_runs_exist_in_both_sources(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_csv(root / "Run_Log.csv", ["run-a", "run-b"])
            self._write_json(root / "Run_Log.json", ["run-a", "run-b"])

            code, output = self._run_main(
                ["precheck_run_ids.py", "run-a", "run-b", "--export-dir", str(root)]
            )

            self.assertEqual(code, 0)
            self.assertIn("Precheck passed", output)

    def test_override_allows_csv_only_presence_with_degraded_confidence_warning(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_csv(root / "Run_Log.csv", ["run-a", "run-b"])
            self._write_json(root / "Run_Log.json", ["run-a"])

            code, output = self._run_main(
                [
                    "precheck_run_ids.py",
                    "run-a",
                    "run-b",
                    "--export-dir",
                    str(root),
                    "--allow-csv-only-triage",
                    "--allow-csv-only-triage-incident-tag",
                    "INC-1234",
                ]
            )

            self.assertEqual(code, 0)
            self.assertIn("degraded_confidence_csv_only_triage", output)
            self.assertIn("run-b: csv_present=true json_present=false", output)

    def test_override_requires_incident_tag(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_csv(root / "Run_Log.csv", ["run-a", "run-b"])
            self._write_json(root / "Run_Log.json", ["run-a"])

            code, output = self._run_main(
                [
                    "precheck_run_ids.py",
                    "run-a",
                    "run-b",
                    "--export-dir",
                    str(root),
                    "--allow-csv-only-triage",
                ]
            )

            self.assertEqual(code, 2)
            self.assertIn("Incident tag is required", output)

    def test_missing_run_ids_prints_source_recent_and_remediation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_csv(root / "Run_Log.csv", ["run-100", "run-101", "run-102"])
            self._write_json(root / "Run_Log.json", ["run-100", "run-101", "run-102"])

            code, output = self._run_main(
                [
                    "precheck_run_ids.py",
                    "run-100",
                    "run-999",
                    "--export-dir",
                    str(root),
                    "--recent-limit",
                    "2",
                ]
            )

            self.assertEqual(code, 2)
            self.assertIn("Searched export source directory/path:", output)
            self.assertIn("Top recent run IDs found (limit=2): run-100, run-101", output)
            self.assertIn("Closest run IDs to run-999:", output)
            self.assertIn("Remediation: point EXPORT_SRC to a fresh batch path", output)


if __name__ == "__main__":
    unittest.main()
