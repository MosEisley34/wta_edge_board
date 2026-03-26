import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "scan_runtime_diagnostics.sh"
FIXTURES = ROOT / "scripts" / "fixtures"


class ScanRuntimeDiagnosticsTests(unittest.TestCase):
    def _run_scan(self, export_dir: Path) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [str(SCRIPT), str(export_dir)],
            check=False,
            capture_output=True,
            text=True,
            cwd=ROOT,
        )

    def test_run_log_json_top_level_list_schema(self):
        with tempfile.TemporaryDirectory() as tmp:
            export_dir = Path(tmp)
            shutil.copyfile(FIXTURES / "run_log_rows_list.json", export_dir / "Run_Log.json")

            result = self._run_scan(export_dir)

            self.assertEqual(0, result.returncode, msg=result.stderr)
            self.assertIn("- provider_returned_null_features: 1", result.stdout)
            self.assertNotIn("failed to parse", result.stderr)

    def test_run_log_json_top_level_object_rows_schema(self):
        with tempfile.TemporaryDirectory() as tmp:
            export_dir = Path(tmp)
            shutil.copyfile(FIXTURES / "run_log_rows_object.json", export_dir / "Run_Log.json")

            result = self._run_scan(export_dir)

            self.assertEqual(0, result.returncode, msg=result.stderr)
            self.assertIn("- provider_returned_null_features: 1", result.stdout)
            self.assertNotIn("failed to parse", result.stderr)


    def test_state_json_top_level_list_schema(self):
        with tempfile.TemporaryDirectory() as tmp:
            export_dir = Path(tmp)
            shutil.copyfile(FIXTURES / "state_rows_list.json", export_dir / "State.json")

            result = self._run_scan(export_dir)

            self.assertEqual(0, result.returncode, msg=result.stderr)
            self.assertIn("Scanned records: 2", result.stdout)
            self.assertIn("- missing_stats: 1", result.stdout)
            self.assertNotIn("ignoring list-style State.json schema", result.stderr)
            self.assertNotIn("failed to parse", result.stderr)

    def test_state_json_top_level_object_rows_schema(self):
        with tempfile.TemporaryDirectory() as tmp:
            export_dir = Path(tmp)
            shutil.copyfile(FIXTURES / "state_rows_object.json", export_dir / "State.json")

            result = self._run_scan(export_dir)

            self.assertEqual(0, result.returncode, msg=result.stderr)
            self.assertIn("Scanned records: 2", result.stdout)
            self.assertIn("- missing_stats: 1", result.stdout)
            self.assertNotIn("failed to parse", result.stderr)

    def test_unreadable_json_keeps_csv_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            export_dir = Path(tmp)
            (export_dir / "Run_Log.json").write_text("{not valid json", encoding="utf-8")
            (export_dir / "Run_Log.csv").write_text(
                "row_type,run_id,stage,reason_codes\n"
                'summary,run-csv,runEdgeBoard,"{""provider_returned_null_features"": 1}"\n',
                encoding="utf-8",
            )

            result = self._run_scan(export_dir)

            self.assertEqual(0, result.returncode, msg=result.stderr)
            self.assertIn("- provider_returned_null_features: 1", result.stdout)
            self.assertIn("failed to parse", result.stderr)


if __name__ == "__main__":
    unittest.main()
