import sys
import json
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from read_daily_slo_status import read_status_lines


class ReadDailySloStatusTests(unittest.TestCase):
    def test_ignores_cli_day_and_summary_helper_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "edge_quality_daily_slo_cli_day0.json").write_text(json.dumps({"status": "fail", "windows": []}))
            (root / "edge_quality_daily_slo_summary_day3.json").write_text(json.dumps({"status": "fail", "windows": []}))
            (root / "edge_quality_daily_slo_helper_notes.json").write_text(json.dumps({"status": "fail", "windows": []}))
            good = root / "edge_quality_daily_slo_20260325T010101Z.json"
            good.write_text(json.dumps({"status": "pass", "windows": [{"days": 3}]}))

            lines = read_status_lines(root)
            self.assertEqual(1, len(lines))
            self.assertIn(str(good), lines[0])
            self.assertIn("status=pass", lines[0])

    def test_schema_warning_on_missing_expected_keys(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            broken = root / "edge_quality_daily_slo_20260325T010102Z.json"
            broken.write_text(json.dumps({"status": "pass"}))

            lines = read_status_lines(root)
            self.assertEqual(1, len(lines))
            self.assertIn(str(broken), lines[0])
            self.assertIn("schema warning", lines[0])
            self.assertIn("windows", lines[0])


if __name__ == "__main__":
    unittest.main()
