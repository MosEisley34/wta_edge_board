import json
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from extract_player_features import _parse_matchmx_rows  # noqa: E402


class TaMatchMxRegressionTest(unittest.TestCase):
    def setUp(self):
        self.fixture_path = ROOT / "scripts" / "fixtures" / "leadersource_wta_sample.js"
        self.payload = self.fixture_path.read_text(encoding="utf-8")

    def test_extract_player_features_fixture_has_players_and_coverage(self):
        rows = _parse_matchmx_rows("tennisabstract_leadersource_wta", self.payload, "2026-01-01T00:00:00+00:00")
        valid_rows = [r for r in rows if r.player_canonical_name and r.reason_code == "ok"]
        self.assertGreater(len(valid_rows), 0)

        non_null_hold = sum(1 for row in valid_rows if row.hold_pct is not None)
        self.assertGreater(non_null_hold, 0)

    def test_check_ta_parity_fixture_has_non_zero_players_and_non_null_coverage(self):
        cmd = [
            sys.executable,
            str(SCRIPTS_DIR / "check_ta_parity.py"),
            "--input",
            str(self.fixture_path),
            "--sample-size",
            "1",
        ]
        result = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, check=True)
        summary = json.loads(result.stdout)
        self.assertGreater(summary["unique_players"], 0)
        self.assertGreater(summary["normalized_non_null_coverage"]["hold_pct"]["non_null"], 0)

    def test_check_ta_parity_reports_clear_guidance_when_matchmx_missing(self):
        fixture_without_matchmx = ROOT / "tmp" / "test_ta_parity_no_matchmx.body"
        fixture_without_matchmx.parent.mkdir(parents=True, exist_ok=True)
        fixture_without_matchmx.write_text('<html><body>no match rows</body></html>', encoding="utf-8")

        cmd = [
            sys.executable,
            str(SCRIPTS_DIR / "check_ta_parity.py"),
            "--input",
            str(fixture_without_matchmx),
        ]
        result = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 1)
        summary = json.loads(result.stdout)
        self.assertEqual(summary["reason_code"], "ta_matchmx_markers_missing")
        self.assertIn("leadersource_wta", summary["reason"])
        self.assertEqual(summary["suggested_input"], "tmp/source_probe_latest/raw/tennisabstract_leadersource_wta.body")


if __name__ == "__main__":
    unittest.main()
