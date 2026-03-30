import sys
import json
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from read_daily_slo_status import read_status_lines_with_health


class ReadDailySloStatusTests(unittest.TestCase):
    def _write(self, root: Path, name: str, payload: object) -> Path:
        path = root / name
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    def test_legacy_schema_status_and_windows(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = self._write(
                root,
                "edge_quality_daily_slo_20260325T010101Z.json",
                {"status": "pass", "windows": [{"days": 3, "verdict": "pass"}]},
            )

            lines, has_invalid = read_status_lines_with_health(root)
            self.assertFalse(has_invalid)
            self.assertEqual(1, len(lines))
            self.assertIn(str(report), lines[0])
            self.assertIn("status=pass", lines[0])
            self.assertIn("schema=legacy", lines[0])
            self.assertIn("parity_contract_status=not_evaluated", lines[0])
            self.assertIn("decisionability_status=insufficient_sample", lines[0])
            self.assertIn("quality_status=insufficient_sample", lines[0])

    def test_current_schema_derives_from_window_verdicts_when_status_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = self._write(
                root,
                "edge_quality_daily_slo_20260325T010102Z.json",
                {
                    "schema": "edge_quality_daily_slo_v1",
                    "gate_reason": "decisionable_window_fail_rate_exceeded_threshold",
                    "decisionable_window_count": 1,
                    "window_reports": [
                        {"window_days": 3, "verdict": "pass"},
                        {"window_days": 7, "verdict": "fail"},
                    ],
                },
            )

            lines, has_invalid = read_status_lines_with_health(root)
            self.assertFalse(has_invalid)
            self.assertEqual(1, len(lines))
            self.assertIn(str(report), lines[0])
            self.assertIn("status=fail", lines[0])
            self.assertIn("schema=current", lines[0])
            self.assertIn("source=derived", lines[0])
            self.assertIn("quality_status=fail", lines[0])

    def test_malformed_and_missing_keys_mark_invalid(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            invalid_json = root / "edge_quality_daily_slo_20260325T010103Z.json"
            invalid_json.write_text("{not-json", encoding="utf-8")
            missing = self._write(
                root,
                "edge_quality_daily_slo_20260325T010104Z.json",
                {"schema": "edge_quality_daily_slo_v1", "gate_reason": "missing windows"},
            )

            lines, has_invalid = read_status_lines_with_health(root, limit=2)
            joined = "\n".join(lines)
            self.assertTrue(has_invalid)
            self.assertIn(str(invalid_json), joined)
            self.assertIn("invalid_json", joined)
            self.assertIn(str(missing), joined)
            self.assertIn("unable_to_resolve_status_or_windows", joined)


if __name__ == "__main__":
    unittest.main()
