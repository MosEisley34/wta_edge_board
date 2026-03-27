import json
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


class CheckFeatureCompletenessPreflightTests(unittest.TestCase):
    def test_exits_nonzero_when_candidate_below_floor(self):
        proc = subprocess.run(
            [
                "python3",
                str(ROOT / "scripts" / "check_feature_completeness_preflight.py"),
                "--export-dir",
                str(ROOT / "scripts" / "fixtures" / "edge_quality_gate_fail.json"),
                "--baseline-run-id",
                "run-baseline",
                "--candidate-run-id",
                "run-candidate",
            ],
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(3, proc.returncode)
        payload = json.loads(proc.stdout.strip())
        self.assertEqual("fail", payload["status"])
        self.assertEqual("FEATURE_COMPLETENESS_BELOW_FLOOR", payload["reason_code"])
        self.assertTrue(payload["should_skip_downstream"])

    def test_force_full_compare_returns_zero_with_override_status(self):
        proc = subprocess.run(
            [
                "python3",
                str(ROOT / "scripts" / "check_feature_completeness_preflight.py"),
                "--export-dir",
                str(ROOT / "scripts" / "fixtures" / "edge_quality_gate_fail.json"),
                "--baseline-run-id",
                "run-baseline",
                "--candidate-run-id",
                "run-candidate",
                "--force-full-compare",
            ],
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(0, proc.returncode)
        payload = json.loads(proc.stdout.strip())
        self.assertEqual("override", payload["status"])
        self.assertEqual("FEATURE_COMPLETENESS_BELOW_FLOOR", payload["reason_code"])
        self.assertFalse(payload["should_skip_downstream"])

    def test_passing_candidate_returns_ok_and_writes_report(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir) / "feature_gate.json"
            proc = subprocess.run(
                [
                    "python3",
                    str(ROOT / "scripts" / "check_feature_completeness_preflight.py"),
                    "--export-dir",
                    str(ROOT / "scripts" / "fixtures" / "edge_quality_gate_pass.json"),
                    "--baseline-run-id",
                    "run-baseline",
                    "--candidate-run-id",
                    "run-candidate",
                    "--report-out",
                    str(report_path),
                ],
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertTrue(report_path.exists())
        self.assertEqual(0, proc.returncode)
        payload = json.loads(proc.stdout.strip())
        self.assertEqual("pass", payload["status"])
        self.assertEqual("OK", payload["reason_code"])


if __name__ == "__main__":
    unittest.main()
