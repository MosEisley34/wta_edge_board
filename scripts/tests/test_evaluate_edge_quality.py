import json
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from evaluate_edge_quality import EdgeQualityGateConfig, evaluate_edge_quality_gate, load_run_log_rows  # noqa: E402


class EvaluateEdgeQualityTests(unittest.TestCase):
    def test_gate_passes_within_thresholds(self):
        rows = load_run_log_rows(str(ROOT / "scripts" / "fixtures" / "edge_quality_gate_pass.json"))
        report = evaluate_edge_quality_gate(
            rows,
            baseline_run_id="run-baseline",
            candidate_run_id="run-candidate",
            config=EdgeQualityGateConfig(
                min_feature_completeness=0.6,
                max_edge_volatility=0.03,
                max_suppression_drift=0.6,
                suppression_min_volume=2,
            ),
        )
        self.assertEqual("pass", report["status"])
        self.assertEqual([], report["failures"])

    def test_gate_fails_with_threshold_breaches(self):
        rows = load_run_log_rows(str(ROOT / "scripts" / "fixtures" / "edge_quality_gate_fail.json"))
        report = evaluate_edge_quality_gate(
            rows,
            baseline_run_id="run-baseline",
            candidate_run_id="run-candidate",
            config=EdgeQualityGateConfig(
                min_feature_completeness=0.6,
                max_edge_volatility=0.03,
                max_suppression_drift=0.6,
                suppression_min_volume=2,
            ),
        )
        self.assertEqual("fail", report["status"])
        self.assertTrue(any("feature_completeness_below_floor" in item for item in report["failures"]))
        self.assertTrue(any("edge_volatility_above_ceiling" in item for item in report["failures"]))
        self.assertTrue(any("suppression_drift_exceeded" in item for item in report["failures"]))

    def test_cli_returns_non_zero_for_failures(self):
        cmd = [
            "python3",
            str(ROOT / "scripts" / "evaluate_edge_quality.py"),
            str(ROOT / "scripts" / "fixtures" / "edge_quality_gate_fail.json"),
            "--baseline-run-id",
            "run-baseline",
            "--candidate-run-id",
            "run-candidate",
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        self.assertEqual(1, proc.returncode)
        payload = json.loads(proc.stdout)
        self.assertEqual("fail", payload["status"])


if __name__ == "__main__":
    unittest.main()
