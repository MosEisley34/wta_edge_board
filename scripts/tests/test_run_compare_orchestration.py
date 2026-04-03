import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "run_compare_orchestration.sh"


class RunCompareOrchestrationTests(unittest.TestCase):
    def test_fails_fast_when_baseline_run_id_is_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / "exports"
            candidate_run_id = "run-candidate-123"
            invalid_artifact = ROOT / "artifacts" / "compare" / f"_vs_{candidate_run_id}.json"
            if invalid_artifact.exists():
                invalid_artifact.unlink()

            proc = subprocess.run(
                [
                    "bash",
                    str(SCRIPT),
                    "--out-dir",
                    str(out_dir),
                    "",
                    candidate_run_id,
                    str(out_dir),
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertNotEqual(0, proc.returncode)
            self.assertIn("PRE_RUN_ID resolved to empty", proc.stderr)
            self.assertIn(f"Candidate run ID: {candidate_run_id}", proc.stderr)
            self.assertIn(f"Export path: {out_dir}", proc.stderr)
            self.assertNotIn("compare_run_diagnostics.py", proc.stdout + proc.stderr)
            self.assertNotIn("evaluate_edge_quality.py", proc.stdout + proc.stderr)
            self.assertFalse(invalid_artifact.exists())


if __name__ == "__main__":
    unittest.main()
