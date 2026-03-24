import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


class CiPreflightReferenceGateTests(unittest.TestCase):
    def test_gate_passes_for_repo_docs_and_wrappers(self):
        proc = subprocess.run(
            [sys.executable, "scripts/ci_preflight_reference_gate.py"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(proc.returncode, 0, msg=proc.stdout + proc.stderr)
        self.assertIn("preflight_reference_gate: PASS", proc.stdout)


if __name__ == "__main__":
    unittest.main()
