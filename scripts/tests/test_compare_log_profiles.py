import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
COMPARE_SCRIPT = ROOT / "scripts" / "compare_log_profiles.py"


class CompareLogProfilesTests(unittest.TestCase):
    def _write_json(self, path: Path, payload):
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def _summary_row(self, run_id: str, reason_code: str = "ok", provider: str = "odds_api"):
        return {
            "row_type": "summary",
            "run_id": run_id,
            "stage": "runEdgeBoard",
            "reason_code": reason_code,
            "rejection_codes": json.dumps({"opening_lag_exceeded": 0}),
            "stage_summaries": json.dumps(
                [
                    {
                        "stage": "stageFetchOdds",
                        "provider": provider,
                        "started_at": "2026-03-12T12:00:00Z",
                        "ended_at": "2026-03-12T12:00:01Z",
                        "duration_ms": 1000,
                    }
                ]
            ),
        }

    def test_fails_when_reduction_below_target(self):
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            verbose = tmp / "verbose.json"
            compact = tmp / "compact.json"
            summary = tmp / "summary.json"
            rows = [self._summary_row("run-1")]
            self._write_json(verbose, rows)
            self._write_json(compact, rows)

            result = subprocess.run(
                [
                    sys.executable,
                    str(COMPARE_SCRIPT),
                    str(verbose),
                    str(compact),
                    "--target-reduction-pct",
                    "60",
                    "--critical-parity-keys",
                    "gate_reasons,source_selection",
                    "--summary-json-out",
                    str(summary),
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(1, result.returncode)
            report = json.loads(summary.read_text(encoding="utf-8"))
            self.assertIn("reduction_below_target", report["quality_gate_failed_reasons"])

    def test_fails_when_critical_parity_mismatch_detected(self):
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            verbose = tmp / "verbose.json"
            compact = tmp / "compact.json"
            self._write_json(verbose, [self._summary_row("run-1", reason_code="ok")])
            self._write_json(compact, [self._summary_row("run-1", reason_code="different_reason")])

            result = subprocess.run(
                [
                    sys.executable,
                    str(COMPARE_SCRIPT),
                    str(verbose),
                    str(compact),
                    "--target-reduction-pct",
                    "0",
                    "--critical-parity-keys",
                    "gate_reasons",
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(1, result.returncode)
            report = json.loads(result.stdout)
            self.assertEqual(["gate_reasons"], report["failed_critical_parity_keys"])

    def test_passes_and_writes_summary_when_gates_are_satisfied(self):
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            verbose = tmp / "verbose.json"
            compact = tmp / "compact.json"
            summary = tmp / "summary.json"

            verbose_rows = [
                self._summary_row("run-1", provider="odds_api"),
                {"row_type": "stage", "run_id": "run-1", "stage": "run_lifecycle", "status": "success", "reason_code": "ok", "padding": "x" * 500},
            ]
            compact_rows = [self._summary_row("run-1", provider="odds_api")]
            self._write_json(verbose, verbose_rows)
            self._write_json(compact, compact_rows)

            result = subprocess.run(
                [
                    sys.executable,
                    str(COMPARE_SCRIPT),
                    str(verbose),
                    str(compact),
                    "--target-reduction-pct",
                    "10",
                    "--critical-parity-keys",
                    "gate_reasons,source_selection",
                    "--summary-json-out",
                    str(summary),
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(0, result.returncode)
            report = json.loads(summary.read_text(encoding="utf-8"))
            self.assertTrue(report["target_met"])
            self.assertTrue(report["critical_parity_passed"])
            self.assertEqual([], report["quality_gate_failed_reasons"])


if __name__ == "__main__":
    unittest.main()
