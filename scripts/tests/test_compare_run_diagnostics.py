import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from compare_run_diagnostics import compare_rows  # noqa: E402


def _full_stage_rows(run_id: str):
    return [
        {"run_id": run_id, "stage": "stageFetchOdds", "row_type": "summary"},
        {"run_id": run_id, "stage": "stageFetchSchedule", "row_type": "summary"},
        {"run_id": run_id, "stage": "stageMatchEvents", "row_type": "summary"},
        {"run_id": run_id, "stage": "stageFetchPlayerStats", "row_type": "summary"},
        {"run_id": run_id, "stage": "stageGenerateSignals", "row_type": "summary"},
        {"run_id": run_id, "stage": "stagePersist", "row_type": "summary"},
    ]


class CompareRunDiagnosticsValidationTests(unittest.TestCase):
    def test_compare_rows_fails_on_disallowed_skip_reason(self):
        rows = _full_stage_rows("run-a") + _full_stage_rows("run-b")
        rows.append({"run_id": "run-b", "stage": "runEdgeBoard", "reason_code": "run_debounced_skip"})

        with self.assertRaisesRegex(ValueError, "replacement run IDs required"):
            compare_rows(rows, "run-a", "run-b")

    def test_compare_rows_fails_on_missing_stage_chain(self):
        rows = _full_stage_rows("run-a") + [
            {"run_id": "run-b", "stage": "stageFetchOdds", "row_type": "summary"},
            {"run_id": "run-b", "stage": "stageFetchSchedule", "row_type": "summary"},
            {"run_id": "run-b", "stage": "stageMatchEvents", "row_type": "summary"},
        ]

        with self.assertRaisesRegex(ValueError, "missing stage chain entries"):
            compare_rows(rows, "run-a", "run-b")


if __name__ == "__main__":
    unittest.main()
