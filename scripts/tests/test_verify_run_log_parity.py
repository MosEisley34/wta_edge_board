import json
import tempfile
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from verify_run_log_parity import ParityError, verify_run_log_parity  # noqa: E402


class VerifyRunLogParityTests(unittest.TestCase):
    def _write_json(self, path: Path, rows):
        path.write_text(json.dumps(rows), encoding="utf-8")

    def _write_csv(self, path: Path, rows):
        headers = ["row_type", "run_id", "stage", "started_at"]
        lines = [",".join(headers)]
        for row in rows:
            values = [str(row.get(h, "")) for h in headers]
            lines.append(",".join(values))
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    def test_verify_run_log_parity_passes_for_matching_latest_batch(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            rows = [
                {"row_type": "ops", "run_id": "run-a", "stage": "stageFetchOdds", "started_at": "2026-03-21T10:00:00Z"},
                {"row_type": "summary", "run_id": "run-a", "stage": "runEdgeBoard", "started_at": "2026-03-21T10:00:01Z"},
            ]
            self._write_json(root / "Run_Log.json", rows)
            self._write_csv(root / "Run_Log.csv", rows)

            verify_run_log_parity(str(root))

    def test_verify_run_log_parity_fails_when_run_id_set_differs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            json_rows = [
                {"row_type": "ops", "run_id": "run-a", "stage": "stageFetchOdds", "started_at": "2026-03-21T10:00:00Z"},
                {"row_type": "summary", "run_id": "run-a", "stage": "runEdgeBoard", "started_at": "2026-03-21T10:00:01Z"},
            ]
            csv_rows = [
                {"row_type": "ops", "run_id": "run-b", "stage": "stageFetchOdds", "started_at": "2026-03-21T10:00:00Z"},
                {"row_type": "summary", "run_id": "run-b", "stage": "runEdgeBoard", "started_at": "2026-03-21T10:00:01Z"},
            ]
            self._write_json(root / "Run_Log.json", json_rows)
            self._write_csv(root / "Run_Log.csv", csv_rows)

            with self.assertRaisesRegex(ParityError, "Latest run_id set mismatch"):
                verify_run_log_parity(str(root))

    def test_verify_run_log_parity_fails_when_summary_presence_differs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            json_rows = [
                {"row_type": "ops", "run_id": "run-a", "stage": "stageFetchOdds", "started_at": "2026-03-21T10:00:00Z"},
                {"row_type": "summary", "run_id": "run-a", "stage": "runEdgeBoard", "started_at": "2026-03-21T10:00:01Z"},
            ]
            csv_rows = [
                {"row_type": "ops", "run_id": "run-a", "stage": "stageFetchOdds", "started_at": "2026-03-21T10:00:00Z"},
                {"row_type": "ops", "run_id": "run-a", "stage": "stagePersist", "started_at": "2026-03-21T10:00:01Z"},
            ]
            self._write_json(root / "Run_Log.json", json_rows)
            self._write_csv(root / "Run_Log.csv", csv_rows)

            with self.assertRaisesRegex(ParityError, "summary presence mismatch"):
                verify_run_log_parity(str(root))


if __name__ == "__main__":
    unittest.main()
