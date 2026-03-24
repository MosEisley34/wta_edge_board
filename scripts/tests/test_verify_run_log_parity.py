import csv
import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from verify_run_log_parity import (  # noqa: E402
    ParityError,
    _write_latest_batch_sidecar,
    verify_run_log_parity,
)


class VerifyRunLogParityTests(unittest.TestCase):
    def _write_json(self, path: Path, rows):
        path.write_text(json.dumps(rows), encoding="utf-8")

    def _write_csv(self, path: Path, rows):
        headers = ["row_type", "run_id", "stage", "started_at", "stage_summaries", "message"]
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=headers)
            writer.writeheader()
            for row in rows:
                normalized = dict(row)
                for h in headers:
                    value = normalized.get(h, "")
                    if isinstance(value, (dict, list)):
                        normalized[h] = json.dumps(value)
                writer.writerow({h: normalized.get(h, "") for h in headers})

    @staticmethod
    def _summary_stage_summaries_payload(metadata=None):
        payload = {
            "schema_id": "reason_alias_schema_v1",
            "stage_summaries": [{"stage": "stageFetchPlayerStats"}],
        }
        if metadata is not None:
            payload["gs_export_parity_contract"] = metadata
        return payload

    @staticmethod
    def _parity_metadata(run_id: str, pass_claim: bool = True):
        return {
            "contract_name": "run_log_export_parity_contract_v1",
            "latest_run_ids": [run_id],
            "summary_presence_by_run_id": {run_id: True},
            "required_stage_summary_presence_by_run_id": {
                run_id: {"stageFetchPlayerStats": True}
            },
            "parity_status": "pass" if pass_claim else "failed",
            "reason_code": "export_parity_contract_pass" if pass_claim else "export_parity_missing_stage_summary",
            "pass": pass_claim,
        }

    def test_verify_run_log_parity_passes_for_matching_latest_batch(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            metadata = self._parity_metadata("run-a", pass_claim=True)
            rows = [
                {"row_type": "ops", "run_id": "run-a", "stage": "stageFetchOdds", "started_at": "2026-03-21T10:00:00Z"},
                {
                    "row_type": "summary",
                    "run_id": "run-a",
                    "stage": "runEdgeBoard",
                    "started_at": "2026-03-21T10:00:01Z",
                    "stage_summaries": self._summary_stage_summaries_payload(metadata=metadata),
                },
            ]
            self._write_json(root / "Run_Log.json", rows)
            self._write_csv(root / "Run_Log.csv", rows)

            result = verify_run_log_parity(str(root))
            self.assertEqual(result.latest_run_ids, ["run-a"])

    def test_verify_run_log_parity_writes_latest_batch_sidecar(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            rows = [
                {"row_type": "ops", "run_id": "run-a", "stage": "stageFetchOdds", "started_at": "2026-03-21T10:00:00Z"},
                {
                    "row_type": "summary",
                    "run_id": "run-a",
                    "stage": "runEdgeBoard",
                    "started_at": "2026-03-21T10:00:01Z",
                    "stage_summaries": self._summary_stage_summaries_payload(),
                },
            ]
            self._write_json(root / "Run_Log.json", rows)
            self._write_csv(root / "Run_Log.csv", rows)

            result = verify_run_log_parity(str(root))
            sidecar_path = Path(_write_latest_batch_sidecar(str(root), result))
            payload = json.loads(sidecar_path.read_text(encoding="utf-8"))

            self.assertEqual(payload["latest_batch_run_ids"], ["run-a"])
            self.assertIn("Run_Log.json", " ".join(payload["verified_sources"]["run_log_json_files"]))
            self.assertIn("Run_Log.csv", " ".join(payload["verified_sources"]["run_log_csv_files"]))

    def test_verify_run_log_parity_fails_when_stale_json_and_fresh_csv(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stale_json_rows = [
                {
                    "row_type": "summary",
                    "run_id": "run-old",
                    "stage": "runEdgeBoard",
                    "started_at": "2026-03-21T09:59:59Z",
                    "stage_summaries": self._summary_stage_summaries_payload(),
                },
            ]
            fresh_csv_rows = [
                {
                    "row_type": "summary",
                    "run_id": "run-new",
                    "stage": "runEdgeBoard",
                    "started_at": "2026-03-21T10:00:01Z",
                    "stage_summaries": self._summary_stage_summaries_payload(),
                },
            ]
            self._write_json(root / "Run_Log.json", stale_json_rows)
            self._write_csv(root / "Run_Log.csv", fresh_csv_rows)

            with self.assertRaisesRegex(ParityError, "Latest run_id set mismatch"):
                verify_run_log_parity(str(root))

    def test_verify_run_log_parity_fails_when_required_stage_summary_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            json_rows = [
                {"row_type": "ops", "run_id": "run-a", "stage": "stageFetchOdds", "started_at": "2026-03-21T10:00:00Z"},
                {"row_type": "summary", "run_id": "run-a", "stage": "runEdgeBoard", "started_at": "2026-03-21T10:00:01Z"},
            ]
            csv_rows = [
                {"row_type": "ops", "run_id": "run-a", "stage": "stageFetchOdds", "started_at": "2026-03-21T10:00:00Z"},
                {
                    "row_type": "summary",
                    "run_id": "run-a",
                    "stage": "runEdgeBoard",
                    "started_at": "2026-03-21T10:00:01Z",
                    "stage_summaries": self._summary_stage_summaries_payload(),
                },
            ]
            self._write_json(root / "Run_Log.json", json_rows)
            self._write_csv(root / "Run_Log.csv", csv_rows)

            with self.assertRaisesRegex(ParityError, "missing required stage summaries in Run_Log.json"):
                verify_run_log_parity(str(root))

    def test_verify_run_log_parity_fails_when_metadata_claims_pass_but_rows_disagree(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            metadata_claims_pass = self._parity_metadata("run-a", pass_claim=True)
            json_rows = [
                {
                    "row_type": "summary",
                    "run_id": "run-a",
                    "stage": "runEdgeBoard",
                    "started_at": "2026-03-21T10:00:01Z",
                    "stage_summaries": {
                        "schema_id": "reason_alias_schema_v1",
                        "stage_summaries": [{"stage": "stagePersist"}],
                        "gs_export_parity_contract": metadata_claims_pass,
                    },
                },
            ]
            csv_rows = [
                {
                    "row_type": "summary",
                    "run_id": "run-a",
                    "stage": "runEdgeBoard",
                    "started_at": "2026-03-21T10:00:01Z",
                    "stage_summaries": self._summary_stage_summaries_payload(metadata=metadata_claims_pass),
                },
            ]
            self._write_json(root / "Run_Log.json", json_rows)
            self._write_csv(root / "Run_Log.csv", csv_rows)

            with self.assertRaisesRegex(ParityError, "claims pass but file rows missing required stage"):
                verify_run_log_parity(str(root))


if __name__ == "__main__":
    unittest.main()
