import csv
import json
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "sync_stage_player_stats_contract.py"


class SyncStagePlayerStatsContractTests(unittest.TestCase):
    @staticmethod
    def _parse_json_like(value):
        if isinstance(value, (dict, list)):
            return value
        if isinstance(value, str):
            return json.loads(value)
        return {}

    def _write_csv_fixture(self, fixture_name: str, path: Path) -> None:
        fixture = json.loads((ROOT / "scripts" / "fixtures" / fixture_name).read_text(encoding="utf-8"))
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fixture["headers"])
            writer.writeheader()
            writer.writerows(fixture["rows"])

    def _write_run_log_csv(self, path: Path) -> None:
        headers = [
            "row_type",
            "run_id",
            "stage",
            "started_at",
            "message",
            "summary",
            "reason_metadata",
            "reason_codes",
            "stage_summaries",
        ]
        rows = [
            {
                "row_type": "stage",
                "run_id": "run-a",
                "stage": "stageFetchPlayerStats",
                "started_at": "2026-03-26T00:00:00Z",
                "message": json.dumps(
                    {
                        "summary": {"status": "ok", "source": "ta"},
                        "reason_metadata": {"requested_player_count": 4, "resolved_player_count": 3},
                    }
                ),
                "reason_codes": json.dumps({"STATS_ENR": 3, "STATS_MISS_A": 1, "STATS_MISS_B": 0}),
            },
            {
                "row_type": "summary",
                "run_id": "run-a",
                "stage": "runEdgeBoard",
                "started_at": "2026-03-26T00:00:01Z",
                "reason_codes": json.dumps({"STATS_ENR": 0, "STATS_MISS_A": 0, "STATS_MISS_B": 0}),
                "stage_summaries": json.dumps(
                    {
                        "stage_summaries": [
                            {
                                "stage": "stageFetchPlayerStats",
                                "summary": {"status": "stale"},
                                "reason_metadata": {"requested_player_count": 0, "resolved_player_count": 0},
                                "reason_codes": {"STATS_ENR": 0, "STATS_MISS_A": 0, "STATS_MISS_B": 0},
                            }
                        ]
                    }
                ),
            },
        ]
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rows)

    def test_sync_updates_csv_and_json_stage_contract(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_run_log_csv(root / "Run_Log.csv")
            (root / "State.csv").write_text("run_id\nrun-a\n", encoding="utf-8")
            (root / "State.json").write_text("[]\n", encoding="utf-8")
            (root / "Run_Log.json").write_text("[]\n", encoding="utf-8")

            subprocess.run(
                [
                    "python3",
                    str(SCRIPT),
                    "--export-dir",
                    str(root),
                    "--run-id",
                    "run-a",
                    "--stage",
                    "stageFetchPlayerStats",
                ],
                check=True,
                capture_output=True,
                text=True,
            )

            payload = json.loads((root / "Run_Log.json").read_text(encoding="utf-8"))
            summary_row = next(row for row in payload if row.get("row_type") == "summary")
            stage_summaries = self._parse_json_like(summary_row["stage_summaries"])
            stage_entry = next(item for item in stage_summaries["stage_summaries"] if item.get("stage") == "stageFetchPlayerStats")

            self.assertEqual(stage_entry["summary"]["status"], "ok")
            self.assertEqual(stage_entry["reason_metadata"]["resolved_player_count"], 3)
            self.assertEqual(stage_entry["reason_codes"]["STATS_ENR"], 3)
            summary_reason_codes = self._parse_json_like(summary_row["reason_codes"])
            self.assertEqual(summary_reason_codes["STATS_MISS_A"], 1)

    def test_no_demand_fixture_normalizes_schema_in_csv_and_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_csv_fixture("sync_stage_player_stats_no_demand.json", root / "Run_Log.csv")
            (root / "State.csv").write_text("run_id\nrun-no-demand\n", encoding="utf-8")
            (root / "State.json").write_text("[]\n", encoding="utf-8")
            (root / "Run_Log.json").write_text("[]\n", encoding="utf-8")

            subprocess.run(
                [
                    "python3",
                    str(SCRIPT),
                    "--export-dir",
                    str(root),
                    "--run-id",
                    "run-no-demand",
                    "--stage",
                    "stageFetchPlayerStats",
                ],
                check=True,
                capture_output=True,
                text=True,
            )

            with (root / "Run_Log.csv").open("r", encoding="utf-8", newline="") as handle:
                csv_rows = list(csv.DictReader(handle))
            stage_row = next(row for row in csv_rows if row.get("row_type") == "stage")
            metadata = json.loads(stage_row["reason_metadata"])
            self.assertEqual({"requested", "resolved", "resolved_rate", "unresolved"}, set(metadata["coverage"].keys()))
            self.assertIn("player_a_source", metadata)
            self.assertIn("player_resolution_source_by_player", metadata)
            self.assertEqual(
                {"upstream_payload_empty_or_changed_shape", "parser_contract_mismatch", "no_demand_cases"},
                set(metadata["reason_code_partitioning"].keys()),
            )
            self.assertEqual({}, json.loads(stage_row["summary"]))
            self.assertEqual({}, json.loads(stage_row["reason_codes"]))

            payload = json.loads((root / "Run_Log.json").read_text(encoding="utf-8"))
            summary_row = next(row for row in payload if row.get("row_type") == "summary")
            stage_summaries = self._parse_json_like(summary_row["stage_summaries"])
            stage_entry = next(item for item in stage_summaries["stage_summaries"] if item.get("stage") == "stageFetchPlayerStats")
            self.assertEqual({}, stage_entry["summary"])
            self.assertEqual({}, stage_entry["reason_codes"])
            self.assertEqual({"requested", "resolved", "resolved_rate", "unresolved"}, set(stage_entry["reason_metadata"]["coverage"].keys()))
            self.assertEqual(
                {"upstream_payload_empty_or_changed_shape", "parser_contract_mismatch", "no_demand_cases"},
                set(stage_entry["reason_metadata"]["reason_code_partitioning"].keys()),
            )

    def test_validate_only_fails_on_key_field_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_run_log_csv(root / "Run_Log.csv")
            (root / "State.csv").write_text("run_id\nrun-a\n", encoding="utf-8")
            mismatched = [
                {
                    "row_type": "summary",
                    "run_id": "run-a",
                    "stage": "runEdgeBoard",
                    "reason_codes": {"STATS_ENR": 9},
                    "stage_summaries": [
                        {
                            "stage": "stageFetchPlayerStats",
                            "summary": {"status": "json-drift"},
                            "reason_metadata": {"requested_player_count": 8},
                            "reason_codes": {"STATS_ENR": 9},
                        }
                    ],
                }
            ]
            (root / "Run_Log.json").write_text(json.dumps(mismatched), encoding="utf-8")
            (root / "State.json").write_text("[]\n", encoding="utf-8")

            result = subprocess.run(
                [
                    "python3",
                    str(SCRIPT),
                    "--export-dir",
                    str(root),
                    "--run-id",
                    "run-a",
                    "--validate-only",
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("CSV/JSON sync mismatch", result.stderr)


if __name__ == "__main__":
    unittest.main()
