import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from preflight_guard import (  # noqa: E402
    CANONICAL_RUNTIME_TABS,
    enforce_preflight_guard,
    write_preflight_sidecar,
)


class PreflightGuardTests(unittest.TestCase):
    def _write_manifest(self, export_dir: Path, generated_at: str = "2026-03-24T00:00:00+00:00"):
        files = []
        for tab in CANONICAL_RUNTIME_TABS:
            csv_name = f"{tab}.csv"
            json_name = f"{tab}.json"
            files.append({"path": csv_name})
            files.append({"path": json_name})
            (export_dir / csv_name).write_text("run_id\nrun-a\nrun-b\n", encoding="utf-8")
            (export_dir / json_name).write_text(
                json.dumps([{"run_id": "run-a"}, {"run_id": "run-b"}]),
                encoding="utf-8",
            )

        payload = {
            "generated_at_utc": generated_at,
            "files": files,
        }
        (export_dir / "runtime_export_manifest.json").write_text(json.dumps(payload), encoding="utf-8")
        (export_dir / "Run_Log.json").write_text(
            json.dumps(
                [
                    {
                        "run_id": "run-a",
                        "row_type": "summary",
                        "stage": "runEdgeBoard",
                        "stage_summaries": [{"stage": stage} for stage in (
                            "stageFetchOdds",
                            "stageFetchSchedule",
                            "stageMatchEvents",
                            "stageFetchPlayerStats",
                            "stageGenerateSignals",
                            "stagePersist",
                        )],
                    },
                    {
                        "run_id": "run-b",
                        "row_type": "summary",
                        "stage": "runEdgeBoard",
                        "stage_summaries": [{"stage": stage} for stage in (
                            "stageFetchOdds",
                            "stageFetchSchedule",
                            "stageMatchEvents",
                            "stageFetchPlayerStats",
                            "stageGenerateSignals",
                            "stagePersist",
                        )],
                    },
                ]
            ),
            encoding="utf-8",
        )

    def test_blocks_when_sidecar_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_manifest(root)
            with self.assertRaises(ValueError):
                enforce_preflight_guard(str(root), "run-a", "run-b", "")

    def test_allows_matching_sidecar(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_manifest(root)
            sidecar_path = write_preflight_sidecar(str(root), "run-a", "run-b", False, "")
            sidecar = json.loads(Path(sidecar_path).read_text(encoding="utf-8"))
            self.assertIn("run_checklist_by_run_id", sidecar["preflight_evidence"])
            self.assertIn("duplicate_summary_diagnostics_by_run_id", sidecar["preflight_evidence"])
            self.assertIn("export_freshness", sidecar["preflight_evidence"])
            self.assertTrue(sidecar["preflight_evidence"]["raw_tab_completeness"]["is_complete"])
            status = enforce_preflight_guard(str(root), "run-b", "run-a", "")
            self.assertEqual(status["status"], "ok")

    def test_sidecar_export_freshness_marks_fresh_export_dir_for_parseable_run_ids(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_manifest(root)
            (root / "Run_Log.csv").write_text(
                "run_id,stage,row_type\nrun-20260330-0000,stageFetchOdds,\nrun-20260330-0100,runEdgeBoard,summary\n",
                encoding="utf-8",
            )
            (root / "Run_Log.json").write_text(
                json.dumps(
                    [
                        {"run_id": "run-20260330-0000", "stage": "stageFetchOdds"},
                        {"run_id": "run-20260330-0100", "row_type": "summary", "stage": "runEdgeBoard", "stage_summaries": []},
                    ]
                ),
                encoding="utf-8",
            )

            sidecar_path = write_preflight_sidecar(
                str(root),
                "run-20260330-0000",
                "run-20260330-0100",
                False,
                "",
            )
            sidecar = json.loads(Path(sidecar_path).read_text(encoding="utf-8"))
            freshness = sidecar["preflight_evidence"]["export_freshness"]
            self.assertEqual("fresh_export_dir", freshness["reason_code"])

    def test_sidecar_includes_non_identical_duplicate_summary_diagnostics(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_manifest(root)
            (root / "Run_Log.json").write_text(
                json.dumps(
                    [
                        {"run_id": "run-a", "row_type": "summary", "stage": "runEdgeBoard", "ended_at": "2026-03-30T00:00:00Z"},
                        {"run_id": "run-a", "row_type": "summary", "stage": "runEdgeBoard", "ended_at": "2026-03-30T00:01:00Z"},
                        {"run_id": "run-b", "row_type": "summary", "stage": "runEdgeBoard", "ended_at": "2026-03-30T00:02:00Z"},
                    ]
                ),
                encoding="utf-8",
            )
            sidecar_path = write_preflight_sidecar(str(root), "run-a", "run-b", False, "")
            sidecar = json.loads(Path(sidecar_path).read_text(encoding="utf-8"))
            duplicate_diag = sidecar["preflight_evidence"]["duplicate_summary_diagnostics_by_run_id"]["run-a"]
            self.assertTrue(duplicate_diag["compare_will_fail_due_to_duplicate_summary_rows"])

    def test_emergency_override_requires_valid_incident_tag(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_manifest(root)
            with self.assertRaises(ValueError):
                enforce_preflight_guard(str(root), "run-a", "run-b", "bad-tag")
            status = enforce_preflight_guard(str(root), "run-a", "run-b", "INC-1234")
            self.assertEqual(status["status"], "emergency_override")

    def test_sidecar_manifest_mismatch_blocks_compare_with_remediation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_manifest(root, generated_at="2026-03-24T00:00:00+00:00")
            write_preflight_sidecar(str(root), "run-a", "run-b", False, "")
            self._write_manifest(root, generated_at="2026-03-24T01:00:00+00:00")
            with self.assertRaisesRegex(
                ValueError,
                r"Preflight guard failed: sidecar manifest stamp does not match current export batch\.",
            ) as ctx:
                enforce_preflight_guard(str(root), "run-a", "run-b", "")
            self.assertIn(
                "Re-run scripts/export_parity_precheck.sh for this export directory.",
                str(ctx.exception),
            )

    def test_sidecar_reports_missing_tab_and_enforce_blocks_compare(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_manifest(root)
            (root / "ProviderHealth.json").unlink()
            sidecar_path = write_preflight_sidecar(str(root), "run-a", "run-b", False, "")
            sidecar = json.loads(Path(sidecar_path).read_text(encoding="utf-8"))
            raw = sidecar["preflight_evidence"]["raw_tab_completeness"]
            self.assertFalse(raw["is_complete"])
            self.assertIn("ProviderHealth", raw["missing_tabs"])
            with self.assertRaisesRegex(ValueError, r"raw runtime tab completeness check is not satisfied"):
                enforce_preflight_guard(str(root), "run-a", "run-b", "")

    def test_sidecar_reports_stale_json_row_count_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_manifest(root)
            (root / "Signals.csv").write_text("run_id\nrun-a\nrun-b\nrun-c\n", encoding="utf-8")
            sidecar_path = write_preflight_sidecar(str(root), "run-a", "run-b", False, "")
            sidecar = json.loads(Path(sidecar_path).read_text(encoding="utf-8"))
            raw = sidecar["preflight_evidence"]["raw_tab_completeness"]
            self.assertFalse(raw["is_complete"])
            mismatched_tabs = {item["tab"] for item in raw["mismatched_tabs"]}
            self.assertIn("Signals", mismatched_tabs)
            with self.assertRaisesRegex(ValueError, r"mismatched_tabs=\['Signals'\]"):
                enforce_preflight_guard(str(root), "run-a", "run-b", "")

    def test_sidecar_reports_row_count_mismatch_for_raw_tab(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_manifest(root)
            (root / "Raw_Odds.json").write_text(json.dumps([{"run_id": "run-a"}]), encoding="utf-8")
            sidecar_path = write_preflight_sidecar(str(root), "run-a", "run-b", False, "")
            sidecar = json.loads(Path(sidecar_path).read_text(encoding="utf-8"))
            raw = sidecar["preflight_evidence"]["raw_tab_completeness"]
            mismatch = next(item for item in raw["mismatched_tabs"] if item["tab"] == "Raw_Odds")
            self.assertEqual(2, mismatch["csv_rows"])
            self.assertEqual(1, mismatch["json_rows"])
            with self.assertRaisesRegex(ValueError, r"mismatched_tabs=\['Raw_Odds'\]"):
                enforce_preflight_guard(str(root), "run-a", "run-b", "")


if __name__ == "__main__":
    unittest.main()
