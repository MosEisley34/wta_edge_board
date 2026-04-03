import csv
import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import precheck_run_ids  # noqa: E402
from evaluate_edge_quality import EdgeQualityGateConfig, _snapshot  # noqa: E402


class PrecheckRunIdsSourceContractTests(unittest.TestCase):
    @staticmethod
    def _gate_ready_summary_row(run_id: str, ended_at: str):
        return {
            "run_id": run_id,
            "row_type": "summary",
            "stage": "runEdgeBoard",
            "ended_at": ended_at,
            "reason_codes": {},
            "stage_summaries": {
                "schema_id": "reason_code_aliases.v1",
                "stage_summaries": [{"stage": stage} for stage in (
                    "stageFetchOdds",
                    "stageFetchSchedule",
                    "stageMatchEvents",
                    "stageFetchPlayerStats",
                    "stageGenerateSignals",
                    "stagePersist",
                )],
                "compare_prerequisites": {
                    "coverage": {"requested": 0, "resolved": 0, "unresolved": 0},
                    "reason_code_placeholders": {"STATS_MISS_A": 0, "STATS_MISS_B": 0},
                },
            },
        }

    def _write_json(self, path: Path, run_ids, summary_run_ids=None):
        summary_run_ids = set(run_ids if summary_run_ids is None else summary_run_ids)
        rows = [{"run_id": run_id, "stage": "stageFetchOdds"} for run_id in run_ids]
        rows.extend(
            {
                "run_id": run_id,
                "row_type": "summary",
                "stage": "runEdgeBoard",
            }
            for run_id in summary_run_ids
        )
        path.write_text(json.dumps(rows), encoding="utf-8")

    def _write_csv(self, path: Path, run_ids, summary_run_ids=None):
        summary_run_ids = set(run_ids if summary_run_ids is None else summary_run_ids)
        headers = ["run_id", "stage", "row_type"]
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=headers)
            writer.writeheader()
            for run_id in run_ids:
                writer.writerow({"run_id": run_id, "stage": "stageFetchOdds", "row_type": ""})
            for run_id in summary_run_ids:
                writer.writerow({"run_id": run_id, "stage": "runEdgeBoard", "row_type": "summary"})

    def _run_main(self, argv):
        with patch.object(sys, "argv", argv):
            output = io.StringIO()
            with redirect_stdout(output):
                code = precheck_run_ids.main()
        return code, output.getvalue()

    def test_fails_when_target_run_exists_only_in_csv(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_csv(root / "Run_Log.csv", ["run-a", "run-b"])
            self._write_json(root / "Run_Log.json", ["run-a"])

            code, output = self._run_main(
                ["precheck_run_ids.py", "run-a", "run-b", "--export-dir", str(root)]
            )

            self.assertEqual(code, 2)
            self.assertIn("run_id_source_mismatch", output)
            self.assertIn("run-b: csv_present=true json_present=false", output)

    def test_fails_when_target_run_exists_only_in_json_even_with_override(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_csv(root / "Run_Log.csv", ["run-a"])
            self._write_json(root / "Run_Log.json", ["run-a", "run-b"])

            code, output = self._run_main(
                [
                    "precheck_run_ids.py",
                    "run-a",
                    "run-b",
                    "--export-dir",
                    str(root),
                    "--allow-csv-only-triage",
                    "--allow-csv-only-triage-incident-tag",
                    "INC-1234",
                ]
            )

            self.assertEqual(code, 2)
            self.assertIn("run_id_source_mismatch", output)
            self.assertIn("run-b: csv_present=false json_present=true", output)

    def test_passes_when_target_runs_exist_in_both_sources(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_csv(root / "Run_Log.csv", ["run-a", "run-b"])
            self._write_json(root / "Run_Log.json", ["run-a", "run-b"])

            code, output = self._run_main(
                ["precheck_run_ids.py", "run-a", "run-b", "--export-dir", str(root)]
            )

            self.assertEqual(code, 0)
            self.assertIn("Precheck passed", output)
            self.assertIn("Preflight evidence checklist (per run):", output)
            self.assertIn("has_runEdgeBoard_summary", output)

    def test_override_allows_csv_only_presence_with_degraded_confidence_warning(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_csv(root / "Run_Log.csv", ["run-a", "run-b"])
            self._write_json(root / "Run_Log.json", ["run-a"])

            code, output = self._run_main(
                [
                    "precheck_run_ids.py",
                    "run-a",
                    "run-b",
                    "--export-dir",
                    str(root),
                    "--allow-csv-only-triage",
                    "--allow-csv-only-triage-incident-tag",
                    "INC-1234",
                ]
            )

            self.assertEqual(code, 0)
            self.assertIn("degraded_confidence_csv_only_triage", output)
            self.assertIn("run-b: csv_present=true json_present=false", output)

    def test_override_requires_incident_tag(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_csv(root / "Run_Log.csv", ["run-a", "run-b"])
            self._write_json(root / "Run_Log.json", ["run-a"])

            code, output = self._run_main(
                [
                    "precheck_run_ids.py",
                    "run-a",
                    "run-b",
                    "--export-dir",
                    str(root),
                    "--allow-csv-only-triage",
                ]
            )

            self.assertEqual(code, 2)
            self.assertIn("Incident tag is required", output)

    def test_missing_run_ids_prints_source_recent_and_remediation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_csv(root / "Run_Log.csv", ["run-100", "run-101", "run-102"])
            self._write_json(root / "Run_Log.json", ["run-100", "run-101", "run-102"])

            code, output = self._run_main(
                [
                    "precheck_run_ids.py",
                    "run-100",
                    "run-999",
                    "--export-dir",
                    str(root),
                    "--recent-limit",
                    "2",
                ]
            )

            self.assertEqual(code, 2)
            self.assertIn("Searched export source directory/path:", output)
            self.assertIn("Top recent run IDs found (limit=2): run-100, run-101", output)
            self.assertIn("Closest run IDs to run-999:", output)
            self.assertIn("Remediation: point EXPORT_SRC to a fresh batch path", output)

    def test_fails_when_run_id_present_but_candidate_summary_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_csv(root / "Run_Log.csv", ["run-a", "run-b"], summary_run_ids=["run-a"])
            self._write_json(root / "Run_Log.json", ["run-a", "run-b"], summary_run_ids=["run-a"])

            code, output = self._run_main(
                ["precheck_run_ids.py", "run-a", "run-b", "--export-dir", str(root)]
            )

            self.assertEqual(code, 2)
            self.assertIn("compare_contract_missing", output)
            self.assertIn("missing candidate summary row", output)

    def test_fails_fast_when_requested_run_ids_are_newer_than_export_batch(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_csv(root / "Run_Log.csv", ["run-20260330-0000", "run-20260330-0100"])
            self._write_json(root / "Run_Log.json", ["run-20260330-0000", "run-20260330-0100"])

            code, output = self._run_main(
                [
                    "precheck_run_ids.py",
                    "run-20260401-0000",
                    "run-20260331-0000",
                    "--export-dir",
                    str(root),
                ]
            )

            self.assertEqual(code, 2)
            self.assertIn("Precheck failed: stale_export_dir.", output)
            self.assertIn("suggested_export_command", output)
            self.assertIn(
                f"scripts/export_parity_precheck.sh --out-dir {root} run-20260401-0000 run-20260331-0000 <fresh-runtime-export-path>",
                output,
            )

    def test_fresh_export_status_does_not_trigger_stale_export_fail(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_csv(root / "Run_Log.csv", ["run-20260330-0000", "run-20260330-0100"])
            self._write_json(root / "Run_Log.json", ["run-20260330-0000", "run-20260330-0100"])

            code, output = self._run_main(
                [
                    "precheck_run_ids.py",
                    "run-20260330-0000",
                    "run-20260330-0100",
                    "--export-dir",
                    str(root),
                ]
            )

            self.assertEqual(code, 0)
            self.assertIn('"reason_code": "fresh_export_dir"', output)
            self.assertIn("Precheck passed", output)

    def test_freshness_uses_parseable_started_at_timestamps_for_non_timestamp_run_ids(self):
        fixture = ROOT / "scripts" / "fixtures" / "precheck_run_ids_freshness_parseable_started_at.json"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "Run_Log.json").write_text(fixture.read_text(encoding="utf-8"), encoding="utf-8")
            (root / "Run_Log.csv").write_text(
                "run_id,stage,row_type\nrun-a,stageFetchOdds,\nrun-b,stageFetchOdds,\n",
                encoding="utf-8",
            )

            code, output = self._run_main(
                ["precheck_run_ids.py", "run-a", "run-b", "--export-dir", str(root)]
            )

            self.assertEqual(code, 0)
            self.assertIn('"reason_code": "fresh_export_dir"', output)
            self.assertNotIn('"reason_code": "requested_run_id_timestamp_unparseable"', output)
            self.assertNotIn('"reason_code": "export_run_id_timestamp_unparseable"', output)
            self.assertIn('"latest_requested_run_id_timestamp_utc": "2026-03-30T00:20:00+00:00"', output)
            self.assertIn('"max_export_run_id_timestamp_utc": "2026-03-30T00:30:00+00:00"', output)

    def test_gate_prereqs_accepts_compare_prereq_placeholders_from_summary_envelope(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = [
                {
                    "run_id": "run-a",
                    "row_type": "summary",
                    "stage": "runEdgeBoard",
                    "reason_codes": {},
                    "stage_summaries": {
                        "schema_id": "reason_code_aliases.v1",
                        "stage_summaries": [{"stage": stage} for stage in (
                            "stageFetchOdds",
                            "stageFetchSchedule",
                            "stageMatchEvents",
                            "stageFetchPlayerStats",
                            "stageGenerateSignals",
                            "stagePersist",
                        )],
                        "compare_prerequisites": {
                            "coverage": {"requested": 0, "resolved": 0, "unresolved": 0},
                            "reason_code_placeholders": {"STATS_MISS_A": 0, "STATS_MISS_B": 0},
                        },
                    },
                },
                {
                    "run_id": "run-b",
                    "row_type": "summary",
                    "stage": "runEdgeBoard",
                    "reason_codes": {},
                    "stage_summaries": {
                        "schema_id": "reason_code_aliases.v1",
                        "stage_summaries": [{"stage": stage} for stage in (
                            "stageFetchOdds",
                            "stageFetchSchedule",
                            "stageMatchEvents",
                            "stageFetchPlayerStats",
                            "stageGenerateSignals",
                            "stagePersist",
                        )],
                        "compare_prerequisites": {
                            "coverage": {"requested": 0, "resolved": 0, "unresolved": 0},
                            "reason_code_placeholders": {"STATS_MISS_A": 0, "STATS_MISS_B": 0},
                        },
                    },
                },
            ]
            (root / "Run_Log.json").write_text(json.dumps(payload), encoding="utf-8")

            code, output = self._run_main(
                [
                    "precheck_run_ids.py",
                    "run-a",
                    "run-b",
                    "--export-dir",
                    str(root),
                    "--require-gate-prereqs",
                ]
            )

            self.assertEqual(code, 0)
            self.assertIn("Precheck passed", output)

    def test_gate_prereqs_fails_when_placeholders_missing_even_with_stages_and_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = [
                {
                    "run_id": "run-a",
                    "row_type": "summary",
                    "stage": "runEdgeBoard",
                    "reason_codes": {},
                    "stage_summaries": {
                        "schema_id": "reason_code_aliases.v1",
                        "stage_summaries": [{"stage": stage} for stage in (
                            "stageFetchOdds",
                            "stageFetchSchedule",
                            "stageMatchEvents",
                            "stageFetchPlayerStats",
                            "stageGenerateSignals",
                            "stagePersist",
                        )],
                        "compare_prerequisites": {
                            "coverage": {"requested": 0, "resolved": 0, "unresolved": 0},
                            "reason_code_placeholders": {},
                        },
                    },
                },
                {
                    "run_id": "run-b",
                    "row_type": "summary",
                    "stage": "runEdgeBoard",
                    "reason_codes": {},
                    "stage_summaries": {
                        "schema_id": "reason_code_aliases.v1",
                        "stage_summaries": [{"stage": stage} for stage in (
                            "stageFetchOdds",
                            "stageFetchSchedule",
                            "stageMatchEvents",
                            "stageFetchPlayerStats",
                            "stageGenerateSignals",
                            "stagePersist",
                        )],
                        "compare_prerequisites": {
                            "coverage": {"requested": 0, "resolved": 0, "unresolved": 0},
                            "reason_code_placeholders": {},
                        },
                    },
                },
            ]
            (root / "Run_Log.json").write_text(json.dumps(payload), encoding="utf-8")

            code, output = self._run_main(
                [
                    "precheck_run_ids.py",
                    "run-a",
                    "run-b",
                    "--export-dir",
                    str(root),
                    "--require-gate-prereqs",
                ]
            )

            self.assertEqual(code, 2)
            self.assertIn("compare_contract_missing", output)
            self.assertIn("compare_ready\": false", output)
            self.assertIn("reason_codes missing STATS_MISS_A", output)
            self.assertIn("reason_codes missing STATS_MISS_B", output)

    def test_cross_tool_non_identical_duplicate_summaries_fail_precheck_and_compare_cardinality(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            rows = [
                {"run_id": "run-a", "row_type": "summary", "stage": "runEdgeBoard", "ended_at": "2026-03-30T00:00:00Z"},
                {"run_id": "run-a", "row_type": "summary", "stage": "runEdgeBoard", "ended_at": "2026-03-30T00:05:00Z"},
                {"run_id": "run-b", "row_type": "summary", "stage": "runEdgeBoard", "ended_at": "2026-03-30T00:10:00Z"},
            ]
            (root / "Run_Log.json").write_text(json.dumps(rows), encoding="utf-8")

            code, output = self._run_main(
                ["precheck_run_ids.py", "run-a", "run-b", "--export-dir", str(root)]
            )
            self.assertEqual(code, 2)
            self.assertIn("compare_will_fail_due_to_duplicate_summary_rows=true", output)

            with self.assertRaisesRegex(ValueError, "Expected exactly one runEdgeBoard summary row"):
                _snapshot(rows=rows, run_id="run-a", config=EdgeQualityGateConfig())

    def test_cross_tool_identical_duplicate_summaries_pass_precheck_and_compare_cardinality(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            duplicate = {
                "run_id": "run-a",
                "row_type": "summary",
                "stage": "runEdgeBoard",
                "ended_at": "2026-03-30T00:00:00Z",
                "feature_completeness": 0.9,
                "edge_volatility": 0.01,
                "signal_decision_summary": "{}",
                "stage_summaries": "[]",
            }
            rows = [
                {**duplicate, "_source_kind": "csv"},
                {**duplicate, "_source_kind": "json"},
                {
                    "run_id": "run-b",
                    "row_type": "summary",
                    "stage": "runEdgeBoard",
                    "ended_at": "2026-03-30T00:10:00Z",
                    "feature_completeness": 0.9,
                    "edge_volatility": 0.01,
                    "signal_decision_summary": "{}",
                    "stage_summaries": "[]",
                },
            ]
            (root / "Run_Log.json").write_text(json.dumps(rows), encoding="utf-8")

            code, output = self._run_main(
                ["precheck_run_ids.py", "run-a", "run-b", "--export-dir", str(root)]
            )
            self.assertEqual(code, 0, msg=output)
            self.assertIn("Precheck passed", output)

            snapshot = _snapshot(rows=rows, run_id="run-a", config=EdgeQualityGateConfig())
            self.assertEqual("run-a", snapshot["run_id"])

    def test_pair_level_contract_fails_when_runs_are_individually_valid_but_duplicates_exist(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run_a_json = self._gate_ready_summary_row("run-a", "2026-03-30T00:00:00Z")
            run_a_csv = self._gate_ready_summary_row("run-a", "2026-03-30T00:05:00Z")
            run_a_csv["_source_kind"] = "csv"
            run_b = self._gate_ready_summary_row("run-b", "2026-03-30T00:10:00Z")
            rows = [run_a_json, run_a_csv, run_b]
            (root / "Run_Log.json").write_text(json.dumps(rows), encoding="utf-8")

            code, output = self._run_main(
                [
                    "precheck_run_ids.py",
                    "run-a",
                    "run-b",
                    "--export-dir",
                    str(root),
                    "--require-gate-prereqs",
                ]
            )

            self.assertEqual(code, 2)
            self.assertIn("\"run_prereq_pass\": true", output)
            self.assertIn("\"pair_contract_pass\": false", output)
            self.assertIn("\"compare_contract_pass\": false", output)
            self.assertIn("\"reason_codes\": [\"pair_duplicate_summary_rows\"]", output)
            self.assertIn(
                "pair_contract_pass=false reason_code=pair_duplicate_summary_rows run_ids=run-a",
                output,
            )


if __name__ == "__main__":
    unittest.main()
