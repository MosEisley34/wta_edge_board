import json
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PREPARE_SCRIPT = ROOT / "scripts" / "prepare_runtime_exports.sh"
PRECHECK_SCRIPT = ROOT / "scripts" / "export_parity_precheck.sh"


class RuntimeExportFailureArtifactsTests(unittest.TestCase):
    def test_prepare_writes_manifest_pointer_and_failure_artifact_on_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "source"
            out_dir = Path(tmp) / "out"
            source.mkdir(parents=True, exist_ok=True)

            # Intentionally incomplete: missing Run_Log.json/State.csv/State.json.
            (source / "Run_Log.csv").write_text("row_type,run_id\nsummary,run-1\n", encoding="utf-8")

            proc = subprocess.run(
                ["bash", str(PREPARE_SCRIPT), "--out-dir", str(out_dir), str(source)],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertNotEqual(0, proc.returncode)
            manifest_path = out_dir / "runtime_export_manifest.json"
            pointer_path = out_dir / "runtime_export_manifest.pointer.json"
            failure_path = out_dir / "runtime_export_failure.json"
            self.assertTrue(manifest_path.is_file())
            self.assertTrue(pointer_path.is_file())
            self.assertTrue(failure_path.is_file())

            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
            self.assertEqual("failed", manifest["status"])
            self.assertEqual("failed", pointer["status"])
            self.assertEqual(str(failure_path), pointer["failure_artifact_path"])

    def test_precheck_writes_failure_pointer_on_gate_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "source"
            out_dir = Path(tmp) / "out"
            source.mkdir(parents=True, exist_ok=True)

            (source / "Run_Log.csv").write_text("row_type,run_id\nsummary,run-a\n", encoding="utf-8")
            (source / "Run_Log.json").write_text('{"rows":[{"row_type":"summary","run_id":"run-a"}]}', encoding="utf-8")
            (source / "State.csv").write_text("run_id,state_key,state_value\nrun-a,k,v\n", encoding="utf-8")
            (source / "State.json").write_text('[{"run_id":"run-a","state_key":"k","state_value":"v"}]', encoding="utf-8")

            proc = subprocess.run(
                [
                    "bash",
                    str(PRECHECK_SCRIPT),
                    "--out-dir",
                    str(out_dir),
                    "run-a",
                    "run-b",
                    str(source),
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertNotEqual(0, proc.returncode)
            self.assertTrue((out_dir / "export_parity_precheck.pointer.json").is_file())
            self.assertTrue((out_dir / "export_parity_precheck_failure.json").is_file())

    def test_precheck_rejects_derived_export_source_without_override(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "exports_live"
            out_dir = Path(tmp) / "out"
            source.mkdir(parents=True, exist_ok=True)
            (source / "runtime_export_manifest.json").write_text('{"status":"ok"}', encoding="utf-8")

            proc = subprocess.run(
                [
                    "bash",
                    str(PRECHECK_SCRIPT),
                    "--out-dir",
                    str(out_dir),
                    "run-a",
                    "run-b",
                    str(source),
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(2, proc.returncode)
            self.assertIn("derived_export_source_rejected", proc.stderr)
            self.assertIn("--allow-derived-export-source", proc.stderr)

    def test_precheck_fails_fast_on_stale_source_with_remediation_message(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "live_runtime_batch"
            out_dir = Path(tmp) / "out"
            source.mkdir(parents=True, exist_ok=True)
            (source / "Run_Log.csv").write_text(
                "run_id,stage,row_type,started_at\n"
                "run-20260330-0000,stageFetchOdds,,2026-03-30T00:00:00Z\n"
                "run-20260330-0100,stageFetchOdds,,2026-03-30T01:00:00Z\n",
                encoding="utf-8",
            )
            (source / "Run_Log.json").write_text(
                json.dumps(
                    [
                        {
                            "run_id": "run-20260330-0000",
                            "stage": "stageFetchOdds",
                            "started_at": "2026-03-30T00:00:00Z",
                        },
                        {
                            "run_id": "run-20260330-0100",
                            "stage": "stageFetchOdds",
                            "started_at": "2026-03-30T01:00:00Z",
                        },
                    ]
                ),
                encoding="utf-8",
            )

            proc = subprocess.run(
                [
                    "bash",
                    str(PRECHECK_SCRIPT),
                    "--out-dir",
                    str(out_dir),
                    "run-20260401-0000",
                    "run-20260331-0000",
                    str(source),
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(2, proc.returncode)
            self.assertIn("source_export_stale", proc.stdout)
            self.assertIn("Suggested source refresh command:", proc.stdout)
            self.assertIn("Remediation: update EXPORT_SRC to a fresher live batch path and rerun.", proc.stdout)

    def test_precheck_stops_early_when_requested_run_ids_missing_from_strongest_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "live_runtime_batch"
            out_dir = Path(tmp) / "out"
            source.mkdir(parents=True, exist_ok=True)
            (source / "Run_Log.csv").write_text(
                "run_id,stage,row_type\n"
                "run-a,stageFetchOdds,\n"
                "run-b,stageFetchOdds,\n",
                encoding="utf-8",
            )
            (source / "Run_Log.json").write_text(
                json.dumps(
                    [
                        {"run_id": "run-a", "stage": "stageFetchOdds"},
                        {"run_id": "run-b", "stage": "stageFetchOdds"},
                    ]
                ),
                encoding="utf-8",
            )

            proc = subprocess.run(
                [
                    "bash",
                    str(PRECHECK_SCRIPT),
                    "--out-dir",
                    str(out_dir),
                    "run-a",
                    "run-z",
                    str(source),
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(2, proc.returncode)
            self.assertIn("source_path_run_id_mismatch", proc.stdout)
            self.assertIn("Strongest-candidate warning", proc.stdout)
            self.assertIn("Remediation: resolve Discord run IDs against live batch path before exporting.", proc.stdout)


if __name__ == "__main__":
    unittest.main()
