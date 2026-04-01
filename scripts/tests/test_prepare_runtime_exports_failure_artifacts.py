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
            self.assertTrue((out_dir / "runtime_export_manifest.json").is_file())
            self.assertTrue((out_dir / "runtime_export_manifest.pointer.json").is_file())
            self.assertTrue((out_dir / "export_parity_precheck.pointer.json").is_file())
            self.assertTrue((out_dir / "export_parity_precheck_failure.json").is_file())


if __name__ == "__main__":
    unittest.main()
