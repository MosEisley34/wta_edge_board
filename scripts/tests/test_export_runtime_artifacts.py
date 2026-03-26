import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "export_runtime_artifacts.sh"


class ExportRuntimeArtifactsTests(unittest.TestCase):
    def test_selects_true_run_log_json_when_sidecar_exists(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "source"
            out_dir = Path(tmp) / "out"
            source.mkdir(parents=True, exist_ok=True)

            run_log_csv = source / "Run_Log.csv"
            run_log_json = source / "Run_Log.json"
            sidecar_note = source / "run_log_latest_batch_note.json"

            run_log_csv.write_text("row_type,run_id\nsummary,run-1\n", encoding="utf-8")
            run_log_json.write_text('{"rows":[{"run_id":"run-1"}]}', encoding="utf-8")
            sidecar_note.write_text('{"note":"sidecar"}', encoding="utf-8")

            proc = subprocess.run(
                ["bash", str(SCRIPT), "--out-dir", str(out_dir), str(source)],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(0, proc.returncode, msg=proc.stderr)
            exported_json = (out_dir / "Run_Log.json").read_text(encoding="utf-8")
            self.assertIn('"run_id":"run-1"', exported_json)
            self.assertNotIn('"note":"sidecar"', exported_json)

    def test_sidecar_note_json_is_not_selected_as_canonical(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "source"
            out_dir = Path(tmp) / "out"
            source.mkdir(parents=True, exist_ok=True)

            (source / "Run_Log.csv").write_text("row_type,run_id\nsummary,run-1\n", encoding="utf-8")
            (source / "run_log_latest_batch_note.json").write_text(
                '{"note":"sidecar"}',
                encoding="utf-8",
            )

            proc = subprocess.run(
                ["bash", str(SCRIPT), "--out-dir", str(out_dir), str(source)],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertNotEqual(0, proc.returncode)
            self.assertIn("could not find a directory snapshot", proc.stderr)


if __name__ == "__main__":
    unittest.main()
