import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from preflight_guard import enforce_preflight_guard, write_preflight_sidecar  # noqa: E402


class PreflightGuardTests(unittest.TestCase):
    def _write_manifest(self, export_dir: Path, generated_at: str = "2026-03-24T00:00:00+00:00"):
        payload = {"generated_at_utc": generated_at, "files": []}
        (export_dir / "runtime_export_manifest.json").write_text(json.dumps(payload), encoding="utf-8")

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
            write_preflight_sidecar(str(root), "run-a", "run-b", False, "")
            status = enforce_preflight_guard(str(root), "run-b", "run-a", "")
            self.assertEqual(status["status"], "ok")

    def test_emergency_override_requires_valid_incident_tag(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_manifest(root)
            with self.assertRaises(ValueError):
                enforce_preflight_guard(str(root), "run-a", "run-b", "bad-tag")
            status = enforce_preflight_guard(str(root), "run-a", "run-b", "INC-1234")
            self.assertEqual(status["status"], "emergency_override")


if __name__ == "__main__":
    unittest.main()
