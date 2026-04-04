import json
import os
import shutil
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT_UNDER_TEST = ROOT / "scripts" / "run_triage_bundle.sh"


def _write_executable(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


class RunTriageBundleTests(unittest.TestCase):
    def test_rejects_unknown_option_before_runtime_export_setup(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            repo = tmp_path / "repo"
            scripts_dir = repo / "scripts"
            scripts_dir.mkdir(parents=True)

            shutil.copy2(SCRIPT_UNDER_TEST, scripts_dir / "run_triage_bundle.sh")

            marker = repo / "prepare_called.marker"
            _write_executable(
                scripts_dir / "prepare_runtime_exports.sh",
                f"""#!/usr/bin/env bash
set -euo pipefail
echo called > "{marker}"
exit 99
""",
            )

            subprocess.run(["git", "init"], cwd=repo, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            (repo / "runtime").mkdir()
            (repo / "runtime" / "dummy.txt").write_text("ok", encoding="utf-8")

            result = subprocess.run(
                [
                    "bash",
                    str(scripts_dir / "run_triage_bundle.sh"),
                    "--export-dir",
                    str(repo / "runtime"),
                ],
                cwd=repo,
                check=False,
                capture_output=True,
                text=True,
                env={**os.environ, "PATH": os.environ.get("PATH", "")},
            )

            self.assertNotEqual(0, result.returncode)
            self.assertIn("Error: unsupported option --export-dir", result.stderr)
            self.assertIn("Usage:", result.stderr)
            self.assertFalse(marker.exists())

    def test_stage_one_writes_derive_json_after_export_prestep(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            repo = tmp_path / "repo"
            scripts_dir = repo / "scripts"
            scripts_dir.mkdir(parents=True)

            shutil.copy2(SCRIPT_UNDER_TEST, scripts_dir / "run_triage_bundle.sh")

            _write_executable(
                scripts_dir / "prepare_runtime_exports.sh",
                """#!/usr/bin/env bash
set -euo pipefail
out_dir=""
inputs=()
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --out-dir)
      shift
      out_dir="$1"
      ;;
    *)
      inputs+=("$1")
      ;;
  esac
  shift
done
rm -rf "$out_dir"
mkdir -p "$out_dir"
cat > "$out_dir/Run_Log.json" <<'JSON'
[
  {"stage": "runEdgeBoard", "run_id": "baseline-1", "ended_at": "2026-04-01T00:00:00Z"},
  {"stage": "runEdgeBoard", "run_id": "candidate-1", "ended_at": "2026-04-02T00:00:00Z"}
]
JSON
cat > "$out_dir/Run_Log.csv" <<'CSV'
stage,run_id,ended_at
runEdgeBoard,baseline-1,2026-04-01T00:00:00Z
runEdgeBoard,candidate-1,2026-04-02T00:00:00Z
CSV
cat > "$out_dir/State.json" <<'JSON'
[]
JSON
cat > "$out_dir/State.csv" <<'CSV'
col
val
CSV
""",
            )

            _write_executable(
                scripts_dir / "precheck_run_ids.py",
                """#!/usr/bin/env python3
import sys
sys.exit(0)
""",
            )
            _write_executable(
                scripts_dir / "compare_run_diagnostics.py",
                """#!/usr/bin/env python3
import sys
sys.exit(0)
""",
            )
            _write_executable(
                scripts_dir / "evaluate_edge_quality.py",
                """#!/usr/bin/env python3
import json
import sys
out = sys.argv[sys.argv.index("--out-json") + 1]
with open(out, "w", encoding="utf-8") as handle:
    json.dump({"status": "ok", "gate_verdict": "pass", "reason_code": "EDGE_QUALITY_OK"}, handle)
""",
            )

            subprocess.run(["git", "init"], cwd=repo, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            (repo / "runtime").mkdir()
            (repo / "runtime" / "dummy.txt").write_text("ok", encoding="utf-8")

            out_dir = repo / "exports_live"
            subprocess.run(
                [
                    "bash",
                    str(scripts_dir / "run_triage_bundle.sh"),
                    "--out-dir",
                    str(out_dir),
                    str(repo / "runtime"),
                ],
                cwd=repo,
                check=True,
                env={**os.environ, "PATH": os.environ.get("PATH", "")},
            )

            triage_dirs = sorted(out_dir.glob("triage_impl_*/out"))
            self.assertEqual(1, len(triage_dirs))
            derive_path = triage_dirs[0] / "derive_run_pair.json"
            self.assertTrue(derive_path.is_file())
            payload = json.loads(derive_path.read_text(encoding="utf-8"))
            self.assertEqual("ok", payload["status"])
            self.assertEqual("candidate-1", payload["candidate_run_id"])
            self.assertEqual("baseline-1", payload["baseline_run_id"])


if __name__ == "__main__":
    unittest.main()
