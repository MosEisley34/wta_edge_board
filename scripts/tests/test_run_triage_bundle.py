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
            shutil.copy2(ROOT / "scripts" / "preflight_guard.py", scripts_dir / "preflight_guard.py")
            shutil.copy2(
                ROOT / "scripts" / "run_summary_cardinality.py",
                scripts_dir / "run_summary_cardinality.py",
            )

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
            shutil.copy2(ROOT / "scripts" / "preflight_guard.py", scripts_dir / "preflight_guard.py")
            shutil.copy2(
                ROOT / "scripts" / "run_summary_cardinality.py",
                scripts_dir / "run_summary_cardinality.py",
            )

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
  {"stage": "runEdgeBoard", "row_type": "summary", "run_id": "baseline-1", "ended_at": "2026-04-01T00:00:00Z", "stage_summaries": [{"stage": "stageFetchOdds"}, {"stage": "stageFetchSchedule"}, {"stage": "stageMatchEvents"}, {"stage": "stageFetchPlayerStats"}, {"stage": "stageGenerateSignals"}, {"stage": "stagePersist"}]},
  {"stage": "runEdgeBoard", "row_type": "summary", "run_id": "candidate-1", "ended_at": "2026-04-02T00:00:00Z", "stage_summaries": [{"stage": "stageFetchOdds"}, {"stage": "stageFetchSchedule"}, {"stage": "stageMatchEvents"}, {"stage": "stageFetchPlayerStats"}, {"stage": "stageGenerateSignals"}, {"stage": "stagePersist"}]}
]
JSON
cat > "$out_dir/Run_Log.csv" <<'CSV'
stage,run_id,ended_at
runEdgeBoard,baseline-1,2026-04-01T00:00:00Z
runEdgeBoard,candidate-1,2026-04-02T00:00:00Z
CSV
cat > "$out_dir/State.json" <<'JSON'
[{"col":"val"}]
JSON
cat > "$out_dir/State.csv" <<'CSV'
col
val
CSV
for tab in Config Raw_Odds Raw_Schedule Raw_Player_Stats Match_Map Signals ProviderHealth; do
  cat > "$out_dir/${tab}.csv" <<'CSV'
id
1
CSV
  cat > "$out_dir/${tab}.json" <<'JSON'
[{"id": 1}]
JSON
done
cat > "$out_dir/runtime_export_manifest.json" <<'JSON'
{
  "generated_at_utc": "2026-04-02T00:00:00Z",
  "files": [
    {"path": "Run_Log.csv"},
    {"path": "Run_Log.json"},
    {"path": "State.csv"},
    {"path": "State.json"}
  ]
}
JSON
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

    def test_stage_three_generates_preflight_before_compare(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            repo = tmp_path / "repo"
            scripts_dir = repo / "scripts"
            scripts_dir.mkdir(parents=True)

            shutil.copy2(SCRIPT_UNDER_TEST, scripts_dir / "run_triage_bundle.sh")
            shutil.copy2(ROOT / "scripts" / "preflight_guard.py", scripts_dir / "preflight_guard.py")
            shutil.copy2(
                ROOT / "scripts" / "run_summary_cardinality.py",
                scripts_dir / "run_summary_cardinality.py",
            )

            _write_executable(
                scripts_dir / "prepare_runtime_exports.sh",
                """#!/usr/bin/env bash
set -euo pipefail
out_dir=""
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --out-dir)
      shift
      out_dir="$1"
      ;;
  esac
  shift
done
rm -rf "$out_dir"
mkdir -p "$out_dir"
cat > "$out_dir/Run_Log.json" <<'JSON'
[
  {
    "stage": "runEdgeBoard",
    "row_type": "summary",
    "run_id": "baseline-1",
    "ended_at": "2026-04-01T00:00:00Z",
    "stage_summaries": [
      {"stage": "stageFetchOdds"},
      {"stage": "stageFetchSchedule"},
      {"stage": "stageMatchEvents"},
      {"stage": "stageFetchPlayerStats"},
      {"stage": "stageGenerateSignals"},
      {"stage": "stagePersist"}
    ]
  },
  {
    "stage": "runEdgeBoard",
    "row_type": "summary",
    "run_id": "candidate-1",
    "ended_at": "2026-04-02T00:00:00Z",
    "stage_summaries": [
      {"stage": "stageFetchOdds"},
      {"stage": "stageFetchSchedule"},
      {"stage": "stageMatchEvents"},
      {"stage": "stageFetchPlayerStats"},
      {"stage": "stageGenerateSignals"},
      {"stage": "stagePersist"}
    ]
  }
]
JSON
cat > "$out_dir/Run_Log.csv" <<'CSV'
stage,run_id,ended_at
runEdgeBoard,baseline-1,2026-04-01T00:00:00Z
runEdgeBoard,candidate-1,2026-04-02T00:00:00Z
CSV
cat > "$out_dir/State.json" <<'JSON'
[{"col":"val"}]
JSON
cat > "$out_dir/State.csv" <<'CSV'
col
val
CSV
for tab in Config Raw_Odds Raw_Schedule Raw_Player_Stats Match_Map Signals ProviderHealth; do
  cat > "$out_dir/${tab}.csv" <<'CSV'
id
1
CSV
  cat > "$out_dir/${tab}.json" <<'JSON'
[{"id": 1}]
JSON
done
cat > "$out_dir/runtime_export_manifest.json" <<'JSON'
{
  "generated_at_utc": "2026-04-02T00:00:00Z",
  "files": [
    {"path": "Run_Log.csv"},
    {"path": "Run_Log.json"},
    {"path": "State.csv"},
    {"path": "State.json"}
  ]
}
JSON
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
import json
import sys
from pathlib import Path
export_dir = Path(sys.argv[sys.argv.index("--export-dir") + 1])
sidecar = export_dir / "run_compare_preflight.json"
if not sidecar.is_file():
    print("missing preflight", file=sys.stderr)
    sys.exit(7)
payload = json.loads(sidecar.read_text(encoding="utf-8"))
if payload.get("run_pair") != ["baseline-1", "candidate-1"]:
    print("unexpected run pair", file=sys.stderr)
    sys.exit(8)
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

            sidecar_path = out_dir / "run_compare_preflight.json"
            self.assertTrue(sidecar_path.is_file())
            sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
            self.assertEqual(["baseline-1", "candidate-1"], sidecar["run_pair"])

            summary_paths = sorted(out_dir.glob("triage_impl_*/out/triage_summary.json"))
            self.assertEqual(1, len(summary_paths))
            summary = json.loads(summary_paths[0].read_text(encoding="utf-8"))
            self.assertEqual("pass", summary["status"])
            self.assertEqual("TRIAGE_BUNDLE_OK", summary["reason_code"])
            self.assertEqual("PASS", summary["classification"])
            self.assertEqual("TRIAGE_BUNDLE_OK", summary["machine_reason_code"])
            self.assertEqual("COMPARE_VALIDATION_PASSED", summary["gate_outcomes"]["compare_validation"]["reason_code"])
            self.assertEqual("pass", summary["gate_outcomes"]["edge_quality"]["status"])
            self.assertEqual("EDGE_QUALITY_GATE_PASSED", summary["gate_outcomes"]["edge_quality"]["reason_code"])

    def test_stage_four_uses_gate_blocked_reason_when_edge_json_is_valid(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            repo = tmp_path / "repo"
            scripts_dir = repo / "scripts"
            scripts_dir.mkdir(parents=True)

            shutil.copy2(SCRIPT_UNDER_TEST, scripts_dir / "run_triage_bundle.sh")
            shutil.copy2(ROOT / "scripts" / "preflight_guard.py", scripts_dir / "preflight_guard.py")
            shutil.copy2(
                ROOT / "scripts" / "run_summary_cardinality.py",
                scripts_dir / "run_summary_cardinality.py",
            )

            _write_executable(
                scripts_dir / "prepare_runtime_exports.sh",
                """#!/usr/bin/env bash
set -euo pipefail
out_dir=""
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --out-dir)
      shift
      out_dir="$1"
      ;;
  esac
  shift
done
rm -rf "$out_dir"
mkdir -p "$out_dir"
cat > "$out_dir/Run_Log.json" <<'JSON'
[
  {"stage": "runEdgeBoard", "row_type": "summary", "run_id": "baseline-1", "ended_at": "2026-04-01T00:00:00Z", "stage_summaries": [{"stage": "stageFetchOdds"}, {"stage": "stageFetchSchedule"}, {"stage": "stageMatchEvents"}, {"stage": "stageFetchPlayerStats"}, {"stage": "stageGenerateSignals"}, {"stage": "stagePersist"}]},
  {"stage": "runEdgeBoard", "row_type": "summary", "run_id": "candidate-1", "ended_at": "2026-04-02T00:00:00Z", "stage_summaries": [{"stage": "stageFetchOdds"}, {"stage": "stageFetchSchedule"}, {"stage": "stageMatchEvents"}, {"stage": "stageFetchPlayerStats"}, {"stage": "stageGenerateSignals"}, {"stage": "stagePersist"}]}
]
JSON
cat > "$out_dir/Run_Log.csv" <<'CSV'
stage,run_id,ended_at
runEdgeBoard,baseline-1,2026-04-01T00:00:00Z
runEdgeBoard,candidate-1,2026-04-02T00:00:00Z
CSV
cat > "$out_dir/State.json" <<'JSON'
[{"col":"val"}]
JSON
cat > "$out_dir/State.csv" <<'CSV'
col
val
CSV
for tab in Config Raw_Odds Raw_Schedule Raw_Player_Stats Match_Map Signals ProviderHealth; do
  cat > "$out_dir/${tab}.csv" <<'CSV'
id
1
CSV
  cat > "$out_dir/${tab}.json" <<'JSON'
[{"id": 1}]
JSON
done
cat > "$out_dir/runtime_export_manifest.json" <<'JSON'
{
  "generated_at_utc": "2026-04-02T00:00:00Z",
  "files": [
    {"path": "Run_Log.csv"},
    {"path": "Run_Log.json"},
    {"path": "State.csv"},
    {"path": "State.json"}
  ]
}
JSON
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
    json.dump(
        {
            "status": "blocked",
            "gate_verdict": "blocked_insufficient_operational_sample",
            "reason_code": "INSUFFICIENT_OPERATIONAL_SAMPLE",
        },
        handle,
    )
sys.exit(2)
""",
            )

            subprocess.run(["git", "init"], cwd=repo, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            (repo / "runtime").mkdir()
            (repo / "runtime" / "dummy.txt").write_text("ok", encoding="utf-8")

            out_dir = repo / "exports_live"
            result = subprocess.run(
                [
                    "bash",
                    str(scripts_dir / "run_triage_bundle.sh"),
                    "--out-dir",
                    str(out_dir),
                    str(repo / "runtime"),
                ],
                cwd=repo,
                check=False,
                env={**os.environ, "PATH": os.environ.get("PATH", "")},
            )

            self.assertNotEqual(0, result.returncode)
            summary_paths = sorted(out_dir.glob("triage_impl_*/out/triage_summary.json"))
            self.assertEqual(1, len(summary_paths))
            summary = json.loads(summary_paths[0].read_text(encoding="utf-8"))
            self.assertEqual("blocked", summary["status"])
            self.assertEqual("EDGE_QUALITY_GATE_BLOCKED_INSUFFICIENT_OPERATIONAL_SAMPLE", summary["reason_code"])
            self.assertEqual("POLICY_BLOCKED", summary["classification"])
            self.assertEqual("EDGE_QUALITY_GATE_BLOCKED_INSUFFICIENT_OPERATIONAL_SAMPLE", summary["machine_reason_code"])
            self.assertEqual("blocked", summary["gate_outcomes"]["edge_quality"]["status"])
            self.assertEqual(
                "EDGE_QUALITY_GATE_BLOCKED_INSUFFICIENT_OPERATIONAL_SAMPLE",
                summary["gate_outcomes"]["edge_quality"]["reason_code"],
            )
            self.assertNotEqual("EDGE_QUALITY_EVAL_FAILED", summary["gate_outcomes"]["edge_quality"]["reason_code"])

    def test_edge_quality_low_volume_strict_sample_block_is_distinguished(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            repo = tmp_path / "repo"
            scripts_dir = repo / "scripts"
            scripts_dir.mkdir(parents=True)

            shutil.copy2(SCRIPT_UNDER_TEST, scripts_dir / "run_triage_bundle.sh")
            shutil.copy2(ROOT / "scripts" / "preflight_guard.py", scripts_dir / "preflight_guard.py")
            shutil.copy2(ROOT / "scripts" / "run_summary_cardinality.py", scripts_dir / "run_summary_cardinality.py")

            _write_executable(
                scripts_dir / "prepare_runtime_exports.sh",
                """#!/usr/bin/env bash
set -euo pipefail
out_dir=""
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --out-dir)
      shift
      out_dir="$1"
      ;;
  esac
  shift
done
rm -rf "$out_dir"
mkdir -p "$out_dir"
cat > "$out_dir/Run_Log.json" <<'JSON'
[
  {"stage": "runEdgeBoard", "row_type": "summary", "run_id": "baseline-1", "ended_at": "2026-04-01T00:00:00Z", "stage_summaries": [{"stage": "stageFetchOdds"}, {"stage": "stageFetchSchedule"}, {"stage": "stageMatchEvents"}, {"stage": "stageFetchPlayerStats"}, {"stage": "stageGenerateSignals"}, {"stage": "stagePersist"}]},
  {"stage": "runEdgeBoard", "row_type": "summary", "run_id": "candidate-1", "ended_at": "2026-04-02T00:00:00Z", "stage_summaries": [{"stage": "stageFetchOdds"}, {"stage": "stageFetchSchedule"}, {"stage": "stageMatchEvents"}, {"stage": "stageFetchPlayerStats"}, {"stage": "stageGenerateSignals"}, {"stage": "stagePersist"}]}
]
JSON
cat > "$out_dir/Run_Log.csv" <<'CSV'
stage,run_id,ended_at
runEdgeBoard,baseline-1,2026-04-01T00:00:00Z
runEdgeBoard,candidate-1,2026-04-02T00:00:00Z
CSV
cat > "$out_dir/State.json" <<'JSON'
[{"col":"val"}]
JSON
cat > "$out_dir/State.csv" <<'CSV'
col
val
CSV
for tab in Config Raw_Odds Raw_Schedule Raw_Player_Stats Match_Map Signals ProviderHealth; do
  cat > "$out_dir/${tab}.csv" <<'CSV'
id
1
CSV
  cat > "$out_dir/${tab}.json" <<'JSON'
[{"id": 1}]
JSON
done
cat > "$out_dir/runtime_export_manifest.json" <<'JSON'
{
  "generated_at_utc": "2026-04-02T00:00:00Z",
  "files": [
    {"path": "Run_Log.csv"},
    {"path": "Run_Log.json"},
    {"path": "State.csv"},
    {"path": "State.json"}
  ]
}
JSON
""",
            )
            _write_executable(scripts_dir / "precheck_run_ids.py", "#!/usr/bin/env python3\nimport sys\nsys.exit(0)\n")
            _write_executable(
                scripts_dir / "compare_run_diagnostics.py",
                "#!/usr/bin/env python3\nimport sys\nsys.exit(0)\n",
            )
            _write_executable(
                scripts_dir / "evaluate_edge_quality.py",
                """#!/usr/bin/env python3
import json
import sys
out = sys.argv[sys.argv.index("--out-json") + 1]
with open(out, "w", encoding="utf-8") as handle:
    json.dump(
        {
            "status": "blocked",
            "gate_verdict": "blocked_low_volume_strict_sample",
            "reason_code": "EDGE_QUALITY_GATE_BLOCKED_LOW_VOLUME_STRICT_SAMPLE",
        },
        handle,
    )
""",
            )

            subprocess.run(["git", "init"], cwd=repo, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            (repo / "runtime").mkdir()
            (repo / "runtime" / "dummy.txt").write_text("ok", encoding="utf-8")

            out_dir = repo / "exports_live"
            result = subprocess.run(
                ["bash", str(scripts_dir / "run_triage_bundle.sh"), "--out-dir", str(out_dir), str(repo / "runtime")],
                cwd=repo,
                check=False,
                env={**os.environ, "PATH": os.environ.get("PATH", "")},
            )

            self.assertNotEqual(0, result.returncode)
            summary_paths = sorted(out_dir.glob("triage_impl_*/out/triage_summary.json"))
            self.assertEqual(1, len(summary_paths))
            summary = json.loads(summary_paths[0].read_text(encoding="utf-8"))
            self.assertEqual("blocked", summary["status"])
            self.assertEqual("EDGE_QUALITY_GATE_BLOCKED_LOW_VOLUME_STRICT_SAMPLE", summary["reason_code"])
            self.assertEqual("LOW_VOLUME_EXPECTED_BLOCK", summary["classification"])
            self.assertEqual("EDGE_QUALITY_GATE_BLOCKED_LOW_VOLUME_STRICT_SAMPLE", summary["machine_reason_code"])
            self.assertEqual(
                "EDGE_QUALITY_GATE_BLOCKED_LOW_VOLUME_STRICT_SAMPLE",
                summary["gate_outcomes"]["edge_quality"]["reason_code"],
            )

    def test_edge_quality_missing_artifact_is_pipeline_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            repo = tmp_path / "repo"
            scripts_dir = repo / "scripts"
            scripts_dir.mkdir(parents=True)

            shutil.copy2(SCRIPT_UNDER_TEST, scripts_dir / "run_triage_bundle.sh")
            shutil.copy2(ROOT / "scripts" / "preflight_guard.py", scripts_dir / "preflight_guard.py")
            shutil.copy2(ROOT / "scripts" / "run_summary_cardinality.py", scripts_dir / "run_summary_cardinality.py")

            _write_executable(
                scripts_dir / "prepare_runtime_exports.sh",
                """#!/usr/bin/env bash
set -euo pipefail
out_dir=""
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --out-dir)
      shift
      out_dir="$1"
      ;;
  esac
  shift
done
rm -rf "$out_dir"
mkdir -p "$out_dir"
cat > "$out_dir/Run_Log.json" <<'JSON'
[
  {"stage": "runEdgeBoard", "row_type": "summary", "run_id": "baseline-1", "ended_at": "2026-04-01T00:00:00Z", "stage_summaries": [{"stage": "stageFetchOdds"}, {"stage": "stageFetchSchedule"}, {"stage": "stageMatchEvents"}, {"stage": "stageFetchPlayerStats"}, {"stage": "stageGenerateSignals"}, {"stage": "stagePersist"}]},
  {"stage": "runEdgeBoard", "row_type": "summary", "run_id": "candidate-1", "ended_at": "2026-04-02T00:00:00Z", "stage_summaries": [{"stage": "stageFetchOdds"}, {"stage": "stageFetchSchedule"}, {"stage": "stageMatchEvents"}, {"stage": "stageFetchPlayerStats"}, {"stage": "stageGenerateSignals"}, {"stage": "stagePersist"}]}
]
JSON
cat > "$out_dir/Run_Log.csv" <<'CSV'
stage,run_id,ended_at
runEdgeBoard,baseline-1,2026-04-01T00:00:00Z
runEdgeBoard,candidate-1,2026-04-02T00:00:00Z
CSV
cat > "$out_dir/State.json" <<'JSON'
[{"col":"val"}]
JSON
cat > "$out_dir/State.csv" <<'CSV'
col
val
CSV
for tab in Config Raw_Odds Raw_Schedule Raw_Player_Stats Match_Map Signals ProviderHealth; do
  cat > "$out_dir/${tab}.csv" <<'CSV'
id
1
CSV
  cat > "$out_dir/${tab}.json" <<'JSON'
[{"id": 1}]
JSON
done
cat > "$out_dir/runtime_export_manifest.json" <<'JSON'
{
  "generated_at_utc": "2026-04-02T00:00:00Z",
  "files": [
    {"path": "Run_Log.csv"},
    {"path": "Run_Log.json"},
    {"path": "State.csv"},
    {"path": "State.json"}
  ]
}
JSON
""",
            )
            _write_executable(scripts_dir / "precheck_run_ids.py", "#!/usr/bin/env python3\nimport sys\nsys.exit(0)\n")
            _write_executable(scripts_dir / "compare_run_diagnostics.py", "#!/usr/bin/env python3\nimport sys\nsys.exit(0)\n")
            _write_executable(
                scripts_dir / "evaluate_edge_quality.py",
                "#!/usr/bin/env python3\nimport sys\nsys.exit(9)\n",
            )

            subprocess.run(["git", "init"], cwd=repo, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            (repo / "runtime").mkdir()
            (repo / "runtime" / "dummy.txt").write_text("ok", encoding="utf-8")

            out_dir = repo / "exports_live"
            result = subprocess.run(
                ["bash", str(scripts_dir / "run_triage_bundle.sh"), "--out-dir", str(out_dir), str(repo / "runtime")],
                cwd=repo,
                check=False,
                env={**os.environ, "PATH": os.environ.get("PATH", "")},
            )

            self.assertNotEqual(0, result.returncode)
            summary_paths = sorted(out_dir.glob("triage_impl_*/out/triage_summary.json"))
            self.assertEqual(1, len(summary_paths))
            summary = json.loads(summary_paths[0].read_text(encoding="utf-8"))
            self.assertEqual("fail", summary["status"])
            self.assertEqual("PIPELINE_FAILURE", summary["classification"])
            self.assertEqual("EDGE_QUALITY_ARTIFACT_MISSING", summary["machine_reason_code"])


if __name__ == "__main__":
    unittest.main()
