import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "mirror_all_runtime_tabs_to_json.py"


class MirrorAllRuntimeTabsToJsonTests(unittest.TestCase):
    def _write_minimal_csvs(self, root: Path) -> None:
        payloads = {
            "Config.csv": "key,value\nRUN_ENABLED,true\n",
            "Run_Log.csv": "run_id,row_type\nrun-1,summary\n",
            "Raw_Odds.csv": "event_id,odds\nmatch-1,-120\n",
            "Raw_Schedule.csv": "event_id,start_time\nmatch-1,2026-03-26T00:00:00Z\n",
            "Raw_Player_Stats.csv": "player,metric,value\nAces,serve_pct,0.62\n",
            "Match_Map.csv": "source_event_id,canonical_event_id\nmatch-1,match-1\n",
            "Signals.csv": "run_id,event_id,signal\nrun-1,match-1,bet\n",
            "State.csv": "state_key,state_value\nfoo,bar\n",
            "ProviderHealth.csv": "provider,status\nodds_api,ok\n",
        }
        for name, text in payloads.items():
            (root / name).write_text(text, encoding="utf-8")

    def test_mirrors_all_tabs_and_writes_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_minimal_csvs(root)
            proc = subprocess.run(
                ["python3", str(SCRIPT), "--export-dir", str(root)],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(0, proc.returncode, msg=proc.stderr)
            summary = json.loads((root / "runtime_tab_json_mirror_summary.json").read_text(encoding="utf-8"))
            self.assertEqual("ok", summary["status"])
            self.assertEqual([], summary["missing_files"])
            self.assertEqual([], summary["mismatches"])
            self.assertTrue((root / "Run_Log.json").is_file())
            self.assertTrue((root / "ProviderHealth.json").is_file())

    def test_missing_csv_tabs_report_machine_readable_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "Run_Log.csv").write_text("run_id,row_type\nrun-1,summary\n", encoding="utf-8")
            proc = subprocess.run(
                ["python3", str(SCRIPT), "--export-dir", str(root)],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(0, proc.returncode)
            summary = json.loads((root / "runtime_tab_json_mirror_summary.json").read_text(encoding="utf-8"))
            self.assertEqual("missing_tabs", summary["status"])
            self.assertGreater(len(summary["missing_files"]), 0)
            self.assertIn(str(root / "Config.csv"), summary["missing_files"])

    def test_row_count_mismatch_exits_non_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_minimal_csvs(root)
            (root / "Run_Log.json").write_text("[]\n", encoding="utf-8")
            proc = subprocess.run(
                ["python3", str(SCRIPT), "--export-dir", str(root)],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertNotEqual(0, proc.returncode)
            summary = json.loads((root / "runtime_tab_json_mirror_summary.json").read_text(encoding="utf-8"))
            self.assertEqual("error", summary["status"])
            self.assertEqual("Run_Log", summary["mismatches"][0]["tab"])


if __name__ == "__main__":
    unittest.main()
