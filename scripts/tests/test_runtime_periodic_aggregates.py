import json
import tempfile
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from runtime_periodic_aggregates import build_periodic_aggregates, main  # noqa: E402


class RuntimePeriodicAggregatesTests(unittest.TestCase):
    def test_builds_daily_rollups_with_blocker_mix_productivity_and_stage_latency(self):
        fixture = ROOT / "scripts" / "fixtures" / "runtime_periodic_rollup_sample.json"
        snapshot = build_periodic_aggregates([str(fixture)])

        self.assertEqual("runtime_periodic_rollup_v1", snapshot["schema"])
        self.assertEqual(2, len(snapshot["rollups"]))
        self.assertFalse(snapshot["metadata"]["reason_alias_normalization_applied"])

        first = snapshot["rollups"][0]
        self.assertEqual("2026-03-10", first["period_start"])
        self.assertEqual(2, first["runs"])
        self.assertEqual(1, first["success_runs"])
        self.assertAlmostEqual(0.5, first["success_ratio"])
        self.assertAlmostEqual(0.65, first["productivity"]["avg_ratio"])
        self.assertEqual(9, first["productivity"]["matched_total"])
        self.assertEqual(3, first["productivity"]["unmatched_total"])
        self.assertEqual("opening_lag_blocked_count", first["blocker_mix"][0]["blocker"])
        self.assertEqual(6, first["blocker_mix"][0]["count"])
        self.assertEqual(600, first["stage_latency_trends"]["stageFetchOdds"]["avg_ms"])
        self.assertEqual(1, first["daily_status"]["runs_completed"])
        self.assertEqual(1, first["daily_status"]["runs_degraded"])
        self.assertEqual(0, first["daily_status"]["odds_not_actionable_yet"])
        self.assertEqual(0, first["daily_status"]["signals_produced"])
        self.assertEqual("not enough history yet", first["what_changed_since_yesterday"])
        self.assertEqual([], first["top_reason_codes"])

        second = snapshot["rollups"][1]
        self.assertEqual("2026-03-11", second["period_start"])
        self.assertEqual(1, second["runs"])
        self.assertEqual(1.0, second["productivity"]["avg_ratio"])
        self.assertEqual(1, second["daily_status"]["runs_completed"])
        self.assertEqual(0, second["daily_status"]["runs_degraded"])
        self.assertEqual(0, second["daily_status"]["odds_not_actionable_yet"])
        self.assertEqual(0, second["daily_status"]["signals_produced"])
        self.assertEqual("2026-03-10", second["what_changed_since_yesterday"]["from_period"])
        self.assertEqual(0, second["what_changed_since_yesterday"]["runs_completed_delta"])

    def test_rollup_top_reason_codes_normalize_legacy_fallback_aliases(self):
        fixture = ROOT / "scripts" / "fixtures" / "runtime_rollup_legacy_alias_runs.json"
        snapshot = build_periodic_aggregates([str(fixture)])

        self.assertTrue(snapshot["metadata"]["reason_alias_normalization_applied"])
        top = snapshot["rollups"][0]["top_reason_codes"]
        self.assertEqual("OPEN_TS_MISS", top[0]["reason_code"])
        self.assertEqual(3, top[0]["count"])
        self.assertEqual("OPEN_LAG_HI", top[1]["reason_code"])
        self.assertEqual(2, top[1]["count"])
        self.assertNotIn("UNK_OPEN_TS", {entry["reason_code"] for entry in top})

    def test_main_writes_dated_snapshot_artifact(self):
        fixture = ROOT / "scripts" / "fixtures" / "runtime_periodic_rollup_sample.json"
        with tempfile.TemporaryDirectory() as tmp:
            rc = main(
                [
                    str(fixture),
                    "--snapshot-dir",
                    tmp,
                    "--snapshot-date",
                    "2026-03-12",
                ]
            )
            self.assertEqual(0, rc)
            out = Path(tmp) / "runtime_periodic_rollup_2026-03-12.json"
            self.assertTrue(out.exists())
            payload = json.loads(out.read_text(encoding="utf-8"))
            self.assertEqual("runtime_periodic_rollup_v1", payload["schema"])


if __name__ == "__main__":
    unittest.main()
