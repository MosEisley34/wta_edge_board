import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from runtime_diagnostics_summary import build_summary  # noqa: E402


class RuntimeDiagnosticsSummaryTests(unittest.TestCase):
    def test_emits_compact_deterministic_summary_lines(self):
        fixture = ROOT / "docs" / "baselines" / "pipeline_log_sample_3h_verbose_2026-03-12.json"
        lines = build_summary([str(fixture)], top_n=4, max_stages=3, warning_limit=2)

        self.assertEqual(8, len(lines))
        self.assertEqual("runs total=36 status=success:36", lines[0])
        self.assertEqual(
            "daily status (2026-03-12) Runs completed=36, Runs degraded=0, Odds not actionable yet=36, Signals produced=36",
            lines[1],
        )
        self.assertEqual("What changed since yesterday: not enough history yet", lines[2])
        self.assertEqual(
            "metadata reason_alias_normalization_applied=true reason_alias_normalization_scope=presentation_only_historical_compat",
            lines[3],
        )
        self.assertEqual(
            "top_reason_codes CMP_ALLOW:36;MATCH_EXACT:36;MKT_H2H:36;MM_UPS:36",
            lines[4],
        )
        self.assertEqual(
            "stage_duration_ms stageFetchOdds[min=1000,avg=1000.0,p95=1000];"
            "stageFetchPlayerStats[min=500,avg=500.0,p95=500];"
            "stageFetchSchedule[min=800,avg=800.0,p95=800]",
            lines[5],
        )
        self.assertEqual("watchdog_trend none", lines[6])
        self.assertEqual("warnings none", lines[7])

    def test_rollup_uses_run_level_reason_counts_without_stage_inflation(self):
        fixture = ROOT / "scripts" / "fixtures" / "runtime_rollup_regression_runs.json"
        lines = build_summary([str(fixture)], top_n=3, max_stages=2, warning_limit=2)

        self.assertEqual("runs total=2 status=success:2", lines[0])
        self.assertEqual(
            "daily status (unknown) Runs completed=2, Runs degraded=0, Odds not actionable yet=0, Signals produced=0",
            lines[1],
        )
        self.assertEqual("What changed since yesterday: not enough history yet", lines[2])
        self.assertEqual(
            "metadata reason_alias_normalization_applied=false reason_alias_normalization_scope=presentation_only_historical_compat",
            lines[3],
        )
        self.assertEqual("top_reason_codes gamma:4;alpha:3;beta:1", lines[4])
        self.assertEqual(
            "stage_duration_ms stageFetchOdds[min=500,avg=600.0,p95=700];"
            "stageFetchSchedule[min=900,avg=1000.0,p95=1100]",
            lines[5],
        )



    def test_top_reason_codes_normalize_legacy_fallback_aliases_for_display(self):
        fixture = ROOT / "scripts" / "fixtures" / "runtime_rollup_legacy_alias_runs.json"
        lines = build_summary([str(fixture)], top_n=3, max_stages=2, warning_limit=2)

        self.assertEqual(
            "metadata reason_alias_normalization_applied=true reason_alias_normalization_scope=presentation_only_historical_compat",
            lines[3],
        )
        self.assertEqual("top_reason_codes OPEN_TS_MISS:3;OPEN_LAG_HI:2", lines[4])

    def test_warning_rollup_collapses_alias_resolution_noise_to_run_level(self):
        fixture = ROOT / "scripts" / "fixtures" / "runtime_rollup_warning_runs.json"
        lines = build_summary([str(fixture)], top_n=3, max_stages=2, warning_limit=3)

        self.assertEqual("runs total=2 status=success:1,warning:1", lines[0])
        self.assertEqual(
            "daily status (unknown) Runs completed=1, Runs degraded=1, Odds not actionable yet=0, Signals produced=0",
            lines[1],
        )
        self.assertEqual("What changed since yesterday: not enough history yet", lines[2])
        self.assertEqual(
            "warnings non_success_runs:1;reason_alias_resolution_warning:runs=1;run_health_no_matches_from_odds:runs=1",
            lines[7],
        )


if __name__ == "__main__":
    unittest.main()
