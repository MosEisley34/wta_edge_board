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

        self.assertEqual(5, len(lines))
        self.assertEqual("runs total=36 status=success:36", lines[0])
        self.assertEqual(
            "top_reason_codes competition_allowed:36;"
            "long_reason_code_key_for_schedule_enrichment_h2h_missing:36;"
            "market_h2h:36;match_map_upserts:36",
            lines[1],
        )
        self.assertEqual(
            "stage_duration_ms stageFetchOdds[min=1000,avg=1000.0,p95=1000];"
            "stageFetchPlayerStats[min=500,avg=500.0,p95=500];"
            "stageFetchSchedule[min=800,avg=800.0,p95=800]",
            lines[2],
        )
        self.assertEqual("watchdog_trend none", lines[3])
        self.assertEqual("warnings none", lines[4])


if __name__ == "__main__":
    unittest.main()
