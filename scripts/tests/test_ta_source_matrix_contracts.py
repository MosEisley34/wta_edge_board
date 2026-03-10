import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from extract_player_features import (  # noqa: E402
    TA_SOURCE_MATRIX,
    _assert_parse_health,
    _compute_parse_health,
    _parse_contract_html_rows,
)


class TaSourceMatrixContractTests(unittest.TestCase):
    def test_source_matrix_contains_required_variants(self):
        required = {
            "ta_leaders_top50_serve",
            "ta_leaders_top50_return",
            "ta_leaders_top50_breaks",
            "ta_leaders_top50_more",
            "ta_leaders_51_100_serve",
            "ta_leaders_51_100_return",
            "ta_leaders_51_100_breaks",
            "ta_leaders_51_100_more",
            "ta_winners_errors",
            "mcp_report_serve",
            "mcp_report_return",
            "mcp_report_rally",
            "mcp_report_tactics",
        }
        self.assertTrue(required.issubset(TA_SOURCE_MATRIX.keys()))

    def test_contract_parser_normalizes_player_and_percent_stats(self):
        html = """
        <html><body>leaders_wta Serve
        <table>
          <tr><th>Player</th><th>Hold%</th><th>Break%</th></tr>
          <tr><td>Iga Swiatek</td><td>83.2%</td><td>45.6%</td></tr>
          <tr><td>Aryna Sabalenka</td><td>0.81</td><td>0.39</td></tr>
        </table>
        </body></html>
        """
        rows = _parse_contract_html_rows("ta_leaders_top50_serve", html, "2026-01-01T00:00:00+00:00")
        valid = [r for r in rows if r.player_canonical_name]
        self.assertEqual(len(valid), 2)
        self.assertAlmostEqual(valid[0].hold_pct, 83.2)
        self.assertAlmostEqual(valid[1].hold_pct, 81.0)

    def test_parse_health_threshold_raises(self):
        html = """
        <html><body>leaders_wta Serve
        <table><tr><th>Player</th><th>Hold%</th></tr>
        <tr><td>Only One</td><td>80%</td></tr>
        </table></body></html>
        """
        rows = _parse_contract_html_rows("ta_leaders_top50_serve", html, "2026-01-01T00:00:00+00:00")
        health = _compute_parse_health(rows)
        with self.assertRaises(RuntimeError):
            _assert_parse_health(health)


if __name__ == "__main__":
    unittest.main()
