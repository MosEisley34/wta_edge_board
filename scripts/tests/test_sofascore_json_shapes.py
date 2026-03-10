import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from extract_player_features import _extract_from_file  # noqa: E402


class SofascoreJsonShapeTests(unittest.TestCase):
    def test_events_payload_normalizes_player_rows_without_parse_error(self):
        path = ROOT / "scripts" / "fixtures" / "sofascore_events_live.json"
        rows = _extract_from_file(path, selected_sources=set())

        self.assertTrue(rows)
        self.assertTrue(all(row.reason_code != "source_parse_error" for row in rows))
        self.assertEqual(["Iga Swiatek", "Aryna Sabalenka"], [row.player_canonical_name for row in rows])

    def test_player_payload_normalizes_single_player_without_parse_error(self):
        path = ROOT / "scripts" / "fixtures" / "sofascore_player_detail.json"
        rows = _extract_from_file(path, selected_sources=set())

        self.assertEqual(1, len(rows))
        self.assertNotEqual("source_parse_error", rows[0].reason_code)
        self.assertEqual("Coco Gauff", rows[0].player_canonical_name)
        self.assertEqual("normalized_from_sofascore_player", rows[0].reason_code_detail)

    def test_statistics_payload_emits_diagnostic_record_without_parse_error(self):
        path = ROOT / "scripts" / "fixtures" / "sofascore_player_stats_overall.json"
        rows = _extract_from_file(path, selected_sources=set())

        self.assertEqual(1, len(rows))
        self.assertEqual("sofascore_statistics_payload", rows[0].reason_code)
        self.assertNotEqual("source_parse_error", rows[0].reason_code)
        self.assertIsNone(rows[0].player_canonical_name)


if __name__ == "__main__":
    unittest.main()
