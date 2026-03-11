import unittest
from pathlib import Path
import sys
import tempfile

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
        self.assertEqual(
            [
                "Coco Gauff",
                "Jessica Pegula",
                "Gabriela Dabrowski",
                "Erin Routliffe",
            ],
            [row.player_canonical_name for row in rows],
        )

    def test_scheduled_events_fixture_normalizes_player_rows_without_parse_error(self):
        path = ROOT / "scripts" / "fixtures" / "sofascore_scheduled_events.json"
        rows = _extract_from_file(path, selected_sources=set())

        self.assertTrue(rows)
        self.assertTrue(all(row.reason_code != "source_parse_error" for row in rows))
        self.assertEqual(
            ["Belinda Bencic", "Lulu Sun", "Donna Vekic", "Petra Martic"],
            [row.player_canonical_name for row in rows],
        )

    def test_player_payload_normalizes_single_player_without_parse_error(self):
        path = ROOT / "scripts" / "fixtures" / "sofascore_player_detail.json"
        rows = _extract_from_file(path, selected_sources=set())

        self.assertEqual(1, len(rows))
        self.assertNotEqual("source_parse_error", rows[0].reason_code)
        self.assertEqual("Coco Gauff", rows[0].player_canonical_name)
        self.assertEqual("normalized_from_sofascore_player", rows[0].reason_code_detail)


    def test_events_payload_filters_non_tennis_and_non_wta(self):
        diagnostics = []
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "sofascore_events_live.json"
            path.write_text(
                '{"events": ['
                '{"sport": {"slug": "football"}, "category": {"name": "WTA"}, "homeTeam": {"name": "Iga Swiatek"}, "awayTeam": {"name": "Aryna Sabalenka"}},'
                '{"sport": {"slug": "tennis"}, "category": {"name": "ATP"}, "homeTeam": {"name": "Iga Swiatek"}, "awayTeam": {"name": "Aryna Sabalenka"}},'
                '{"sport": {"slug": "tennis"}, "category": {"name": "WTA"}, "homeTeam": {"name": "Iga Swiatek"}, "awayTeam": {"name": "Aryna Sabalenka"}}'
                ']}' ,
                encoding="utf-8",
            )

            rows = _extract_from_file(path, selected_sources=set(), diagnostics=diagnostics)

        self.assertEqual(2, len(rows))
        self.assertEqual(["Iga Swiatek", "Aryna Sabalenka"], [row.player_canonical_name for row in rows])

    def test_events_payload_supports_doubles_pairs_and_player_lists(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "sofascore_events_live.json"
            path.write_text(
                '{"events": ['
                '{"sport": {"slug": "tennis"}, "category": {"name": "WTA"}, "homeTeam": {"name": "Iga Swiatek / Aryna Sabalenka"}, "awayTeam": {"name": "Coco Gauff & Jessica Pegula"}},'
                '{"sport": {"slug": "tennis"}, "category": {"name": "WTA"}, "homeTeam": {"players": [{"name": "Ons Jabeur"}, {"name": "Marketa Vondrousova"}]}, "awayTeam": {"players": [{"name": "Elena Rybakina"}]}}'
                ']}' ,
                encoding="utf-8",
            )

            rows = _extract_from_file(path, selected_sources=set())

        self.assertEqual(7, len(rows))
        self.assertEqual(
            [
                "Iga Swiatek",
                "Aryna Sabalenka",
                "Coco Gauff",
                "Jessica Pegula",
                "Ons Jabeur",
                "Marketa Vondrousova",
                "Elena Rybakina",
            ],
            [row.player_canonical_name for row in rows],
        )

    def test_events_payload_prefers_single_player_identity_from_team_object(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "sofascore_events_live.json"
            path.write_text(
                '{"events": ['
                '{"sport": {"slug": "tennis"}, "category": {"name": "WTA"}, "homeTeam": {"name": "Home Team", "player": {"name": "Mirra Andreeva"}}, "awayTeam": {"name": "Away Team", "players": [{"name": "Jessica Pegula"}]}}'
                ']}' ,
                encoding="utf-8",
            )

            rows = _extract_from_file(path, selected_sources=set())

        self.assertEqual(["Mirra Andreeva", "Jessica Pegula"], [row.player_canonical_name for row in rows])


    def test_scheduled_events_payload_supports_nested_team_and_player_shapes(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "sofascore_scheduled_events.json"
            path.write_text(
                '{"events": ['
                '{"sport": {"slug": "tennis"}, "category": {"name": "WTA"}, "homeTeam": {"team": {"players": [{"player": {"name": "Belinda Bencic"}}, {"player": {"name": "Lulu Sun"}}]}}, "awayTeam": {"participants": [{"name": "Donna Vekic"}, {"name": "Petra Martic"}]}}'
                ']}' ,
                encoding="utf-8",
            )

            rows = _extract_from_file(path, selected_sources=set())

        self.assertEqual(["Belinda Bencic", "Lulu Sun", "Donna Vekic", "Petra Martic"], [row.player_canonical_name for row in rows])

    def test_events_payload_supports_event_level_home_away_name_fallbacks(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "sofascore_events_live.json"
            path.write_text(
                '{"events": ['
                '{"sport": {"slug": "tennis"}, "category": {"name": "WTA"}, '
                '"homeTeam": {}, "awayTeam": {}, '
                '"homeTeamName": "Beatriz Haddad Maia", "awayParticipantName": "Elina Svitolina"}'
                ']}' ,
                encoding="utf-8",
            )

            rows = _extract_from_file(path, selected_sources=set())

        self.assertEqual(["Beatriz Haddad Maia", "Elina Svitolina"], [row.player_canonical_name for row in rows])

    def test_events_payload_prefers_field_fallback_order_over_slug(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "sofascore_events_live.json"
            path.write_text(
                '{"events": ['
                '{"sport": {"slug": "tennis"}, "category": {"name": "WTA"}, '
                '"homeTeam": {"fullName": "Qinwen Zheng", "slug": "qinwen-zheng-test"}, '
                '"awayTeam": {"participantName": "Karolina Muchova", "slug": "karolina-muchova-test"}}'
                ']}' ,
                encoding="utf-8",
            )

            rows = _extract_from_file(path, selected_sources=set())

        self.assertEqual(["Qinwen Zheng", "Karolina Muchova"], [row.player_canonical_name for row in rows])

    def test_events_payload_emits_diagnostics_only_row_when_participant_floor_unmet(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "sofascore_events_live.json"
            path.write_text(
                '{"events": ['
                '{"sport": {"slug": "tennis"}, "category": {"name": "WTA"}, "homeTeam": {"name": "Iga Swiatek"}, "awayTeam": {"name": ""}}'
                ']}' ,
                encoding="utf-8",
            )

            rows = _extract_from_file(path, selected_sources=set())

        self.assertEqual(1, len(rows))
        self.assertEqual("sofascore_events_participant_floor_unmet", rows[0].reason_code)
        self.assertIsNone(rows[0].player_canonical_name)

    def test_statistics_payload_emits_diagnostic_record_without_parse_error(self):
        path = ROOT / "scripts" / "fixtures" / "sofascore_player_stats_overall.json"
        rows = _extract_from_file(path, selected_sources=set())

        self.assertEqual(1, len(rows))
        self.assertEqual("sofascore_statistics_payload", rows[0].reason_code)
        self.assertNotEqual("source_parse_error", rows[0].reason_code)
        self.assertIsNone(rows[0].player_canonical_name)

    def test_pointer_source_payload_is_skipped_and_recorded_in_diagnostics(self):
        diagnostics = []
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "tennisabstract_leaders.body"
            path.write_text('matchmx[0] = ["2026-01-01"]', encoding="utf-8")
            rows = _extract_from_file(path, selected_sources=set(), diagnostics=diagnostics)

        self.assertEqual([], rows)
        self.assertEqual(1, len(diagnostics))
        self.assertEqual("source_role_pointer_skipped", diagnostics[0].issue_code)

    def test_hard_api_error_payload_is_routed_to_diagnostics(self):
        diagnostics = []
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "sofascore_events_live.json"
            path.write_text('{"status": 500, "error": "upstream timeout"}', encoding="utf-8")

            rows = _extract_from_file(path, selected_sources=set(), diagnostics=diagnostics)

        self.assertEqual([], rows)
        self.assertEqual(1, len(diagnostics))
        self.assertEqual("api_hard_error", diagnostics[0].issue_code)


if __name__ == "__main__":
    unittest.main()
