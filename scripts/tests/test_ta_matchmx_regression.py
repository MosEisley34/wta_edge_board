import json
import re
import statistics
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from extract_player_features import _parse_matchmx_rows  # noqa: E402
from matchmx_parser import (  # noqa: E402
    MATCHMX_LONG_LIVE_ROW_IDX,
    MATCHMX_LONG_ROW_IDX,
    MATCHMX_NEW_ROW_IDX,
    MATCHMX_OLD_ROW_IDX,
    get_matchmx_row_idx,
    iter_matchmx_rows,
    parse_matchmx_player_row,
)

class TaMatchMxRegressionTest(unittest.TestCase):
    def setUp(self):
        self.fixture_path = ROOT / "scripts" / "fixtures" / "tennisabstract_leadersource_wta.body"
        self.live_shape_fixture_path = (
            ROOT / "scripts" / "fixtures" / "tennisabstract_leadersource_wta_live_shape_regression.body"
        )
        self.payload = self.fixture_path.read_text(encoding="utf-8")
        self.live_shape_payload = self.live_shape_fixture_path.read_text(encoding="utf-8")

    def _load_live_probe_payload(self) -> str:
        return self._probe_payload_path().read_text(encoding="utf-8")

    def _probe_payload_path(self) -> Path:
        live_probe_path = ROOT / "tmp" / "source_probe_latest" / "raw" / "tennisabstract_leadersource_wta.body"
        if live_probe_path.exists():
            return live_probe_path
        return ROOT / "scripts" / "fixtures" / "tennisabstract_probe_long_live.body"

    def _has_probe_payload(self) -> bool:
        return self._probe_payload_path().exists()

    def _collect_probe_rows(self, max_rows: int = 25):
        rows = []
        for parsed in iter_matchmx_rows(self._load_live_probe_payload()):
            if not parsed.row_shape_valid:
                continue
            row, error = parse_matchmx_player_row(parsed.tokens)
            if error or row is None:
                continue
            rows.append(row)
            if len(rows) >= max_rows:
                break
        return rows

    @staticmethod
    def _live_long_tokens_template() -> list[str]:
        return [
            "2026-04-01",  # DATE
            "Madrid",      # EVENT
            "Clay",        # SURFACE
            "Main Draw",   # TOURNAMENT_PHASE
            "W",           # RESULT_FLAG
            "Iga Swiatek", # PLAYER_NAME
            "2",           # RANKING
            "1",           # SEED
            "",            # ENTRY
            "R16",         # ROUND
            "6-2 6-4",     # SCORE
            "3",           # BEST_OF
            "Opponent Q",  # OPPONENT
            "98",          # MATCH_MINUTES
            "WTA1000",     # LEVEL
            "WTA",         # HAND
            "R",           # TOUR
            "2026",        # SEASON
            "23.5",        # AGE
            "POL",         # COUNTRY
            "POL",         # COURT
            "3",           # ACES
            "2",           # DOUBLE_FAULTS
            "63",          # FIRST_SERVE_IN_RAW
            "64",          # DRAW_SIZE
            "132",         # RECENT_FORM (aux numeric)
            "0.88",        # RECENT_FORM
            "0.81",        # SURFACE_WIN_RATE
            "64.2",        # BP_SAVED_PCT
            "48.5",        # BP_CONV_PCT
            "71.4",        # HOLD_PCT
            "39.6",        # BREAK_PCT
            "62.1",        # FIRST_SERVE_IN_PCT
            "69.3",        # FIRST_SERVE_POINTS_WON_PCT
            "51.0",        # SECOND_SERVE_POINTS_WON_PCT
            "41.4",        # RETURN_POINTS_WON_PCT
            "1.18",        # DOMINANCE_RATIO
            "54.1",        # TOTAL_POINTS_WON_PCT
            "10",          # SERVICE_GAMES
            "10",          # RETURN_GAMES
            "132",         # POINTS_PLAYED
            "2-0",         # TB_RECORD
            "-120",        # OPENER_ODDS
            "-135",        # CLOSING_ODDS
            "phase-shifted live row",  # NOTES
        ]

    def test_extract_player_features_fixture_has_players_coverage_and_name_quality(self):
        rows = _parse_matchmx_rows("tennisabstract_leadersource_wta", self.payload, "2026-01-01T00:00:00+00:00")
        valid_rows = [r for r in rows if r.player_canonical_name and r.reason_code == "ok"]
        self.assertGreater(len(valid_rows), 6)

        non_null_rank = sum(1 for row in valid_rows if row.ranking is not None)
        non_null_hold = sum(1 for row in valid_rows if row.hold_pct is not None)
        non_null_break = sum(1 for row in valid_rows if row.break_pct is not None)
        self.assertGreater(non_null_rank, 0)
        self.assertGreater(non_null_hold, 0)
        self.assertGreater(non_null_break, 0)

        self.assertFalse(any(len(row.player_canonical_name.strip()) == 1 for row in valid_rows if row.player_canonical_name))

    def test_check_ta_parity_fixture_has_threshold_players_non_null_coverage_and_no_single_letter_names(self):
        cmd = [
            sys.executable,
            str(SCRIPTS_DIR / "check_ta_parity.py"),
            "--input",
            str(self.fixture_path),
            "--sample-size",
            "1",
        ]
        result = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, check=True)
        summary = json.loads(result.stdout)
        self.assertGreater(summary["unique_players"], 6)
        self.assertGreater(summary["normalized_non_null_coverage"]["ranking"]["non_null"], 0)
        self.assertGreater(summary["normalized_non_null_coverage"]["hold_pct"]["non_null"], 0)
        self.assertGreater(summary["normalized_non_null_coverage"]["break_pct"]["non_null"], 0)
        self.assertFalse(any(len(item["player"].strip()) == 1 for item in summary["sample_normalized_records"]))

    def test_parse_matchmx_handles_quoted_commas_escaped_quotes_and_nested_arrays(self):
        payload = '\n'.join([
            'matchmx[0] = ["2026-01-01","Open","Hard","Doe, Jane","Anna-Marie Smith","6-4 6-4","12","0.8","0.7","65","38","60","50","58","67","49","41","1.12","54"];',
            'matchmx[1] = ["2026-01-02","Open",["Hard","Indoor"],"Carla Cruz","Beth Brown","6-4, 6-4","9","0.7","0.6","63","36","58","49","56","65","47","39","1.05","51"];',
            'matchmx[2] = ["2026-01-03","Open","Hard","A","Doe, Jane","6-4 6-4","12"];',
        ])
        rows = _parse_matchmx_rows("tennisabstract_leadersource_wta", payload, "2026-01-01T00:00:00+00:00")
        ok_rows = [row for row in rows if row.reason_code == "ok"]
        self.assertEqual(len(ok_rows), 2)
        self.assertEqual(ok_rows[0].player_canonical_name, "Anna-Marie Smith")
        self.assertEqual(ok_rows[1].player_canonical_name, "Beth Brown")
        unusable_rows = [row for row in rows if row.reason_code == "ta_matchmx_unusable_payload"]
        self.assertGreaterEqual(len(unusable_rows), 1)

    def test_parse_matchmx_old_schema_row_parses_to_canonical_name(self):
        payload = 'matchmx[0] = ["2026-03-01","Open","Hard","Opponent A","Iga Swiatek","6-1 6-2","1","0.9","0.8","70","40","60","50","58","67","49","41","1.12","54"];'
        rows = _parse_matchmx_rows("tennisabstract_leadersource_wta", payload, "2026-01-01T00:00:00+00:00")
        ok_rows = [row for row in rows if row.reason_code == "ok"]
        self.assertEqual(len(ok_rows), 1)
        self.assertEqual(ok_rows[0].player_canonical_name, "Iga Swiatek")

    def test_parse_matchmx_new_schema_row_with_result_flag_parses_without_name_rejection(self):
        payload = 'matchmx[0] = ["2026-03-02","Open","Hard","Opponent B","W","Coco Gauff","6-2 6-2","3","0.8","0.7","68","39","59","49","57","66","48","40","1.08","52"];'
        rows = _parse_matchmx_rows("tennisabstract_leadersource_wta", payload, "2026-01-01T00:00:00+00:00")
        ok_rows = [row for row in rows if row.reason_code == "ok"]
        self.assertEqual(len(ok_rows), 1)
        self.assertEqual(ok_rows[0].player_canonical_name, "Coco Gauff")
        self.assertFalse(any(row.reason_code == "canonical_name_rejected" for row in rows))

    def test_parse_matchmx_new_schema_row_uses_shifted_metric_indices(self):
        payload = 'matchmx[0] = ["2026-03-03","Open","Hard","Opponent C","L","Jessica Pegula","6-4 4-6 6-3","6","0.91","0.83","67","42","61","48","59","70","51","43","1.17","55"];'
        rows = _parse_matchmx_rows("tennisabstract_leadersource_wta", payload, "2026-01-01T00:00:00+00:00")
        ok_rows = [row for row in rows if row.reason_code == "ok"]
        self.assertEqual(len(ok_rows), 1)

        row = ok_rows[0]
        self.assertIsNotNone(row.hold_pct)
        self.assertEqual(row.hold_pct, 67.0)
        self.assertIsNotNone(row.break_pct)
        self.assertEqual(row.break_pct, 42.0)

        self.assertNotEqual(row.hold_pct, 0.83)
        self.assertNotEqual(row.break_pct, 67.0)
        self.assertEqual(row.ranking, 6.0)

    def _assert_schema_metric_window(self, row_idx, tokens, expected_hold, expected_break):
        self.assertGreaterEqual(expected_hold, 40.0)
        self.assertLessEqual(expected_hold, 95.0)
        self.assertGreaterEqual(expected_break, 5.0)
        self.assertLessEqual(expected_break, 70.0)

        self.assertEqual(float(tokens[row_idx["HOLD_PCT"]]), expected_hold)
        self.assertEqual(float(tokens[row_idx["BREAK_PCT"]]), expected_break)

        parsed, error = parse_matchmx_player_row(tokens)
        self.assertIsNone(error)
        assert parsed is not None
        self.assertEqual(parsed.hold_pct, expected_hold)
        self.assertEqual(parsed.break_pct, expected_break)

    def test_debug_helper_validates_hold_break_positions_for_old_and_new_schema_rows(self):
        old_tokens = [
            "2026-03-10",
            "Doha",
            "Hard",
            "Opponent X",
            "Iga Swiatek",
            "6-3 6-4",
            "2",
            "0.88",
            "0.81",
            "74.2",
            "31.9",
            "67.0",
            "52.3",
            "63.2",
            "71.1",
            "54.1",
            "44.7",
            "1.23",
            "55.0",
        ]
        new_tokens = [
            "2026-03-11",
            "Doha",
            "Hard",
            "Opponent Y",
            "W",
            "Coco Gauff",
            "6-4 7-5",
            "3",
            "0.79",
            "0.73",
            "69.4",
            "28.6",
            "64.8",
            "50.2",
            "62.0",
            "68.1",
            "51.7",
            "41.5",
            "1.10",
            "53.8",
        ]

        self._assert_schema_metric_window(MATCHMX_OLD_ROW_IDX, old_tokens, expected_hold=74.2, expected_break=31.9)
        self._assert_schema_metric_window(MATCHMX_NEW_ROW_IDX, new_tokens, expected_hold=69.4, expected_break=28.6)


    def test_parse_matchmx_long_schema_row_uses_long_index_map_for_hold_and_break(self):
        long_tokens = [
            "2026-01-06",
            "Auckland",
            "Hard",
            "Opponent Z",
            "W",
            "Emma Navarro",
            "R32",
            "32",
            "Outdoor",
            "3",
            "97",
            "M123",
            "USA",
            "WTA250",
            "WTA",
            "2026",
            "6",
            "",
            "23.1",
            "R",
            "5",
            "2",
            "61",
            "6-4 6-2",
            "12",
            "0.83",
            "0.76",
            "68.5",
            "37.9",
            "59.2",
            "46.8",
            "62.7",
            "69.4",
            "49.5",
            "40.6",
            "1.14",
            "53.2",
            "12",
            "11",
            "129",
            "1-0",
            "-130",
            "-145",
            "Auckland-style long row",
        ]

        self.assertEqual(float(long_tokens[MATCHMX_LONG_ROW_IDX["HOLD_PCT"]]), 68.5)
        self.assertEqual(float(long_tokens[MATCHMX_LONG_ROW_IDX["BREAK_PCT"]]), 37.9)

        parsed, error = parse_matchmx_player_row(long_tokens)
        self.assertIsNone(error)
        assert parsed is not None
        self.assertEqual(parsed.ranking, 12.0)
        self.assertEqual(parsed.hold_pct, 68.5)
        self.assertEqual(parsed.break_pct, 37.9)

        self.assertIsNotNone(parsed.hold_pct)
        self.assertIsNotNone(parsed.break_pct)
        self.assertNotEqual(parsed.hold_pct, parsed.ranking)
        self.assertNotEqual(parsed.break_pct, parsed.ranking)
        self.assertNotEqual(parsed.hold_pct, 6.0)
        self.assertNotEqual(parsed.break_pct, 6.0)
        self.assertNotEqual(parsed.hold_pct, 32.0)
        self.assertNotEqual(parsed.break_pct, 32.0)
        self.assertNotEqual(parsed.hold_pct, float(long_tokens[MATCHMX_LONG_ROW_IDX["SEED"]]))
        self.assertNotEqual(parsed.break_pct, float(long_tokens[MATCHMX_LONG_ROW_IDX["SEED"]]))

    def test_parse_matchmx_live_long_schema_phase_prefixed_row_uses_live_long_index_map(self):
        live_long_tokens = self._live_long_tokens_template()

        self.assertEqual(float(live_long_tokens[MATCHMX_LONG_LIVE_ROW_IDX["RANKING"]]), 2.0)
        self.assertEqual(float(live_long_tokens[MATCHMX_LONG_LIVE_ROW_IDX["HOLD_PCT"]]), 71.4)
        self.assertEqual(float(live_long_tokens[MATCHMX_LONG_LIVE_ROW_IDX["BREAK_PCT"]]), 39.6)

        parsed, error = parse_matchmx_player_row(live_long_tokens)
        self.assertIsNone(error)
        assert parsed is not None
        self.assertEqual(parsed.ranking, 2.0)
        self.assertEqual(parsed.hold_pct, 71.4)
        self.assertEqual(parsed.break_pct, 39.6)
        self.assertNotEqual(parsed.hold_pct, parsed.ranking)
        self.assertNotEqual(parsed.break_pct, parsed.ranking)


    def test_get_matchmx_row_idx_penalizes_seed_map_when_hold_is_null_and_other_metrics_exist(self):
        tokens = [
            "2026-04-01", "Madrid", "Clay", "Opponent", "W", "Iga Swiatek", "6-2 6-3", "3",
            "0.90", "0.82", "72.4", "37.1", "64.2", "49.8", "61.7", "70.1", "52.5", "41.3", "1.14", "53.9",
        ]
        idx = get_matchmx_row_idx(tokens)
        self.assertEqual(idx, MATCHMX_NEW_ROW_IDX)

    def test_get_matchmx_row_idx_uses_multi_row_sampling_to_avoid_constant_integer_break_mapping(self):
        rows = [
            [
                "2026-04-01", "Rome", "Clay", "Opp A", "W", "Player A", "6-3 6-3", "4",
                "0.86", "0.79", "71.5", "35", "63.2", "48.1", "60.3", "68.7", "51.4", "39.8", "1.10", "52.9",
            ],
            [
                "2026-04-02", "Rome", "Clay", "Opp B", "L", "Player B", "4-6 6-4 4-6", "8",
                "0.78", "0.72", "68.9", "29", "59.8", "45.7", "57.6", "65.1", "48.9", "37.4", "1.03", "50.8",
            ],
            [
                "2026-04-03", "Rome", "Clay", "Opp C", "W", "Player C", "7-5 6-4", "11",
                "0.74", "0.69", "66.2", "33", "58.4", "44.2", "56.1", "63.7", "47.3", "36.1", "1.01", "49.6",
            ],
        ]

        idx = get_matchmx_row_idx(rows[0], sample_rows=rows)
        self.assertEqual(idx, MATCHMX_NEW_ROW_IDX)

    def test_get_matchmx_row_idx_rejects_new_with_seed_when_break_token_is_name_like(self):
        tokens = [
            "2026-04-01", "Madrid", "Clay", "Opponent", "W", "Iga Swiatek", "6-2 6-3", "3", "9",
            "0.90", "0.82", "72.4", "Aryna Sabalenka", "64.2", "49.8", "61.7", "70.1", "52.5", "41.3", "1.14", "53.9",
        ]

        idx = get_matchmx_row_idx(tokens)
        self.assertEqual(idx, MATCHMX_OLD_ROW_IDX)


    def test_get_matchmx_row_idx_rejects_candidate_when_sampled_hold_break_tokens_are_non_numeric(self):
        rows = [
            [
                "2026-04-01", "Rome", "Clay", "Opp A", "W", "Player A", "6-3 6-3", "4",
                "0.86", "0.79", "71.5", "35", "63.2", "48.1", "60.3", "68.7", "51.4", "39.8", "1.10", "52.9",
            ],
            [
                "2026-04-02", "Rome", "Clay", "Opp B", "L", "Player B", "4-6 6-4 4-6", "8",
                "0.78", "0.72", "69.1", "NaNish", "59.8", "45.7", "57.6", "65.1", "48.9", "37.4", "1.03", "50.8",
            ],
            [
                "2026-04-03", "Rome", "Clay", "Opp C", "W", "Player C", "7-5 6-4", "11",
                "0.74", "0.69", "66.2", "Aryna Sabalenka", "58.4", "44.2", "56.1", "63.7", "47.3", "36.1", "1.01", "49.6",
            ],
        ]

        idx = get_matchmx_row_idx(rows[0], sample_rows=rows)
        self.assertEqual(idx, MATCHMX_OLD_ROW_IDX)

    def test_get_matchmx_row_idx_rejects_candidate_when_break_resolves_to_name_like_tokens(self):
        rows = [
            [
                "2026-04-01", "Rome", "Clay", "Opp A", "W", "Player A", "6-3 6-3", "4",
                "0.86", "0.79", "71.5", "35", "63.2", "48.1", "60.3", "68.7", "51.4", "39.8", "1.10", "52.9",
            ],
            [
                "2026-04-02", "Rome", "Clay", "Opp B", "L", "Player B", "4-6 6-4 4-6", "8",
                "0.78", "0.72", "69.1", "Iga Swiatek", "59.8", "45.7", "57.6", "65.1", "48.9", "37.4", "1.03", "50.8",
            ],
            [
                "2026-04-03", "Rome", "Clay", "Opp C", "W", "Player C", "7-5 6-4", "11",
                "0.74", "0.69", "66.2", "Jessica Pegula", "58.4", "44.2", "56.1", "63.7", "47.3", "36.1", "1.01", "49.6",
            ],
        ]

        idx = get_matchmx_row_idx(rows[0], sample_rows=rows)
        self.assertEqual(idx, MATCHMX_OLD_ROW_IDX)

    def test_get_matchmx_row_idx_rejects_candidate_when_hold_or_break_valid_counts_are_too_low(self):
        rows = [
            [
                "2026-04-01", "Rome", "Clay", "Opp A", "W", "Player A", "6-3 6-3", "4",
                "0.86", "0.79", "71.5", "35.1", "63.2", "48.1", "60.3", "68.7", "51.4", "39.8", "1.10", "52.9",
            ],
            [
                "2026-04-02", "Rome", "Clay", "Opp B", "L", "Player B", "4-6 6-4 4-6", "8",
                "0.78", "0.72", "null", "null", "59.8", "45.7", "57.6", "65.1", "48.9", "37.4", "1.03", "50.8",
            ],
            [
                "2026-04-03", "Rome", "Clay", "Opp C", "W", "Player C", "7-5 6-4", "11",
                "0.74", "0.69", "undefined", "undefined", "58.4", "44.2", "56.1", "63.7", "47.3", "36.1", "1.01", "49.6",
            ],
        ]

        idx = get_matchmx_row_idx(rows[0], sample_rows=rows)
        self.assertEqual(idx, MATCHMX_OLD_ROW_IDX)

    def test_get_matchmx_row_idx_uses_live_45_shape_only_with_plausible_numeric_hold_break(self):
        tokens = self._live_long_tokens_template()

        idx = get_matchmx_row_idx(tokens)
        self.assertEqual(idx, MATCHMX_LONG_LIVE_ROW_IDX)

    def test_get_matchmx_row_idx_rejects_old_map_when_old_player_name_is_result_flag(self):
        tokens = self._live_long_tokens_template()
        tokens[MATCHMX_OLD_ROW_IDX["PLAYER_NAME"]] = "W"

        idx = get_matchmx_row_idx(tokens)
        self.assertEqual(idx, MATCHMX_LONG_LIVE_ROW_IDX)

    def test_get_matchmx_row_idx_rejects_old_map_when_old_hold_pct_is_round_label(self):
        tokens = self._live_long_tokens_template()

        tokens[MATCHMX_OLD_ROW_IDX["HOLD_PCT"]] = "QF"

        idx = get_matchmx_row_idx(tokens)
        self.assertEqual(idx, MATCHMX_LONG_LIVE_ROW_IDX)

    def test_get_matchmx_row_idx_rejects_old_map_when_old_break_pct_is_score_like(self):
        tokens = self._live_long_tokens_template()

        tokens[MATCHMX_OLD_ROW_IDX["BREAK_PCT"]] = "6-3 6-1"

        idx = get_matchmx_row_idx(tokens)
        self.assertEqual(idx, MATCHMX_LONG_LIVE_ROW_IDX)

    def test_realistic_new_schema_rows_keep_hold_non_null_and_break_non_constant_and_distinct_from_ranking(self):
        payload = '\n'.join([
            'matchmx[0] = ["2026-03-20","Miami","Hard","Opponent A","W","Iga Swiatek","6-2 6-3","2","0.88","0.82","73.6","41.1","66.5","51.2","62.1","70.3","53.4","43.0","1.20","54.8"];',
            'matchmx[1] = ["2026-03-21","Miami","Hard","Opponent B","L","Coco Gauff","4-6 6-3 4-6","3","0.81","0.77","69.2","36.4","61.1","48.6","59.4","67.0","50.8","40.2","1.09","52.6"];',
            'matchmx[2] = ["2026-03-22","Miami","Hard","Opponent C","W","Jessica Pegula","7-5 6-4","5","0.79","0.74","67.8","33.7","60.4","47.3","58.6","65.2","49.1","38.4","1.04","51.1"];',
        ])
        rows = _parse_matchmx_rows("tennisabstract_leadersource_wta", payload, "2026-01-01T00:00:00+00:00")
        ok_rows = [row for row in rows if row.reason_code == "ok"]
        self.assertEqual(len(ok_rows), 3)

        hold_values = [row.hold_pct for row in ok_rows]
        break_values = [row.break_pct for row in ok_rows]
        ranking_values = [row.ranking for row in ok_rows]

        self.assertTrue(all(value is not None for value in hold_values))
        self.assertTrue(all(value is not None for value in break_values))
        self.assertGreater(len(set(break_values)), 1)

        self.assertTrue(all(35.0 <= float(v) <= 95.0 for v in hold_values if v is not None))
        self.assertTrue(all(5.0 <= float(v) <= 70.0 for v in break_values if v is not None))
        self.assertTrue(all(float(h) not in set(float(r) for r in ranking_values if r is not None) for h in hold_values if h is not None))
        self.assertTrue(all(float(b) not in set(float(r) for r in ranking_values if r is not None) for b in break_values if b is not None))

    def test_probe_rows_have_plausible_hold_band_and_non_constant_break_values(self):
        if not self._has_probe_payload():
            self.skipTest("probe artifact not present")

        payload = self._load_live_probe_payload()
        probe_token_rows = []
        for parsed in iter_matchmx_rows(payload):
            if parsed.row_shape_valid:
                probe_token_rows.append(parsed.tokens)
            if len(probe_token_rows) >= 25:
                break

        self.assertGreaterEqual(len(probe_token_rows), 5)

        selected_idx = get_matchmx_row_idx(probe_token_rows[0], sample_rows=probe_token_rows)
        self.assertEqual(selected_idx, MATCHMX_LONG_LIVE_ROW_IDX)

        name_like_token = re.compile(r"^[A-Za-z][A-Za-z .'-]*[A-Za-z]$")
        score_like_token = re.compile(r"\d\s*-\s*\d")

        def _is_full_name_like(value: str) -> bool:
            candidate = value.strip()
            return bool(name_like_token.fullmatch(candidate) and (" " in candidate or "-" in candidate))

        hold_values = []
        break_values = []

        for tokens in probe_token_rows:
            player_token = str(tokens[selected_idx["PLAYER_NAME"]]).strip()
            hold_token = str(tokens[selected_idx["HOLD_PCT"]]).strip()
            break_token = str(tokens[selected_idx["BREAK_PCT"]]).strip()

            self.assertTrue(_is_full_name_like(player_token), f"player token is not full-name-like: {player_token!r}")

            self.assertRegex(
                hold_token,
                r"^-?\d+(?:\.\d+)?$",
                f"hold token is not numeric under selected schema: {hold_token!r}",
            )
            self.assertRegex(
                break_token,
                r"^-?\d+(?:\.\d+)?$",
                f"break token is not numeric under selected schema: {break_token!r}",
            )

            self.assertFalse(_is_full_name_like(hold_token), f"hold token looks name-like: {hold_token!r}")
            self.assertFalse(_is_full_name_like(break_token), f"break token looks name-like: {break_token!r}")
            self.assertFalse(score_like_token.search(break_token), f"break token looks score-like: {break_token!r}")

            hold_value = float(hold_token)
            break_value = float(break_token)

            self.assertLessEqual(hold_value, 95.0, f"hold value out of plausible band: {hold_value}")
            self.assertGreaterEqual(break_value, 0.0, f"break value out of plausible band: {break_value}")
            self.assertLessEqual(break_value, 70.0, f"break value out of plausible band: {break_value}")

            hold_values.append(hold_value)
            break_values.append(break_value)

        self.assertGreater(sum(1 for value in break_values if value != 0.0), 0)
        self.assertGreater(len(set(break_values)), 1)

        self.assertGreaterEqual(min(hold_values), 20.0, f"hold value out of plausible floor: {min(hold_values)}")
        self.assertGreaterEqual(
            statistics.median(hold_values),
            35.0,
            f"hold value median below plausible floor: {statistics.median(hold_values)}",
        )

    def test_live_shape_regression_fixture_includes_hold_none_and_break_three_rows(self):
        rows = _parse_matchmx_rows(
            "tennisabstract_leadersource_wta",
            self.live_shape_payload,
            "2026-01-01T00:00:00+00:00",
        )
        ok_rows = [row for row in rows if row.reason_code == "ok"]
        self.assertTrue(any(row.hold_pct is None and row.break_pct == 3.0 for row in ok_rows))

    def test_live_shape_regression_fixture_has_non_zero_hold_pct_coverage_and_non_constant_break_pct(self):
        rows = _parse_matchmx_rows(
            "tennisabstract_leadersource_wta",
            self.live_shape_payload,
            "2026-01-01T00:00:00+00:00",
        )
        ok_rows = [row for row in rows if row.reason_code == "ok"]

        hold_values = [row.hold_pct for row in ok_rows if row.hold_pct is not None]
        break_values = [row.break_pct for row in ok_rows if row.break_pct is not None]

        self.assertGreater(len(hold_values), 0)
        self.assertGreater(len(break_values), 0)
        self.assertGreater(len(set(break_values)), 1)

    def test_live_shape_regression_fixture_hold_break_not_equal_to_rank_or_seed_columns(self):
        parsed_rows = []
        for parsed in iter_matchmx_rows(self.live_shape_payload):
            if not parsed.row_shape_valid:
                continue
            row, error = parse_matchmx_player_row(parsed.tokens)
            if error or row is None:
                continue
            parsed_rows.append((parsed.tokens, row))

        self.assertGreater(len(parsed_rows), 0)
        compared_rows = 0
        seed_indices = [8, 16, 17]

        for tokens, row in parsed_rows:
            if row.hold_pct is not None and row.ranking is not None:
                compared_rows += 1
                self.assertNotEqual(row.hold_pct, row.ranking)
            if row.break_pct is not None and row.ranking is not None:
                compared_rows += 1
                self.assertNotEqual(row.break_pct, row.ranking)

            for idx in seed_indices:
                if idx >= len(tokens):
                    continue
                raw_seed = str(tokens[idx]).strip()
                if not raw_seed:
                    continue
                try:
                    seed = float(raw_seed)
                except ValueError:
                    continue
                if row.hold_pct is not None:
                    compared_rows += 1
                    self.assertNotEqual(row.hold_pct, seed)
                if row.break_pct is not None:
                    compared_rows += 1
                    self.assertNotEqual(row.break_pct, seed)

        self.assertGreater(compared_rows, 0)

    def test_parse_matchmx_new_schema_seed_variant_keeps_hold_break_distinct_from_ranking_and_seed(self):
        payload = 'matchmx[0] = ["2026-03-15","Indian Wells","Hard","Aryna Sabalenka","W","Mirra Andreeva","7-6(4) 6-4","12","9","0.64","0.59","71.3","34.8","63.1","45.5","61.9","69.0","50.4","39.2","1.09","52.7"];'
        rows = _parse_matchmx_rows("tennisabstract_leadersource_wta", payload, "2026-01-01T00:00:00+00:00")
        ok_rows = [row for row in rows if row.reason_code == "ok"]
        self.assertEqual(len(ok_rows), 1)

        row = ok_rows[0]
        self.assertEqual(row.ranking, 12.0)
        self.assertEqual(row.hold_pct, 71.3)
        self.assertEqual(row.break_pct, 34.8)
        self.assertNotEqual(row.hold_pct, row.ranking)
        self.assertNotEqual(row.break_pct, row.ranking)
        self.assertNotEqual(row.hold_pct, 9.0)
        self.assertNotEqual(row.break_pct, 9.0)

    def test_parse_matchmx_realistic_new_schema_row_keeps_hold_break_distinct_from_ranking(self):
        payload = 'matchmx[0] = ["2026-03-14","Indian Wells","Hard","Aryna Sabalenka","W","Mirra Andreeva","7-6(4) 6-4","12","0.64","0.59","71.3","34.8","63.1","45.5","61.9","69.0","50.4","39.2","1.09","52.7"];'
        rows = _parse_matchmx_rows("tennisabstract_leadersource_wta", payload, "2026-01-01T00:00:00+00:00")
        ok_rows = [row for row in rows if row.reason_code == "ok"]
        self.assertEqual(len(ok_rows), 1)

        row = ok_rows[0]
        self.assertEqual(row.hold_pct, 71.3)
        self.assertEqual(row.break_pct, 34.8)
        self.assertNotEqual(row.hold_pct, row.break_pct)
        self.assertNotEqual(row.hold_pct, row.ranking)
        self.assertNotEqual(row.break_pct, row.ranking)
        self.assertNotEqual(row.hold_pct, 0.59)
        self.assertNotEqual(row.break_pct, 12.0)

    def test_parse_matchmx_parses_each_row_independently_from_array_assignment(self):
        payload = 'matchmx = [["2026-01-01","Open","Hard","Opponent A","Iga Swiatek","6-1 6-1","1","0.9","0.8","70","40","60","50","58","67","49","41","1.12","54"],["2026-01-02","Open","Hard","Opponent B","Coco Gauff","6-2 6-2","3","0.8","0.7","68","39","59","49","57","66","48","40","1.08","52"]];'
        rows = _parse_matchmx_rows("tennisabstract_leadersource_wta", payload, "2026-01-01T00:00:00+00:00")
        ok_rows = [r for r in rows if r.reason_code == "ok"]
        self.assertEqual(len(ok_rows), 2)
        self.assertEqual(ok_rows[0].player_canonical_name, "Iga Swiatek")
        self.assertEqual(ok_rows[1].player_canonical_name, "Coco Gauff")

    def test_check_ta_parity_emits_unusable_payload_metrics_for_bad_names(self):
        fixture_with_bad_name = ROOT / "tmp" / "test_ta_parity_bad_name.body"
        fixture_with_bad_name.parent.mkdir(parents=True, exist_ok=True)
        fixture_with_bad_name.write_text(
            'matchmx[0] = ["2026-01-01","Open","Hard","A","Opponent","6-0 6-0","1","0.8","0.7","65","38","60","50","58","67","49","41","1.12","54"];',
            encoding="utf-8",
        )

        cmd = [
            sys.executable,
            str(SCRIPTS_DIR / "check_ta_parity.py"),
            "--input",
            str(fixture_with_bad_name),
        ]
        result = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 1)
        summary = json.loads(result.stdout)
        self.assertEqual(summary["reason_code"], "ta_matchmx_unusable_payload")
        self.assertEqual(summary["matchmx_unusable_rows"], 1)

    def test_check_ta_parity_reason_code_prefers_parser_failure_when_unusable_payload_present(self):
        fixture = ROOT / "tmp" / "test_ta_parity_parser_and_threshold_failure.body"
        fixture.parent.mkdir(parents=True, exist_ok=True)
        fixture.write_text(
            '\n'.join([
                'matchmx[0] = ["2026-01-01","Open","Hard","A","Opponent","6-0 6-0","1","0.8","0.7","65","38","60","50","58","67","49","41","1.12","54"];',
                'matchmx[1] = ["2026-01-02","Open","Hard","Opponent B","Iga Swiatek","6-2 6-2","1","0.8","0.7","65","","60","50","58","67","49","41","1.12","54"];',
            ]),
            encoding="utf-8",
        )

        cmd = [
            sys.executable,
            str(SCRIPTS_DIR / "check_ta_parity.py"),
            "--input",
            str(fixture),
            "--min-rows",
            "1",
            "--min-unique-players",
            "1",
        ]
        result = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 1)
        summary = json.loads(result.stdout)
        self.assertEqual(summary["reason_code"], "ta_matchmx_unusable_payload")
        self.assertIn("break_pct_non_null_coverage", summary["threshold_failure_keys"])

    def test_check_ta_parity_reason_code_threshold_only_failure(self):
        fixture = ROOT / "tmp" / "test_ta_parity_threshold_only_failure.body"
        fixture.parent.mkdir(parents=True, exist_ok=True)
        fixture.write_text(
            'matchmx[0] = ["2026-01-01","Open","Hard","Opponent A","Iga Swiatek","6-0 6-0","1","0.8","0.7","","38","60","50","58","67","49","41","1.12","54"];',
            encoding="utf-8",
        )

        cmd = [
            sys.executable,
            str(SCRIPTS_DIR / "check_ta_parity.py"),
            "--input",
            str(fixture),
            "--min-rows",
            "1",
            "--min-unique-players",
            "1",
        ]
        result = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 1)
        summary = json.loads(result.stdout)
        self.assertEqual(summary["reason_code"], "ta_matchmx_threshold_failure")
        self.assertEqual(summary["matchmx_unusable_rows"], 0)
        self.assertIn("hold_pct_non_null_coverage", summary["threshold_failure_keys"])

    def test_check_ta_parity_success_case_has_no_failure_reason(self):
        cmd = [
            sys.executable,
            str(SCRIPTS_DIR / "check_ta_parity.py"),
            "--input",
            str(self.fixture_path),
            "--sample-size",
            "1",
        ]
        result = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, check=True)
        summary = json.loads(result.stdout)
        self.assertNotIn("reason_code", summary)
        self.assertEqual(summary["threshold_errors"], [])
        self.assertEqual(summary["threshold_failure_keys"], [])

    def test_parse_matchmx_first_20_rows_resolve_to_canonical_names_with_row_diagnostics(self):
        canonical_names = [
            "Amanda Anisimova",
            "Elina Svitolina",
            "Iga Swiatek",
            "Coco Gauff",
            "Jessica Pegula",
            "Aryna Sabalenka",
            "Elena Rybakina",
            "Ons Jabeur",
            "Marketa Vondrousova",
            "Daria Kasatkina",
            "Madison Keys",
            "Caroline Garcia",
            "Maria Sakkari",
            "Jelena Ostapenko",
            "Qinwen Zheng",
            "Paula Badosa",
            "Beatriz Haddad Maia",
            "Donna Vekic",
            "Marta Kostyuk",
            "Anna Kalinskaya",
        ]
        rows_payload = []
        for idx, canonical_name in enumerate(canonical_names):
            sentinel = "I P" if idx % 2 == 0 else "P I"
            rows_payload.append(
                'matchmx[{idx}] = ["2026-02-{day:02d}","Doha","Hard","{sentinel}","{canonical_name}","6-4 6-4","{rank}","0.81","0.75","65","38","60","50","58","67","49","41","1.12","54"];'.format(
                    idx=idx,
                    day=idx + 1,
                    sentinel=sentinel,
                    canonical_name=canonical_name,
                    rank=idx + 1,
                )
            )

        payload = "\n".join(rows_payload)
        rows = _parse_matchmx_rows("tennisabstract_leadersource_wta", payload, "2026-01-01T00:00:00+00:00")
        ok_rows = [row for row in rows if row.reason_code == "ok"]
        self.assertEqual(len(ok_rows), 20)

        name_pattern = re.compile(r"^[A-Za-z][A-Za-z .'-]*\s[A-Za-z .'-]*[A-Za-z]$")
        for row_idx, row in enumerate(ok_rows[:20]):
            name = (row.player_canonical_name or "").strip()
            diagnostic = f"row_idx={row_idx} name={name!r}"
            self.assertGreaterEqual(len(name), 3, diagnostic)
            self.assertRegex(name, name_pattern, diagnostic)
            self.assertEqual(name, canonical_names[row_idx], diagnostic)

        self.assertGreater(sum(1 for row in ok_rows[:20] if row.ranking is not None), 0)
        self.assertGreater(sum(1 for row in ok_rows[:20] if row.hold_pct is not None), 0)
        self.assertGreater(sum(1 for row in ok_rows[:20] if row.break_pct is not None), 0)

if __name__ == "__main__":
    unittest.main()
