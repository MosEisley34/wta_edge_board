import json
import re
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from extract_player_features import _parse_matchmx_rows  # noqa: E402


class TaMatchMxRegressionTest(unittest.TestCase):
    def setUp(self):
        self.fixture_path = ROOT / "scripts" / "fixtures" / "tennisabstract_leadersource_wta.body"
        self.payload = self.fixture_path.read_text(encoding="utf-8")

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
