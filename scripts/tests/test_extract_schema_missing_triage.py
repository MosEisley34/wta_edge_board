import csv
import json
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


class ExtractSchemaMissingTriageTests(unittest.TestCase):
    def test_extracts_and_aggregates_subreasons(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_log = tmp_path / "compare.log"
            payloads = [
                {"status": "error", "reason_code": "schema_missing", "schema_missing_details": ["missing_summary"]},
                {
                    "status": "error",
                    "reason_code": "schema_missing",
                    "schema_missing_details": ["missing_summary", "missing_coverage_counters"],
                },
            ]
            input_log.write_text("\n".join(json.dumps(item) for item in payloads), encoding="utf-8")
            out_csv = tmp_path / "triage_last20_next.csv"

            subprocess.run(
                [
                    "python3",
                    str(ROOT / "scripts" / "extract_schema_missing_triage.py"),
                    "--out",
                    str(out_csv),
                    str(input_log),
                ],
                check=True,
                cwd=ROOT,
            )

            with out_csv.open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            by_subreason = {row["schema_missing_subreason"]: int(row["count"]) for row in rows}
            self.assertEqual(2, by_subreason["missing_summary"])
            self.assertEqual(1, by_subreason["missing_coverage_counters"])


if __name__ == "__main__":
    unittest.main()
