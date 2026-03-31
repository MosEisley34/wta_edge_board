import json
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


class BuildLogTriageBundleTests(unittest.TestCase):
    def test_builds_expected_sections(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_log = tmp_path / "run_log.ndjson"
            rows = [
                {"run_id": "r1", "timestamp": "2026-03-30T00:00:01Z", "status": "pass", "reason_code": "ok"},
                {"run_id": "r1", "timestamp": "2026-03-30T00:00:02Z", "status": "fail", "reason_code": "F_A", "payload": {"a": 1}},
                {"run_id": "r1", "timestamp": "2026-03-30T00:00:03Z", "status": "insufficient_sample", "reason_code": "F_B"},
                {"run_id": "r1", "timestamp": "2026-03-30T00:00:04Z", "status": "fail", "reason_code": "F_A", "payload": {"a": 2}},
                {"run_id": "r1", "timestamp": "2026-03-30T00:00:05Z", "status": "warning", "reason_code": "W_A"},
            ]
            input_log.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")

            out_path = tmp_path / "bundle.json"
            subprocess.run(
                [
                    "python3",
                    str(ROOT / "scripts" / "build_log_triage_bundle.py"),
                    str(input_log),
                    "--out",
                    str(out_path),
                    "--include-fingerprint",
                ],
                check=True,
                cwd=ROOT,
            )

            bundle = json.loads(out_path.read_text(encoding="utf-8"))
            self.assertEqual("r1", bundle["metadata"]["run_id"])
            self.assertEqual(5, bundle["metadata"]["total_records"])
            self.assertEqual(1, bundle["status_counts"]["pass"])
            self.assertEqual(2, bundle["status_counts"]["fail"])
            self.assertTrue(bundle["transition_points"])
            self.assertIn("F_A", bundle["representative_samples"])
            self.assertIn("fingerprint", bundle)

    def test_enforces_size_cap_and_preserves_summary_and_top_exemplar(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_log = tmp_path / "large.ndjson"
            payload = "x" * 1000
            rows = []
            for idx in range(30):
                rows.append(
                    {
                        "run_id": "r2",
                        "timestamp": f"2026-03-30T00:00:{idx:02d}Z",
                        "status": "fail",
                        "reason_code": "TOP_FAIL",
                        "payload": payload,
                    }
                )
            rows.append(
                {
                    "run_id": "r2",
                    "timestamp": "2026-03-30T00:01:00Z",
                    "status": "warning",
                    "reason_code": "WARN_X",
                    "payload": payload,
                }
            )
            input_log.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")

            out_path = tmp_path / "bundle.json"
            subprocess.run(
                [
                    "python3",
                    str(ROOT / "scripts" / "build_log_triage_bundle.py"),
                    str(input_log),
                    "--out",
                    str(out_path),
                    "--max-chars",
                    "2500",
                    "--samples-per-failure",
                    "5",
                    "--include-fingerprint",
                ],
                check=True,
                cwd=ROOT,
            )

            rendered = out_path.read_text(encoding="utf-8")
            bundle = json.loads(rendered)
            self.assertLessEqual(len(rendered), 2500)
            self.assertIn("metadata", bundle)
            self.assertIn("status_counts", bundle)
            self.assertIn("top_failure_warning_codes", bundle)
            self.assertIn("TOP_FAIL", bundle.get("representative_samples", {}))


if __name__ == "__main__":
    unittest.main()
