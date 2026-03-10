import json
import tempfile
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from validate_source_payloads import (
    _has_valid_tennis_player_prerequisite,
    _load_active_probe_sources,
    validate_json_source,
)  # noqa: E402


class ValidateJsonSourceTests(unittest.TestCase):
    def _write_payload(self, payload_obj):
        handle = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
        path = Path(handle.name)
        with handle:
            json.dump(payload_obj, handle)
        return path

    def test_required_paths_detected_returns_ready_before_nonempty_fallback(self):
        path = self._write_payload({"events": []})
        result = validate_json_source(
            payload="",
            source="sofascore_events_live",
            payload_path=path,
            marker_paths=[("events",)],
            required_paths=[("events",)],
        )
        self.assertTrue(result.ready_for_extraction)
        self.assertEqual(result.reason_code, "json_required_paths_detected")

    def test_missing_required_paths_still_fails_for_nonempty_dict(self):
        path = self._write_payload({"foo": "bar"})
        result = validate_json_source(
            payload="",
            source="sofascore_player_detail",
            payload_path=path,
            marker_paths=[("player",)],
            required_paths=[("player",)],
        )
        self.assertFalse(result.ready_for_extraction)
        self.assertEqual(result.reason_code, "json_contract_required_paths_missing")

    def test_404_and_api_error_payload_detection_remain_intact(self):
        path_404 = self._write_payload({"code": 404, "message": "Not Found"})
        result_404 = validate_json_source(
            payload="",
            source="sofascore_player_recent",
            payload_path=path_404,
            marker_paths=[("events",)],
            required_paths=[("events",)],
        )
        self.assertFalse(result_404.ready_for_extraction)
        self.assertEqual(result_404.reason_code, "json_404_error_payload")

        path_api_error = self._write_payload({"error": {"code": "RATE_LIMIT", "message": "Too many requests"}})
        result_api = validate_json_source(
            payload="",
            source="sofascore_player_recent",
            payload_path=path_api_error,
            marker_paths=[("events",)],
            required_paths=[("events",)],
        )
        self.assertFalse(result_api.ready_for_extraction)
        self.assertEqual(result_api.reason_code, "json_api_error_payload")


class SofascorePrerequisiteTests(unittest.TestCase):
    def _write_player_detail(self, root: Path, payload_text: str) -> None:
        raw = root / "raw"
        raw.mkdir(parents=True, exist_ok=True)
        (raw / "sofascore_player_detail.body").write_text(payload_text, encoding="utf-8")

    def test_non_tennis_domain_reports_explicit_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            out_dir = Path(tmp_dir)
            self._write_player_detail(
                out_dir,
                '{"player": {"id": 123, "sport": {"slug": "football"}}}',
            )

            ok, reason = _has_valid_tennis_player_prerequisite(out_dir)

        self.assertFalse(ok)
        self.assertEqual("player_detail_domain_mismatch:football", reason)

    def test_tennis_domain_with_player_id_passes(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            out_dir = Path(tmp_dir)
            self._write_player_detail(
                out_dir,
                '{"player": {"id": 321, "sport": {"slug": "tennis"}}}',
            )

            ok, reason = _has_valid_tennis_player_prerequisite(out_dir)

        self.assertTrue(ok)
        self.assertEqual("valid_tennis_player_id_detected", reason)


class ActiveProbeSourcesTests(unittest.TestCase):
    def test_loads_sources_from_summary(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            out_dir = Path(tmp_dir)
            summary_path = out_dir / "summary.json"
            summary_path.write_text(
                json.dumps({
                    "sources": [
                        {"source_key": "tennisabstract_leaders"},
                        {"source_key": "sofascore_events_live"},
                        {"source_key": ""},
                    ]
                }),
                encoding="utf-8",
            )

            active = _load_active_probe_sources(out_dir)

        self.assertEqual(active, {"tennisabstract_leaders", "sofascore_events_live"})

    def test_missing_summary_returns_empty_set(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            out_dir = Path(tmp_dir)
            active = _load_active_probe_sources(out_dir)

        self.assertEqual(active, set())


if __name__ == "__main__":
    unittest.main()
