import json
import tempfile
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from validate_source_payloads import validate_json_source  # noqa: E402


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


if __name__ == "__main__":
    unittest.main()
