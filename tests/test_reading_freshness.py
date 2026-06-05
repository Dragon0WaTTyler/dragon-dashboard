import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import app as dragon_app


class ReadingFreshnessTests(unittest.TestCase):
    def setUp(self):
        dragon_app.app.config["TESTING"] = True
        self.client = dragon_app.app.test_client()

    def _build_view(self, payload=None, *, file_mtime=None):
        with tempfile.TemporaryDirectory() as temp_dir:
            reading_data_path = Path(temp_dir) / "reading_data.json"
            if payload is not None:
                reading_data_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
                if file_mtime is not None:
                    reading_data_path.touch()
                    import os

                    os.utime(reading_data_path, (file_mtime, file_mtime))
            with patch.object(dragon_app, "READING_DATA_PATH", reading_data_path), patch.object(
                dragon_app, "_READING_CACHE_ACCESS", None
            ), patch.object(
                dragon_app, "_READING_RUNTIME_SERVICE", None
            ), patch.object(
                dragon_app, "_READING_RUNTIME_PROJECTION_SERVICE", None
            ), patch.object(
                dragon_app,
                "load_reading_data_cached",
                return_value=payload if payload is not None else dragon_app.default_reading_data(),
            ):
                with dragon_app.app.test_request_context("/reading"):
                    return dragon_app.build_reading_view()

    def test_reading_context_includes_stale_freshness_when_snapshot_timestamp_is_old(self):
        payload = {
            "version": 1,
            "last_sync_at": "2026-06-01T00:10:00+00:00",
            "last_sync_message": "Imported 1 new items.",
            "sources": [],
            "entries": [],
        }
        old_timestamp = 1_717_257_600  # 2024-06-01T00:00:00Z

        view = self._build_view(payload, file_mtime=old_timestamp)

        self.assertEqual(view["freshness"]["state"], "stale")
        self.assertTrue(view["freshness"]["is_stale"])

    def test_reading_context_failed_sync_maps_to_failed_with_sanitized_safe_error(self):
        payload = {
            "version": 1,
            "last_sync_at": "2026-06-05T00:10:00+00:00",
            "last_sync_message": "Fetch failed: ProxyError token=abc at C:\\secret\\feed.xml",
            "sources": [
                {
                    "id": "src-1",
                    "source_id": "src-1",
                    "name": "Broken Source",
                    "url": "https://example.com/feed.xml",
                    "primary_url": "https://example.com/feed.xml",
                    "category": "news",
                    "active": True,
                    "last_sync_status": "error",
                    "last_sync_status_code": 500,
                    "last_synced_at": "2026-06-05T00:00:00+00:00",
                    "last_sync_message": "Fetch failed: ProxyError token=abc at C:\\secret\\feed.xml",
                }
            ],
            "entries": [],
        }

        view = self._build_view(payload)

        self.assertEqual(view["freshness"]["state"], "failed")
        self.assertEqual(view["freshness"]["safe_error"], "Refresh failed. Try again later.")
        self.assertEqual(view["source_status_summary"]["items"][0]["message"], "Refresh failed. Try again later.")

    def test_reading_page_renders_safe_freshness_note_without_raw_sync_error_details(self):
        payload = {
            "version": 1,
            "last_sync_at": "2026-06-05T00:10:00+00:00",
            "last_sync_message": "Fetch failed: ProxyError token=abc at C:\\secret\\feed.xml",
            "sources": [
                {
                    "id": "src-1",
                    "source_id": "src-1",
                    "name": "Broken Source",
                    "url": "https://example.com/feed.xml",
                    "primary_url": "https://example.com/feed.xml",
                    "category": "news",
                    "active": True,
                    "last_sync_status": "error",
                    "last_sync_status_code": 500,
                    "last_synced_at": "2026-06-05T00:00:00+00:00",
                    "last_sync_message": "Fetch failed: ProxyError token=abc at C:\\secret\\feed.xml",
                }
            ],
            "entries": [],
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            reading_data_path = Path(temp_dir) / "reading_data.json"
            reading_data_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            with patch.object(dragon_app, "READING_DATA_PATH", reading_data_path), patch.object(
                dragon_app, "_READING_CACHE_ACCESS", None
            ), patch.object(
                dragon_app, "_READING_RUNTIME_SERVICE", None
            ), patch.object(
                dragon_app, "_READING_RUNTIME_PROJECTION_SERVICE", None
            ), patch.object(
                dragon_app,
                "load_reading_data_cached",
                return_value=payload,
            ):
                response = self.client.get("/reading")

        html = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("Failed", html)
        self.assertIn("Refresh failed. Try again later.", html)
        self.assertNotIn("ProxyError", html)
        self.assertNotIn("token=abc", html)
        self.assertNotIn("C:\\secret\\feed.xml", html)


if __name__ == "__main__":
    unittest.main()
