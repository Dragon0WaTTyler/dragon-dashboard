import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import app as dragon_app


class ReadingFulltextRequestStatusTests(unittest.TestCase):
    def setUp(self):
        dragon_app.app.config["TESTING"] = True
        self.client = dragon_app.app.test_client()

    def _payload(self, article_url="https://example.com/articles/1"):
        return {
            "version": 1,
            "last_sync_at": "2026-06-05T00:10:00+00:00",
            "sources": [
                {
                    "id": "reading-src-1",
                    "source_id": "reading-src-1",
                    "name": "Example Source",
                    "url": "https://example.com/feed.xml",
                    "primary_url": "https://example.com/feed.xml",
                    "category": "news",
                    "active": True,
                }
            ],
            "entries": [
                {
                    "id": "reading-entry-1",
                    "source_id": "reading-src-1",
                    "source": "Example Source",
                    "title": "Example Article",
                    "url": article_url,
                    "original_url": article_url,
                    "published_at": "2026-06-05T00:00:00+00:00",
                    "added_at": "2026-06-05T00:00:00+00:00",
                    "status": "unread",
                    "topic": "News",
                    "excerpt": "Short excerpt",
                }
            ],
        }

    def _patched_runtime(self, reading_data_path, cache_dir):
        return patch.object(dragon_app, "READING_DATA_PATH", reading_data_path), patch.object(
            dragon_app, "READING_FULLTEXT_CACHE_DIR", cache_dir
        ), patch.object(
            dragon_app, "_READING_CACHE_ACCESS", None
        ), patch.object(
            dragon_app, "_READING_RUNTIME_SERVICE", None
        ), patch.object(
            dragon_app, "_READING_RUNTIME_PROJECTION_SERVICE", None
        ), patch.object(
            dragon_app, "_READING_RECIPE_OF_DAY_SERVICE", None
        )

    def test_cached_article_returns_cached_status(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reading_data_path = root / "reading_data.json"
            cache_dir = root / "fulltext-cache"
            reading_data_path.write_text(json.dumps(self._payload(), ensure_ascii=False, indent=2), encoding="utf-8")
            patches = self._patched_runtime(reading_data_path, cache_dir)
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
                dragon_app.reading_article_fulltext_save(
                    "https://example.com/articles/1",
                    {
                        "url": "https://example.com/articles/1",
                        "title": "Example Article",
                        "source": "Example Source",
                        "fetched_at": "2026-06-05T00:30:00+00:00",
                        "status": "ok",
                        "content_text": "Cached full article body.",
                        "content_html": "<p>Cached full article body.</p>",
                        "excerpt": "Cached full article body.",
                        "word_count": 4,
                        "error": "",
                    },
                )
                response = self.client.get("/reading/article/reading-entry-1/fulltext-status")

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertEqual(payload["article_id"], "reading-entry-1")
            self.assertEqual(payload["status"], "cached")
            self.assertEqual(payload["cached_at"], "2026-06-05T00:30:00+00:00")
            self.assertFalse(payload["safe_error"])
            self.assertEqual(payload["display_label"], "Cached")

    def test_missing_cache_returns_disabled_safely(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reading_data_path = root / "reading_data.json"
            cache_dir = root / "fulltext-cache"
            reading_data_path.write_text(json.dumps(self._payload(), ensure_ascii=False, indent=2), encoding="utf-8")
            patches = self._patched_runtime(reading_data_path, cache_dir)
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
                response = self.client.get("/reading/article/reading-entry-1/fulltext-status")

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertEqual(payload["status"], "disabled")
            self.assertFalse(payload["can_request"])
            self.assertEqual(payload["safe_error"], "Full article loading is not available on this host. Open original source.")
            self.assertNotIn("ProxyError", payload["display_message"])

    def test_get_status_does_not_trigger_network_extraction(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reading_data_path = root / "reading_data.json"
            cache_dir = root / "fulltext-cache"
            reading_data_path.write_text(json.dumps(self._payload(), ensure_ascii=False, indent=2), encoding="utf-8")
            patches = self._patched_runtime(reading_data_path, cache_dir)
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patch.object(
                dragon_app,
                "extract_reading_article_page",
                side_effect=AssertionError("Fulltext status GET should not extract article content"),
            ):
                response = self.client.get("/reading/article/reading-entry-1/fulltext-status")

            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.get_json()["status"], "disabled")

    def test_post_request_returns_safe_disabled_when_dispatch_unavailable(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reading_data_path = root / "reading_data.json"
            cache_dir = root / "fulltext-cache"
            reading_data_path.write_text(json.dumps(self._payload(), ensure_ascii=False, indent=2), encoding="utf-8")
            patches = self._patched_runtime(reading_data_path, cache_dir)
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patch.object(
                dragon_app,
                "extract_reading_article_page",
                side_effect=AssertionError("Fulltext request POST should not extract article content when dispatch is unavailable"),
            ):
                response = self.client.post("/reading/article/reading-entry-1/request-fulltext")

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertEqual(payload["status"], "disabled")
            self.assertFalse(payload["can_request"])
            self.assertIn("Open original source", payload["display_message"])


if __name__ == "__main__":
    unittest.main()
