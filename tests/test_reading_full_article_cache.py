import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import app as dragon_app


class ReadingFullArticleCacheTests(unittest.TestCase):
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

    def test_load_full_article_writes_separate_cache_and_keeps_snapshot_lightweight(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reading_data_path = root / "reading_data.json"
            cache_dir = root / "fulltext-cache"
            reading_data_path.write_text(json.dumps(self._payload(), ensure_ascii=False, indent=2), encoding="utf-8")
            extraction = {
                "status": "ok",
                "content_html": "<p>Full article body.</p>",
                "content_text": "Full article body.",
                "excerpt": "Full article body.",
                "error": "",
            }

            patches = self._patched_runtime(reading_data_path, cache_dir)
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patch.object(
                dragon_app,
                "DRAGON_ALLOW_LIVE_ARTICLE_EXTRACTION",
                True,
            ), patch.object(
                dragon_app,
                "extract_reading_article_page",
                return_value=extraction,
            ) as extractor:
                response = self.client.post("/reading/article/reading-entry-1/load-full", data={"next": "/reading/article/reading-entry-1"})
                article_response = self.client.get("/reading/article/reading-entry-1")

            self.assertEqual(response.status_code, 302)
            self.assertIn("full_loaded=", response.headers.get("Location", ""))
            self.assertEqual(article_response.status_code, 200)
            self.assertIn("Full article body.", article_response.get_data(as_text=True))
            extractor.assert_called_once()

            cache_files = list(cache_dir.rglob("*.json"))
            self.assertEqual(len(cache_files), 1)
            cache_payload = json.loads(cache_files[0].read_text(encoding="utf-8"))
            self.assertEqual(cache_payload.get("url"), "https://example.com/articles/1")
            self.assertEqual(cache_payload.get("status"), "ok")
            self.assertEqual(cache_payload.get("content_text"), "Full article body.")
            self.assertEqual(cache_payload.get("word_count"), 3)

            saved_payload = json.loads(reading_data_path.read_text(encoding="utf-8"))
            saved_entry = saved_payload["entries"][0]
            self.assertEqual(saved_entry.get("status"), "reading")
            self.assertFalse(str(saved_entry.get("content_html", "") or "").strip())
            self.assertFalse(str(saved_entry.get("content_text", "") or "").strip())
            self.assertEqual(
                sum(
                    1
                    for item in (saved_payload.get("entries", []) or [])
                    if str(item.get("content_html", "") or "").strip() or str(item.get("content_text", "") or "").strip()
                ),
                0,
            )

    def test_cached_full_article_is_reused_without_reextracting(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reading_data_path = root / "reading_data.json"
            cache_dir = root / "fulltext-cache"
            reading_data_path.write_text(json.dumps(self._payload(), ensure_ascii=False, indent=2), encoding="utf-8")
            extraction = {
                "status": "ok",
                "content_html": "<p>Cached body.</p>",
                "content_text": "Cached body.",
                "excerpt": "Cached body.",
                "error": "",
            }

            patches = self._patched_runtime(reading_data_path, cache_dir)
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patch.object(
                dragon_app,
                "DRAGON_ALLOW_LIVE_ARTICLE_EXTRACTION",
                True,
            ), patch.object(
                dragon_app,
                "extract_reading_article_page",
                return_value=extraction,
            ) as extractor:
                first = self.client.post("/reading/article/reading-entry-1/load-full", data={"next": "/reading/article/reading-entry-1"})
                second = self.client.post("/reading/article/reading-entry-1/load-full", data={"next": "/reading/article/reading-entry-1"})

            self.assertEqual(first.status_code, 302)
            self.assertEqual(second.status_code, 302)
            self.assertEqual(extractor.call_count, 1)
            self.assertIn("full_loaded=", second.headers.get("Location", ""))

    def test_disabled_full_article_load_does_not_fetch_network_and_shows_safe_message(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reading_data_path = root / "reading_data.json"
            cache_dir = root / "fulltext-cache"
            reading_data_path.write_text(json.dumps(self._payload(), ensure_ascii=False, indent=2), encoding="utf-8")

            patches = self._patched_runtime(reading_data_path, cache_dir)
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patch.object(
                dragon_app,
                "DRAGON_ALLOW_LIVE_ARTICLE_EXTRACTION",
                False,
            ), patch.object(
                dragon_app,
                "extract_reading_article_page",
                side_effect=AssertionError("Extractor should not run when full article loading is disabled"),
            ):
                response = self.client.post("/reading/article/reading-entry-1/load-full", data={"next": "/reading/article/reading-entry-1"})
                article_response = self.client.get(response.headers.get("Location", ""))

            self.assertEqual(response.status_code, 302)
            html = article_response.get_data(as_text=True)
            self.assertIn("Full article loading is not available on this host. Open original source.", html)
            self.assertNotIn("ProxyError", html)
            self.assertNotIn("Tunnel connection failed", html)

    def test_cached_full_article_still_renders_when_live_extraction_is_disabled(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reading_data_path = root / "reading_data.json"
            cache_dir = root / "fulltext-cache"
            reading_data_path.write_text(json.dumps(self._payload(), ensure_ascii=False, indent=2), encoding="utf-8")
            extraction = {
                "status": "ok",
                "content_html": "<p>Cached full body.</p>",
                "content_text": "Cached full body.",
                "excerpt": "Cached full body.",
                "error": "",
            }

            patches = self._patched_runtime(reading_data_path, cache_dir)
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patch.object(
                dragon_app,
                "DRAGON_ALLOW_LIVE_ARTICLE_EXTRACTION",
                True,
            ), patch.object(
                dragon_app,
                "extract_reading_article_page",
                return_value=extraction,
            ):
                self.client.post("/reading/article/reading-entry-1/load-full", data={"next": "/reading/article/reading-entry-1"})

            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patch.object(
                dragon_app,
                "DRAGON_ALLOW_LIVE_ARTICLE_EXTRACTION",
                False,
            ), patch.object(
                dragon_app,
                "extract_reading_article_page",
                side_effect=AssertionError("Extractor should not run when cached full article exists"),
            ):
                article_response = self.client.get("/reading/article/reading-entry-1")

            self.assertEqual(article_response.status_code, 200)
            self.assertIn("Cached full body.", article_response.get_data(as_text=True))

    def test_failed_or_unsafe_full_article_load_returns_safe_error(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reading_data_path = root / "reading_data.json"
            cache_dir = root / "fulltext-cache"
            reading_data_path.write_text(json.dumps(self._payload("http://localhost/private"), ensure_ascii=False, indent=2), encoding="utf-8")

            patches = self._patched_runtime(reading_data_path, cache_dir)
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patch.object(
                dragon_app,
                "DRAGON_ALLOW_LIVE_ARTICLE_EXTRACTION",
                True,
            ), patch.object(
                dragon_app,
                "extract_reading_article_page",
                side_effect=AssertionError("Extractor should not run for unsafe URLs"),
            ):
                unsafe = self.client.post("/reading/article/reading-entry-1/load-full", data={"next": "/reading/article/reading-entry-1"})
                missing = self.client.post("/reading/article/does-not-exist/load-full", data={"next": "/reading/article/does-not-exist"})

            self.assertEqual(unsafe.status_code, 302)
            self.assertIn("full_error=", unsafe.headers.get("Location", ""))
            self.assertEqual(missing.status_code, 404)

    def test_cached_failed_status_does_not_render_raw_error_details(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reading_data_path = root / "reading_data.json"
            cache_dir = root / "fulltext-cache"
            reading_data_path.write_text(json.dumps(self._payload(), ensure_ascii=False, indent=2), encoding="utf-8")

            cache_path = dragon_app.reading_article_fulltext_cache_path("https://example.com/articles/1")
            self.assertIsNotNone(cache_path)

            patches = self._patched_runtime(reading_data_path, cache_dir)
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
                cache_path = dragon_app.reading_article_fulltext_cache_path("https://example.com/articles/1")
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                cache_path.write_text(
                    json.dumps(
                        {
                            "url": "https://example.com/articles/1",
                            "title": "Example Article",
                            "source": "Example Source",
                            "fetched_at": "2026-06-05T00:00:00+00:00",
                            "status": "failed",
                            "content_text": "",
                            "content_html": "",
                            "excerpt": "",
                            "word_count": 0,
                            "error": "ProxyError: Tunnel connection failed: 403 Forbidden",
                        },
                        ensure_ascii=False,
                        indent=2,
                    ),
                    encoding="utf-8",
                )
                article_response = self.client.get("/reading/article/reading-entry-1")

            self.assertEqual(article_response.status_code, 200)
            html = article_response.get_data(as_text=True)
            self.assertNotIn("ProxyError", html)
            self.assertNotIn("Tunnel connection failed", html)
            self.assertNotIn("Full article cache: failed", html)
            self.assertNotIn("Reader cache:", html)

    def test_reading_and_article_get_do_not_trigger_extraction(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reading_data_path = root / "reading_data.json"
            cache_dir = root / "fulltext-cache"
            reading_data_path.write_text(json.dumps(self._payload(), ensure_ascii=False, indent=2), encoding="utf-8")

            patches = self._patched_runtime(reading_data_path, cache_dir)
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patch.object(
                dragon_app,
                "DRAGON_ALLOW_LIVE_ARTICLE_EXTRACTION",
                True,
            ), patch.object(
                dragon_app,
                "extract_reading_article_page",
                side_effect=AssertionError("Extraction should not run during GET requests"),
            ):
                reading_response = self.client.get("/reading")
                article_response = self.client.get("/reading/article/reading-entry-1")

            self.assertEqual(reading_response.status_code, 200)
            self.assertEqual(article_response.status_code, 200)

    def test_article_page_only_shows_request_button_when_can_request(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reading_data_path = root / "reading_data.json"
            cache_dir = root / "fulltext-cache"
            reading_data_path.write_text(json.dumps(self._payload(), ensure_ascii=False, indent=2), encoding="utf-8")

            patches = self._patched_runtime(reading_data_path, cache_dir)
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patch.object(
                dragon_app,
                "DRAGON_READING_FULLTEXT_REQUESTS_ENABLED",
                True,
            ), patch.object(
                dragon_app,
                "DRAGON_READING_FULLTEXT_DISPATCH_MODE",
                "disabled",
            ):
                enabled_response = self.client.get("/reading/article/reading-entry-1")

            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patch.object(
                dragon_app,
                "DRAGON_READING_FULLTEXT_REQUESTS_ENABLED",
                False,
            ), patch.object(
                dragon_app,
                "DRAGON_READING_FULLTEXT_DISPATCH_MODE",
                "disabled",
            ):
                disabled_response = self.client.get("/reading/article/reading-entry-1")

            self.assertEqual(enabled_response.status_code, 200)
            self.assertEqual(disabled_response.status_code, 200)
            self.assertIn("Request Full Article Cache", enabled_response.get_data(as_text=True))
            self.assertNotIn("Request Full Article Cache", disabled_response.get_data(as_text=True))


if __name__ == "__main__":
    unittest.main()
