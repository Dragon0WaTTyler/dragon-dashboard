import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import app as dragon_app
import scripts.sync_reading_feeds as sync_reading_feeds


class ReadingFulltextTargetedSyncTests(unittest.TestCase):
    def _payload(self):
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
                    "title": "First Article",
                    "url": "https://example.com/articles/1",
                    "original_url": "https://example.com/articles/1",
                    "published_at": "2026-06-05T00:00:00+00:00",
                    "added_at": "2026-06-05T00:00:00+00:00",
                    "status": "unread",
                    "topic": "News",
                    "excerpt": "Short excerpt 1",
                },
                {
                    "id": "reading-entry-2",
                    "source_id": "reading-src-1",
                    "source": "Example Source",
                    "title": "Second Article",
                    "url": "https://example.com/articles/2",
                    "original_url": "https://example.com/articles/2",
                    "published_at": "2026-06-05T00:05:00+00:00",
                    "added_at": "2026-06-05T00:05:00+00:00",
                    "status": "unread",
                    "topic": "News",
                    "excerpt": "Short excerpt 2",
                },
            ],
        }

    def _patched_runtime(self, reading_data_path, cache_dir):
        return (
            patch.object(dragon_app, "READING_DATA_PATH", reading_data_path),
            patch.object(dragon_app, "READING_FULLTEXT_CACHE_DIR", cache_dir),
            patch.object(dragon_app, "_READING_CACHE_ACCESS", None),
            patch.object(dragon_app, "_READING_RUNTIME_SERVICE", None),
            patch.object(dragon_app, "_READING_RUNTIME_PROJECTION_SERVICE", None),
            patch.object(dragon_app, "_READING_RECIPE_OF_DAY_SERVICE", None),
            patch.object(dragon_app, "_READING_SYNC_SERVICE", None),
        )

    def test_resolve_sync_invocation_options_maps_fulltext_request_mode(self):
        options = sync_reading_feeds.resolve_sync_invocation_options(
            env={
                "DRAGON_READING_SYNC_WORKFLOW_MODE": "fulltext_request",
                "DRAGON_READING_SYNC_REQUEST_ARTICLE_ID": "reading-entry-2",
                "DRAGON_READING_SYNC_REQUEST_MAX_ARTICLES": "99",
            }
        )
        self.assertEqual(options["mode"], "fulltext_request")
        self.assertEqual(options["article_id"], "reading-entry-2")
        self.assertEqual(options["max_articles"], 1)

    def test_run_sync_routes_fulltext_request_to_targeted_sync(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            reading_data_path = Path(temp_dir) / "reading_data.json"
            reading_data_path.write_text(json.dumps(self._payload(), ensure_ascii=False, indent=2), encoding="utf-8")
            with patch.object(sync_reading_feeds.dragon_app, "READING_DATA_PATH", reading_data_path), patch.object(
                sync_reading_feeds.dragon_app,
                "sync_reading_fulltext_request",
                return_value={"ok": True, "status": "cached", "cached_articles": 1, "failed_articles": 0, "cache_path": ""},
            ) as targeted_sync:
                exit_code = sync_reading_feeds.run_sync(
                    data_path=str(reading_data_path),
                    mode="fulltext_request",
                    article_id="reading-entry-2",
                    max_articles=5,
                )
        self.assertEqual(exit_code, 0)
        targeted_sync.assert_called_once_with(article_id="reading-entry-2", max_articles=1)

    def test_targeted_fulltext_sync_extracts_only_requested_article_and_writes_cache(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reading_data_path = root / "reading_data.json"
            cache_dir = root / "fulltext-cache"
            reading_data_path.write_text(json.dumps(self._payload(), ensure_ascii=False, indent=2), encoding="utf-8")

            extraction_calls = []

            def fake_extract(url, timeout_seconds=None):
                extraction_calls.append((url, timeout_seconds))
                return {
                    "status": "ok",
                    "content_html": "<p>Targeted full article body.</p>",
                    "content_text": "Targeted full article body.",
                    "excerpt": "Targeted full article body.",
                    "error": "",
                }

            patches = self._patched_runtime(reading_data_path, cache_dir)
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patch.object(
                dragon_app,
                "extract_reading_article_page",
                side_effect=fake_extract,
            ):
                result = dragon_app.sync_reading_fulltext_request(article_id="reading-entry-2", max_articles=1)
                request_record = dragon_app.reading_article_fulltext_request_load("reading-entry-2")

            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "cached")
            self.assertEqual(len(extraction_calls), 1)
            self.assertEqual(extraction_calls[0][0], "https://example.com/articles/2")
            cache_files = list(cache_dir.rglob("*.json"))
            self.assertEqual(len(cache_files), 2)
            cache_payloads = [json.loads(path.read_text(encoding="utf-8")) for path in cache_files]
            fulltext_payload = next(payload for payload in cache_payloads if payload.get("url"))
            self.assertEqual(fulltext_payload.get("url"), "https://example.com/articles/2")
            self.assertEqual(fulltext_payload.get("content_text"), "Targeted full article body.")
            saved_payload = json.loads(reading_data_path.read_text(encoding="utf-8"))
            self.assertEqual(
                sum(
                    1
                    for item in (saved_payload.get("entries", []) or [])
                    if str(item.get("content_html", "") or "").strip() or str(item.get("content_text", "") or "").strip()
                ),
                0,
            )
            self.assertEqual(request_record.get("status"), "cached")
            self.assertEqual(request_record.get("dispatch_status"), "cache_saved")

    def test_targeted_fulltext_sync_failed_extraction_records_safe_failure(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reading_data_path = root / "reading_data.json"
            cache_dir = root / "fulltext-cache"
            reading_data_path.write_text(json.dumps(self._payload(), ensure_ascii=False, indent=2), encoding="utf-8")

            patches = self._patched_runtime(reading_data_path, cache_dir)
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patch.object(
                dragon_app,
                "extract_reading_article_page",
                return_value={
                    "status": "failed",
                    "content_html": "",
                    "content_text": "",
                    "excerpt": "",
                    "error": "ProxyError: token leaked from C:\\private\\path",
                },
            ):
                result = dragon_app.sync_reading_fulltext_request(article_id="reading-entry-1", max_articles=1)
                request_record = dragon_app.reading_article_fulltext_request_load("reading-entry-1")

            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "failed")
            self.assertEqual(request_record.get("status"), "failed")
            self.assertEqual(request_record.get("dispatch_status"), "extract_failed")
            self.assertNotIn("ProxyError", request_record.get("safe_error", ""))
            self.assertEqual(len(list(cache_dir.rglob("*.json"))), 1)

    def test_refresh_deployed_reading_fulltext_from_github_downloads_targeted_cache(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reading_data_path = root / "reading_data.json"
            cache_dir = root / "fulltext-cache"
            reading_data_path.write_text(json.dumps(self._payload(), ensure_ascii=False, indent=2), encoding="utf-8")
            patches = self._patched_runtime(reading_data_path, cache_dir)
            response_mock = Mock(status_code=200)
            response_mock.json.return_value = {
                "url": "https://example.com/articles/1",
                "title": "First Article",
                "source": "Example Source",
                "fetched_at": "2026-06-06T00:00:00+00:00",
                "status": "ok",
                "content_text": "Downloaded cached body.",
                "content_html": "<p>Downloaded cached body.</p>",
                "excerpt": "Downloaded cached body.",
                "word_count": 3,
                "error": "",
            }
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patch.object(
                dragon_app.requests,
                "get",
                return_value=response_mock,
            ):
                result = dragon_app.refresh_deployed_reading_fulltext_from_github("reading-entry-1")
                request_record = dragon_app.reading_article_fulltext_request_load("reading-entry-1")
                cache_record = dragon_app.reading_article_fulltext_load("https://example.com/articles/1")

            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "cached")
            self.assertEqual(cache_record.get("content_text"), "Downloaded cached body.")
            self.assertEqual(request_record.get("status"), "cached")
            self.assertEqual(request_record.get("dispatch_status"), "remote_cache_downloaded")


if __name__ == "__main__":
    unittest.main()
