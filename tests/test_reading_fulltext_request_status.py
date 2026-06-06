import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import app as dragon_app
import requests


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

    def test_post_request_creates_local_queued_record_when_requests_enabled(self):
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
            ), patch.object(
                dragon_app,
                "extract_reading_article_page",
                side_effect=AssertionError("Fulltext request POST should not extract article content in local queue mode"),
            ):
                response = self.client.post("/reading/article/reading-entry-1/request-fulltext")
                request_record = dragon_app.reading_article_fulltext_request_load("reading-entry-1")

            self.assertEqual(response.status_code, 202)
            payload = response.get_json()
            self.assertEqual(payload["status"], "queued")
            self.assertFalse(payload["can_request"])
            self.assertIsInstance(request_record, dict)
            self.assertEqual(request_record.get("status"), "queued")
            self.assertEqual(request_record.get("dispatch_mode"), "disabled")
            self.assertEqual(request_record.get("dispatch_status"), "queued_local_only")
            self.assertEqual(request_record.get("article_id"), "reading-entry-1")
            self.assertEqual(request_record.get("source_url"), "https://example.com/articles/1")

    def test_post_request_dispatches_safely_when_github_action_mode_is_mocked(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reading_data_path = root / "reading_data.json"
            cache_dir = root / "fulltext-cache"
            reading_data_path.write_text(json.dumps(self._payload(), ensure_ascii=False, indent=2), encoding="utf-8")
            response_mock = Mock(status_code=204, text="")
            patches = self._patched_runtime(reading_data_path, cache_dir)
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patch.object(
                dragon_app,
                "DRAGON_READING_FULLTEXT_REQUESTS_ENABLED",
                True,
            ), patch.object(
                dragon_app,
                "DRAGON_READING_FULLTEXT_DISPATCH_MODE",
                "github_action",
            ), patch.object(
                dragon_app,
                "GITHUB_ACTIONS_TOKEN",
                "test-token",
            ), patch.object(
                dragon_app.requests,
                "post",
                return_value=response_mock,
            ) as post_mock:
                response = self.client.post("/reading/article/reading-entry-1/request-fulltext")
                request_record = dragon_app.reading_article_fulltext_request_load("reading-entry-1")

            self.assertEqual(response.status_code, 202)
            payload = response.get_json()
            self.assertEqual(payload["status"], "queued")
            self.assertEqual(request_record.get("dispatch_mode"), "github_action")
            self.assertEqual(request_record.get("dispatch_status"), "workflow_dispatch_accepted")
            self.assertEqual(request_record.get("attempts"), 1)
            post_mock.assert_called_once()
            dispatch_payload = post_mock.call_args.kwargs["json"]
            self.assertEqual(dispatch_payload["ref"], dragon_app.DRAGON_READING_FULLTEXT_GITHUB_BRANCH)
            self.assertEqual(dispatch_payload["inputs"]["article_id"], "reading-entry-1")
            self.assertEqual(dispatch_payload["inputs"]["mode"], "fulltext_request")
            self.assertEqual(dispatch_payload["inputs"]["max_articles"], "1")

    def test_dispatch_failure_returns_sanitized_failed_status(self):
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
                "github_action",
            ), patch.object(
                dragon_app,
                "GITHUB_ACTIONS_TOKEN",
                "test-token",
            ), patch.object(
                dragon_app.requests,
                "post",
                side_effect=requests.RequestException("ProxyError: token leaked from C:\\private\\path"),
            ):
                response = self.client.post("/reading/article/reading-entry-1/request-fulltext")
                request_record = dragon_app.reading_article_fulltext_request_load("reading-entry-1")

            self.assertEqual(response.status_code, 502)
            payload = response.get_json()
            self.assertEqual(payload["status"], "failed")
            self.assertNotIn("ProxyError", payload["display_message"])
            self.assertNotIn("token", payload["display_message"].lower())
            self.assertIsInstance(request_record, dict)
            self.assertEqual(request_record.get("status"), "failed")
            self.assertEqual(request_record.get("dispatch_status"), "dispatch_failed")
            self.assertNotIn("ProxyError", request_record.get("safe_error", ""))

    def test_get_status_returns_request_store_states(self):
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
                dragon_app.reading_article_fulltext_request_save(
                    "reading-entry-1",
                    {
                        "article_id": "reading-entry-1",
                        "status": "running",
                        "requested_at": "2026-06-05T01:00:00+00:00",
                        "updated_at": "2026-06-05T01:05:00+00:00",
                        "last_dispatch_at": "",
                        "dispatch_mode": "disabled",
                        "dispatch_status": "running_local",
                        "safe_error": "",
                        "attempts": 1,
                        "source_url": "https://example.com/articles/1",
                        "title": "Example Article",
                        "source": "Example Source",
                    },
                )
                response = self.client.get("/reading/article/reading-entry-1/fulltext-status")

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertEqual(payload["status"], "running")
            self.assertFalse(payload["can_request"])

    def test_cached_status_wins_over_request_record(self):
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
                dragon_app.reading_article_fulltext_request_save(
                    "reading-entry-1",
                    {
                        "article_id": "reading-entry-1",
                        "status": "failed",
                        "requested_at": "2026-06-05T01:00:00+00:00",
                        "updated_at": "2026-06-05T01:05:00+00:00",
                        "last_dispatch_at": "2026-06-05T01:01:00+00:00",
                        "dispatch_mode": "github_action",
                        "dispatch_status": "dispatch_failed",
                        "safe_error": "Could not queue full article cache request right now.",
                        "attempts": 1,
                        "source_url": "https://example.com/articles/1",
                        "title": "Example Article",
                        "source": "Example Source",
                    },
                )
                response = self.client.get("/reading/article/reading-entry-1/fulltext-status")

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertEqual(payload["status"], "cached")


if __name__ == "__main__":
    unittest.main()
