import json
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

import app as dragon_app


class IOSArticleDetailApiTests(unittest.TestCase):
    def setUp(self):
        dragon_app.app.config["TESTING"] = True
        dragon_app.app.config["SESSION_COOKIE_SECURE"] = False
        dragon_app.DRAGON_ADMIN_USERNAME = ""
        dragon_app.DRAGON_ADMIN_PASSWORD = ""
        dragon_app.DRAGON_PROTECT_WHOLE_SITE = False
        self.client = dragon_app.app.test_client()

    def _reading_payload(self):
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
                    "url": "https://example.com/articles/1",
                    "original_url": "https://example.com/articles/1",
                    "published_at": "2026-06-05T00:00:00+00:00",
                    "saved_at": "2026-06-05T00:05:00+00:00",
                    "added_at": "2026-06-05T00:05:00+00:00",
                    "status": "reading",
                    "topic": "News",
                    "excerpt": "Short excerpt",
                    "image_url": "https://example.com/image.jpg",
                    "lead_image_url": "https://example.com/lead.jpg",
                }
            ],
        }

    def _runtime_patches(self, reading_data_path, cache_dir):
        return (
            patch("domains.api.v1.READING_DATA_PATH", reading_data_path),
            patch.object(dragon_app, "READING_FULLTEXT_CACHE_DIR", cache_dir),
            patch.object(dragon_app, "DRAGON_ALLOW_LIVE_ARTICLE_EXTRACTION", False),
            patch.object(dragon_app, "DRAGON_READING_FULLTEXT_REQUESTS_ENABLED", False),
            patch.object(dragon_app, "DRAGON_READING_FULLTEXT_DISPATCH_MODE", "disabled"),
        )

    def test_article_detail_returns_metadata_and_empty_content_without_cache(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reading_data_path = root / "reading_data.json"
            cache_dir = root / "fulltext-cache"
            reading_data_path.write_text(json.dumps(self._reading_payload(), ensure_ascii=False, indent=2), encoding="utf-8")

            patches = self._runtime_patches(reading_data_path, cache_dir)
            with ExitStack() as stack:
                for item in patches:
                    stack.enter_context(item)
                stack.enter_context(
                    patch.object(
                        dragon_app,
                        "extract_reading_article_page",
                        side_effect=AssertionError("Detail API must not trigger live extraction"),
                    )
                )
                stack.enter_context(
                    patch.object(
                        dragon_app,
                        "reading_article_fulltext_fetch",
                        side_effect=AssertionError("Detail API must not load fulltext live"),
                    )
                )
                response = self.client.get("/api/v1/articles/reading-entry-1")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["api_version"], "v1")
        item = payload["item"]
        self.assertEqual(
            set(item.keys()),
            {
                "id",
                "title",
                "source",
                "url",
                "published_at",
                "saved_at",
                "excerpt",
                "image",
                "thumbnail",
                "status",
                "read_state",
                "fulltext_status",
                "content_text",
                "content_html",
            },
        )
        self.assertEqual(item["id"], "reading-entry-1")
        self.assertEqual(item["title"], "Example Article")
        self.assertEqual(item["source"], "Example Source")
        self.assertEqual(item["url"], "https://example.com/articles/1")
        self.assertEqual(item["published_at"], "2026-06-05T00:00:00+00:00")
        self.assertEqual(item["saved_at"], "2026-06-05T00:05:00+00:00")
        self.assertEqual(item["excerpt"], "Short excerpt")
        self.assertEqual(item["image"], "https://example.com/lead.jpg")
        self.assertEqual(item["thumbnail"], "https://example.com/lead.jpg")
        self.assertEqual(item["status"], "reading")
        self.assertEqual(item["read_state"], "read")
        self.assertIsInstance(item["fulltext_status"], dict)
        self.assertEqual(item["fulltext_status"]["status"], "disabled")
        self.assertEqual(item["content_text"], "")
        self.assertEqual(item["content_html"], "")

    def test_article_detail_returns_404_for_missing_article(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reading_data_path = root / "reading_data.json"
            cache_dir = root / "fulltext-cache"
            reading_data_path.write_text(json.dumps(self._reading_payload(), ensure_ascii=False, indent=2), encoding="utf-8")

            patches = self._runtime_patches(reading_data_path, cache_dir)
            with ExitStack() as stack:
                for item in patches:
                    stack.enter_context(item)
                response = self.client.get("/api/v1/articles/does-not-exist")

        self.assertEqual(response.status_code, 404)
        self.assertEqual(
            response.get_json(),
            {
                "ok": False,
                "api_version": "v1",
                "error": "Article not found.",
            },
        )

    def test_article_detail_returns_cached_fulltext_and_sanitized_html(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reading_data_path = root / "reading_data.json"
            cache_dir = root / "fulltext-cache"
            reading_data_path.write_text(json.dumps(self._reading_payload(), ensure_ascii=False, indent=2), encoding="utf-8")

            patches = self._runtime_patches(reading_data_path, cache_dir)
            with ExitStack() as stack:
                for item in patches:
                    stack.enter_context(item)
                dragon_app.reading_article_fulltext_save(
                    "https://example.com/articles/1",
                    {
                        "url": "https://example.com/articles/1",
                        "title": "Example Article",
                        "source": "Example Source",
                        "fetched_at": "2026-06-05T00:30:00+00:00",
                        "status": "ok",
                        "content_text": "Cached full article body.",
                        "content_html": "<script>alert(1)</script><p>Cached <strong>body</strong>.</p><img src=\"javascript:alert(1)\">",
                        "excerpt": "Cached full article body.",
                        "word_count": 4,
                        "error": "",
                    },
                )
                stack.enter_context(
                    patch.object(
                        dragon_app,
                        "extract_reading_article_page",
                        side_effect=AssertionError("Detail API must not trigger live extraction"),
                    )
                )
                response = self.client.get("/api/v1/articles/reading-entry-1")

        self.assertEqual(response.status_code, 200)
        item = response.get_json()["item"]
        self.assertEqual(item["fulltext_status"]["status"], "cached")
        self.assertEqual(item["content_text"], "Cached full article body.")
        self.assertIn("<strong>body</strong>", item["content_html"])
        self.assertIn("Cached", item["content_html"])
        self.assertNotIn("<script", item["content_html"].lower())
        self.assertNotIn("javascript:", item["content_html"].lower())

    def test_article_detail_does_not_expose_internal_or_sensitive_fields(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reading_data_path = root / "reading_data.json"
            cache_dir = root / "fulltext-cache"
            reading_data_path.write_text(json.dumps(self._reading_payload(), ensure_ascii=False, indent=2), encoding="utf-8")

            patches = self._runtime_patches(reading_data_path, cache_dir)
            with ExitStack() as stack:
                for item in patches:
                    stack.enter_context(item)
                dragon_app.reading_article_fulltext_request_save(
                    "reading-entry-1",
                    {
                        "article_id": "reading-entry-1",
                        "status": "failed",
                        "safe_error": "Traceback: token secret GITHUB_ACTIONS_TOKEN /tmp/private dispatch_status github_action",
                        "dispatch_mode": "github_action",
                        "dispatch_status": "remote_cache_downloaded",
                    },
                )
                response = self.client.get("/api/v1/articles/reading-entry-1")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        lowered = body.lower()
        self.assertNotIn("cache_path", lowered)
        self.assertNotIn("traceback", lowered)
        self.assertNotIn("token", lowered)
        self.assertNotIn("secret", lowered)
        self.assertNotIn("raw error", lowered)
        self.assertNotIn("github_actions_token", lowered)
        self.assertNotIn("dispatch_mode", lowered)
        self.assertNotIn("dispatch_status", lowered)
        self.assertNotIn("github_action", lowered)
        self.assertNotIn(str(root).lower(), lowered)

    def test_articles_list_still_omits_body_fields(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reading_data_path = root / "reading_data.json"
            cache_dir = root / "fulltext-cache"
            payload = self._reading_payload()
            payload["entries"][0]["content_text"] = "Should stay hidden"
            payload["entries"][0]["content_html"] = "<p>Should stay hidden</p>"
            reading_data_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

            patches = self._runtime_patches(reading_data_path, cache_dir)
            with ExitStack() as stack:
                for item in patches:
                    stack.enter_context(item)
                response = self.client.get("/api/v1/articles", query_string={"limit": 1})

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["count"], 1)
        body = response.get_data(as_text=True).lower()
        self.assertNotIn("content_text", body)
        self.assertNotIn("content_html", body)


if __name__ == "__main__":
    unittest.main()
