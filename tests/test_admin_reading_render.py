import unittest
from unittest.mock import patch

import app as dragon_app


class AdminReadingRenderTests(unittest.TestCase):
    def setUp(self):
        dragon_app.app.config["TESTING"] = True
        self.client = dragon_app.app.test_client()
        with self.client.session_transaction() as session:
            session["dragon_authenticated"] = True

    def test_admin_reading_renders_inactive_blocked_source_as_activate(self):
        reading_payload = {
            "version": 1,
            "sources": [
                {
                    "id": "reading-src-blocked",
                    "name": "Blocked Source",
                    "url": "https://example.com/feed",
                    "category": "news",
                    "active": False,
                    "last_sync_status": "blocked_source",
                    "last_sync_status_code": 403,
                    "last_sync_message": "HTTP 403 from source",
                    "last_sync_error": "HTTP 403",
                }
            ],
            "entries": [],
        }

        with patch.object(dragon_app, "load_reading_data", return_value=reading_payload), patch.object(
            dragon_app,
            "load_admin_data",
            return_value={},
        ), patch.object(
            dragon_app,
            "build_combined_sections",
            return_value=[],
        ), patch.object(
            dragon_app,
            "build_admin_table_rows",
            return_value=[],
        ):
            response = self.client.get("/admin/reading")

        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("Blocked Source", html)
        self.assertIn("INACTIVE", html)
        self.assertIn(">Activate<", html)
        self.assertNotIn(">Deactivate<", html)

    def test_admin_reading_shows_working_and_blocked_source_diagnostics_without_retry_noise(self):
        reading_payload = {
            "version": 1,
            "last_sync_at": "2026-06-05T00:10:00+00:00",
            "last_sync_message": "Imported 88 new items from 6 active source(s)",
            "sources": [
                {
                    "id": "reading-src-healthy",
                    "name": "Healthy Source",
                    "url": "https://example.com/healthy-feed",
                    "category": "news",
                    "active": True,
                    "last_synced_at": "2026-06-05T00:10:00+00:00",
                    "last_sync_status": "ok",
                    "last_sync_status_code": 200,
                    "last_sync_message": "Imported 14 new items",
                    "last_sync_imported_count": 14,
                    "last_sync_raw_count": 14,
                    "last_sync_normalized_count": 14,
                },
                {
                    "id": "reading-src-blocked",
                    "name": "Blocked Source",
                    "url": "https://example.com/blocked-feed",
                    "category": "opinion",
                    "active": False,
                    "needs_replacement": True,
                    "disabled_reason": "Confirmed HTTP 403 from GitHub Actions even with request profile",
                    "last_repair_status": "blocked_in_github_actions",
                    "last_sync_status": "blocked_source",
                    "last_sync_status_code": 403,
                    "last_sync_message": "Confirmed HTTP 403 from GitHub Actions even with request profile",
                    "last_sync_error": "HTTP 403",
                },
            ],
            "entries": [
                {
                    "id": "reading-entry-1",
                    "source": "Healthy Source",
                    "source_id": "reading-src-healthy",
                    "title": "Healthy article",
                    "url": "https://example.com/article-1",
                    "published_at": "2026-06-05T00:00:00+00:00",
                    "added_at": "2026-06-05T00:00:00+00:00",
                    "status": "unread",
                    "topic": "News",
                }
            ],
        }

        with patch.object(dragon_app, "IS_PRODUCTION", True), patch.object(
            dragon_app,
            "load_reading_data",
            return_value=reading_payload,
        ), patch.object(
            dragon_app,
            "load_admin_data",
            return_value={},
        ), patch.object(
            dragon_app,
            "build_combined_sections",
            return_value=[],
        ), patch.object(
            dragon_app,
            "build_admin_table_rows",
            return_value=[],
        ):
            response = self.client.get("/admin/reading")

        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("2 RSS sources", html)
        self.assertIn("1 active", html)
        self.assertIn("1 inactive", html)
        self.assertIn("Healthy Source", html)
        self.assertIn("Blocked Source", html)
        self.assertIn("Retry disabled online", html)
        self.assertIn("INACTIVE", html)
        self.assertIn("Healthy", html)


if __name__ == "__main__":
    unittest.main()
