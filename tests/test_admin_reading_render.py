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
        self.assertIn("Inactive", html)
        self.assertIn("paused", html)
        self.assertIn(">Activate<", html)
        self.assertNotIn(">Deactivate<", html)


if __name__ == "__main__":
    unittest.main()
