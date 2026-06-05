import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import app as dragon_app


class ReadingRefreshRouteTests(unittest.TestCase):
    def setUp(self):
        dragon_app.app.config["TESTING"] = True
        self.client = dragon_app.app.test_client()

    def test_pull_latest_articles_route_still_calls_snapshot_pull(self):
        with patch.object(dragon_app, "pull_latest_articles_snapshot", return_value={"entry_count": 3, "source_count": 2}) as pull_mock:
            response = self.client.post(
                "/reading/pull-latest",
                data={"next": "/reading"},
                follow_redirects=False,
            )

        self.assertEqual(response.status_code, 302)
        self.assertIn("/reading?success=", response.headers.get("Location", ""))
        pull_mock.assert_called_once_with()

    def test_sync_and_pull_stay_separate_operations(self):
        with patch.object(dragon_app, "trigger_reading_github_actions_sync", return_value=({"status": "started"}, 200)) as sync_mock, patch.object(
            dragon_app,
            "pull_latest_articles_snapshot",
            side_effect=AssertionError("pull should not run during sync"),
        ):
            sync_response = self.client.post("/reading/trigger-sync")

        self.assertEqual(sync_response.status_code, 200)
        self.assertEqual(sync_response.get_json(), {"status": "started"})
        sync_mock.assert_called_once_with()

        with patch.object(dragon_app, "pull_latest_articles_snapshot", return_value={"entry_count": 1, "source_count": 1}) as pull_mock, patch.object(
            dragon_app,
            "trigger_reading_github_actions_sync",
            side_effect=AssertionError("sync should not run during pull"),
        ):
            pull_response = self.client.post(
                "/reading/pull-latest",
                data={"next": "/reading"},
                follow_redirects=False,
            )

        self.assertEqual(pull_response.status_code, 302)
        pull_mock.assert_called_once_with()

    def test_missing_snapshot_does_not_crash_reading_view(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            missing_path = Path(temp_dir) / "missing-reading-data.json"
            reading_backups_dir = Path(temp_dir) / "backups" / "reading"
            with patch.object(dragon_app, "READING_DATA_PATH", missing_path), patch.object(
                dragon_app, "READING_BACKUPS_DIR", reading_backups_dir
            ), patch.object(
                dragon_app, "_READING_CACHE_ACCESS", None
            ), patch.object(
                dragon_app, "_READING_RUNTIME_SERVICE", None
            ), patch.object(
                dragon_app, "_READING_RUNTIME_PROJECTION_SERVICE", None
            ), patch.object(
                dragon_app,
                "load_reading_data_cached",
                return_value=dragon_app.default_reading_data(),
            ):
                with dragon_app.app.test_request_context("/reading"):
                    view = dragon_app.build_reading_view()

        self.assertEqual(view["last_refreshed_at"], "")
        self.assertEqual(view["refresh_status"], "missing")
        self.assertEqual(view["refresh_error"], "")
        self.assertTrue(view["is_stale"])
        self.assertEqual(view["freshness"]["state"], "unknown")
        self.assertEqual(view["freshness"]["display_label"], "Unknown")
        self.assertFalse(view["snapshot_status"]["exists"])
        self.assertEqual(view["snapshot_status"]["backup_count"], 0)
        self.assertEqual(view["snapshot_freshness_state"], "missing")
        self.assertEqual(view["snapshot_freshness_label"], "Snapshot missing")


if __name__ == "__main__":
    unittest.main()
