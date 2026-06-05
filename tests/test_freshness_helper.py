import unittest

from domains.shared.refresh import build_freshness, sanitize_freshness_error


class FreshnessHelperTests(unittest.TestCase):
    def test_fresh_when_last_refreshed_is_recent(self):
        freshness = build_freshness(
            last_refreshed_at="2026-06-05T11:59:00+00:00",
            now="2026-06-05T12:00:00+00:00",
            ttl_seconds=3600,
            source_label="PocketTube snapshot",
        ).to_dict()

        self.assertEqual(freshness["state"], "fresh")
        self.assertFalse(freshness["is_stale"])
        self.assertEqual(freshness["age_seconds"], 60)

    def test_stale_when_older_than_ttl(self):
        freshness = build_freshness(
            last_refreshed_at="2026-06-05T08:00:00+00:00",
            now="2026-06-05T12:00:00+00:00",
            ttl_seconds=3600,
            source_label="PocketTube snapshot",
        ).to_dict()

        self.assertEqual(freshness["state"], "stale")
        self.assertTrue(freshness["is_stale"])

    def test_failed_with_safe_error(self):
        freshness = build_freshness(
            last_refreshed_at="2026-06-05T11:00:00+00:00",
            now="2026-06-05T12:00:00+00:00",
            ttl_seconds=3600,
            source_label="PocketTube snapshot",
            error="workflow failed",
        ).to_dict()

        self.assertEqual(freshness["state"], "failed")
        self.assertEqual(freshness["safe_error"], "workflow failed")
        self.assertTrue(freshness["is_stale"])

    def test_disabled_when_refresh_is_unavailable(self):
        freshness = build_freshness(
            last_refreshed_at="2026-06-05T11:00:00+00:00",
            now="2026-06-05T12:00:00+00:00",
            ttl_seconds=3600,
            source_label="PocketTube snapshot",
            refresh_available=False,
        ).to_dict()

        self.assertEqual(freshness["state"], "disabled")
        self.assertFalse(freshness["refresh_available"])
        self.assertEqual(freshness["next_action"], "none")

    def test_unknown_when_no_timestamp_exists(self):
        freshness = build_freshness(
            last_refreshed_at="",
            now="2026-06-05T12:00:00+00:00",
            ttl_seconds=3600,
            source_label="PocketTube snapshot",
        ).to_dict()

        self.assertEqual(freshness["state"], "unknown")
        self.assertTrue(freshness["is_stale"])
        self.assertIsNone(freshness["age_seconds"])

    def test_sanitize_freshness_error_hides_sensitive_details(self):
        safe_error = sanitize_freshness_error("Traceback: token=abc proxy failure at C:\\secret\\file.py")

        self.assertEqual(safe_error, "Refresh failed. Try again later.")


if __name__ == "__main__":
    unittest.main()
