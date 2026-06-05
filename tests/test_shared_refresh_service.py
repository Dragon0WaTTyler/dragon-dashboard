import unittest

from domains.shared.refresh import RefreshResult, RefreshService


class RefreshServiceTests(unittest.TestCase):
    def setUp(self):
        self.service = RefreshService(
            format_timestamp_label=lambda value, default="Unknown": value or default
        )

    def test_detect_stale_respects_fresh_aging_and_stale_thresholds(self):
        fresh = self.service.detect_stale(last_refreshed_at="2026-06-05T00:00:00+00:00", age_seconds=60)
        aging = self.service.detect_stale(last_refreshed_at="2026-06-05T00:00:00+00:00", age_seconds=7 * 60 * 60)
        stale = self.service.detect_stale(last_refreshed_at="2026-06-05T00:00:00+00:00", age_seconds=25 * 60 * 60)

        self.assertEqual(fresh.state, "fresh")
        self.assertFalse(fresh.is_stale)
        self.assertEqual(aging.state, "aging")
        self.assertFalse(aging.is_stale)
        self.assertEqual(stale.state, "stale")
        self.assertTrue(stale.is_stale)

    def test_apply_result_transitions_missing_state_to_updated_then_failed(self):
        initial = self.service.build_state(missing=True, refresh_status="missing")

        updated = self.service.apply_result(
            initial,
            RefreshResult(
                ok=True,
                status="updated",
                last_refreshed_at="2026-06-05T00:00:00+00:00",
            ),
        )
        failed = self.service.apply_result(
            updated,
            RefreshResult(
                ok=False,
                status="failed",
                last_refreshed_at="2026-06-05T00:00:00+00:00",
                refresh_error="network timeout",
            ),
        )

        self.assertEqual(initial.refresh_status, "missing")
        self.assertEqual(updated.refresh_status, "updated")
        self.assertEqual(updated.last_refreshed_at, "2026-06-05T00:00:00+00:00")
        self.assertEqual(updated.stale.state, "fresh")
        self.assertEqual(failed.refresh_status, "failed")
        self.assertEqual(failed.refresh_error, "network timeout")
        self.assertEqual(failed.last_refreshed_at, "2026-06-05T00:00:00+00:00")
        self.assertEqual(failed.background_revalidate.key, "background_revalidate")
        self.assertTrue(failed.background_revalidate.placeholder)


if __name__ == "__main__":
    unittest.main()
