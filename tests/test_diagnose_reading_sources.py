import unittest

import app as dragon_app
import scripts.diagnose_reading_sources as diagnose_reading_sources


class DiagnoseReadingSourcesTests(unittest.TestCase):
    def test_apply_verified_repairs_keeps_403_source_inactive_without_verified_candidate(self):
        registry_sources = [
            {
                "id": "reading-src-blocked",
                "name": "Blocked Source",
                "url": "https://example.com/feed",
                "category": "news",
                "active": False,
                "disabled_reason": "HTTP 403 from GitHub Actions",
            }
        ]
        reports = [
            {
                "source": dragon_app.normalize_reading_source(registry_sources[0]),
                "best_result": {
                    "verified": False,
                    "recommended_action": "keep_blocked",
                    "status_code": 403,
                    "profile": "default",
                    "normalized_article_count": 0,
                    "candidate_url": "https://example.com/feed",
                },
            }
        ]

        updated_sources, applied_repairs = diagnose_reading_sources.apply_verified_repairs(
            registry_sources,
            reports,
            repaired_at="2026-06-04T12:00:00+00:00",
        )

        self.assertEqual(applied_repairs, [])
        self.assertFalse(bool(updated_sources[0].get("active", True)))
        self.assertEqual(updated_sources[0].get("disabled_reason"), "HTTP 403 from GitHub Actions")
        self.assertEqual(updated_sources[0].get("url"), "https://example.com/feed")

    def test_apply_verified_repairs_updates_url_and_reactivates_source(self):
        registry_sources = [
            {
                "id": "reading-src-repair",
                "name": "Needs Repair",
                "url": "https://example.com/old-feed",
                "category": "opinion",
                "active": False,
                "disabled_reason": "HTTP 403 from GitHub Actions",
            }
        ]
        reports = [
            {
                "source": dragon_app.normalize_reading_source(registry_sources[0]),
                "best_result": {
                    "verified": True,
                    "recommended_action": "replace_url",
                    "status_code": 200,
                    "profile": "rss_accept",
                    "normalized_article_count": 7,
                    "candidate_url": "https://example.com/new-feed",
                },
            }
        ]

        updated_sources, applied_repairs = diagnose_reading_sources.apply_verified_repairs(
            registry_sources,
            reports,
            repaired_at="2026-06-04T12:00:00+00:00",
        )

        self.assertEqual(len(applied_repairs), 1)
        self.assertTrue(bool(updated_sources[0].get("active")))
        self.assertEqual(updated_sources[0].get("url"), "https://example.com/new-feed")
        self.assertEqual(updated_sources[0].get("primary_url"), "https://example.com/new-feed")
        self.assertEqual(updated_sources[0].get("replacement_of"), "https://example.com/old-feed")
        self.assertEqual(updated_sources[0].get("repaired_at"), "2026-06-04T12:00:00+00:00")
        self.assertIn("profile=rss_accept", str(updated_sources[0].get("repair_reason", "") or ""))
        self.assertNotIn("disabled_reason", updated_sources[0])

    def test_strip_content_fields_and_flags_remain_disabled(self):
        entries = [
            {
                "title": "Article",
                "url": "https://example.com/article",
                "content_html": "<p>Hello</p>",
                "content_text": "Hello",
                "excerpt": "Hello",
            }
        ]

        sanitized = diagnose_reading_sources.strip_content_fields(entries)

        self.assertEqual(len(sanitized), 1)
        self.assertNotIn("content_html", sanitized[0])
        self.assertNotIn("content_text", sanitized[0])
        self.assertFalse(bool(dragon_app.DRAGON_READING_SYNC_EXTRACT_FULL_CONTENT))
        self.assertFalse(bool(dragon_app.DRAGON_ALLOW_LIVE_ARTICLE_EXTRACTION))


if __name__ == "__main__":
    unittest.main()
