import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import app as dragon_app


class ReadingPageSnapshotRuntimeTests(unittest.TestCase):
    def setUp(self):
        dragon_app.app.config["TESTING"] = True
        self.client = dragon_app.app.test_client()

    def _source(self, index, *, active=True, needs_replacement=False):
        blocked_names = {
            2: "MAP News English",
            5: "مجتمع – هوية بريس",
            6: "ربورتاج | جريدة الصباح",
            9: "كتاب الرأي – هوية بريس",
            11: "حوار | جريدة الصباح",
        }
        name = blocked_names.get(index, f"Working Source {index}")
        profile = "browser_ua" if name in {"MAP News English", "مجتمع – هوية بريس", "كتاب الرأي – هوية بريس"} else "default"
        source = {
            "id": f"reading-src-{index}",
            "source_id": f"reading-src-{index}",
            "name": name,
            "url": f"https://example.com/feed-{index}.xml",
            "primary_url": f"https://example.com/feed-{index}.xml",
            "category": "news" if index <= 4 else "culture" if index <= 8 else "opinion",
            "active": active,
            "request_profile": profile,
            "last_sync_status": "ok" if active else "blocked_source",
            "last_sync_status_code": 200 if active else 403,
            "last_synced_at": "2026-06-05T00:00:00+00:00" if active else "",
            "last_sync_message": "Imported items" if active else "Confirmed HTTP 403 from GitHub Actions even with request profile",
            "last_sync_imported_count": 4 if active else 0,
            "needs_replacement": needs_replacement,
            "disabled_reason": "Confirmed HTTP 403 from GitHub Actions even with request profile" if needs_replacement else "",
            "last_repair_status": "blocked_in_github_actions" if needs_replacement else "",
        }
        return source

    def _entry(self, index, source):
        return {
            "id": f"reading-entry-{index}",
            "source_id": source["id"],
            "source": source["name"],
            "title": f"Snapshot article {index}",
            "url": f"https://example.com/articles/{index}",
            "published_at": "2026-06-05T00:00:00+00:00",
            "added_at": "2026-06-05T00:00:00+00:00",
            "status": "unread",
            "topic": "News",
            "excerpt": f"Excerpt {index}",
        }

    def test_reading_route_uses_configured_snapshot_without_sync_or_extraction(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            reading_data_path = Path(temp_dir) / "reading_data.json"
            reading_backups_dir = Path(temp_dir) / "backups" / "reading"
            reading_backups_dir.mkdir(parents=True, exist_ok=True)
            (reading_backups_dir / "reading-data-20260605-000000-save.json").write_text("{}", encoding="utf-8")
            sources = [
                self._source(1, active=True),
                self._source(2, active=False, needs_replacement=True),
                self._source(3, active=True),
                self._source(4, active=True),
                self._source(5, active=False, needs_replacement=True),
                self._source(6, active=False, needs_replacement=True),
                self._source(7, active=True),
                self._source(8, active=True),
                self._source(9, active=False, needs_replacement=True),
                self._source(10, active=True),
                self._source(11, active=False, needs_replacement=True),
                self._source(12, active=False),
            ]
            entries = [self._entry(index, sources[(index - 1) % len(sources)]) for index in range(1, 89)]
            payload = {
                "version": 1,
                "last_sync_at": "2026-06-05T00:10:00+00:00",
                "last_sync_count": 88,
                "last_sync_sources": 6,
                "last_sync_message": "Imported 88 new items from 6 active source(s)",
                "sources": sources,
                "entries": entries,
            }
            reading_data_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

            with patch.object(dragon_app, "READING_DATA_PATH", reading_data_path), patch.object(
                dragon_app, "READING_BACKUPS_DIR", reading_backups_dir
            ), patch.object(
                dragon_app, "_READING_CACHE_ACCESS", None
            ), patch.object(
                dragon_app, "_READING_RUNTIME_SERVICE", None
            ), patch.object(
                dragon_app, "_READING_RUNTIME_PROJECTION_SERVICE", None
            ), patch.object(
                dragon_app, "sync_reading_sources", side_effect=AssertionError("RSS sync should not run during /reading")
            ), patch.object(
                dragon_app, "pull_latest_articles_snapshot", side_effect=AssertionError("Snapshot pull should not run during /reading")
            ), patch.object(
                dragon_app, "extract_reading_article_page", side_effect=AssertionError("Full article extraction should not run during /reading")
            ):
                with dragon_app.app.test_request_context("/reading"):
                    reading_view = dragon_app.build_reading_view()

                response = self.client.get("/reading")

            self.assertEqual(response.status_code, 200)
            html = response.get_data(as_text=True)
            self.assertEqual(reading_view["source_count"], 12)
            self.assertEqual(reading_view["active_source_count"], 6)
            self.assertEqual(reading_view["total_matching"], 88)
            self.assertEqual(reading_view["total_filtered"], 88)
            self.assertEqual(reading_view["summary"]["total"], 88)
            self.assertEqual(reading_view["refresh_status"], "idle")
            self.assertEqual(reading_view["refresh_error"], "")
            self.assertEqual(reading_view["refresh_now"]["key"], "refresh_now")
            self.assertTrue(reading_view["refresh_now"]["enabled"])
            self.assertEqual(reading_view["background_revalidate"]["key"], "background_revalidate")
            self.assertTrue(reading_view["background_revalidate"]["placeholder"])
            self.assertFalse(reading_view["is_stale"])
            self.assertEqual(reading_view["freshness"]["state"], "fresh")
            self.assertEqual(reading_view["freshness"]["display_label"], "Fresh")
            self.assertTrue(reading_view["snapshot_status"]["exists"])
            self.assertGreaterEqual(reading_view["snapshot_status"]["backup_count"], 1)
            self.assertTrue(reading_view["snapshot_status"]["restore_available"])
            self.assertIn("12 sources", html)
            self.assertIn("<strong>88</strong> currently rendered.", html)
            self.assertIn("Snapshot article 1", html)
            self.assertIn("MAP News English", html)
            self.assertIn("backup", html)
            self.assertTrue(all("content_html" not in entry for entry in reading_view["entries"]))
            self.assertTrue(all("content_text" not in entry for entry in reading_view["entries"]))


if __name__ == "__main__":
    unittest.main()
