import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import app as dragon_app
import scripts.sync_reading_feeds as sync_reading_feeds


class _FakeFeedResponse:
    def __init__(self, url, content, status_code=200, content_type="application/rss+xml; charset=utf-8"):
        self.url = url
        self.content = content
        self.status_code = status_code
        self.headers = {"Content-Type": content_type}


class ReadingRssSyncProfileTests(unittest.TestCase):
    def test_sync_honors_browser_ua_request_profile_after_registry_repair(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reading_data_path = root / "reading_data.json"
            registry_path = root / "reading_sources.json"
            reading_payload = {
                "version": 1,
                "sources": [
                    {
                        "id": "reading-src-profiled",
                        "name": "Profiled Source",
                        "url": "https://example.com/old-feed",
                        "category": "news",
                        "active": False,
                        "disabled_reason": "HTTP 403 from GitHub Actions",
                    }
                ],
                "entries": [],
            }
            registry_payload = [
                {
                    "source_id": "reading-src-profiled",
                    "name": "Profiled Source",
                    "url": "https://example.com/new-feed",
                    "category": "news",
                    "active": True,
                    "request_profile": "browser_ua",
                    "disabled_reason": "",
                    "repair_reason": "Verified by diagnose_reading_sources.py with profile=browser_ua status=200 count=1",
                    "repaired_at": "2026-06-04T12:00:00+00:00",
                    "replacement_of": "https://example.com/old-feed",
                    "last_repair_status": "verified",
                }
            ]
            reading_data_path.write_text(json.dumps(reading_payload, ensure_ascii=False, indent=2), encoding="utf-8")
            registry_path.write_text(json.dumps(registry_payload, ensure_ascii=False, indent=2), encoding="utf-8")

            request_log = []
            feed_xml = b"""<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<rss version=\"2.0\">
  <channel>
    <title>Profiled Source</title>
    <item>
      <title>Imported by browser profile</title>
      <link>https://example.com/article-1</link>
      <guid>article-1</guid>
      <pubDate>Thu, 04 Jun 2026 12:00:00 GMT</pubDate>
    </item>
  </channel>
</rss>
"""

            def fake_get(url, timeout=20, allow_redirects=True, headers=None):
                request_log.append({
                    "url": url,
                    "timeout": timeout,
                    "headers": dict(headers or {}),
                })
                return _FakeFeedResponse(url, feed_xml)

            with patch.object(sync_reading_feeds.dragon_app, "READING_SOURCES_REGISTRY_PATH", registry_path), patch.object(
                dragon_app,
                "READING_SOURCES_REGISTRY_PATH",
                registry_path,
            ), patch.object(
                dragon_app,
                "READING_DATA_PATH",
                reading_data_path,
            ), patch.object(
                dragon_app,
                "_READING_CACHE_ACCESS",
                None,
            ), patch.object(
                dragon_app,
                "_READING_RSS_SERVICE",
                None,
            ), patch.object(
                dragon_app,
                "_READING_SYNC_SERVICE",
                None,
            ), patch.object(
                dragon_app.READING_HTTP_SESSION,
                "get",
                side_effect=fake_get,
            ):
                exit_code = sync_reading_feeds.run_sync(data_path=str(reading_data_path))

            self.assertEqual(exit_code, 0)
            self.assertEqual(len(request_log), 1)
            self.assertEqual(request_log[0]["url"], "https://example.com/new-feed")
            self.assertEqual(request_log[0]["headers"].get("User-Agent"), dragon_app.READING_BROWSER_USER_AGENT)
            self.assertIn("application/rss+xml", str(request_log[0]["headers"].get("Accept", "") or ""))
            self.assertEqual(request_log[0]["headers"].get("Sec-Fetch-Mode"), "navigate")

            saved_payload = json.loads(reading_data_path.read_text(encoding="utf-8"))
            self.assertEqual(len(saved_payload.get("entries", []) or []), 1)
            self.assertEqual(
                sum(
                    1
                    for entry in (saved_payload.get("entries", []) or [])
                    if str(entry.get("content_html", "") or "").strip() or str(entry.get("content_text", "") or "").strip()
                ),
                0,
            )
            saved_source = saved_payload["sources"][0]
            self.assertTrue(bool(saved_source.get("active")))
            self.assertEqual(saved_source.get("request_profile"), "browser_ua")
            self.assertEqual(saved_source.get("last_sync_status"), "ok")
            self.assertEqual(saved_source.get("last_sync_imported_count"), 1)
            self.assertEqual(dragon_app.reading_source_health(saved_source, known_entries=1), "healthy")
            self.assertFalse(dragon_app.reading_source_is_blocked(saved_source))


if __name__ == "__main__":
    unittest.main()
