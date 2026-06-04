import json
import shutil
import tempfile
import threading
import unittest
from pathlib import Path

import app as dragon_app
from domains.reading.data.snapshot_access import ReadingSnapshotAccess


class _DummyResponse:
    def __init__(self, payload_bytes, status_code=200):
        self.payload_bytes = payload_bytes
        self.status_code = status_code

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def raise_for_status(self):
        if self.status_code >= 400:
            raise dragon_app.requests.HTTPError(f"HTTP {self.status_code}")

    def iter_content(self, chunk_size=1024 * 1024):
        for start in range(0, len(self.payload_bytes), chunk_size):
            yield self.payload_bytes[start:start + chunk_size]


class _DummySession:
    def __init__(self, payload_bytes, status_code=200):
        self.payload_bytes = payload_bytes
        self.status_code = status_code

    def get(self, url, timeout=30, stream=True):
        return _DummyResponse(self.payload_bytes, status_code=self.status_code)


class ReadingSnapshotAccessTests(unittest.TestCase):
    def test_pull_latest_articles_snapshot_preserves_local_sources_and_merges_new_remote_sources(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reading_data_path = root / "reading_data.json"
            local_sources = [
                {"name": "Local Source 1", "url": "https://example.com/feed-1", "active": True, "category": "news"},
                {"name": "Local Source 2", "url": "https://example.com/feed-2", "active": True, "category": "news"},
                {"name": "Local Source 3", "url": "https://example.com/feed-3", "active": True, "category": "news"},
                {"name": "Local Source 4", "url": "https://example.com/feed-4", "active": True, "category": "news"},
                {"name": "Local Source 5", "url": "https://example.com/feed-5", "active": True, "category": "news"},
                {"name": "Local Source 6", "url": "https://example.com/feed-6", "active": True, "category": "news"},
                {"name": "Local Source 7", "url": "https://example.com/feed-7", "active": True, "category": "news"},
                {"name": "Local Source 8", "url": "https://example.com/feed-8", "active": True, "category": "news"},
                {"name": "Al Jazeera", "url": "https://www.aljazeera.com/xml/rss/all.xml", "active": True, "category": "news", "status": "trusted"},
                {"name": "Hespress", "url": "https://www.hespress.com/feed", "active": True, "category": "news", "status": "trusted"},
                {"name": "Local Source 11", "url": "https://example.com/feed-11", "active": False, "category": "science"},
                {"name": "Local Source 12", "url": "https://example.com/feed-12", "active": True, "category": "culture"},
            ]
            reading_data_path.write_text(json.dumps({"version": 1, "sources": local_sources, "entries": []}), encoding="utf-8")

            remote_payload = {
                "version": 1,
                "last_sync_at": "2026-06-04T00:00:00+00:00",
                "sources": [
                    {"name": "Al Jazeera Mirror", "url": "https://www.aljazeera.com/xml/rss/all.xml"},
                    {"name": "Hespress", "url": "https://www.hespress.com/feed"},
                    {"name": "Remote New Source", "url": "https://remote.example.com/feed"},
                ],
                "entries": [
                    {
                        "source": "Al Jazeera Mirror",
                        "source_id": "remote-source-a",
                        "title": "Article One",
                        "url": "https://example.com/article-1",
                        "published_at": "2026-06-04T00:00:00+00:00",
                        "added_at": "2026-06-04T00:00:00+00:00",
                        "status": "unread",
                        "topic": "News",
                        "excerpt": "Short summary",
                        "content_html": "<p>hello</p>",
                        "content_text": "hello",
                    },
                    {
                        "source": "Hespress",
                        "source_id": "remote-source-b",
                        "title": "Article Two",
                        "url": "https://example.com/article-2",
                        "published_at": "2026-06-04T00:10:00+00:00",
                        "added_at": "2026-06-04T00:10:00+00:00",
                        "status": "unread",
                        "topic": "News",
                    },
                    {
                        "source": "Remote New Source",
                        "source_id": "remote-source-c",
                        "title": "Article Three",
                        "url": "https://example.com/article-3",
                        "published_at": "2026-06-04T00:20:00+00:00",
                        "added_at": "2026-06-04T00:20:00+00:00",
                        "status": "unread",
                        "topic": "News",
                    },
                ],
            }
            remote_bytes = json.dumps(remote_payload).encode("utf-8")
            if len(remote_bytes) < 2048:
                remote_payload["padding"] = "x" * (2048 - len(remote_bytes))
                remote_bytes = json.dumps(remote_payload).encode("utf-8")

            backup_path = root / "reading-data-remote-pull.json"
            runtime = type(
                "Runtime",
                (),
                {
                    "github_refresh_lock": threading.Lock(),
                    "data_cache_lock": threading.Lock(),
                    "data_cache": {"fingerprint": None, "data": None},
                },
            )()

            access = ReadingSnapshotAccess(
                app_logger=dragon_app.app.logger,
                reading_runtime=runtime,
                reading_data_path=reading_data_path,
                base_dir=root,
                temp_file_factory=tempfile.NamedTemporaryFile,
                path_class=Path,
                requests_module=dragon_app.requests,
                reading_http_session=_DummySession(remote_bytes),
                reading_snapshot_url="https://example.com/reading_data.json",
                reading_snapshot_pull_enabled=True,
                validate_snapshot_payload=dragon_app._reading_snapshot_payload_is_valid,
                normalize_reading_data=dragon_app.normalize_reading_data,
                build_lightweight_snapshot=dragon_app.build_lightweight_articles_snapshot,
                backup_reading_data_file=lambda reason="save": (shutil.copy2(reading_data_path, backup_path) or str(backup_path)),
                rotate_webhook_backup=lambda: "",
                clear_reading_data_cache=lambda: None,
                reading_data_cache_fingerprint=lambda: ("fingerprint", reading_data_path.stat().st_size),
                reading_format_mtime=lambda path: "mtime",
                monotonic=lambda: 0.0,
            )

            result = access.pull_latest_articles_snapshot()

            saved_payload = json.loads(reading_data_path.read_text(encoding="utf-8"))
            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "updated")
            self.assertEqual(result["entry_count"], 3)
            self.assertEqual(result["local_source_count"], 12)
            self.assertEqual(result["remote_source_count"], 3)
            self.assertEqual(result["merged_source_count"], 13)
            self.assertEqual(result["source_count"], 13)
            self.assertEqual(result["with_content_count"], 0)
            self.assertGreater(int(result["downloaded_bytes"]), 0)
            self.assertGreater(int(result["written_bytes"]), 0)
            self.assertTrue(result["updated_at"])
            self.assertTrue(backup_path.exists())
            self.assertEqual(len(saved_payload.get("entries", []) or []), 3)
            self.assertGreaterEqual(len(saved_payload.get("sources", []) or []), 12)
            self.assertEqual(len(saved_payload.get("sources", []) or []), 13)
            self.assertTrue(saved_payload.get("snapshot_updated_at"))
            self.assertTrue(all("content_html" not in entry for entry in saved_payload.get("entries", [])))
            self.assertTrue(all("content_text" not in entry for entry in saved_payload.get("entries", [])))
            self.assertEqual(sum(1 for entry in saved_payload.get("entries", []) if entry.get("content_html") or entry.get("content_text")), 0)
            saved_sources = list(saved_payload.get("sources", []) or [])
            saved_source_names = {str(source.get("name", "") or "").strip() for source in saved_sources if isinstance(source, dict)}
            self.assertIn("Al Jazeera", saved_source_names)
            self.assertIn("Hespress", saved_source_names)
            self.assertIn("Remote New Source", saved_source_names)
            self.assertNotIn("Al Jazeera Mirror", saved_source_names)
            al_jazeera_source = next(source for source in saved_sources if isinstance(source, dict) and source.get("name") == "Al Jazeera")
            self.assertEqual(al_jazeera_source.get("status"), "trusted")
            remote_entry_one = next(entry for entry in saved_payload.get("entries", []) if entry.get("title") == "Article One")
            self.assertEqual(remote_entry_one.get("source"), "Al Jazeera")
            self.assertTrue(str(remote_entry_one.get("source_id", "") or "").strip())
            self.assertIsInstance(runtime.data_cache.get("data"), dict)
            self.assertEqual(len((runtime.data_cache.get("data") or {}).get("entries", []) or []), 3)

    def test_pull_latest_articles_snapshot_rejects_tiny_invalid_snapshot(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reading_data_path = root / "reading_data.json"
            reading_data_path.write_text(json.dumps({"version": 1, "sources": [], "entries": []}), encoding="utf-8")
            tiny_payload = json.dumps({"version": 1, "sources": [], "entries": []}).encode("utf-8")
            runtime = type(
                "Runtime",
                (),
                {
                    "github_refresh_lock": threading.Lock(),
                    "data_cache_lock": threading.Lock(),
                    "data_cache": {"fingerprint": None, "data": None},
                },
            )()

            access = ReadingSnapshotAccess(
                app_logger=dragon_app.app.logger,
                reading_runtime=runtime,
                reading_data_path=reading_data_path,
                base_dir=root,
                temp_file_factory=tempfile.NamedTemporaryFile,
                path_class=Path,
                requests_module=dragon_app.requests,
                reading_http_session=_DummySession(tiny_payload),
                reading_snapshot_url="https://example.com/reading_data.json",
                reading_snapshot_pull_enabled=True,
                validate_snapshot_payload=dragon_app._reading_snapshot_payload_is_valid,
                normalize_reading_data=dragon_app.normalize_reading_data,
                build_lightweight_snapshot=dragon_app.build_lightweight_articles_snapshot,
                backup_reading_data_file=lambda reason="save": "",
                rotate_webhook_backup=lambda: "",
                clear_reading_data_cache=lambda: None,
                reading_data_cache_fingerprint=lambda: None,
                reading_format_mtime=lambda path: "mtime",
                monotonic=lambda: 0.0,
            )

            with self.assertRaises(RuntimeError):
                access.pull_latest_articles_snapshot()


if __name__ == "__main__":
    unittest.main()
