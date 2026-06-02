import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock

from domains.reading.data.cache_access import ReadingCacheAccess


class _ReadingRuntime:
    def __init__(self):
        self.data_cache_lock = threading.Lock()
        self.data_cache = {"fingerprint": None, "data": None}


class _ProjectionService:
    def build_source_context(self, data, context_label="cache_normalize"):
        sources = list(data.get("sources", []) or [])
        return {
            "sources": sources,
            "source_lookup": {source.get("name", "").lower(): source.get("id", "") for source in sources},
            "source_category_lookup": {source.get("id", ""): source.get("category", "news") for source in sources},
        }


class ReadingCacheAccessTests(unittest.TestCase):
    def _build_access(self, data_path, runtime):
        def fingerprint():
            try:
                stat_result = data_path.stat()
            except OSError:
                return None
            return (stat_result.st_mtime_ns, stat_result.st_size)

        def load_json_file(path, default):
            try:
                return json.loads(Path(path).read_text(encoding="utf-8"))
            except Exception:
                return default

        def save_json_file(path, payload):
            Path(path).write_text(json.dumps(payload), encoding="utf-8")

        return ReadingCacheAccess(
            app_logger=Mock(),
            default_reading_data=lambda: {"version": 1, "sources": [], "entries": []},
            normalize_reading_source=lambda source, index=0: dict(source or {}),
            normalize_reading_entry=lambda entry, index=0, source_lookup=None, source_category_lookup=None: dict(entry or {}),
            normalize_reading_url=lambda value: str(value or "").strip(),
            strip_reading_demo_entries=lambda data: False,
            backup_reading_data_file=lambda reason="save": "",
            apply_reading_retention_policy=lambda data: (data, {"changed": False, "archived_total": 0}),
            load_reading_backup_payload=lambda: None,
            load_json_file=load_json_file,
            save_json_file=save_json_file,
            reading_data_path=data_path,
            reading_runtime_projection_service=_ProjectionService(),
            reading_runtime=runtime,
            reading_retention_cap=100,
            reading_map_news_english_feed_url="https://www.mapnews.ma/en/rss.xml",
            reading_morocco_world_news_name="Morocco World News",
            reading_data_cache_fingerprint=fingerprint,
            monotonic=lambda: 0.0,
        )

    def test_save_reading_data_hydrates_cached_snapshot(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            data_path = Path(temp_dir) / "reading_data.json"
            data_path.write_text(
                json.dumps({
                    "version": 1,
                    "sources": [],
                    "entries": [{"id": "entry-1", "status": "unread", "title": "Old"}],
                }),
                encoding="utf-8",
            )
            runtime = _ReadingRuntime()
            access = self._build_access(data_path, runtime)

            self.assertEqual(access.load_reading_data_cached()["entries"][0]["status"], "unread")

            access.save_reading_data({
                "version": 1,
                "sources": [],
                "entries": [{"id": "entry-1", "status": "finished", "title": "Old"}],
            })

            cached = access.load_reading_data_cached()
            self.assertEqual(cached["entries"][0]["status"], "finished")
            self.assertEqual(runtime.data_cache["data"]["entries"][0]["status"], "finished")


if __name__ == "__main__":
    unittest.main()
