import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import app as dragon_app
import scripts.sync_reading_feeds as sync_reading_feeds


class ReadingSourcesRegistryTests(unittest.TestCase):
    def test_registry_seed_creates_snapshot_when_reading_data_is_missing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            registry_path = root / "reading_sources.json"
            reading_data_path = root / "reading_data.json"
            registry_payload = [
                {
                    "source_id": "reading-src-hespress",
                    "name": "Hespress - هسبريس جريدة إلكترونية مغربية",
                    "url": "https://www.hespress.com/feed/index.rss",
                    "category": "news",
                    "active": True,
                },
                {
                    "source_id": "reading-src-aljazeera",
                    "name": "Aljazeera - أخبار العالم",
                    "url": "https://plink.anyfeeder.com/aljazeera/news",
                    "category": "news",
                    "active": True,
                },
            ]
            registry_path.write_text(json.dumps(registry_payload, ensure_ascii=False, indent=2), encoding="utf-8")

            with patch.object(dragon_app, "READING_SOURCES_REGISTRY_PATH", registry_path):
                result = dragon_app.ensure_reading_sources_registry_seeded(
                    reading_data_path=reading_data_path,
                    registry_path=registry_path,
                )

            self.assertTrue(result["seeded"])
            self.assertEqual(result["reason"], "missing_snapshot")
            self.assertEqual(result["registry_source_count"], 2)
            self.assertEqual(result["tracked_sources"], 2)
            saved_payload = json.loads(reading_data_path.read_text(encoding="utf-8"))
            self.assertEqual(len(saved_payload.get("sources", []) or []), 2)
            self.assertEqual(saved_payload.get("entries", []), [])
            saved_names = {str(item.get("name", "") or "") for item in saved_payload.get("sources", [])}
            self.assertIn("Hespress - هسبريس جريدة إلكترونية مغربية", saved_names)
            self.assertIn("Aljazeera - أخبار العالم", saved_names)

    def test_run_sync_uses_tracked_registry_sources_when_snapshot_is_missing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            reading_data_path = Path(temp_dir) / "reading_data.json"
            registry_path = Path("config/reading_sources.json").resolve()
            registry_payload = json.loads(registry_path.read_text(encoding="utf-8"))
            expected_registry_count = len(registry_payload)

            def fake_sync_reading_sources(source_id=""):
                payload = json.loads(reading_data_path.read_text(encoding="utf-8"))
                sources = [source for source in (payload.get("sources", []) or []) if isinstance(source, dict)]
                active_sources = [
                    source for source in sources
                    if source.get("active", True) and str(source.get("url", "") or source.get("feed_url", "") or "").strip()
                ]
                return {
                    "imported_total": 0,
                    "source_results": [],
                    "zero_import_reasons": {},
                    "source_count": len(sources),
                    "active_source_count": len(active_sources),
                    "last_sync_at": "",
                    "last_sync_message": "seeded from registry",
                    "retention_summary": {},
                    "extraction_summary": {},
                }

            with patch.object(sync_reading_feeds.dragon_app, "READING_SOURCES_REGISTRY_PATH", registry_path), patch.object(
                sync_reading_feeds.dragon_app,
                "sync_reading_sources",
                side_effect=fake_sync_reading_sources,
            ):
                exit_code = sync_reading_feeds.run_sync(data_path=str(reading_data_path))

            self.assertEqual(exit_code, 0)
            saved_payload = json.loads(reading_data_path.read_text(encoding="utf-8"))
            saved_sources = [source for source in (saved_payload.get("sources", []) or []) if isinstance(source, dict)]
            self.assertEqual(len(saved_sources), expected_registry_count)
            saved_names = {str(source.get("name", "") or "") for source in saved_sources}
            self.assertIn("Hespress - هسبريس جريدة إلكترونية مغربية", saved_names)
            self.assertIn("Aljazeera - أخبار العالم", saved_names)


if __name__ == "__main__":
    unittest.main()
