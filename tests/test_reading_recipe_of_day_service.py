import json
import tempfile
import threading
import unittest
from collections import namedtuple
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from dragon.cache import load_json_file, save_json_file
from domains.reading.services.recipe_of_day_service import ReadingRecipeOfDayService


FIXED_NOW = datetime(2026, 6, 2, 12, 0, 0, tzinfo=timezone.utc)


class _FixedDateTimeModule:
    @staticmethod
    def now(tz=None):
        return FIXED_NOW.astimezone(tz or timezone.utc)

    @staticmethod
    def fromtimestamp(value, tz=None):
        return datetime.fromtimestamp(value, tz or timezone.utc)


class _ProjectionService:
    def build_projection(self, data, context_label="recipe"):
        sources = [dict(source) for source in data.get("sources", []) or []]
        source_lookup = {str(source.get("name", "") or "").lower(): source.get("id", "") for source in sources}
        source_lookup.update({str(source.get("id", "") or ""): source.get("id", "") for source in sources})
        source_category_lookup = {str(source.get("id", "") or ""): source.get("category", "news") for source in sources}
        entries = [dict(entry) for entry in data.get("entries", []) or []]
        projection = namedtuple("Projection", "sources source_lookup source_category_lookup lightweight_entries")
        return projection(
            sources=tuple(sources),
            source_lookup=source_lookup,
            source_category_lookup=source_category_lookup,
            lightweight_entries=tuple(entries),
        )


class ReadingRecipeOfDayServiceTests(unittest.TestCase):
    def _build_service(self, data_state, recipe_path):
        return ReadingRecipeOfDayService(
            app_logger=Mock(),
            load_reading_data_cached=lambda: data_state["data"],
            default_reading_data=lambda: {"version": 1, "sources": [], "entries": []},
            reading_runtime_projection_service=_ProjectionService(),
            normalize_reading_status=lambda value: str(value or "").strip().lower() or "unread",
            parse_timestamp=lambda value: self._parse_timestamp(value),
            format_timestamp_label=lambda value, default="": self._format_timestamp_label(value, default=default),
            normalize_reading_url=lambda value: str(value or "").strip(),
            save_json_file=save_json_file,
            load_json_file=load_json_file,
            reading_recipe_of_day_path=recipe_path,
            datetime_module=_FixedDateTimeModule,
            monotonic=lambda: 0.0,
        )

    @staticmethod
    def _parse_timestamp(value):
        raw = str(value or "").strip()
        if not raw:
            return None
        if raw.endswith("Z"):
            raw = f"{raw[:-1]}+00:00"
        try:
            return datetime.fromisoformat(raw)
        except ValueError:
            return None

    @staticmethod
    def _format_timestamp_label(value, default=""):
        timestamp = ReadingRecipeOfDayServiceTests._parse_timestamp(value)
        if not timestamp:
            return default
        return timestamp.astimezone(timezone.utc).strftime("%b %d, %Y %H:%M")

    def _base_data(self, entries, sources=None):
        return {
            "version": 1,
            "sources": sources or [
                {
                    "id": "source-1",
                    "name": "Source One",
                    "url": "https://example.com/feed",
                    "category": "news",
                    "active": True,
                    "last_sync_status": "ok",
                    "last_sync_imported_count": 5,
                    "last_sync_raw_count": 8,
                    "last_synced_at": "2026-06-02T10:00:00+00:00",
                }
            ],
            "entries": entries,
        }

    def _entry(self, index, *, status="unread", published_at="2026-06-02T11:00:00+00:00", title=None, url=None, source_id="source-1", source="Source One"):
        return {
            "id": f"entry-{index}",
            "title": title or f"Article {index}",
            "status": status,
            "published_at": published_at,
            "added_at": published_at,
            "imported_at": published_at,
            "source_id": source_id,
            "source": source,
            "url": url or f"https://example.com/article-{index}",
            "original_url": url or f"https://example.com/article-{index}",
            "topic": "News",
            "category": "news",
            "starred": False,
        }

    def test_deterministic_scoring(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            recipe_path = Path(temp_dir) / "reading_recipe_of_day.json"
            service = self._build_service({"data": self._base_data([])}, recipe_path)
            candidate = self._entry(1)
            source = self._base_data([])["sources"][0]
            score_a = service.score_article_candidate(candidate, source=source, keyword_profile=["article", "news"])
            score_b = service.score_article_candidate(candidate, source=source, keyword_profile=["article", "news"])
            self.assertEqual(score_a, score_b)

    def test_max_seven_items(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            recipe_path = Path(temp_dir) / "reading_recipe_of_day.json"
            data_state = {"data": self._base_data([self._entry(i) for i in range(10)])}
            service = self._build_service(data_state, recipe_path)
            recipe = service.build_today_recipe(force=True)
            self.assertEqual(len(recipe["selected_articles"]), 7)

    def test_reuse_existing_same_day_recipe(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            recipe_path = Path(temp_dir) / "reading_recipe_of_day.json"
            data_state = {"data": self._base_data([self._entry(i) for i in range(3)])}
            service = self._build_service(data_state, recipe_path)
            first_recipe = service.build_today_recipe(force=False)
            data_state["data"] = self._base_data([self._entry(i + 10) for i in range(3)])
            second_recipe = service.build_today_recipe(force=False)
            self.assertEqual(first_recipe["generated_at"], second_recipe["generated_at"])
            self.assertEqual([item["id"] for item in first_recipe["selected_articles"]], [item["id"] for item in second_recipe["selected_articles"]])
            self.assertTrue(second_recipe["reused_existing_snapshot"])

    def test_force_regenerate_replaces_recipe(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            recipe_path = Path(temp_dir) / "reading_recipe_of_day.json"
            data_state = {"data": self._base_data([self._entry(1, title="First pick"), self._entry(2, title="Second pick")])}
            service = self._build_service(data_state, recipe_path)
            first_recipe = service.build_today_recipe(force=False)
            data_state["data"] = self._base_data([self._entry(99, title="Replacement pick")])
            second_recipe = service.build_today_recipe(force=True)
            self.assertNotEqual(
                [item["id"] for item in first_recipe["selected_articles"]],
                [item["id"] for item in second_recipe["selected_articles"]],
            )
            self.assertFalse(second_recipe["reused_existing_snapshot"])

    def test_archived_and_finished_are_deprioritized_or_excluded(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            recipe_path = Path(temp_dir) / "reading_recipe_of_day.json"
            active = [self._entry(i, status="unread", title=f"Active {i}") for i in range(7)]
            archived = [
                self._entry(100, status="archived", title="Archived 1"),
                self._entry(101, status="finished", title="Finished 1"),
            ]
            data_state = {"data": self._base_data(active + archived)}
            service = self._build_service(data_state, recipe_path)
            recipe = service.build_today_recipe(force=True)
            selected_statuses = {item["status"] for item in recipe["selected_articles"]}
            self.assertFalse(selected_statuses.intersection({"archived", "finished"}))


if __name__ == "__main__":
    unittest.main()
