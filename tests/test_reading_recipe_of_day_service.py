import tempfile
import unittest
from collections import Counter, namedtuple
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import app as dragon_app
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
        entries = [dict(entry) for entry in data.get("entries", []) or []]
        source_lookup = {str(source.get("name", "") or "").lower(): source.get("id", "") for source in sources}
        source_lookup.update({str(source.get("id", "") or ""): source.get("id", "") for source in sources})
        source_category_lookup = {str(source.get("id", "") or ""): source.get("category", "news") for source in sources}
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
            parse_timestamp=self._parse_timestamp,
            format_timestamp_label=self._format_timestamp_label,
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

    def _timestamp(self, hours_ago):
        return (FIXED_NOW - timedelta(hours=hours_ago)).isoformat()

    def _source(self, source_id, name, *, status="ok", imported_count=5, raw_count=8):
        return {
            "id": source_id,
            "name": name,
            "url": f"https://example.com/{source_id}.xml",
            "category": "news",
            "active": True,
            "last_sync_status": status,
            "last_sync_imported_count": imported_count,
            "last_sync_raw_count": raw_count,
            "last_synced_at": self._timestamp(1),
        }

    def _entry(self, index, *, source_id="source-1", source="Source One", status="unread", hours_ago=1, title=None, url=None, starred=False):
        ts = self._timestamp(hours_ago)
        return {
            "id": f"entry-{index}",
            "title": title or f"Article {index}",
            "status": status,
            "published_at": ts,
            "added_at": ts,
            "imported_at": ts,
            "source_id": source_id,
            "source": source,
            "url": url or f"https://example.com/article-{index}",
            "original_url": url or f"https://example.com/article-{index}",
            "topic": "News",
            "category": "news",
            "starred": starred,
        }

    def _base_data(self, entries, sources):
        return {
            "version": 1,
            "sources": sources,
            "entries": entries,
        }

    def test_deterministic_result_order(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            recipe_path = Path(temp_dir) / "reading_recipe_of_day.json"
            data_state = {
                "data": self._base_data(
                    [
                        self._entry(1, source_id="source-1", source="Source One", hours_ago=2),
                        self._entry(2, source_id="source-2", source="Source Two", hours_ago=1),
                        self._entry(3, source_id="source-3", source="Source Three", hours_ago=3),
                    ],
                    [
                        self._source("source-1", "Source One"),
                        self._source("source-2", "Source Two"),
                        self._source("source-3", "Source Three"),
                    ],
                )
            }
            service = self._build_service(data_state, recipe_path)

            first = service.build_today_recipe(force=True)
            second = service.build_today_recipe(force=True)

            self.assertEqual(
                [item["id"] for item in first["selected_articles"]],
                [item["id"] for item in second["selected_articles"]],
            )

    def test_al_jazeera_variants_share_same_publisher_family(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            recipe_path = Path(temp_dir) / "reading_recipe_of_day.json"
            data_state = {
                "data": self._base_data(
                    [
                        self._entry(1, source_id="aj-main", source="AL JAZEERA ENGLISH", hours_ago=1, url="https://www.aljazeera.com/news/one"),
                        self._entry(2, source_id="aj-opinion", source="AL JAZEERA ENGLISH (Opinion)", hours_ago=2, url="https://www.aljazeera.com/opinions/two"),
                        self._entry(3, source_id="aj-opinion-ar", source="Al Jazeera Opinion", hours_ago=3, url="https://www.aljazeera.com/opinions/three"),
                    ],
                    [
                        self._source("aj-main", "AL JAZEERA ENGLISH"),
                        self._source("aj-opinion", "AL JAZEERA ENGLISH (Opinion)"),
                        self._source("aj-opinion-ar", "Al Jazeera Opinion"),
                    ],
                )
            }
            service = self._build_service(data_state, recipe_path)
            recipe = service.build_today_recipe(force=True)

            families = {item["publisher_family_key"] for item in recipe["selected_articles"]}
            self.assertEqual(families, {"al-jazeera"})

    def test_no_more_than_two_articles_per_publisher_family(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            recipe_path = Path(temp_dir) / "reading_recipe_of_day.json"
            entries = [
                self._entry(i, source_id=f"aj-{i}", source="AL JAZEERA ENGLISH (Opinion)" if i % 2 else "AL JAZEERA ENGLISH", hours_ago=i + 1, url=f"https://www.aljazeera.com/story-{i}")
                for i in range(1, 8)
            ]
            entries.extend([
                self._entry(100, source_id="source-2", source="Source Two", hours_ago=2),
                self._entry(101, source_id="source-3", source="Source Three", hours_ago=3),
            ])
            data_state = {
                "data": self._base_data(
                    entries,
                    [
                        self._source("aj-1", "AL JAZEERA ENGLISH"),
                        self._source("aj-2", "AL JAZEERA ENGLISH (Opinion)"),
                        self._source("aj-3", "AL JAZEERA ENGLISH"),
                        self._source("aj-4", "AL JAZEERA ENGLISH (Opinion)"),
                        self._source("aj-5", "AL JAZEERA ENGLISH"),
                        self._source("aj-6", "AL JAZEERA ENGLISH (Opinion)"),
                        self._source("aj-7", "AL JAZEERA ENGLISH"),
                        self._source("source-2", "Source Two"),
                        self._source("source-3", "Source Three"),
                    ],
                )
            }
            service = self._build_service(data_state, recipe_path)
            recipe = service.build_today_recipe(force=True)

            family_counts = Counter(item["publisher_family_key"] for item in recipe["selected_articles"])
            self.assertLessEqual(family_counts["al-jazeera"], 2)

    def test_first_pass_selects_across_different_publisher_families(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            recipe_path = Path(temp_dir) / "reading_recipe_of_day.json"
            entries = [
                self._entry(1, source_id="aj-main", source="AL JAZEERA ENGLISH", hours_ago=1, url="https://www.aljazeera.com/a"),
                self._entry(2, source_id="aj-opinion", source="AL JAZEERA ENGLISH (Opinion)", hours_ago=2, url="https://www.aljazeera.com/b"),
                self._entry(3, source_id="bbc-main", source="BBC Culture", hours_ago=1, url="https://www.bbc.com/c"),
                self._entry(4, source_id="hespress-main", source="Hespress", hours_ago=1, url="https://www.hespress.com/d"),
                self._entry(5, source_id="map-main", source="MAP News English", hours_ago=1, url="https://www.mapnews.ma/e"),
            ]
            sources = [
                self._source("aj-main", "AL JAZEERA ENGLISH"),
                self._source("aj-opinion", "AL JAZEERA ENGLISH (Opinion)"),
                self._source("bbc-main", "BBC Culture"),
                self._source("hespress-main", "Hespress"),
                self._source("map-main", "MAP News English"),
            ]
            data_state = {"data": self._base_data(entries, sources)}
            service = self._build_service(data_state, recipe_path)

            recipe = service.build_today_recipe(force=True)
            first_pass_families = [
                item["publisher_family_key"]
                for item in recipe["selected_articles"]
                if item.get("recipe_phase") == "source-first-pass"
            ]

            self.assertEqual(len(first_pass_families), len(set(first_pass_families)))

    def test_last_24h_articles_are_preferred(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            recipe_path = Path(temp_dir) / "reading_recipe_of_day.json"
            recent_entries = [
                self._entry(index, source_id=f"source-{index}", source=f"Source {index}", hours_ago=2)
                for index in range(1, 8)
            ]
            old_high_score = self._entry(
                99,
                source_id="source-old",
                source="Old Source",
                hours_ago=48,
                title="Very Old but Strong",
                starred=True,
            )
            data_state = {
                "data": self._base_data(
                    recent_entries + [old_high_score],
                    [self._source(f"source-{index}", f"Source {index}") for index in range(1, 8)] + [self._source("source-old", "Old Source")],
                )
            }
            service = self._build_service(data_state, recipe_path)
            recipe = service.build_today_recipe(force=True)

            selected_ids = {item["id"] for item in recipe["selected_articles"]}
            self.assertIn("entry-1", selected_ids)
            self.assertNotIn("entry-99", selected_ids)

    def test_same_day_reuse_still_works(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            recipe_path = Path(temp_dir) / "reading_recipe_of_day.json"
            data_state = {
                "data": self._base_data(
                    [self._entry(1, source_id="source-1", source="Source One", hours_ago=2)],
                    [self._source("source-1", "Source One")],
                )
            }
            service = self._build_service(data_state, recipe_path)

            first = service.build_today_recipe(force=False)
            data_state["data"] = self._base_data(
                [self._entry(2, source_id="source-1", source="Source One", hours_ago=2)],
                [self._source("source-1", "Source One")],
            )
            second = service.build_today_recipe(force=False)

            self.assertEqual([item["id"] for item in first["selected_articles"]], [item["id"] for item in second["selected_articles"]])
            self.assertTrue(second["reused_existing_snapshot"])

    def test_force_regenerate_replaces_old_cached_same_day_recipe(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            recipe_path = Path(temp_dir) / "reading_recipe_of_day.json"
            data_state = {
                "data": self._base_data(
                    [self._entry(1, source_id="aj-main", source="AL JAZEERA ENGLISH", hours_ago=1, url="https://www.aljazeera.com/a")],
                    [self._source("aj-main", "AL JAZEERA ENGLISH")],
                )
            }
            service = self._build_service(data_state, recipe_path)
            first = service.build_today_recipe(force=False)

            data_state["data"] = self._base_data(
                [
                    self._entry(2, source_id="bbc-main", source="BBC Culture", hours_ago=1, url="https://www.bbc.com/b"),
                    self._entry(3, source_id="hespress-main", source="Hespress", hours_ago=2, url="https://www.hespress.com/c"),
                ],
                [
                    self._source("bbc-main", "BBC Culture"),
                    self._source("hespress-main", "Hespress"),
                ],
            )
            second = service.build_today_recipe(force=True)

            self.assertNotEqual(
                [item["id"] for item in first["selected_articles"]],
                [item["id"] for item in second["selected_articles"]],
            )
            self.assertFalse(second["reused_existing_snapshot"])

    def test_start_route_chooses_first_article(self):
        dragon_app.app.config["TESTING"] = True
        client = dragon_app.app.test_client()
        fake_service = Mock()
        fake_service.build_today_recipe.return_value = {
            "selected_articles": [
                {"id": "first", "title": "First", "source": "Source A", "source_dir": "auto", "title_dir": "auto", "reason_tags": []},
                {"id": "second", "title": "Second", "source": "Source B", "source_dir": "auto", "title_dir": "auto", "reason_tags": []},
            ]
        }

        with patch.object(dragon_app, "_get_reading_recipe_of_day_service", return_value=fake_service):
            response = client.get("/reading/recipe/start")

        self.assertEqual(response.status_code, 302)
        self.assertIn("/reading/article/first", response.location)
        self.assertIn("recipe=1", response.location)
        self.assertIn("recipe_index=0", response.location)


if __name__ == "__main__":
    unittest.main()
