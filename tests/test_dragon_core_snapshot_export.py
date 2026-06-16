import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from domains.api import v1 as api_v1


class DragonCoreSnapshotExportTests(unittest.TestCase):
    def _write_json(self, path, payload):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _temp_paths(self, temp_dir):
        root = Path(temp_dir)
        exports_dir = root / "exports"
        return {
            "root": root,
            "reading_path": root / "reading_data.json",
            "books_path": root / "books_snapshot.json",
            "chess_path": root / "chess_data.json",
            "exports_dir": exports_dir,
            "movies_path": exports_dir / "movies_export.json",
            "youtube_path": root / "youtube_latest_snapshot.json",
            "playlists_path": root / "playlists.json",
            "cache_data_path": root / "cache_data.json",
        }

    def _patch_paths(self, paths):
        return patch.multiple(
            "domains.api.v1",
            READING_DATA_PATH=paths["reading_path"],
            BOOKS_SNAPSHOT_PATH=paths["books_path"],
            CHESS_DATA_PATH=paths["chess_path"],
            EXPORTS_DIR=paths["exports_dir"],
            YOUTUBE_LATEST_SNAPSHOT_PATH=paths["youtube_path"],
            PLAYLISTS_PATH=paths["playlists_path"],
            CACHE_DATA_PATH=paths["cache_data_path"],
        )

    def _write_valid_sources(self, paths):
        self._write_json(
            paths["reading_path"],
            {
                "entries": [
                    {
                        "id": "article-1",
                        "title": "Article One",
                        "source": "Example Source",
                        "url": "https://example.com/articles/1",
                        "published_at": "2026-06-10T10:00:00Z",
                        "saved_at": "2026-06-10T11:00:00Z",
                        "excerpt": "Article excerpt",
                        "lead_image_url": "https://example.com/article.jpg",
                        "content_text": "hidden body",
                        "content_html": "<p>hidden body</p>",
                    }
                ]
            },
        )
        self._write_json(
            paths["books_path"],
            {
                "entries": [
                    {
                        "id": "book-1",
                        "title": "Book One",
                        "author": "Author One",
                        "authors": ["Author One"],
                        "cover": "https://example.com/book.jpg",
                        "year": "2024",
                        "status": "reading",
                        "score": "9",
                        "excerpt": "Book excerpt",
                        "token": "hidden",
                        "secret": "hidden",
                    }
                ]
            },
        )
        self._write_json(paths["chess_path"], {"games": [{"id": "game-1"}]})
        self._write_json(
            paths["movies_path"],
            [
                {
                    "name": "Movie One",
                    "category": "movie",
                    "status": "Finished",
                    "score": "8",
                    "poster": "https://example.com/movie.jpg",
                    "year": "2025",
                    "overview": "Movie overview",
                    "magnet": "magnet:?xt=urn:btih:hidden",
                    "runtime_path": "/tmp/hidden.mp4",
                }
            ],
        )
        self._write_json(
            paths["youtube_path"],
            {
                "groups": {
                    "Favorites": {
                        "section_name": "Favorites",
                        "videos": [
                            {
                                "id": "video-1",
                                "video_id": "video-1",
                                "title": "Video One",
                                "channel": "Dragon",
                                "thumbnail": "https://example.com/video.jpg",
                                "url": "https://www.youtube.com/watch?v=video-1",
                                "published_at": "2026-06-11T10:00:00Z",
                                "saved_at": "2026-06-11T11:00:00Z",
                                "duration": "12:34",
                            }
                        ],
                    }
                }
            },
        )
        self._write_json(paths["playlists_path"], {})
        self._write_json(paths["cache_data_path"], {})

    def test_snapshot_contains_required_top_level_keys(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            paths = self._temp_paths(temp_dir)
            self._write_valid_sources(paths)
            with self._patch_paths(paths):
                snapshot = api_v1.build_dragon_core_snapshot()

        self.assertEqual(
            set(snapshot.keys()),
            {"schema_version", "generated_at", "producer", "status", "home", "books", "articles", "movies", "youtube"},
        )
        self.assertEqual(snapshot["schema_version"], api_v1.DRAGON_CORE_SNAPSHOT_SCHEMA_VERSION)
        self.assertEqual(snapshot["producer"]["kind"], "flask_dashboard")
        self.assertEqual(snapshot["producer"]["source"], "local_exports_and_snapshots")
        self.assertIn("sections", snapshot["home"])
        self.assertIn("items", snapshot["books"])
        self.assertIn("items", snapshot["articles"])
        self.assertIn("items", snapshot["movies"])
        self.assertIn("sections", snapshot["youtube"])
        self.assertIn("videos", snapshot["youtube"])

    def test_snapshot_does_not_include_forbidden_fields(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            paths = self._temp_paths(temp_dir)
            self._write_valid_sources(paths)
            with self._patch_paths(paths):
                snapshot = api_v1.build_dragon_core_snapshot()

        body = json.dumps(snapshot, ensure_ascii=False).lower()
        for forbidden in (
            "token",
            "secret",
            "oauth",
            "notion_payload",
            "cache_path",
            "local_path",
            "runtime_path",
            "magnet",
            "torrent",
            "session_id",
            "stream_url",
            "playback_url",
            "traceback",
        ):
            self.assertNotIn(forbidden, body)

    def test_missing_or_malformed_sources_produce_partial_snapshot_with_warnings(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            paths = self._temp_paths(temp_dir)
            self._write_json(paths["chess_path"], {"games": []})
            self._write_json(paths["playlists_path"], {})
            self._write_json(paths["cache_data_path"], {})
            paths["movies_path"].parent.mkdir(parents=True, exist_ok=True)
            paths["movies_path"].write_text("{not-json", encoding="utf-8")
            with self._patch_paths(paths):
                snapshot = api_v1.build_dragon_core_snapshot()

        self.assertTrue(snapshot["status"]["partial"])
        self.assertGreaterEqual(len(snapshot["status"]["warnings"]), 1)
        self.assertEqual(snapshot["articles"], {"total": 0, "items": []})
        self.assertEqual(snapshot["books"], {"total": 0, "items": []})
        self.assertEqual(snapshot["movies"], {"total": 0, "items": []})
        self.assertEqual(snapshot["youtube"]["sections"], [])
        self.assertEqual(snapshot["youtube"]["videos"], [])

    def test_movies_use_current_projected_fields(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            paths = self._temp_paths(temp_dir)
            self._write_valid_sources(paths)
            with self._patch_paths(paths):
                snapshot = api_v1.build_dragon_core_snapshot()

        self.assertEqual(
            snapshot["movies"]["items"][0],
            {
                "id": "film-movie-one",
                "title": "Movie One",
                "year": "2025",
                "poster": "https://example.com/movie.jpg",
                "status": "Finished",
                "score": 8,
                "type": "movie",
                "overview": "Movie overview",
            },
        )

    def test_articles_include_images_but_not_detail_content(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            paths = self._temp_paths(temp_dir)
            self._write_valid_sources(paths)
            with self._patch_paths(paths):
                snapshot = api_v1.build_dragon_core_snapshot()

        self.assertEqual(snapshot["articles"]["items"][0]["image"], "https://example.com/article.jpg")
        self.assertEqual(snapshot["articles"]["items"][0]["thumbnail"], "https://example.com/article.jpg")
        article_body = json.dumps(snapshot["articles"], ensure_ascii=False).lower()
        self.assertNotIn("content_text", article_body)
        self.assertNotIn("content_html", article_body)

    def test_export_function_writes_valid_json(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            paths = self._temp_paths(temp_dir)
            self._write_valid_sources(paths)
            output_path = paths["exports_dir"] / "dragon_core_snapshot.json"
            with self._patch_paths(paths):
                summary = api_v1.export_dragon_core_snapshot(output_path)

            self.assertTrue(output_path.exists())
            written = json.loads(output_path.read_text(encoding="utf-8"))

        self.assertEqual(written["schema_version"], api_v1.DRAGON_CORE_SNAPSHOT_SCHEMA_VERSION)
        self.assertEqual(summary["output_path"], str(output_path))
        self.assertEqual(summary["books_count"], 1)
        self.assertEqual(summary["articles_count"], 1)
        self.assertEqual(summary["movies_count"], 1)
        self.assertEqual(summary["youtube_sections_count"], 1)
        self.assertEqual(summary["youtube_videos_count"], 1)


if __name__ == "__main__":
    unittest.main()
