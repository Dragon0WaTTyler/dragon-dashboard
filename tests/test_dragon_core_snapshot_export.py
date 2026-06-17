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
            DRAGON_CORE_SNAPSHOT_PATH=paths["exports_dir"] / "dragon_core_snapshot.json",
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
        self._write_json(
            paths["playlists_path"],
            {
                "YouTube Watch Later": [
                    {
                        "id": "PLA9RaIVS6nz25rdZd3SihId_AsAA06nPP",
                        "name": "My YouTube Watch Later",
                    }
                ]
            },
        )
        self._write_json(
            paths["cache_data_path"],
            {
                "films": {
                    "all": {
                        "updated_at": "2026-06-11T11:30:00Z",
                        "data": [
                            {
                                "notion_page_id": "movie-one",
                                "name": "Movie One",
                                "category": "movie",
                                "status": "Finished",
                                "score": "8",
                                "poster": "https://example.com/movie.jpg",
                                "year": "2025",
                                "overview": "Movie overview",
                            },
                            {
                                "notion_page_id": "movie-two",
                                "name": "Movie Two",
                                "category": "movie",
                                "status": "Watching",
                                "score": "7",
                                "poster": "https://example.com/movie-2.jpg",
                                "year": "2024",
                                "overview": "Movie overview two",
                            },
                        ],
                    }
                },
                "youtube_playlists": {
                    "PLA9RaIVS6nz25rdZd3SihId_AsAA06nPP": {
                        "updated_at": "2026-06-11T11:30:00Z",
                        "data": [
                            {
                                "playlist_item_id": "pli-1",
                                "video_id": "wl-video-1",
                                "title": "Watch Later Video",
                                "channel_title": "Dragon Later",
                                "thumbnail_url": "https://example.com/watchlater.jpg",
                                "url": "https://www.youtube.com/watch?v=wl-video-1",
                                "published_at": "2026-06-11T09:00:00Z",
                                "saved_at": "2026-06-11T11:00:00Z",
                                "duration": "05:43",
                            }
                        ],
                    }
                },
            },
        )

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

    def test_missing_primary_sources_fall_back_to_existing_snapshot(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            paths = self._temp_paths(temp_dir)
            fallback_snapshot_path = paths["exports_dir"] / "dragon_core_snapshot.json"
            self._write_json(
                fallback_snapshot_path,
                {
                    "schema_version": api_v1.DRAGON_CORE_SNAPSHOT_SCHEMA_VERSION,
                    "generated_at": "2026-06-16T00:00:00Z",
                    "producer": {
                        "kind": "flask_dashboard",
                        "version": "v1",
                        "source": "local_exports_and_snapshots",
                    },
                    "status": {
                        "partial": False,
                        "warnings": [],
                    },
                    "home": {
                        "app_name": "Dragon",
                        "service": "dragon",
                        "sections": [],
                    },
                    "books": {
                        "total": 1,
                        "items": [
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
                            }
                        ],
                    },
                    "articles": {
                        "total": 1,
                        "items": [
                            {
                                "id": "article-1",
                                "title": "Article One",
                                "source": "Example Source",
                                "url": "https://example.com/articles/1",
                                "published_at": "2026-06-10T10:00:00Z",
                                "saved_at": "2026-06-10T11:00:00Z",
                                "excerpt": "Article excerpt",
                                "image": "https://example.com/article.jpg",
                                "thumbnail": "https://example.com/article.jpg",
                            }
                        ],
                    },
                    "movies": {
                        "total": 1,
                        "items": [
                            {
                                "id": "film-movie-one",
                                "title": "Movie One",
                                "year": "2025",
                                "poster": "https://example.com/movie.jpg",
                                "status": "Finished",
                                "score": 8,
                                "type": "movie",
                                "overview": "Movie overview",
                            }
                        ],
                    },
                    "youtube": {
                        "sections": [
                            {
                                "key": "watchlater",
                                "label": "Watch Later",
                                "count": 1,
                            },
                            {
                                "key": "Favorites",
                                "label": "Favorites",
                                "count": 1,
                            }
                        ],
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
                                "section": "YouTube Watch Later",
                                "group": "",
                                "playlist": "Watch Later",
                                "source": "watchlater",
                            },
                            {
                                "id": "video-2",
                                "video_id": "video-2",
                                "title": "Video Two",
                                "channel": "Dragon Two",
                                "thumbnail": "https://example.com/video-2.jpg",
                                "url": "https://www.youtube.com/watch?v=video-2",
                                "published_at": "2026-06-11T12:00:00Z",
                                "saved_at": "2026-06-11T12:30:00Z",
                                "duration": "09:12",
                                "section": "Favorites",
                                "group": "Favorites",
                                "playlist": "",
                                "source": "pockettube",
                            }
                        ],
                    },
                },
            )

            with self._patch_paths(paths):
                snapshot = api_v1.build_dragon_core_snapshot()

        self.assertFalse(snapshot["status"]["partial"])
        self.assertEqual(snapshot["status"]["warnings"], ["youtube_source_stale"])
        self.assertEqual(snapshot["status"]["sources"]["books"]["state"], "fallback_snapshot")
        self.assertEqual(snapshot["status"]["sources"]["articles"]["state"], "fallback_snapshot")
        self.assertEqual(snapshot["status"]["sources"]["movies"]["state"], "fallback_snapshot")
        self.assertEqual(snapshot["status"]["sources"]["youtube"]["state"], "fallback_snapshot")
        self.assertEqual(snapshot["books"]["total"], 1)
        self.assertEqual(snapshot["articles"]["total"], 1)
        self.assertEqual(snapshot["movies"]["total"], 1)
        self.assertEqual(len(snapshot["youtube"]["videos"]), 2)

    def test_movies_use_current_projected_fields(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            paths = self._temp_paths(temp_dir)
            self._write_valid_sources(paths)
            with self._patch_paths(paths):
                snapshot = api_v1.build_dragon_core_snapshot()

        self.assertEqual(snapshot["status"]["sources"]["movies"]["source_kind"], "notion_export")
        self.assertEqual(snapshot["status"]["sources"]["movies"]["source_name"], "cache_data_films_all")
        self.assertEqual(snapshot["status"]["sources"]["movies"]["state"], "primary")
        self.assertEqual(snapshot["movies"]["total"], 2)
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
        self.assertEqual(written["movies"]["total"], 2)
        self.assertEqual(summary["output_path"], str(output_path))
        self.assertEqual(summary["books_count"], 1)
        self.assertEqual(summary["articles_count"], 1)
        self.assertEqual(summary["movies_count"], 2)
        self.assertEqual(summary["youtube_sections_count"], 2)
        self.assertEqual(summary["youtube_videos_count"], 2)
        home_movies_section = next(section for section in written["home"]["sections"] if section["key"] == "movies")
        self.assertEqual(home_movies_section["count"], 2)

    def test_snapshot_keeps_watchlater_and_pockettube_videos_separate(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            paths = self._temp_paths(temp_dir)
            self._write_valid_sources(paths)
            with self._patch_paths(paths):
                snapshot = api_v1.build_dragon_core_snapshot()

        videos = snapshot["youtube"]["videos"]
        watchlater_videos = [video for video in videos if str(video.get("source", "")).lower() == "watchlater"]
        pockettube_videos = [video for video in videos if str(video.get("source", "")).lower() == "pockettube"]

        self.assertEqual(len(watchlater_videos), 1)
        self.assertEqual(len(pockettube_videos), 1)
        self.assertEqual(watchlater_videos[0]["section"], "YouTube Watch Later")
        self.assertEqual(watchlater_videos[0]["playlist"], "My YouTube Watch Later")
        self.assertEqual(pockettube_videos[0]["section"], "Favorites")

    def test_snapshot_strips_signed_media_query_parameters(self):
        signed_url = (
            "https://cdn.example.com/media/image.jpg"
            "?X-Amz-Algorithm=AWS4-HMAC-SHA256"
            "&X-Amz-Security-Token=secret-token"
            "&X-Amz-Signature=abcdef123456"
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            paths = self._temp_paths(temp_dir)
            self._write_valid_sources(paths)

            reading_payload = json.loads(paths["reading_path"].read_text(encoding="utf-8"))
            reading_payload["entries"][0]["lead_image_url"] = signed_url
            self._write_json(paths["reading_path"], reading_payload)

            books_payload = json.loads(paths["books_path"].read_text(encoding="utf-8"))
            books_payload["entries"][0]["cover"] = signed_url
            self._write_json(paths["books_path"], books_payload)

            movies_payload = json.loads(paths["cache_data_path"].read_text(encoding="utf-8"))
            movies_payload["films"]["all"]["data"][0]["poster"] = signed_url
            self._write_json(paths["cache_data_path"], movies_payload)

            youtube_payload = json.loads(paths["youtube_path"].read_text(encoding="utf-8"))
            youtube_payload["groups"]["Favorites"]["videos"][0]["thumbnail"] = signed_url
            self._write_json(paths["youtube_path"], youtube_payload)

            watchlater_payload = json.loads(paths["cache_data_path"].read_text(encoding="utf-8"))
            watchlater_payload["youtube_playlists"]["PLA9RaIVS6nz25rdZd3SihId_AsAA06nPP"]["data"][0]["thumbnail_url"] = signed_url
            self._write_json(paths["cache_data_path"], watchlater_payload)

            with self._patch_paths(paths):
                snapshot = api_v1.build_dragon_core_snapshot()

        expected_url = "https://cdn.example.com/media/image.jpg"
        self.assertEqual(snapshot["articles"]["items"][0]["image"], expected_url)
        self.assertEqual(snapshot["books"]["items"][0]["cover"], expected_url)
        self.assertEqual(snapshot["movies"]["items"][0]["poster"], expected_url)
        self.assertEqual(snapshot["youtube"]["videos"][0]["thumbnail"], expected_url)
        self.assertEqual(snapshot["youtube"]["videos"][1]["thumbnail"], expected_url)
        serialized = json.dumps(snapshot, ensure_ascii=False)
        self.assertNotIn("X-Amz-", serialized)
        self.assertNotIn("secret-token", serialized)

    def test_snapshot_movies_prefer_fuller_semantic_library_source(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            paths = self._temp_paths(temp_dir)
            self._write_valid_sources(paths)
            with self._patch_paths(paths):
                snapshot = api_v1.build_dragon_core_snapshot()

        self.assertEqual(snapshot["status"]["sources"]["movies"]["source_kind"], "notion_export")
        self.assertEqual(snapshot["status"]["sources"]["movies"]["source_name"], "cache_data_films_all")
        self.assertEqual(snapshot["movies"]["total"], 2)
        self.assertEqual([item["title"] for item in snapshot["movies"]["items"]], ["Movie One", "Movie Two"])

    def test_snapshot_movie_limit_includes_all_semantic_items_under_limit(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            paths = self._temp_paths(temp_dir)
            self._write_valid_sources(paths)

            large_movie_library = []
            for index in range(679):
                large_movie_library.append(
                    {
                        "notion_page_id": f"movie-{index}",
                        "name": f"Movie {index}",
                        "category": "movie",
                        "status": "Finished" if index % 2 == 0 else "i want to",
                        "score": str((index % 10) + 1),
                        "poster": f"https://www.themoviedb.org/t/p/w1280/poster-{index}.jpg",
                        "year": str(2000 + (index % 20)),
                        "overview": f"Overview {index}",
                    }
                )

            cache_payload = json.loads(paths["cache_data_path"].read_text(encoding="utf-8"))
            cache_payload["films"]["all"]["data"] = large_movie_library
            self._write_json(paths["cache_data_path"], cache_payload)

            with self._patch_paths(paths):
                snapshot = api_v1.build_dragon_core_snapshot()

        self.assertEqual(snapshot["movies"]["total"], 679)
        self.assertEqual(len(snapshot["movies"]["items"]), 679)
        self.assertEqual(snapshot["movies"]["items"][0]["poster"], "https://image.tmdb.org/t/p/w1280/poster-0.jpg")


if __name__ == "__main__":
    unittest.main()
