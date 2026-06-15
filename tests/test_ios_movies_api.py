import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import app as dragon_app


class IOSMoviesAPITests(unittest.TestCase):
    def setUp(self):
        self.client = dragon_app.app.test_client()

    def test_export_shaped_rows_map_name_category_and_derived_id(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)
            exports_dir = temp_dir / "exports"
            exports_dir.mkdir(parents=True, exist_ok=True)
            (exports_dir / "movies_export.json").write_text(
                json.dumps(
                    [
                        {
                            "name": "Iron Man 3",
                            "category": "movie",
                            "status": "Finished",
                            "score": "good",
                            "poster": "https://example.com/iron-man-3.jpg",
                            "year": 2013,
                            "overview": "Tony Stark fights back.",
                        }
                    ]
                ),
                encoding="utf-8",
            )

            with patch("domains.api.v1.EXPORTS_DIR", exports_dir):
                response = self.client.get("/api/v1/movies", query_string={"limit": 1})

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["count"], 1)
        item = payload["items"][0]
        self.assertEqual(item["title"], "Iron Man 3")
        self.assertEqual(item["type"], "movie")
        self.assertEqual(item["id"], "film-iron-man-3")
        self.assertTrue(item["id"].startswith("film-"))

    def test_movies_endpoint_preserves_explicit_id(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)
            exports_dir = temp_dir / "exports"
            exports_dir.mkdir(parents=True, exist_ok=True)
            (exports_dir / "movies_export.json").write_text(
                json.dumps(
                    [
                        {
                            "id": "movie-123",
                            "name": "Preserved ID Movie",
                            "category": "movie",
                            "status": "Queued",
                            "score": 8.5,
                            "poster": "https://example.com/poster.jpg",
                            "year": "2026",
                            "overview": "Overview",
                        }
                    ]
                ),
                encoding="utf-8",
            )

            with patch("domains.api.v1.EXPORTS_DIR", exports_dir):
                response = self.client.get("/api/v1/movies", query_string={"limit": 1})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["items"][0]["id"], "movie-123")

    def test_movies_endpoint_returns_empty_payload_for_missing_or_malformed_data(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)
            exports_dir = temp_dir / "exports"
            exports_dir.mkdir(parents=True, exist_ok=True)
            (exports_dir / "movies_export.json").write_text("{not-json", encoding="utf-8")

            with patch("domains.api.v1.EXPORTS_DIR", temp_dir / "missing-exports"):
                missing_response = self.client.get("/api/v1/movies")
            with patch("domains.api.v1.EXPORTS_DIR", exports_dir):
                malformed_response = self.client.get("/api/v1/movies")

        expected = {"ok": True, "api_version": "v1", "items": [], "count": 0}
        self.assertEqual(missing_response.status_code, 200)
        self.assertEqual(missing_response.get_json(), expected)
        self.assertEqual(malformed_response.status_code, 200)
        self.assertEqual(malformed_response.get_json(), expected)

    def test_movies_endpoint_does_not_leak_runtime_or_local_fields(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)
            exports_dir = temp_dir / "exports"
            exports_dir.mkdir(parents=True, exist_ok=True)
            (exports_dir / "movies_export.json").write_text(
                json.dumps(
                    [
                        {
                            "name": "Safe Movie",
                            "category": "movie",
                            "status": "watching",
                            "score": 8.5,
                            "poster": "https://example.com/poster.jpg",
                            "year": "2026",
                            "overview": "Short overview",
                            "magnet": "magnet:?xt=urn:btih:secret",
                            "torrent": {"url": "secret"},
                            "stream_url": "https://stream.example.com",
                            "playback_url": "https://play.example.com",
                            "runtime_path": "/tmp/runtime/movie.mp4",
                            "session_id": "hidden",
                            "local_path": "/Users/example/movie.mp4",
                            "providers": ["hidden"],
                            "sources": [{"secret": True}],
                            "runtime": {"state": "hidden"},
                            "session": {"id": "hidden"},
                            "private_notes": "secret",
                            "notes": "secret",
                            "raw": {"secret": True},
                            "tmdb_payload": {"secret": True},
                            "notion_payload": {"secret": True},
                        }
                    ]
                ),
                encoding="utf-8",
            )

            with patch("domains.api.v1.EXPORTS_DIR", exports_dir):
                response = self.client.get("/api/v1/movies", query_string={"limit": 1})

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True).lower()
        for forbidden in (
            "magnet",
            "torrent",
            "stream_url",
            "playback_url",
            "runtime_path",
            "session_id",
            "local_path",
            "providers",
            "sources",
            "runtime",
            "session",
            "private_notes",
            "notes",
            "raw",
            "tmdb_payload",
            "notion_payload",
        ):
            self.assertNotIn(forbidden, body)

    def test_movies_endpoint_limit_behavior_still_works(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)
            exports_dir = temp_dir / "exports"
            exports_dir.mkdir(parents=True, exist_ok=True)
            (exports_dir / "movies_export.json").write_text(
                json.dumps(
                    [
                        {
                            "name": f"Movie {index}",
                            "category": "movie",
                            "status": "watched",
                            "score": index,
                            "poster": f"https://example.com/poster-{index}.jpg",
                            "year": str(2000 + index),
                            "overview": f"Overview {index}",
                        }
                        for index in range(6)
                    ]
                ),
                encoding="utf-8",
            )

            with patch("domains.api.v1.EXPORTS_DIR", exports_dir):
                response = self.client.get("/api/v1/movies", query_string={"limit": 3})

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["count"], 3)
        self.assertEqual(len(payload["items"]), 3)
        self.assertEqual(payload["items"][0]["title"], "Movie 0")
        self.assertEqual(payload["items"][0]["id"], "film-movie-0")
        self.assertEqual(payload["items"][0]["type"], "movie")


if __name__ == "__main__":
    unittest.main()
