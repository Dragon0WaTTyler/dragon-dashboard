import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import app as dragon_app


class IOSMoviesAPITests(unittest.TestCase):
    def setUp(self):
        self.client = dragon_app.app.test_client()

    def _write_movies_export(self, rows):
        temp_dir = tempfile.TemporaryDirectory()
        temp_path = Path(temp_dir.name)
        exports_dir = temp_path / "exports"
        exports_dir.mkdir(parents=True, exist_ok=True)
        (exports_dir / "movies_export.json").write_text(json.dumps(rows), encoding="utf-8")
        return temp_dir, exports_dir

    def test_export_shaped_rows_map_name_category_and_derived_id_with_pagination_metadata(self):
        temp_dir, exports_dir = self._write_movies_export(
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
        )
        with temp_dir:
            with patch("domains.api.v1.EXPORTS_DIR", exports_dir):
                response = self.client.get("/api/v1/movies", query_string={"limit": 1, "offset": 0})

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["total"], 1)
        self.assertEqual(payload["limit"], 1)
        self.assertEqual(payload["offset"], 0)
        self.assertIsNone(payload["next_offset"])
        self.assertFalse(payload["has_more"])
        self.assertEqual(payload["count"], 1)
        item = payload["items"][0]
        self.assertEqual(item["title"], "Iron Man 3")
        self.assertEqual(item["type"], "movie")
        self.assertEqual(item["id"], "film-iron-man-3")
        self.assertTrue(item["id"].startswith("film-"))

    def test_movies_endpoint_preserves_explicit_id(self):
        temp_dir, exports_dir = self._write_movies_export(
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
        )
        with temp_dir:
            with patch("domains.api.v1.EXPORTS_DIR", exports_dir):
                response = self.client.get("/api/v1/movies", query_string={"limit": 1})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["items"][0]["id"], "movie-123")

    def test_movies_endpoint_supports_offset_and_has_more_across_pages(self):
        temp_dir, exports_dir = self._write_movies_export(
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
                for index in range(5)
            ]
        )
        with temp_dir:
            with patch("domains.api.v1.EXPORTS_DIR", exports_dir):
                first_response = self.client.get("/api/v1/movies", query_string={"limit": 2, "offset": 0})
                second_response = self.client.get("/api/v1/movies", query_string={"limit": 2, "offset": 2})
                last_response = self.client.get("/api/v1/movies", query_string={"limit": 2, "offset": 4})

        first_payload = first_response.get_json()
        second_payload = second_response.get_json()
        last_payload = last_response.get_json()

        self.assertEqual(first_response.status_code, 200)
        self.assertEqual(first_payload["count"], 2)
        self.assertEqual(first_payload["total"], 5)
        self.assertEqual(first_payload["offset"], 0)
        self.assertEqual(first_payload["next_offset"], 2)
        self.assertTrue(first_payload["has_more"])
        self.assertEqual(first_payload["items"][0]["title"], "Movie 0")

        self.assertEqual(second_response.status_code, 200)
        self.assertEqual(second_payload["count"], 2)
        self.assertEqual(second_payload["total"], 5)
        self.assertEqual(second_payload["offset"], 2)
        self.assertEqual(second_payload["next_offset"], 4)
        self.assertTrue(second_payload["has_more"])
        self.assertEqual(second_payload["items"][0]["title"], "Movie 2")
        self.assertNotEqual(first_payload["items"][0]["id"], second_payload["items"][0]["id"])

        self.assertEqual(last_response.status_code, 200)
        self.assertEqual(last_payload["count"], 1)
        self.assertEqual(last_payload["total"], 5)
        self.assertEqual(last_payload["offset"], 4)
        self.assertIsNone(last_payload["next_offset"])
        self.assertFalse(last_payload["has_more"])
        self.assertEqual(last_payload["items"][0]["title"], "Movie 4")

    def test_movies_endpoint_invalid_offset_and_huge_limit_are_safely_normalized(self):
        temp_dir, exports_dir = self._write_movies_export(
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
                for index in range(150)
            ]
        )
        with temp_dir:
            with patch("domains.api.v1.EXPORTS_DIR", exports_dir):
                invalid_offset_response = self.client.get("/api/v1/movies", query_string={"limit": 2, "offset": -10})
                capped_limit_response = self.client.get("/api/v1/movies", query_string={"limit": 1000, "offset": 0})

        invalid_offset_payload = invalid_offset_response.get_json()
        capped_limit_payload = capped_limit_response.get_json()

        self.assertEqual(invalid_offset_response.status_code, 200)
        self.assertEqual(invalid_offset_payload["offset"], 0)
        self.assertEqual(invalid_offset_payload["items"][0]["title"], "Movie 0")

        self.assertEqual(capped_limit_response.status_code, 200)
        self.assertEqual(capped_limit_payload["limit"], 100)
        self.assertEqual(capped_limit_payload["count"], 100)
        self.assertEqual(capped_limit_payload["total"], 150)
        self.assertEqual(capped_limit_payload["next_offset"], 100)
        self.assertTrue(capped_limit_payload["has_more"])

    def test_movies_endpoint_returns_empty_payload_for_missing_or_malformed_data(self):
        with tempfile.TemporaryDirectory() as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            exports_dir = temp_dir / "exports"
            exports_dir.mkdir(parents=True, exist_ok=True)
            (exports_dir / "movies_export.json").write_text("{not-json", encoding="utf-8")

            with patch("domains.api.v1.EXPORTS_DIR", temp_dir / "missing-exports"):
                missing_response = self.client.get("/api/v1/movies")
            with patch("domains.api.v1.EXPORTS_DIR", exports_dir):
                malformed_response = self.client.get("/api/v1/movies")

        expected = {
            "ok": True,
            "api_version": "v1",
            "items": [],
            "count": 0,
            "total": 0,
            "limit": 20,
            "offset": 0,
            "next_offset": None,
            "has_more": False,
        }
        self.assertEqual(missing_response.status_code, 200)
        self.assertEqual(missing_response.get_json(), expected)
        self.assertEqual(malformed_response.status_code, 200)
        self.assertEqual(malformed_response.get_json(), expected)

    def test_movies_endpoint_does_not_leak_runtime_or_local_fields(self):
        temp_dir, exports_dir = self._write_movies_export(
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
        )
        with temp_dir:
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


if __name__ == "__main__":
    unittest.main()
