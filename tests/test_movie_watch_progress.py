import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import app as dragon_app
from domains.magnets.playback.watch_progress import MovieWatchProgressService


TEST_MAGNET = "magnet:?xt=urn:btih:1234567890ABCDEF1234567890ABCDEF12345678"


class MovieWatchProgressTests(unittest.TestCase):
    def setUp(self):
        self.client = dragon_app.app.test_client()

    def _service(self, temp_dir: str) -> MovieWatchProgressService:
        return MovieWatchProgressService(Path(temp_dir) / "movie_watch_progress.json")

    def test_saving_valid_progress(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service = self._service(temp_dir)
            with patch.object(dragon_app, "MOVIE_WATCH_PROGRESS_SERVICE", service):
                response = self.client.post(
                    "/api/movies/watch-progress",
                    json={
                        "movie_id": "film-test",
                        "title": "Test Film",
                        "current_time": 125.4,
                        "duration": 7200.0,
                        "completed": False,
                    },
                )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertTrue(payload["saved"])
        self.assertFalse(payload["completed"])
        self.assertEqual(payload["movie_id"], "film-test")
        self.assertGreaterEqual(payload["current_time"], 125.0)

    def test_rejecting_invalid_progress(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service = self._service(temp_dir)
            with patch.object(dragon_app, "MOVIE_WATCH_PROGRESS_SERVICE", service):
                response = self.client.post(
                    "/api/movies/watch-progress",
                    json={
                        "movie_id": "film-test",
                        "title": "Test Film",
                        "current_time": -1,
                        "duration": 0,
                        "completed": False,
                    },
                )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["code"], "invalid_progress")

    def test_completed_detection_near_end(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service = self._service(temp_dir)
            with patch.object(dragon_app, "MOVIE_WATCH_PROGRESS_SERVICE", service):
                save_response = self.client.post(
                    "/api/movies/watch-progress",
                    json={
                        "movie_id": "film-test",
                        "title": "Test Film",
                        "current_time": 6500.0,
                        "duration": 7200.0,
                        "completed": False,
                    },
                )
                load_response = self.client.get("/api/movies/watch-progress", query_string={"movie_id": "film-test"})

        self.assertEqual(save_response.status_code, 200)
        self.assertTrue(save_response.get_json()["completed"])
        loaded = load_response.get_json()
        self.assertTrue(loaded["completed"])
        self.assertFalse(loaded["resume_available"])
        self.assertEqual(loaded["local_state"], "watched")

    def test_loading_saved_progress(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service = self._service(temp_dir)
            service.save_progress(
                {
                    "movie_id": "film-test",
                    "title": "Test Film",
                    "current_time": 242.0,
                    "duration": 7200.0,
                    "completed": False,
                }
            )
            with patch.object(dragon_app, "MOVIE_WATCH_PROGRESS_SERVICE", service):
                response = self.client.get("/api/movies/watch-progress", query_string={"movie_id": "film-test"})

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["has_progress"])
        self.assertTrue(payload["resume_available"])
        self.assertGreaterEqual(payload["resume_time"], 240.0)

    def test_player_page_includes_watch_progress_script_and_data_attributes(self):
        ready_session = {
            "session_id": "sess-1",
            "state": "ready",
            "status": "ready_to_play",
            "stream_url": "http://127.0.0.1:5000/api/runtime/stream/sess-1",
            "selected_file": {"name": "movie.mp4"},
            "source_quality": {"state": "playable", "code": "playable", "can_open_stream": True},
            "stream_readiness": {"stream_openable": True},
        }

        with patch.object(dragon_app.PLAYBACK_RUNTIME_MANAGER, "create_session", return_value=ready_session):
            response = self.client.get(
                "/watch",
                query_string={
                    "magnet": TEST_MAGNET,
                    "title": "Test Film",
                    "movie_id": "film-test",
                    "tmdb_id": "550",
                    "entry_id": "film-test",
                },
            )

        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("data-watch-progress-endpoint=\"/api/movies/watch-progress\"", html)
        self.assertIn("data-movie-id=\"film-test\"", html)
        self.assertIn("data-tmdb-id=\"550\"", html)
        self.assertIn("Resume from", html)
        self.assertIn("resumeStatus", html)

    def test_watch_page_hides_debug_details_in_normal_mode(self):
        ready_session = {
            "session_id": "sess-1",
            "state": "ready",
            "status": "ready_to_play",
            "stream_url": "http://127.0.0.1:5000/api/runtime/stream/sess-1",
            "selected_file": {"name": "movie.mp4"},
            "source_quality": {"state": "playable", "code": "playable", "can_open_stream": True},
            "stream_readiness": {"stream_openable": True},
        }

        with patch.object(dragon_app.PLAYBACK_RUNTIME_MANAGER, "create_session", return_value=ready_session):
            response = self.client.get(
                "/watch",
                query_string={
                    "magnet": TEST_MAGNET,
                    "title": "Test Film",
                    "movie_id": "film-test",
                    "entry_id": "film-test",
                },
            )

        html = response.get_data(as_text=True)
        self.assertNotIn("Developer details", html)
        self.assertNotIn("runtime_session", html)


if __name__ == "__main__":
    unittest.main()
