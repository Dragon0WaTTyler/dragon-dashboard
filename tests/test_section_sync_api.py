import unittest
from unittest.mock import Mock, patch

import app as dragon_app


class SectionSyncApiTests(unittest.TestCase):
    def setUp(self):
        dragon_app.app.config["TESTING"] = True
        self.client = dragon_app.app.test_client()

    def test_valid_sections_return_json(self):
        for section in ["articles", "youtube", "books", "movies", "chess", "german"]:
            with self.subTest(section=section):
                with patch.dict(
                    dragon_app.SECTION_SYNC_HANDLERS,
                    {section: lambda scope="", _section=section: {"status": "synced", "message": f"{_section} ok"}},
                    clear=False,
                ), patch.object(
                    dragon_app,
                    "_run_section_refresh",
                    return_value={"section": section, "message": "refreshed"},
                ):
                    response = self.client.post("/api/section-sync", json={"section": section})
                self.assertEqual(response.status_code, 200)
                payload = response.get_json()
                self.assertTrue(payload["ok"])
                self.assertEqual(payload["section"], section)
                self.assertIn("status", payload)
                self.assertIsInstance(payload.get("refresh"), dict)
                self.assertTrue(payload["reload"])

    def test_unknown_section_returns_400(self):
        response = self.client.post("/api/section-sync", json={"section": "unknown"})
        self.assertEqual(response.status_code, 400)
        payload = response.get_json()
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["message"], "Unknown section.")

    def test_articles_sync_does_not_call_global_refresh_and_runs_refresh(self):
        global_refresh = Mock()
        refresh_mock = Mock(return_value={"section": "articles", "message": "Articles snapshot refreshed."})
        with patch.object(dragon_app, "refresh_all_cached_data", global_refresh), patch.object(
            dragon_app,
            "trigger_reading_github_actions_sync",
            return_value=({"status": "started"}, 200),
        ), patch.object(
            dragon_app,
            "_reading_github_actions_headers",
            return_value={"Authorization": "Bearer test"},
        ), patch.object(
            dragon_app,
            "refresh_deployed_reading_snapshot_from_github",
            return_value={"ok": True},
        ), patch.object(
            dragon_app,
            "_run_section_refresh",
            refresh_mock,
        ):
            response = self.client.post("/api/section-sync", json={"section": "articles"})

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["status"], "pending")
        global_refresh.assert_not_called()
        refresh_mock.assert_called_once_with("articles", scope="")

    def test_youtube_sync_stays_watchlater_scoped_and_runs_refresh(self):
        global_refresh = Mock()
        refresh_mock = Mock(return_value={"section": "youtube", "message": "YouTube cache refreshed."})
        playlist_refresh = Mock(return_value=[{"video_id": "abc"}])
        with patch.object(dragon_app, "refresh_all_cached_data", global_refresh), patch.object(
            dragon_app,
            "_watchlater_playlist_ids",
            return_value=["WL1"],
        ), patch.object(
            dragon_app,
            "get_all_playlist_videos",
            playlist_refresh,
        ), patch.object(
            dragon_app,
            "_run_section_refresh",
            refresh_mock,
        ):
            response = self.client.post("/api/section-sync", json={"section": "youtube"})

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["status"], "synced")
        global_refresh.assert_not_called()
        refresh_mock.assert_called_once_with("youtube", scope="")
        playlist_refresh.assert_called_once_with(
            "WL1",
            force_refresh=True,
            allow_global_invalidation=False,
            refresh_reason="section_sync_watchlater",
        )

    def test_books_and_movies_do_not_call_global_refresh(self):
        cases = [
            ("books", "fetch_books_entries", {"entries": [{"id": "b1"}], "error": ""}),
            ("movies", "refresh_film_cache_from_source", [{"id": "m1"}]),
        ]
        for section, method_name, method_result in cases:
            with self.subTest(section=section):
                global_refresh = Mock()
                refresh_mock = Mock(return_value={"section": section, "message": "refreshed"})
                with patch.object(dragon_app, "refresh_all_cached_data", global_refresh), patch.object(
                    dragon_app,
                    method_name,
                    return_value=method_result,
                ), patch.object(
                    dragon_app,
                    "_run_section_refresh",
                    refresh_mock,
                ):
                    response = self.client.post("/api/section-sync", json={"section": section})

                self.assertEqual(response.status_code, 200)
                payload = response.get_json()
                self.assertTrue(payload["ok"])
                self.assertEqual(payload["status"], "synced")
                global_refresh.assert_not_called()
                refresh_mock.assert_called_once_with(section, scope="")

    def test_chess_and_german_do_not_perform_heavy_work(self):
        refresh_mock = Mock(return_value={"section": "noop", "message": "refreshed"})
        heavy_articles = Mock()
        heavy_youtube = Mock()
        heavy_books = Mock()
        heavy_movies = Mock()
        with patch.object(dragon_app, "trigger_reading_github_actions_sync", heavy_articles), patch.object(
            dragon_app,
            "get_all_playlist_videos",
            heavy_youtube,
        ), patch.object(
            dragon_app,
            "fetch_books_entries",
            heavy_books,
        ), patch.object(
            dragon_app,
            "refresh_film_cache_from_source",
            heavy_movies,
        ), patch.object(
            dragon_app,
            "_run_section_refresh",
            refresh_mock,
        ):
            chess_response = self.client.post("/api/section-sync", json={"section": "chess"})
            german_response = self.client.post("/api/section-sync", json={"section": "german"})

        self.assertEqual(chess_response.status_code, 200)
        self.assertEqual(german_response.status_code, 200)
        self.assertEqual(chess_response.get_json()["status"], "unsupported")
        self.assertEqual(german_response.get_json()["status"], "unsupported")
        heavy_articles.assert_not_called()
        heavy_youtube.assert_not_called()
        heavy_books.assert_not_called()
        heavy_movies.assert_not_called()
        self.assertEqual(refresh_mock.call_count, 2)


if __name__ == "__main__":
    unittest.main()
