import unittest
from unittest.mock import patch

from flask import render_template

import app as dragon_app


class MovieRuntimeHandoffTests(unittest.TestCase):
    def _film_entry(self, *, magnet=False):
        entry = {
            "entry_id": "film-test",
            "title": "Test Film",
            "name": "Test Film",
            "year": "2026",
            "category": "movie",
            "poster": "https://image.tmdb.org/t/p/w342/test.jpg",
            "fallback_url": "https://fallback.example/embed/test",
            "tmdb_id": "550",
            "torrent_hd": "https://example.com/test.torrent",
            "torrent_fhd": "",
            "magnet_hd": "magnet:?xt=urn:btih:1234567890ABCDEF1234567890ABCDEF12345678" if magnet else "",
            "magnet_fhd": "",
            "status": "Ready",
            "score": 0,
            "tmdb_rating": "",
        }
        return entry

    def test_build_movie_runtime_watch_url_uses_watch_route_when_magnet_exists(self):
        with dragon_app.app.test_request_context("/video/film-test"):
            url = dragon_app.build_movie_runtime_watch_url(self._film_entry(magnet=True))

        self.assertTrue(url.startswith("/watch?"))
        self.assertIn("magnet%3A%3Fxt%3Durn%3Abtih%3A1234567890ABCDEF1234567890ABCDEF12345678", url)
        self.assertIn("title=Test+Film", url)
        self.assertIn("entry_id=film-test", url)
        self.assertIn("tmdb_id=550", url)
        self.assertIn("poster=https%3A%2F%2Fimage.tmdb.org%2Ft%2Fp%2Fw342%2Ftest.jpg", url)
        self.assertIn("fallback_url=https%3A%2F%2Ffallback.example%2Fembed%2Ftest", url)

    def test_real_video_detail_context_includes_dragon_runtime_watch_url_for_magnet_handoff(self):
        film = {
            "entry_id": "film-test",
            "name": "Test",
            "title": "Test Film",
            "year": "2026",
            "category": "movie",
            "magnet_hd": "magnet:?xt=urn:btih:1234567890ABCDEF1234567890ABCDEF12345678",
            "magnet_fhd": "",
            "torrent_hd": "",
            "torrent_fhd": "",
            "status": "Ready",
        }

        with patch.object(dragon_app, "fetch_library_films_for_flagged_paths", return_value=[film]), \
             patch.object(dragon_app, "ensure_film_torrent_fields", side_effect=lambda detail, force_refresh=False: dict(detail)), \
             patch.object(dragon_app, "fetch_tmdb_enrichment", return_value=None), \
             patch.object(dragon_app, "movie_player_sources", return_value=[]), \
             patch.object(dragon_app, "get_vidsrc_embed_urls", return_value=[]), \
             patch.object(dragon_app, "rank_movie_detail_related_entries", return_value=[]), \
             patch.object(dragon_app.MOVIE_SOURCES_SERVICE, "get_movie_sources", return_value={"sources": []}), \
             patch.object(dragon_app, "prepare_playback_runtime", return_value={"playback_runtime": "browser_runtime"}), \
             patch.object(dragon_app, "serialize_playback_runtime", side_effect=lambda payload: dict(payload)), \
             patch.object(dragon_app, "get_runtime_profiles_catalog", return_value=[]):
            with dragon_app.app.test_request_context("/video/film-test"):
                context = dragon_app.get_video_detail_context("film-test")
                rendered = render_template(
                    "video_detail.html",
                    missing=False,
                    **context,
                    score_display=dragon_app.SCORE_DISPLAY,
                    score_color=dragon_app.SCORE_COLOR,
                    yts_url=dragon_app.yts_url,
                    build_query_url=dragon_app.build_query_url,
                )

        self.assertIsNotNone(context)
        self.assertIn("torrent_handoff_url", context)
        self.assertEqual(context["torrent_handoff_url"], film["magnet_hd"])
        self.assertIn("dragon_runtime_watch_url", context)
        self.assertTrue(context["dragon_runtime_watch_url"].startswith("/watch?"))
        self.assertIn("magnet=", context["dragon_runtime_watch_url"])
        self.assertIn("title=Test+Film", context["dragon_runtime_watch_url"])
        self.assertIn("movie_id=film-test", context["dragon_runtime_watch_url"])
        self.assertIn("entry_id=film-test", context["dragon_runtime_watch_url"])
        self.assertIn("Open in Dragon Runtime", rendered)
        self.assertIn("Open in qBittorrent", rendered)


if __name__ == "__main__":
    unittest.main()
