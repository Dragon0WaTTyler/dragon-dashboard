import json
import tempfile
import unittest
from urllib.parse import quote
from pathlib import Path
from unittest.mock import patch

import app as dragon_app


class IOSApiFoundationTests(unittest.TestCase):
    def setUp(self):
        dragon_app.app.config["TESTING"] = True
        dragon_app.app.config["SESSION_COOKIE_SECURE"] = False
        dragon_app.DRAGON_ADMIN_USERNAME = ""
        dragon_app.DRAGON_ADMIN_PASSWORD = ""
        dragon_app.DRAGON_PROTECT_WHOLE_SITE = False
        self.client = dragon_app.app.test_client()

    def test_health_endpoint_returns_expected_shape(self):
        response = self.client.get("/api/v1/health")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.get_json(),
            {
                "ok": True,
                "service": "dragon",
                "api_version": "v1",
            },
        )

    def test_home_endpoint_reads_local_snapshots(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)
            reading_path = temp_dir / "reading_data.json"
            chess_path = temp_dir / "chess_data.json"
            books_path = temp_dir / "books_snapshot.json"
            exports_dir = temp_dir / "exports"
            youtube_path = temp_dir / "youtube_latest_snapshot.json"
            exports_dir.mkdir(parents=True, exist_ok=True)

            reading_path.write_text(json.dumps({"entries": [{"id": "a"}, {"id": "b"}]}), encoding="utf-8")
            chess_path.write_text(json.dumps({"games": [{"id": "g1"}]}), encoding="utf-8")
            books_path.write_text(json.dumps({"entries": [{"id": "book-1"}]}), encoding="utf-8")
            (exports_dir / "movies_export.json").write_text(json.dumps([{"id": "m1"}, {"id": "m2"}, {"id": "m3"}, {"id": "m4"}]), encoding="utf-8")
            youtube_path.write_text(
                json.dumps(
                    {
                        "groups": {
                            "group-a": {"videos": [{"id": "v1"}, {"id": "v2"}]},
                            "group-b": {"videos": [{"id": "v3"}]},
                        }
                    }
                ),
                encoding="utf-8",
            )

            with patch("domains.api.v1.READING_DATA_PATH", reading_path), patch("domains.api.v1.CHESS_DATA_PATH", chess_path), patch(
                "domains.api.v1.BOOKS_SNAPSHOT_PATH", books_path
            ), patch("domains.api.v1.EXPORTS_DIR", exports_dir), patch("domains.api.v1.YOUTUBE_LATEST_SNAPSHOT_PATH", youtube_path):
                response = self.client.get("/api/v1/home")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(
            payload,
            {
                "ok": True,
                "service": "dragon",
                "api_version": "v1",
                "sections": [
                    {"key": "articles", "label": "Articles", "status": "available", "count": 2},
                    {"key": "movies", "label": "Movies", "status": "available", "count": 4},
                    {"key": "books", "label": "Books", "status": "available", "count": 1},
                    {"key": "youtube", "label": "YouTube", "status": "available", "count": 3},
                    {"key": "chess", "label": "Chess", "status": "available", "count": 1},
                ],
            },
        )

    def test_home_endpoint_handles_missing_snapshot_files(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)
            missing_exports_dir = temp_dir / "exports"
            with patch("domains.api.v1.READING_DATA_PATH", temp_dir / "missing_reading.json"), patch(
                "domains.api.v1.CHESS_DATA_PATH", temp_dir / "missing_chess.json"
            ), patch("domains.api.v1.BOOKS_SNAPSHOT_PATH", temp_dir / "missing_books.json"), patch(
                "domains.api.v1.EXPORTS_DIR", missing_exports_dir
            ), patch(
                "domains.api.v1.YOUTUBE_LATEST_SNAPSHOT_PATH", temp_dir / "missing_youtube.json"
            ):
                response = self.client.get("/api/v1/home")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(
            payload["sections"],
            [
                {"key": "articles", "label": "Articles", "status": "unknown", "count": None},
                {"key": "movies", "label": "Movies", "status": "unknown", "count": None},
                {"key": "books", "label": "Books", "status": "unknown", "count": None},
                {"key": "youtube", "label": "YouTube", "status": "unknown", "count": None},
                {"key": "chess", "label": "Chess", "status": "unknown", "count": None},
            ],
        )

    def test_articles_endpoint_limits_and_sorts_local_entries(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)
            reading_path = temp_dir / "reading_data.json"
            reading_path.write_text(
                json.dumps(
                    {
                        "entries": [
                            {
                                "id": "older",
                                "title": "Older item",
                                "source": "source-a",
                                "url": "https://example.com/older",
                                "published_at": "2026-01-01T10:00:00Z",
                                "saved_at": "2026-01-01T10:05:00Z",
                                "excerpt": "Older excerpt",
                                "content_html": "<p>hidden</p>",
                                "content_text": "hidden",
                            },
                            {
                                "id": "newer",
                                "title": "Newer item",
                                "source": "source-b",
                                "url": "https://example.com/newer",
                                "published_at": "2026-01-02T10:00:00Z",
                                "saved_at": "2026-01-02T10:05:00Z",
                                "excerpt": "Newer excerpt",
                            },
                            {
                                "id": "middle",
                                "title": "Middle item",
                                "source": "source-c",
                                "url": "https://example.com/middle",
                                "saved_at": "2026-01-01T12:00:00Z",
                                "excerpt": "Middle excerpt",
                            },
                        ]
                    }
                ),
                encoding="utf-8",
            )

            with patch("domains.api.v1.READING_DATA_PATH", reading_path):
                response = self.client.get("/api/v1/articles", query_string={"limit": 5})

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["api_version"], "v1")
        self.assertEqual(payload["count"], 3)
        self.assertEqual([item["id"] for item in payload["items"]], ["newer", "middle", "older"])
        self.assertEqual(len(payload["items"]), 3)
        self.assertEqual(payload["items"][0], {
            "id": "newer",
            "title": "Newer item",
            "source": "source-b",
            "url": "https://example.com/newer",
            "published_at": "2026-01-02T10:00:00Z",
            "saved_at": "2026-01-02T10:05:00Z",
            "excerpt": "Newer excerpt",
        })
        body = response.get_data(as_text=True)
        self.assertNotIn("content_html", body.lower())
        self.assertNotIn("content_text", body.lower())

    def test_articles_endpoint_returns_empty_payload_for_missing_or_malformed_data(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)
            missing_path = temp_dir / "missing_reading.json"
            malformed_path = temp_dir / "malformed_reading.json"
            malformed_path.write_text("{not-json", encoding="utf-8")

            with patch("domains.api.v1.READING_DATA_PATH", missing_path):
                missing_response = self.client.get("/api/v1/articles")
            with patch("domains.api.v1.READING_DATA_PATH", malformed_path):
                malformed_response = self.client.get("/api/v1/articles")

        for response in (missing_response, malformed_response):
            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertEqual(payload, {"ok": True, "api_version": "v1", "items": [], "count": 0})

    def test_me_endpoint_reports_auth_state_without_leaking_secrets(self):
        dragon_app.app.config["SESSION_COOKIE_SECURE"] = True
        with self.client.session_transaction() as session:
            session["dragon_authenticated"] = True

        response = self.client.get("/api/v1/me")
        payload = response.get_json()
        body = response.get_data(as_text=True)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            payload,
            {
                "ok": True,
                "authenticated": True,
                "production": True,
            },
        )
        self.assertNotIn("dragon_authenticated", body)
        self.assertNotIn("password", body.lower())
        self.assertNotIn("token", body.lower())
        self.assertNotIn("secret", body.lower())
        self.assertNotIn("path", body.lower())

    def test_chess_home_endpoint_projects_existing_data(self):
        with patch("domains.chess.api_projection.load_chess_data", return_value={
            "profiles": [{"id": "p1"}],
            "games": [{"id": "g1"}, {"id": "g2"}],
            "review_queue": [{"id": "rq1"}],
            "puzzle_seeds": [],
            "auto_puzzle_candidates": [{"id": "c1"}],
        }), patch("domains.chess.api_projection.load_chess_courses_data", return_value={
            "courses": [{"id": "c1"}, {"id": "c2"}, {"id": "c3"}],
            "updated_at": "2026-01-01T00:00:00Z",
        }):
            response = self.client.get("/api/v1/chess/home")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["section"], "chess")
        self.assertEqual(payload["title"], "Lotus Chess")
        self.assertEqual(payload["available"], True)
        self.assertEqual(payload["summary"], {
            "games_count": 2,
            "profiles_count": 1,
            "courses_count": 3,
            "training_available": True,
        })
        self.assertEqual(
            payload["next_actions"],
            [
                {"key": "train_today", "label": "Train Today"},
                {"key": "games", "label": "Games"},
                {"key": "openings", "label": "Openings"},
            ],
        )

    def test_chess_home_endpoint_handles_missing_data(self):
        with patch("domains.chess.api_projection.load_chess_data", side_effect=FileNotFoundError("missing")), patch(
            "domains.chess.api_projection.load_chess_courses_data",
            side_effect=FileNotFoundError("missing"),
        ):
            response = self.client.get("/api/v1/chess/home")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["summary"], {
            "games_count": 0,
            "profiles_count": 0,
            "courses_count": 0,
            "training_available": False,
        })
        self.assertEqual(
            payload["next_actions"],
            [
                {"key": "train_today", "label": "Train Today"},
                {"key": "games", "label": "Games"},
                {"key": "openings", "label": "Openings"},
            ],
        )

    def test_chess_games_endpoint_defaults_limit_and_omits_sensitive_fields(self):
        mocked_games = [
            {
                "id": f"g{i}",
                "source": "lichess" if i % 2 == 0 else "chess.com",
                "white": "Alpha",
                "black": "Beta",
                "user_color": "white",
                "user_result": "win" if i % 3 == 0 else "loss",
                "result": "1-0",
                "date": "2026-01-01",
                "time_class": "rapid",
                "opening": {"name": "Test Opening", "eco": "C20"},
                "pgn": "hidden",
                "moves": ["e4", "e5"],
                "raw_source": {"token": "hidden"},
            }
            for i in range(120)
        ]
        with patch("domains.chess.api_projection.load_chess_data", return_value={"games": mocked_games}):
            response = self.client.get("/api/v1/chess/games")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["section"], "chess")
        self.assertEqual(payload["limit"], 50)
        self.assertEqual(payload["offset"], 0)
        self.assertEqual(payload["count"], 120)
        self.assertEqual(len(payload["items"]), 50)
        body = response.get_data(as_text=True)
        self.assertNotIn("pgn", body.lower())
        self.assertNotIn("moves", body.lower())
        self.assertNotIn("raw_source", body.lower())
        self.assertNotIn("token", body.lower())
        self.assertNotIn("secret", body.lower())
        self.assertNotIn("path", body.lower())

    def test_chess_games_endpoint_caps_limit_at_hundred_and_supports_filters(self):
        mocked_games = [
            {
                "id": "lichess-win",
                "source": "lichess",
                "white": "White",
                "black": "Black",
                "user_color": "black",
                "user_result": "win",
                "result": "0-1",
                "date": "2026-01-02",
                "time_class": "blitz",
                "opening": {"name": "French Defense", "eco": "C00"},
            },
            {
                "id": "chesscom-loss",
                "source": "chess.com",
                "white": "One",
                "black": "Two",
                "user_color": "white",
                "user_result": "loss",
                "result": "0-1",
                "date": "2026-01-03",
                "time_class": "rapid",
                "opening": {"name": "Queen's Gambit", "eco": "D06"},
            },
            {
                "id": "lichess-draw",
                "source": "lichess",
                "white": "A",
                "black": "B",
                "user_color": "white",
                "user_result": "draw",
                "result": "1/2-1/2",
                "date": "2026-01-04",
                "time_class": "rapid",
                "opening": {"name": "Italian Game", "eco": "C50"},
            },
        ]
        with patch("domains.chess.api_projection.load_chess_data", return_value={"games": mocked_games}):
            response = self.client.get("/api/v1/chess/games", query_string={"limit": 999, "source": "lichess", "result": "win"})

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["limit"], 100)
        self.assertEqual(payload["offset"], 0)
        self.assertEqual(payload["count"], 1)
        self.assertEqual(len(payload["items"]), 1)
        item = payload["items"][0]
        self.assertEqual(item["id"], "lichess-win")
        self.assertEqual(item["source"], "lichess")
        self.assertEqual(item["user_result"], "win")
        self.assertEqual(item["opening"], {"name": "French Defense", "eco": "C00"})

    def test_chess_games_endpoint_handles_missing_data(self):
        with patch("domains.chess.api_projection.load_chess_data", side_effect=FileNotFoundError("missing")):
            response = self.client.get("/api/v1/chess/games")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["items"], [])
        self.assertEqual(payload["count"], 0)
        self.assertEqual(payload["limit"], 50)
        self.assertEqual(payload["offset"], 0)

    def test_chess_game_detail_endpoint_returns_safe_item(self):
        game = {
            "id": "lichess:game 1",
            "source": "lichess",
            "white": "Alpha",
            "black": "Beta",
            "user_color": "black",
            "user_result": "win",
            "result": "0-1",
            "date": "2026-01-05",
            "time_class": "rapid",
            "time_control": "10+0",
            "rated": True,
            "url": "https://lichess.org/game1",
            "opening": {"name": "French Defense", "eco": "C00", "variation": "Advance"},
            "pgn": "[Event \"hidden\"]",
            "moves": ["e4", "e5"],
            "raw_source": {"token": "hidden"},
        }
        with patch("domains.chess.api_projection.load_chess_data", return_value={"games": [game]}):
            response = self.client.get("/api/v1/chess/games/lichess:game%201")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["section"], "chess")
        self.assertEqual(payload["item"]["id"], "lichess:game 1")
        self.assertEqual(payload["item"]["source"], "lichess")
        self.assertEqual(payload["item"]["opening"], {"name": "French Defense", "eco": "C00", "variation": "Advance"})
        self.assertTrue(payload["item"]["pgn_available"])
        self.assertTrue(payload["item"]["moves_available"])
        self.assertEqual(
            set(payload["item"].keys()),
            {
                "id",
                "source",
                "white",
                "black",
                "user_color",
                "user_result",
                "result",
                "date",
                "time_class",
                "time_control",
                "opening",
                "url",
                "rated",
                "pgn_available",
                "moves_available",
            },
        )
        body = response.get_data(as_text=True)
        self.assertNotIn("raw_source", body.lower())
        self.assertNotIn("token", body.lower())
        self.assertNotIn("secret", body.lower())
        self.assertNotIn("path", body.lower())

    def test_chess_game_detail_endpoint_handles_missing_game(self):
        with patch("domains.chess.api_projection.load_chess_data", return_value={"games": []}):
            response = self.client.get("/api/v1/chess/games/missing-id")

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.get_json(), {"ok": False, "error": "game_not_found"})

    def test_chess_game_detail_endpoint_supports_url_encoded_ids(self):
        game_id = "lichess:space id"
        encoded_id = quote(game_id, safe="")
        with patch("domains.chess.api_projection.load_chess_data", return_value={
            "games": [
                {
                    "id": game_id,
                    "source": "lichess",
                    "white": "A",
                    "black": "B",
                    "user_color": "white",
                    "user_result": "draw",
                    "result": "1/2-1/2",
                    "date": "2026-01-06",
                    "time_class": "blitz",
                    "time_control": "3+2",
                    "rated": False,
                    "url": "",
                    "opening": {"name": "", "eco": "", "variation": ""},
                    "moves": [],
                }
            ]
        }):
            response = self.client.get(f"/api/v1/chess/games/{encoded_id}")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["item"]["id"], game_id)
        self.assertEqual(payload["item"]["user_result"], "draw")

    def test_chess_train_today_endpoint_returns_ok_true(self):
        with patch("domains.chess.api_projection.load_chess_data", return_value={
            "review_queue": [
                {
                    "id": "review-1",
                    "type": "game",
                    "title": "Alpha vs Beta",
                    "reason": "Review from your games.",
                    "game_id": "game-1",
                    "opening_label": "C00 · French Defense",
                    "status": "active",
                }
            ],
            "auto_puzzle_candidates": [
                {
                    "id": "candidate-1",
                    "training_type_label": "Opening repair",
                    "title": "French Defense",
                    "subtitle": "Low score branch",
                    "game_id": "game-2",
                    "opening_label": "C00 · French Defense",
                    "opening_eco": "C00",
                    "priority_score": 88,
                    "status": "candidate",
                }
            ],
        }):
            response = self.client.get("/api/v1/chess/train-today")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["section"], "chess")
        self.assertEqual(payload["title"], "Train Today")
        self.assertIsInstance(payload["items"], list)
        self.assertGreaterEqual(payload["count"], 1)
        self.assertEqual(payload["available"], True)
        first = payload["items"][0]
        self.assertIn(first["type"], {"opening_repair", "win_the_position", "puzzle", "review", "unknown"})
        self.assertIn("opening", first)
        self.assertIn("priority", first)
        self.assertIn("completed", first)
        body = response.get_data(as_text=True)
        self.assertNotIn("pgn", body.lower())
        self.assertNotIn("moves", body.lower())
        self.assertNotIn("raw_source", body.lower())
        self.assertNotIn("token", body.lower())
        self.assertNotIn("secret", body.lower())
        self.assertNotIn("path", body.lower())

    def test_chess_train_today_endpoint_handles_missing_data(self):
        with patch("domains.chess.api_projection.load_chess_data", side_effect=FileNotFoundError("missing")):
            response = self.client.get("/api/v1/chess/train-today")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["section"], "chess")
        self.assertEqual(payload["title"], "Train Today")
        self.assertEqual(payload["items"], [])
        self.assertEqual(payload["count"], 0)
        self.assertEqual(payload["available"], False)

    def test_chess_openings_endpoint_returns_ok_true(self):
        mocked_games = [
            {
                "id": "g1",
                "source": "lichess",
                "white": "A",
                "black": "B",
                "user_color": "white",
                "user_result": "win",
                "result": "1-0",
                "date": "2026-01-01",
                "time_class": "rapid",
                "opening": {"name": "French Defense", "eco": "C00"},
            },
            {
                "id": "g2",
                "source": "lichess",
                "white": "C",
                "black": "D",
                "user_color": "white",
                "user_result": "loss",
                "result": "0-1",
                "date": "2026-01-02",
                "time_class": "blitz",
                "opening": {"name": "French Defense", "eco": "C00"},
            },
            {
                "id": "g3",
                "source": "chess.com",
                "white": "E",
                "black": "F",
                "user_color": "black",
                "user_result": "draw",
                "result": "1/2-1/2",
                "date": "2026-01-03",
                "time_class": "rapid",
                "opening": {"name": "Queen's Gambit", "eco": "D06"},
            },
            {
                "id": "g4",
                "source": "lichess",
                "white": "G",
                "black": "H",
                "user_color": "white",
                "user_result": "loss",
                "result": "0-1",
                "date": "2026-01-04",
                "time_class": "rapid",
                "opening": {"name": "", "eco": "A40"},
            },
            {
                "id": "g5",
                "source": "lichess",
                "white": "I",
                "black": "J",
                "user_color": "black",
                "user_result": "draw",
                "result": "1/2-1/2",
                "date": "2026-01-05",
                "time_class": "rapid",
                "opening": {"name": "", "eco": ""},
            },
        ]
        with patch("domains.chess.api_projection.load_chess_data", return_value={"games": mocked_games}):
            response = self.client.get("/api/v1/chess/openings")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["section"], "chess")
        self.assertEqual(payload["title"], "Openings")
        self.assertEqual(payload["count"], 4)
        self.assertEqual(len(payload["items"]), 4)
        first = payload["items"][0]
        self.assertEqual(
            set(first.keys()),
            {
                "key",
                "name",
                "eco",
                "side",
                "games_count",
                "wins",
                "losses",
                "draws",
                "score_label",
                "needs_work",
            },
        )
        self.assertTrue(all(item["name"] for item in payload["items"]))
        self.assertTrue(all(not str(item["key"]).endswith("|") for item in payload["items"]))
        self.assertIn("A40 Opening", [item["name"] for item in payload["items"]])
        self.assertIn("Unknown Opening", [item["name"] for item in payload["items"]])
        body = response.get_data(as_text=True)
        self.assertNotIn("pgn", body.lower())
        self.assertNotIn("moves", body.lower())
        self.assertNotIn("raw_source", body.lower())
        self.assertNotIn("token", body.lower())
        self.assertNotIn("secret", body.lower())
        self.assertNotIn("path", body.lower())

    def test_chess_openings_endpoint_caps_limit_and_filters_safely(self):
        mocked_games = []
        for i in range(120):
            opening_index = i
            for j in range(3):
                mocked_games.append(
                    {
                        "id": f"g{i}-{j}",
                        "source": "lichess" if (i + j) % 2 == 0 else "chess.com",
                        "white": "A",
                        "black": "B",
                        "user_color": "white",
                        "user_result": "loss" if j < 2 else "win",
                        "result": "1-0" if j == 2 else "0-1",
                        "date": "2026-01-01",
                        "time_class": "rapid",
                        "opening": {"name": f"Opening {opening_index}", "eco": f"C{opening_index:02d}"},
                    }
                )
        with patch("domains.chess.api_projection.load_chess_data", return_value={"games": mocked_games}):
            response = self.client.get("/api/v1/chess/openings", query_string={"limit": 999, "side": "white", "needs_work": "true"})

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["count"], 120)
        self.assertLessEqual(len(payload["items"]), 100)
        self.assertTrue(all(item["side"] == "white" for item in payload["items"]))
        self.assertTrue(all(item["needs_work"] for item in payload["items"]))

    def test_chess_openings_endpoint_handles_missing_data(self):
        with patch("domains.chess.api_projection.load_chess_data", side_effect=FileNotFoundError("missing")):
            response = self.client.get("/api/v1/chess/openings")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["items"], [])
        self.assertEqual(payload["count"], 0)

    def test_chess_courses_endpoint_returns_ok_true(self):
        with patch("domains.chess.api_projection.load_chess_courses_data", return_value={
            "courses": [
                {
                    "id": "course-1",
                    "title": "Opening Principles",
                    "category": "opening",
                    "source": "youtube",
                    "url": "https://example.com/opening-principles",
                    "related_opening_key": "c00|french defense",
                    "related_opening_label": "French Defense",
                    "level": "beginner",
                    "status": "active",
                    "notes": "Intro to core ideas.",
                    "created_at": "2026-01-01",
                    "updated_at": "2026-01-02",
                },
                {
                    "id": "course-2",
                    "title": "Endgame Basics",
                    "category": "endgame",
                    "source": "book",
                    "url": "https://example.com/endgame-basics",
                    "related_opening_key": "",
                    "related_opening_label": "",
                    "level": "",
                    "status": "planned",
                    "notes": "",
                },
            ],
            "updated_at": "2026-01-02T00:00:00Z",
        }):
            response = self.client.get("/api/v1/chess/courses")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["section"], "chess")
        self.assertEqual(payload["title"], "Courses")
        self.assertEqual(payload["count"], 2)
        self.assertEqual(len(payload["items"]), 2)
        first = payload["items"][0]
        self.assertEqual(
            set(first.keys()),
            {
                "id",
                "title",
                "category",
                "source",
                "url",
                "related_opening_key",
                "related_opening_label",
                "level",
                "status",
                "notes",
            },
        )
        body = response.get_data(as_text=True)
        self.assertNotIn("path", body.lower())
        self.assertNotIn("secret", body.lower())
        self.assertNotIn("token", body.lower())
        self.assertNotIn("api_key", body.lower())

    def test_chess_courses_endpoint_caps_limit_and_filters_safely(self):
        mocked_courses = []
        for i in range(120):
            mocked_courses.append(
                {
                    "id": f"course-{i}",
                    "title": f"Course {i}",
                    "category": "opening" if i % 2 == 0 else "endgame",
                    "source": "youtube" if i % 3 == 0 else "book",
                    "url": f"https://example.com/course-{i}",
                    "related_opening_key": f"c{i:02d}|opening",
                    "related_opening_label": f"Opening {i}",
                    "level": "beginner" if i % 2 == 0 else "advanced",
                    "status": "active" if i % 4 else "planned",
                    "notes": f"Notes {i}",
                }
            )
        with patch("domains.chess.api_projection.load_chess_courses_data", return_value={"courses": mocked_courses}):
            response = self.client.get("/api/v1/chess/courses", query_string={"limit": 999, "category": "opening", "status": "active"})

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["count"], 30)
        self.assertLessEqual(len(payload["items"]), 100)
        self.assertTrue(all(item["category"] == "opening" for item in payload["items"]))
        self.assertTrue(all(item["status"] == "active" for item in payload["items"]))

    def test_chess_courses_endpoint_defaults_limit_to_fifty(self):
        mocked_courses = [
            {
                "id": f"course-{i}",
                "title": f"Course {i}",
                "category": "opening",
                "source": "youtube",
                "url": f"https://example.com/course-{i}",
                "related_opening_key": f"c{i:02d}|opening",
                "related_opening_label": f"Opening {i}",
                "level": "beginner",
                "status": "active",
                "notes": f"Notes {i}",
            }
            for i in range(80)
        ]
        with patch("domains.chess.api_projection.load_chess_courses_data", return_value={"courses": mocked_courses}):
            response = self.client.get("/api/v1/chess/courses")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["count"], 80)
        self.assertEqual(len(payload["items"]), 50)

    def test_chess_courses_endpoint_caps_limit_at_hundred(self):
        mocked_courses = [
            {
                "id": f"course-{i}",
                "title": f"Course {i}",
                "category": "opening",
                "source": "youtube",
                "url": f"https://example.com/course-{i}",
                "related_opening_key": f"c{i:02d}|opening",
                "related_opening_label": f"Opening {i}",
                "level": "beginner",
                "status": "active",
                "notes": f"Notes {i}",
            }
            for i in range(120)
        ]
        with patch("domains.chess.api_projection.load_chess_courses_data", return_value={"courses": mocked_courses}):
            response = self.client.get("/api/v1/chess/courses", query_string={"limit": 999})

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["count"], 120)
        self.assertEqual(len(payload["items"]), 100)

    def test_chess_courses_endpoint_handles_missing_data(self):
        with patch("domains.chess.api_projection.load_chess_courses_data", side_effect=FileNotFoundError("missing")):
            response = self.client.get("/api/v1/chess/courses")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["items"], [])
        self.assertEqual(payload["count"], 0)

    def test_chess_progress_endpoint_returns_ok_true(self):
        with patch("domains.chess.api_projection.load_chess_data", return_value={
            "profiles": [{"id": "p1"}],
            "games": [
                {
                    "id": "g1",
                    "source": "lichess",
                    "user_result": "win",
                    "opening": {"name": "French Defense", "eco": "C00"},
                },
                {
                    "id": "g2",
                    "source": "chess.com",
                    "user_result": "loss",
                    "opening": {"name": "Queen's Gambit", "eco": "D06"},
                },
                {
                    "id": "g3",
                    "source": "lichess",
                    "user_result": "draw",
                    "opening": {"name": "Queen's Gambit", "eco": "D06"},
                },
                {
                    "id": "g4",
                    "source": "lichess",
                    "user_result": "",
                    "opening": {"name": "A40 Opening", "eco": "A40"},
                },
            ],
            "review_queue": [
                {"id": "rq1", "status": "active"},
                {"id": "rq2", "status": "done"},
            ],
            "puzzle_seeds": [],
            "auto_puzzle_candidates": [
                {"id": "c1", "status": "candidate"},
                {"id": "c2", "status": "done"},
            ],
        }), patch("domains.chess.api_projection.load_chess_courses_data", return_value={
            "courses": [
                {"id": "course-1", "title": "Opening Principles"},
                {"id": "course-2", "title": "Endgame Basics"},
            ],
            "updated_at": "2026-01-02T00:00:00Z",
        }):
            response = self.client.get("/api/v1/chess/progress")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["section"], "chess")
        self.assertEqual(payload["title"], "Progress")
        self.assertEqual(payload["summary"], {
            "games_count": 4,
            "profiles_count": 1,
            "openings_count": 3,
            "courses_count": 2,
            "training_count": 2,
            "wins": 1,
            "losses": 1,
            "draws": 1,
            "unknown_results": 1,
            "review_due_count": 1,
        })
        body = response.get_data(as_text=True)
        self.assertNotIn("pgn", body.lower())
        self.assertNotIn("moves", body.lower())
        self.assertNotIn("raw_source", body.lower())
        self.assertNotIn("path", body.lower())
        self.assertNotIn("secret", body.lower())
        self.assertNotIn("token", body.lower())
        self.assertNotIn("api_key", body.lower())
        self.assertNotIn("env", body.lower())

    def test_chess_progress_endpoint_handles_missing_data(self):
        with patch("domains.chess.api_projection.load_chess_data", side_effect=FileNotFoundError("missing")), patch(
            "domains.chess.api_projection.load_chess_courses_data",
            side_effect=FileNotFoundError("missing"),
        ):
            response = self.client.get("/api/v1/chess/progress")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["title"], "Progress")
        self.assertEqual(payload["summary"], {
            "games_count": 0,
            "profiles_count": 0,
            "openings_count": 0,
            "courses_count": 0,
            "training_count": 0,
            "wins": 0,
            "losses": 0,
            "draws": 0,
            "unknown_results": 0,
            "review_due_count": 0,
        })
