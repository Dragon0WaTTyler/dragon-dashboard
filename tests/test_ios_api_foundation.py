import unittest
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
