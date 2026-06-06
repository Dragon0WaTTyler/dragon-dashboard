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

