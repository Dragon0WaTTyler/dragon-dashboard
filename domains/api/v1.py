"""Versioned JSON API routes for Dragon."""

from __future__ import annotations

from flask import Blueprint, current_app, jsonify, request, session

from domains.chess.api_projection import build_chess_games_projection, build_chess_home_projection

api_v1_bp = Blueprint("api_v1", __name__)


@api_v1_bp.get("/api/v1/health")
def api_v1_health():
    return jsonify(
        {
            "ok": True,
            "service": "dragon",
            "api_version": "v1",
        }
    )


@api_v1_bp.get("/api/v1/me")
def api_v1_me():
    return jsonify(
        {
            "ok": True,
            "authenticated": bool(session.get("dragon_authenticated")),
            "production": bool(current_app.config.get("SESSION_COOKIE_SECURE")),
        }
    )


@api_v1_bp.get("/api/v1/chess/home")
def api_v1_chess_home():
    return jsonify(build_chess_home_projection())


@api_v1_bp.get("/api/v1/chess/games")
def api_v1_chess_games():
    return jsonify(
        build_chess_games_projection(
            limit=request.args.get("limit", 50),
            offset=request.args.get("offset", 0),
            source=request.args.get("source", ""),
            result=request.args.get("result", ""),
        )
    )
