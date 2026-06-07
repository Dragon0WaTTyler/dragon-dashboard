"""Versioned JSON API routes for Dragon."""

from __future__ import annotations

from flask import Blueprint, current_app, jsonify, request, session

from domains.chess.api_projection import (
    build_chess_game_detail_projection,
    build_chess_games_projection,
    build_chess_courses_projection,
    build_chess_home_projection,
    build_chess_openings_projection,
    build_chess_train_today_projection,
)

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


@api_v1_bp.get("/api/v1/chess/games/<path:game_id>")
def api_v1_chess_game_detail(game_id):
    payload = build_chess_game_detail_projection(game_id)
    if payload is None:
        return jsonify({"ok": False, "error": "game_not_found"}), 404
    return jsonify(payload)


@api_v1_bp.get("/api/v1/chess/train-today")
def api_v1_chess_train_today():
    return jsonify(build_chess_train_today_projection())


@api_v1_bp.get("/api/v1/chess/openings")
def api_v1_chess_openings():
    return jsonify(
        build_chess_openings_projection(
            limit=request.args.get("limit", 50),
            offset=request.args.get("offset", 0),
            side=request.args.get("side", ""),
            needs_work=request.args.get("needs_work", ""),
        )
    )


@api_v1_bp.get("/api/v1/chess/courses")
def api_v1_chess_courses():
    return jsonify(
        build_chess_courses_projection(
            limit=request.args.get("limit", 50),
            offset=request.args.get("offset", 0),
            category=request.args.get("category", ""),
            status=request.args.get("status", ""),
        )
    )
