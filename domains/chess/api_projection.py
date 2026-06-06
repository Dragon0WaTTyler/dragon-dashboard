"""Safe chess API projections for external clients."""

from __future__ import annotations

from typing import Any

from dragon.cache import load_json_file
from dragon.paths import CHESS_COURSES_PATH

from .runtime import default_chess_data, load_chess_data


def default_chess_courses():
    return {
        "courses": [],
        "updated_at": "",
    }


def load_chess_courses_data():
    raw = load_json_file(CHESS_COURSES_PATH, default_chess_courses())
    if not isinstance(raw, dict):
        raw = {}
    data = default_chess_courses()
    courses = raw.get("courses", [])
    data["courses"] = [dict(item) for item in courses if isinstance(item, dict)] if isinstance(courses, list) else []
    data["updated_at"] = str(raw.get("updated_at", "") or "").strip()
    return data


def _safe_load(loader, fallback):
    try:
        data = loader()
    except Exception:
        data = fallback() if callable(fallback) else fallback
    if not isinstance(data, dict):
        data = fallback() if callable(fallback) else fallback
    return data


def _count_dict_items(items: Any) -> int:
    return len([item for item in (items or []) if isinstance(item, dict)])


def _training_available(chess_data):
    payload = chess_data if isinstance(chess_data, dict) else default_chess_data()
    return bool(
        _count_dict_items(payload.get("games", []))
        or _count_dict_items(payload.get("review_queue", []))
        or _count_dict_items(payload.get("puzzle_seeds", []))
        or _count_dict_items(payload.get("auto_puzzle_candidates", []))
    )


def build_chess_home_projection():
    chess_data = _safe_load(load_chess_data, default_chess_data)
    chess_courses_data = _safe_load(load_chess_courses_data, default_chess_courses)

    profiles_count = _count_dict_items(chess_data.get("profiles", []))
    games_count = _count_dict_items(chess_data.get("games", []))
    courses_count = _count_dict_items(chess_courses_data.get("courses", []))

    return {
        "ok": True,
        "section": "chess",
        "title": "Lotus Chess",
        "available": True,
        "summary": {
            "games_count": games_count,
            "profiles_count": profiles_count,
            "courses_count": courses_count,
            "training_available": _training_available(chess_data),
        },
        "next_actions": [
            {"key": "train_today", "label": "Train Today"},
            {"key": "games", "label": "Games"},
            {"key": "openings", "label": "Openings"},
        ],
    }
