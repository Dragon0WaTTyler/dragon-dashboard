"""Safe chess API projections for external clients."""

from __future__ import annotations

from urllib.parse import unquote
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


def _normalize_chess_source(value):
    source = str(value or "").strip().lower()
    if source in {"chess.com", "lichess"}:
        return source
    return ""


def _normalize_chess_result(value):
    result = str(value or "").strip().lower()
    if result in {"win", "loss", "draw", "unknown"}:
        return result
    return "unknown"


def _safe_text(value, fallback=""):
    text = str(value or "").strip()
    return text if text else str(fallback or "").strip()


def _project_chess_game_item(game):
    payload = game if isinstance(game, dict) else {}
    opening = payload.get("opening", {}) if isinstance(payload.get("opening", {}), dict) else {}
    source = _normalize_chess_source(payload.get("source", ""))
    user_result = _normalize_chess_result(payload.get("user_result", ""))
    result = _safe_text(payload.get("result", ""), "Unknown")
    date_value = _safe_text(payload.get("date", ""), "")
    time_class = _safe_text(payload.get("time_class", ""), "other").lower()
    opening_name = _safe_text(opening.get("name", ""), "")
    opening_eco = _safe_text(opening.get("eco", ""), "")
    return {
        "id": _safe_text(payload.get("id", ""), ""),
        "source": source,
        "white": _safe_text(payload.get("white", ""), ""),
        "black": _safe_text(payload.get("black", ""), ""),
        "user_color": _safe_text(payload.get("user_color", ""), "unknown").lower(),
        "user_result": user_result,
        "result": result,
        "date": date_value,
        "time_class": time_class,
        "opening": {
            "name": opening_name,
            "eco": opening_eco,
        },
    }


def _safe_projected_games(chess_data, *, source=None, result=None):
    payload = chess_data if isinstance(chess_data, dict) else default_chess_data()
    games = [dict(item) for item in (payload.get("games", []) or []) if isinstance(item, dict)]
    source_filter = _normalize_chess_source(source) if source else ""
    result_filter = _normalize_chess_result(result) if result else ""
    projected = []
    for game in games:
        item_source = _normalize_chess_source(game.get("source", ""))
        item_result = _normalize_chess_result(game.get("user_result", ""))
        if source_filter and item_source != source_filter:
            continue
        if result_filter and item_result != result_filter:
            continue
        projected.append(_project_chess_game_item(game))
    return projected


def _find_chess_game(chess_data, game_id):
    payload = chess_data if isinstance(chess_data, dict) else default_chess_data()
    target = unquote(str(game_id or "").strip())
    if not target:
        return None
    for game in (payload.get("games", []) or []):
        if not isinstance(game, dict):
            continue
        game_id_value = str(game.get("id", "") or "").strip()
        source_game_id_value = str(game.get("source_game_id", "") or "").strip()
        if game_id_value == target or source_game_id_value == target:
            return dict(game)
    return None


def _project_chess_game_detail_item(game):
    payload = game if isinstance(game, dict) else {}
    opening = payload.get("opening", {}) if isinstance(payload.get("opening", {}), dict) else {}
    source = _normalize_chess_source(payload.get("source", ""))
    user_result = _normalize_chess_result(payload.get("user_result", ""))
    opening_name = _safe_text(opening.get("name", ""), "")
    opening_eco = _safe_text(opening.get("eco", ""), "")
    opening_variation = _safe_text(opening.get("variation", ""), "")
    return {
        "id": _safe_text(payload.get("id", ""), ""),
        "source": source,
        "white": _safe_text(payload.get("white", ""), ""),
        "black": _safe_text(payload.get("black", ""), ""),
        "user_color": _safe_text(payload.get("user_color", ""), "unknown").lower(),
        "user_result": user_result,
        "result": _safe_text(payload.get("result", ""), "Unknown"),
        "date": _safe_text(payload.get("date", ""), ""),
        "time_class": _safe_text(payload.get("time_class", ""), "other").lower(),
        "time_control": _safe_text(payload.get("time_control", ""), ""),
        "opening": {
            "name": opening_name,
            "eco": opening_eco,
            "variation": opening_variation,
        },
        "url": _safe_text(payload.get("url", ""), ""),
        "rated": bool(payload.get("rated", False)),
        "pgn_available": bool(str(payload.get("pgn", "") or "").strip()),
        "moves_available": bool([move for move in (payload.get("moves", []) or []) if str(move or "").strip()]),
    }


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


def build_chess_games_projection(*, limit=50, offset=0, source=None, result=None):
    chess_data = _safe_load(load_chess_data, default_chess_data)
    all_items = _safe_projected_games(chess_data, source=source, result=result)
    safe_limit = max(0, min(int(limit or 50), 100))
    safe_offset = max(0, int(offset or 0))
    paged_items = all_items[safe_offset:safe_offset + safe_limit] if safe_limit else []
    return {
        "ok": True,
        "section": "chess",
        "items": paged_items,
        "count": len(all_items),
        "limit": safe_limit,
        "offset": safe_offset,
    }


def build_chess_game_detail_projection(game_id):
    chess_data = _safe_load(load_chess_data, default_chess_data)
    game = _find_chess_game(chess_data, game_id)
    if not game:
        return None
    return {
        "ok": True,
        "section": "chess",
        "item": _project_chess_game_detail_item(game),
    }
