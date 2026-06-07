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


def _normalize_chess_side(value):
    side = str(value or "").strip().lower()
    if side in {"white", "black"}:
        return side
    return "unknown"


def _chess_opening_key(game):
    payload = game if isinstance(game, dict) else {}
    opening = payload.get("opening", {}) if isinstance(payload.get("opening", {}), dict) else {}
    eco = _safe_text(opening.get("eco", ""), "").lower()
    name = _safe_text(opening.get("name", ""), "").lower()
    if eco and name:
        return f"{eco}|{name}"
    if eco:
        return eco
    if name:
        return name
    return ""


def _chess_opening_label(opening_name, opening_eco):
    name = _safe_text(opening_name, "")
    eco = _safe_text(opening_eco, "")
    if eco and name:
        return f"{eco} · {name}"
    if name:
        return name
    if eco:
        return f"{eco} Opening"
    return "Unknown Opening"


def _parse_optional_bool(value):
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _project_chess_opening_item(summary):
    payload = summary if isinstance(summary, dict) else {}
    games_count = int(payload.get("games_count", 0) or 0)
    wins = int(payload.get("wins", 0) or 0)
    losses = int(payload.get("losses", 0) or 0)
    draws = int(payload.get("draws", 0) or 0)
    score_percent = float(payload.get("score_percent", 0.0) or 0.0)
    opening_name = _safe_text(payload.get("name", ""), "")
    opening_eco = _safe_text(payload.get("eco", ""), "")
    return {
        "key": _safe_text(payload.get("key", ""), ""),
        "name": opening_name or _chess_opening_label("", opening_eco),
        "eco": opening_eco,
        "side": _normalize_chess_side(payload.get("side", "")),
        "games_count": games_count,
        "wins": wins,
        "losses": losses,
        "draws": draws,
        "score_label": f"{round(score_percent, 1)}%",
        "needs_work": bool(payload.get("needs_work", False)),
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


def _normalize_train_today_type(value):
    normalized = str(value or "").strip().lower().replace(" ", "_")
    if normalized in {"opening_repair", "win_the_position", "puzzle", "review"}:
        return normalized
    if normalized in {"opening repair"}:
        return "opening_repair"
    if normalized in {"win the position"}:
        return "win_the_position"
    if normalized in {"review from your games", "review_from_your_games"}:
        return "review"
    return "unknown"


def _safe_train_today_opening(item):
    payload = item if isinstance(item, dict) else {}
    return {
        "name": _safe_text(payload.get("opening_label", "") or payload.get("opening_name", ""), ""),
        "eco": _safe_text(payload.get("opening_eco", ""), ""),
    }


def _project_train_today_item(item, *, priority=0):
    payload = item if isinstance(item, dict) else {}
    item_type = _normalize_train_today_type(payload.get("training_type_label", payload.get("type", "")))
    if item_type == "unknown":
        raw_type = str(payload.get("type", "") or "").strip().lower()
        if raw_type in {"game", "opening", "line"}:
            item_type = "review"
    source_game_id = _safe_text(payload.get("game_id", "") or payload.get("source_game_id", ""), "")
    if not source_game_id and str(payload.get("id", "") or "").strip().startswith("review-"):
        source_game_id = _safe_text(payload.get("game_id", ""), "")
    completed = str(payload.get("status", "") or "").strip().lower() == "done"
    title = _safe_text(payload.get("title", ""), "")
    subtitle = _safe_text(payload.get("subtitle", ""), "")
    if not subtitle:
        subtitle = _safe_text(payload.get("reason", ""), "")
    if not subtitle:
        subtitle = _safe_text(payload.get("line_label", "") or payload.get("opening_label", ""), "")
    return {
        "id": _safe_text(payload.get("id", ""), f"train-{priority}"),
        "type": item_type,
        "title": title or "Training item",
        "subtitle": subtitle,
        "source_game_id": source_game_id,
        "opening": _safe_train_today_opening(payload),
        "priority": max(0, int(payload.get("priority_score", priority) or priority)),
        "completed": bool(completed),
    }


def build_chess_train_today_projection():
    chess_data = _safe_load(load_chess_data, default_chess_data)
    payload = chess_data if isinstance(chess_data, dict) else default_chess_data()
    items = []
    seen_ids = set()

    review_snapshot = [dict(item) for item in (payload.get("review_queue", []) or []) if isinstance(item, dict)]
    for index, review_item in enumerate(review_snapshot):
        status = str(review_item.get("status", "active") or "active").strip().lower() or "active"
        if status == "done":
            continue
        projected = _project_train_today_item(
            {
                "id": review_item.get("id", ""),
                "type": review_item.get("type", "game"),
                "title": review_item.get("title", ""),
                "subtitle": review_item.get("reason", ""),
                "game_id": review_item.get("game_id", ""),
                "opening_label": review_item.get("opening_label", ""),
                "opening_eco": "",
                "status": status,
                "training_type_label": "Review from your games",
            },
            priority=index,
        )
        if projected["id"] in seen_ids:
            continue
        items.append(projected)
        seen_ids.add(projected["id"])

    candidate_snapshot = [dict(item) for item in (payload.get("auto_puzzle_candidates", []) or []) if isinstance(item, dict)]
    for index, candidate in enumerate(candidate_snapshot):
        status = str(candidate.get("status", "candidate") or "candidate").strip().lower() or "candidate"
        if status not in {"candidate", "queued", "training"}:
            continue
        projected = _project_train_today_item(candidate, priority=100 + index)
        if projected["id"] in seen_ids:
            continue
        if projected["type"] == "unknown":
            projected["type"] = "puzzle"
        items.append(projected)
        seen_ids.add(projected["id"])

    items.sort(key=lambda item: (-int(item.get("priority", 0) or 0), str(item.get("title", "") or "").lower()))
    return {
        "ok": True,
        "section": "chess",
        "title": "Train Today",
        "available": bool(items),
        "items": items,
        "count": len(items),
    }


def build_chess_openings_projection(*, limit=50, offset=0, side=None, needs_work=None):
    chess_data = _safe_load(load_chess_data, default_chess_data)
    payload = chess_data if isinstance(chess_data, dict) else default_chess_data()
    games = [dict(item) for item in (payload.get("games", []) or []) if isinstance(item, dict)]
    side_filter = _normalize_chess_side(side) if side else ""
    needs_work_filter = _parse_optional_bool(needs_work)
    openings_map = {}

    for game in games:
        opening = game.get("opening", {}) if isinstance(game.get("opening", {}), dict) else {}
        opening_name = _safe_text(opening.get("name", ""), "")
        opening_eco = _safe_text(opening.get("eco", ""), "")
        key = _chess_opening_key(game)
        display_name = _chess_opening_label(opening_name, opening_eco)
        if not key:
            key = display_name.lower()
        item = openings_map.setdefault(
            key,
            {
                "key": key,
                "name": display_name,
                "eco": opening_eco,
                "side_counts": {"white": 0, "black": 0},
                "games_count": 0,
                "wins": 0,
                "losses": 0,
                "draws": 0,
                "score_total": 0.0,
            },
        )
        item["games_count"] += 1
        user_side = _normalize_chess_side(game.get("user_color", ""))
        if user_side in {"white", "black"}:
            item["side_counts"][user_side] += 1
        result = _normalize_chess_result(game.get("user_result", ""))
        if result == "win":
            item["wins"] += 1
            item["score_total"] += 1.0
        elif result == "loss":
            item["losses"] += 1
        elif result == "draw":
            item["draws"] += 1
            item["score_total"] += 0.5

    projected = []
    for item in openings_map.values():
        games_count = int(item.get("games_count", 0) or 0)
        wins = int(item.get("wins", 0) or 0)
        losses = int(item.get("losses", 0) or 0)
        draws = int(item.get("draws", 0) or 0)
        white_games = int(item.get("side_counts", {}).get("white", 0) or 0)
        black_games = int(item.get("side_counts", {}).get("black", 0) or 0)
        if white_games and black_games:
            opening_side = "unknown"
        elif white_games:
            opening_side = "white"
        elif black_games:
            opening_side = "black"
        else:
            opening_side = "unknown"
        score_percent = 0.0 if games_count <= 0 else round((float(item.get("score_total", 0.0) or 0.0) / games_count) * 100.0, 1)
        needs_work = bool(games_count >= 2 and (losses > wins or score_percent <= 45.0))
        summary_item = {
            "key": item.get("key", ""),
            "name": item.get("name", ""),
            "eco": item.get("eco", ""),
            "side": opening_side,
            "games_count": games_count,
            "wins": wins,
            "losses": losses,
            "draws": draws,
            "score_percent": score_percent,
            "needs_work": needs_work,
        }
        if side_filter and summary_item["side"] != side_filter:
            continue
        if needs_work_filter is not None and summary_item["needs_work"] != needs_work_filter:
            continue
        projected.append(summary_item)

    projected.sort(key=lambda item: (-int(item.get("games_count", 0) or 0), -float(item.get("score_percent", 0.0) or 0.0), str(item.get("name", "") or "").lower()))
    safe_limit = max(0, min(int(limit or 50), 100))
    safe_offset = max(0, int(offset or 0))
    paged_items = projected[safe_offset:safe_offset + safe_limit] if safe_limit else []
    return {
        "ok": True,
        "section": "chess",
        "title": "Openings",
        "items": [_project_chess_opening_item(item) for item in paged_items],
        "count": len(projected),
    }
