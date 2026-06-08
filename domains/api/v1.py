"""Versioned JSON API routes for Dragon."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from flask import Blueprint, current_app, jsonify, request, session

from domains.chess.api_projection import (
    build_chess_game_detail_projection,
    build_chess_games_projection,
    build_chess_courses_projection,
    build_chess_home_projection,
    build_chess_openings_projection,
    build_chess_progress_projection,
    build_chess_train_today_projection,
)
from dragon.paths import BOOKS_SNAPSHOT_PATH, CHESS_DATA_PATH, EXPORTS_DIR, READING_DATA_PATH, YOUTUBE_LATEST_SNAPSHOT_PATH

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


def _load_local_json(path: Path):
    try:
        if not Path(path).exists():
            return None
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return None


def _count_list_items(payload, key):
    if not isinstance(payload, dict):
        return None
    value = payload.get(key)
    if isinstance(value, list):
        return len(value)
    return None


def _count_youtube_items(payload):
    if not isinstance(payload, dict):
        return None

    for key in ("videos", "items"):
        value = payload.get(key)
        if isinstance(value, list):
            return len(value)

    groups = payload.get("groups")
    if isinstance(groups, dict):
        total = 0
        found = False
        for group in groups.values():
            if not isinstance(group, dict):
                continue
            videos = group.get("videos")
            if isinstance(videos, list):
                total += len(videos)
                found = True
        if found:
            return total

    return None


def _section_status_and_count(count):
    if count is None:
        return {"status": "unknown", "count": None}
    return {"status": "available", "count": count}


def _parse_article_datetime(value):
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _article_sort_datetime(entry):
    item = entry if isinstance(entry, dict) else {}
    for key in ("published_at", "saved_at", "date", "date_published", "created_at", "updated_at"):
        parsed = _parse_article_datetime(item.get(key, ""))
        if parsed is not None:
            return parsed
    return None


def _article_text(entry, *keys):
    item = entry if isinstance(entry, dict) else {}
    for key in keys:
        text = str(item.get(key, "") or "").strip()
        if text:
            return text
    return ""


def _project_article_item(entry):
    item = entry if isinstance(entry, dict) else {}
    return {
        "id": _article_text(item, "id"),
        "title": _article_text(item, "title"),
        "source": _article_text(item, "source"),
        "url": _article_text(item, "url"),
        "published_at": _article_text(item, "published_at"),
        "saved_at": _article_text(item, "saved_at"),
        "excerpt": _article_text(item, "excerpt"),
    }


def _normalize_limit(value, default=20, minimum=1, maximum=100):
    try:
        limit = int(str(value).strip() or default)
    except Exception:
        return default
    if limit < minimum:
        return minimum
    if limit > maximum:
        return maximum
    return limit


def _load_article_entries():
    payload = _load_local_json(READING_DATA_PATH)
    if not isinstance(payload, dict):
        return None
    entries = payload.get("entries")
    if not isinstance(entries, list):
        return None
    return [dict(entry) for entry in entries if isinstance(entry, dict)]


def _build_articles_response(limit):
    entries = _load_article_entries()
    if entries is None:
        return {"ok": True, "api_version": "v1", "items": [], "count": 0}

    sort_keys = [_article_sort_datetime(entry) for entry in entries]
    if any(value is not None for value in sort_keys):
        entries = [
            item
            for _, item in sorted(
                enumerate(entries),
                key=lambda pair: (
                    0 if _article_sort_datetime(pair[1]) is not None else 1,
                    -_article_sort_datetime(pair[1]).timestamp() if _article_sort_datetime(pair[1]) is not None else 0,
                    pair[0],
                ),
            )
        ]

    items = [_project_article_item(entry) for entry in entries[:limit]]
    return {
        "ok": True,
        "api_version": "v1",
        "items": items,
        "count": len(items),
    }


def _book_entries_from_payload(payload):
    if isinstance(payload, list):
        return [dict(entry) for entry in payload if isinstance(entry, dict)]
    if not isinstance(payload, dict):
        return None
    for key in ("entries", "books", "items"):
        value = payload.get(key)
        if isinstance(value, list):
            return [dict(entry) for entry in value if isinstance(entry, dict)]
    return None


def _book_text(entry, *keys, default=""):
    item = entry if isinstance(entry, dict) else {}
    for key in keys:
        text = str(item.get(key, "") or "").strip()
        if text:
            return text
    return str(default or "").strip()


def _book_authors(entry):
    item = entry if isinstance(entry, dict) else {}
    raw_authors = item.get("authors", [])
    authors = []
    if isinstance(raw_authors, list):
        authors = [str(author).strip() for author in raw_authors if str(author).strip()]
    elif isinstance(raw_authors, str) and raw_authors.strip():
        authors = [raw_authors.strip()]
    if not authors:
        fallback = _book_text(item, "author", "authors_display")
        if fallback:
            authors = [fallback]
    return authors


def _book_sort_datetime(entry):
    item = entry if isinstance(entry, dict) else {}
    for key in ("updated_at", "saved_at", "last_edited_time", "created_time", "date_finished"):
        parsed = _parse_article_datetime(item.get(key, ""))
        if parsed is not None:
            return parsed
    return None


def _project_book_item(entry):
    item = entry if isinstance(entry, dict) else {}
    authors = _book_authors(item)
    cover = _book_text(item, "cover", "cover_url", "cover_source")
    if not cover:
        cover = ""
    return {
        "id": _book_text(item, "id"),
        "title": _book_text(item, "title", default="Untitled book"),
        "author": _book_text(item, "author", "authors_display"),
        "authors": authors,
        "cover": cover,
        "year": _book_text(item, "year"),
        "status": _book_text(item, "status", "status_label"),
        "score": _book_text(item, "score", "rating"),
        "excerpt": _book_text(item, "excerpt"),
    }


def _load_book_entries():
    payload = _load_local_json(BOOKS_SNAPSHOT_PATH)
    entries = _book_entries_from_payload(payload)
    if entries is None:
        return None
    return entries


def _build_books_response(limit):
    entries = _load_book_entries()
    if entries is None:
        return {"ok": True, "api_version": "v1", "items": [], "count": 0}

    sort_keys = [_book_sort_datetime(entry) for entry in entries]
    if any(value is not None for value in sort_keys):
        entries = [
            item
            for _, item in sorted(
                enumerate(entries),
                key=lambda pair: (
                    0 if _book_sort_datetime(pair[1]) is not None else 1,
                    -_book_sort_datetime(pair[1]).timestamp() if _book_sort_datetime(pair[1]) is not None else 0,
                    pair[0],
                ),
            )
        ]

    items = [_project_book_item(entry) for entry in entries[:limit]]
    return {
        "ok": True,
        "api_version": "v1",
        "items": items,
        "count": len(items),
    }


@api_v1_bp.get("/api/v1/home")
def api_v1_home():
    reading_payload = _load_local_json(READING_DATA_PATH)
    movies_payload = _load_local_json(EXPORTS_DIR / "movies_export.json")
    chess_payload = _load_local_json(CHESS_DATA_PATH)
    books_payload = _load_local_json(BOOKS_SNAPSHOT_PATH)
    youtube_payload = _load_local_json(YOUTUBE_LATEST_SNAPSHOT_PATH)

    sections = [
        {
            "key": "articles",
            "label": "Articles",
            **_section_status_and_count(_count_list_items(reading_payload, "entries")),
        },
        {
            "key": "movies",
            "label": "Movies",
            **_section_status_and_count(len(movies_payload) if isinstance(movies_payload, list) else None),
        },
        {
            "key": "books",
            "label": "Books",
            **_section_status_and_count(_count_list_items(books_payload, "entries")),
        },
        {
            "key": "youtube",
            "label": "YouTube",
            **_section_status_and_count(_count_youtube_items(youtube_payload)),
        },
        {
            "key": "chess",
            "label": "Chess",
            **_section_status_and_count(_count_list_items(chess_payload, "games")),
        },
    ]

    return jsonify(
        {
            "ok": True,
            "service": "dragon",
            "api_version": "v1",
            "sections": sections,
        }
    )


@api_v1_bp.get("/api/v1/articles")
def api_v1_articles():
    limit = _normalize_limit(request.args.get("limit", 20))
    return jsonify(_build_articles_response(limit))


@api_v1_bp.get("/api/v1/books")
def api_v1_books():
    limit = _normalize_limit(request.args.get("limit", 20))
    return jsonify(_build_books_response(limit))


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


@api_v1_bp.get("/api/v1/chess/progress")
def api_v1_chess_progress():
    return jsonify(build_chess_progress_projection())
