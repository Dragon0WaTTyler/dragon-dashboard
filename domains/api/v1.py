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


def _movie_entries_from_payload(payload):
    if isinstance(payload, list):
        return [dict(entry) for entry in payload if isinstance(entry, dict)]
    if not isinstance(payload, dict):
        return None
    for key in ("entries", "movies", "items"):
        value = payload.get(key)
        if isinstance(value, list):
            return [dict(entry) for entry in value if isinstance(entry, dict)]
    return None


def _movie_text(entry, *keys, default=""):
    item = entry if isinstance(entry, dict) else {}
    for key in keys:
        text = str(item.get(key, "") or "").strip()
        if text:
            return text
    return str(default or "").strip()


def _movie_score(entry):
    item = entry if isinstance(entry, dict) else {}
    value = item.get("score", item.get("rating", ""))
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        if "." in text:
            return float(text)
        return int(text)
    except Exception:
        return text


def _movie_overview(entry):
    overview = _movie_text(entry, "overview", "summary", "excerpt")
    if len(overview) > 280:
        return overview[:280]
    return overview


def _movie_sort_datetime(entry):
    item = entry if isinstance(entry, dict) else {}
    for key in ("updated_at", "saved_at", "last_edited_time", "created_time", "date"):
        parsed = _parse_article_datetime(item.get(key, ""))
        if parsed is not None:
            return parsed
    return None


def _project_movie_item(entry):
    item = entry if isinstance(entry, dict) else {}
    return {
        "id": _movie_text(item, "id"),
        "title": _movie_text(item, "title", default="Untitled movie"),
        "year": _movie_text(item, "year"),
        "poster": _movie_text(item, "poster", "poster_url", "cover"),
        "status": _movie_text(item, "status", "state", "watch_status"),
        "score": _movie_score(item),
        "type": _movie_text(item, "type", "media_type", default=""),
        "overview": _movie_overview(item),
    }


def _load_movie_entries():
    payload = _load_local_json(EXPORTS_DIR / "movies_export.json")
    entries = _movie_entries_from_payload(payload)
    if entries is None:
        return None
    return entries


def _build_movies_response(limit):
    entries = _load_movie_entries()
    if entries is None:
        return {"ok": True, "api_version": "v1", "items": [], "count": 0}

    sort_keys = [_movie_sort_datetime(entry) for entry in entries]
    if any(value is not None for value in sort_keys):
        entries = [
            item
            for _, item in sorted(
                enumerate(entries),
                key=lambda pair: (
                    0 if _movie_sort_datetime(pair[1]) is not None else 1,
                    -_movie_sort_datetime(pair[1]).timestamp() if _movie_sort_datetime(pair[1]) is not None else 0,
                    pair[0],
                ),
            )
        ]

    items = [_project_movie_item(entry) for entry in entries[:limit]]
    return {
        "ok": True,
        "api_version": "v1",
        "items": items,
        "count": len(items),
    }


def _youtube_entries_from_payload(payload):
    entries = []

    def visit(node, group_name="", channel_name=""):
        if isinstance(node, list):
            for item in node:
                visit(item, group_name=group_name, channel_name=channel_name)
            return

        if not isinstance(node, dict):
            return

        containers_found = False

        for key in ("videos", "items"):
            value = node.get(key)
            if isinstance(value, list):
                containers_found = True
                for item in value:
                    if not isinstance(item, dict):
                        continue
                    augmented = dict(item)
                    if group_name and not _youtube_text(augmented, "group"):
                        augmented["group"] = group_name
                    if channel_name and not _youtube_text(augmented, "channel"):
                        augmented["channel"] = channel_name
                    if group_name or channel_name:
                        augmented["_youtube_context"] = "pockettube"
                    entries.append(augmented)

        groups = node.get("groups")
        if isinstance(groups, dict):
            containers_found = True
            for name, group in groups.items():
                next_group = _youtube_text(group, "group", default=str(name))
                visit(group, group_name=next_group, channel_name=channel_name)

        channels = node.get("channels")
        if isinstance(channels, dict):
            containers_found = True
            for name, channel in channels.items():
                next_channel = _youtube_text(channel, "channel", default=str(name))
                visit(channel, group_name=group_name, channel_name=next_channel)

        if not containers_found and any(
            key in node
            for key in (
                "video_id",
                "videoId",
                "youtube_id",
                "title",
                "url",
                "thumbnail",
                "published_at",
                "saved_at",
                "duration",
                "section",
                "playlist",
            )
        ):
            augmented = dict(node)
            if group_name and not _youtube_text(augmented, "group"):
                augmented["group"] = group_name
            if channel_name and not _youtube_text(augmented, "channel"):
                augmented["channel"] = channel_name
            if group_name or channel_name:
                augmented["_youtube_context"] = "pockettube"
            entries.append(augmented)

    visit(payload)
    return entries


def _youtube_text(entry, *keys, default=""):
    item = entry if isinstance(entry, dict) else {}
    for key in keys:
        text = str(item.get(key, "") or "").strip()
        if text:
            return text
    return str(default or "").strip()


def _youtube_sort_datetime(entry):
    item = entry if isinstance(entry, dict) else {}
    for key in (
        "published_at",
        "publishedAt",
        "saved_at",
        "savedAt",
        "date",
        "date_published",
        "updated_at",
        "created_at",
        "uploaded_at",
        "upload_date",
    ):
        parsed = _parse_article_datetime(item.get(key, ""))
        if parsed is not None:
            return parsed
    return None


def _youtube_is_watchlater(entry):
    item = entry if isinstance(entry, dict) else {}
    if str(item.get("_youtube_context", "")).strip().lower() == "pockettube":
        return False

    for key in ("playlist", "section", "group", "title", "channel", "url", "thumbnail"):
        text = _youtube_text(item, key).lower()
        if "watch later" in text or "watchlater" in text:
            return True

    return False


def _youtube_detect_source(entry):
    item = entry if isinstance(entry, dict) else {}
    if str(item.get("_youtube_context", "")).strip().lower() == "pockettube":
        return "pockettube"
    if _youtube_is_watchlater(item):
        return "watchlater"
    return "unknown"


def _youtube_normalize_source_filter(value):
    source = str(value or "").strip().lower()
    if source in ("all", "watchlater", "pockettube"):
        return source
    return "all"


def _youtube_matches_requested_source(entry, requested_source):
    source = _youtube_detect_source(entry)
    if requested_source == "all":
        return True
    return source == requested_source


def _youtube_matches_section(entry, section_name):
    if not section_name:
        return True
    item = entry if isinstance(entry, dict) else {}
    normalized = section_name.strip().lower()
    for key in ("section", "group", "playlist"):
        if _youtube_text(item, key).strip().lower() == normalized:
            return True
    return False


def _project_youtube_item(entry):
    item = entry if isinstance(entry, dict) else {}
    video_id = _youtube_text(item, "video_id", "videoId", "youtube_id")
    url = _youtube_text(item, "url")
    if not url and video_id:
        url = f"https://www.youtube.com/watch?v={video_id}"

    return {
        "id": _youtube_text(item, "id", default=video_id),
        "video_id": video_id,
        "title": _youtube_text(item, "title", default="Untitled video"),
        "channel": _youtube_text(item, "channel", "channel_title"),
        "thumbnail": _youtube_text(item, "thumbnail", "thumbnail_url", "thumb", "thumbnailUrl"),
        "url": url,
        "published_at": _youtube_text(item, "published_at", "publishedAt"),
        "saved_at": _youtube_text(item, "saved_at", "savedAt"),
        "duration": _youtube_text(item, "duration", "length"),
        "section": _youtube_text(item, "section"),
        "group": _youtube_text(item, "group"),
        "playlist": _youtube_text(item, "playlist", "playlist_title"),
        "source": _youtube_detect_source(item),
    }


def _load_youtube_entries():
    payload = _load_local_json(YOUTUBE_LATEST_SNAPSHOT_PATH)
    entries = _youtube_entries_from_payload(payload)
    if entries is None:
        return None
    return entries


def _build_youtube_response(limit, source="all", section=""):
    entries = _load_youtube_entries()
    if entries is None:
        return {"ok": True, "api_version": "v1", "items": [], "count": 0}

    requested_source = _youtube_normalize_source_filter(source)
    requested_section = _youtube_text({"section": section}, "section")

    entries = [
        entry
        for entry in entries
        if _youtube_matches_requested_source(entry, requested_source)
        and _youtube_matches_section(entry, requested_section)
    ]

    sort_keys = [_youtube_sort_datetime(entry) for entry in entries]
    if any(value is not None for value in sort_keys):
        entries = [
            item
            for _, item in sorted(
                enumerate(entries),
                key=lambda pair: (
                    0 if _youtube_sort_datetime(pair[1]) is not None else 1,
                    -_youtube_sort_datetime(pair[1]).timestamp() if _youtube_sort_datetime(pair[1]) is not None else 0,
                    pair[0],
                ),
            )
        ]

    items = [_project_youtube_item(entry) for entry in entries[:limit]]
    return {
        "ok": True,
        "api_version": "v1",
        "items": items,
        "count": len(items),
    }


def _youtube_sections_from_entries(entries):
    counts = {}

    for entry in entries:
        source = _youtube_detect_source(entry)
        if source == "watchlater":
            key = "watchlater"
            label = "Watch Later"
        else:
            label = _youtube_text(entry, "group", "section", "playlist")
            if not label:
                continue
            key = label

        current = counts.get(key)
        if current is None:
            counts[key] = {"key": key, "label": label, "count": 1}
        else:
            current["count"] += 1

    sections = list(counts.values())
    sections.sort(key=lambda item: (0 if item["key"] == "watchlater" else 1, item["label"].lower(), item["key"].lower()))
    return sections


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


@api_v1_bp.get("/api/v1/movies")
def api_v1_movies():
    limit = _normalize_limit(request.args.get("limit", 20))
    return jsonify(_build_movies_response(limit))


@api_v1_bp.get("/api/v1/youtube")
def api_v1_youtube():
    limit = _normalize_limit(request.args.get("limit", 20))
    source = request.args.get("source", "all")
    section = request.args.get("section", "")
    return jsonify(_build_youtube_response(limit, source=source, section=section))


@api_v1_bp.get("/api/v1/youtube/sections")
def api_v1_youtube_sections():
    entries = _load_youtube_entries()
    if entries is None:
        return jsonify({"ok": True, "api_version": "v1", "sections": []})

    return jsonify(
        {
            "ok": True,
            "api_version": "v1",
            "sections": _youtube_sections_from_entries(entries),
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


@api_v1_bp.get("/api/v1/chess/progress")
def api_v1_chess_progress():
    return jsonify(build_chess_progress_projection())
