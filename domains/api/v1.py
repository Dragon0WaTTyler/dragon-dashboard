"""Versioned JSON API routes for Dragon."""

from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse

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
from dragon.paths import BOOKS_SNAPSHOT_PATH, CACHE_DATA_PATH, CHESS_DATA_PATH, EXPORTS_DIR, PLAYLISTS_PATH, READING_DATA_PATH, YOUTUBE_LATEST_SNAPSHOT_PATH

api_v1_bp = Blueprint("api_v1", __name__)

DRAGON_CORE_SNAPSHOT_SCHEMA_VERSION = "dragon-core-snapshot.v1"
DRAGON_CORE_SNAPSHOT_PATH = EXPORTS_DIR / "dragon_core_snapshot.json"
DRAGON_CORE_SNAPSHOT_LIMITS = {
    "books": 500,
    "articles": 500,
    "movies": 500,
    "youtube_videos": 500,
}


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


def _server_time_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _section_payload(*, key, label, count, href):
    return {
        "key": key,
        "label": label,
        "href": href,
        "api_path": href,
        **_section_status_and_count(count),
    }


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


def _article_image(entry):
    item = entry if isinstance(entry, dict) else {}
    return _article_text(item, "lead_image_url", "image_url")


def _project_article_item(entry):
    item = entry if isinstance(entry, dict) else {}
    image = _article_image(item)
    return {
        "id": _article_text(item, "id"),
        "title": _article_text(item, "title"),
        "source": _article_text(item, "source"),
        "url": _article_text(item, "url"),
        "published_at": _article_text(item, "published_at"),
        "saved_at": _article_text(item, "saved_at"),
        "excerpt": _article_text(item, "excerpt"),
        "image": image,
        "thumbnail": image,
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


def _normalize_offset(value, default=0, minimum=0):
    try:
        offset = int(str(value).strip() or default)
    except Exception:
        return default
    if offset < minimum:
        return minimum
    return offset


def _normalize_search_query(value):
    return str(value or "").strip()


def _normalized_search_text(value):
    return str(value or "").strip().lower()


def _matches_search_query(values, query):
    normalized_query = _normalized_search_text(query)
    if not normalized_query:
        return True

    for value in values:
        if normalized_query in _normalized_search_text(value):
            return True
    return False


def _paginate_items(items, limit, offset):
    total = len(items)
    safe_offset = min(max(int(offset or 0), 0), total)
    safe_limit = max(int(limit or 0), 0)
    paged_items = items[safe_offset:safe_offset + safe_limit]
    count = len(paged_items)
    next_offset = safe_offset + count
    has_more = next_offset < total
    return {
        "items": paged_items,
        "count": count,
        "total": total,
        "limit": safe_limit,
        "offset": safe_offset,
        "has_more": has_more,
        "next_offset": next_offset if has_more else None,
    }


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


def _load_reading_api_helpers():
    from app import (
        build_reading_article_fulltext_status,
        normalize_reading_entry,
        reading_article_fulltext_load,
        reading_article_fulltext_request_load,
        reading_article_url_candidates,
        sanitize_reading_article_html,
    )

    return {
        "build_reading_article_fulltext_status": build_reading_article_fulltext_status,
        "normalize_reading_entry": normalize_reading_entry,
        "reading_article_fulltext_load": reading_article_fulltext_load,
        "reading_article_fulltext_request_load": reading_article_fulltext_request_load,
        "reading_article_url_candidates": reading_article_url_candidates,
        "sanitize_reading_article_html": sanitize_reading_article_html,
    }


def _find_article_entry(article_id):
    normalized_article_id = str(article_id or "").strip()
    if not normalized_article_id:
        return None
    entries = _load_article_entries()
    if entries is None:
        return None
    for entry in entries:
        if _article_text(entry, "id") == normalized_article_id:
            return dict(entry)
    return None


def _article_read_state(entry):
    item = entry if isinstance(entry, dict) else {}
    return "read" if bool(item.get("read")) else "unread"


def _project_article_detail_item(entry):
    item = entry if isinstance(entry, dict) else {}
    helpers = _load_reading_api_helpers()
    normalized_entry = helpers["normalize_reading_entry"](item, include_body_image_scan=False)
    base_item = _project_article_item(item)
    article_url = (helpers["reading_article_url_candidates"](normalized_entry) or [""])[0]
    cache_record = helpers["reading_article_fulltext_load"](article_url)
    request_record = helpers["reading_article_fulltext_request_load"](_article_text(item, "id"))
    fulltext_status = helpers["build_reading_article_fulltext_status"](
        normalized_entry,
        cache_record=cache_record,
        request_record=request_record,
    )
    content_text = ""
    content_html = ""
    if isinstance(cache_record, dict) and fulltext_status.get("status") == "cached":
        content_text = str(cache_record.get("content_text", "") or "").strip()
        content_html = helpers["sanitize_reading_article_html"](
            cache_record.get("content_html", ""),
            base_url=normalized_entry.get("original_url", "") or normalized_entry.get("url", ""),
            hero_image="",
            author_image="",
        )
        if not content_text:
            content_html = ""
    return {
        **base_item,
        "status": str(normalized_entry.get("status", "") or "").strip(),
        "read_state": _article_read_state(normalized_entry),
        "fulltext_status": fulltext_status,
        "content_text": content_text,
        "content_html": content_html,
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


def _book_search_values(entry):
    item = entry if isinstance(entry, dict) else {}
    authors = _book_authors(item)
    values = [
        _book_text(item, "title"),
        _book_text(item, "author", "authors_display"),
        " ".join(authors),
        _book_text(item, "description", "excerpt", "summary"),
        _book_text(item, "isbn", "isbn13", "isbn10"),
    ]
    return values + authors


def _build_books_response(limit, offset, query=""):
    entries = _load_book_entries()
    if entries is None:
        page = _paginate_items([], limit, offset)
        return {
            "ok": True,
            "api_version": "v1",
            **page,
            "meta": {
                "search_query": _normalize_search_query(query),
            },
        }

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

    normalized_query = _normalize_search_query(query)
    if normalized_query:
        entries = [entry for entry in entries if _matches_search_query(_book_search_values(entry), normalized_query)]

    page = _paginate_items(entries, limit, offset)
    items = [_project_book_item(entry) for entry in page["items"]]
    return {
        "ok": True,
        "api_version": "v1",
        "items": items,
        "count": page["count"],
        "total": page["total"],
        "limit": page["limit"],
        "offset": page["offset"],
        "has_more": page["has_more"],
        "next_offset": page["next_offset"],
        "meta": {
            "search_query": normalized_query,
        },
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


def _movie_slugify(value):
    text = re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower()).strip("-")
    return text or "entry"


def _movie_title(entry):
    return _movie_text(entry, "title", "name", default="Untitled movie")


def _movie_id(entry):
    explicit_id = _movie_text(entry, "id")
    if explicit_id:
        return explicit_id

    title = _movie_text(entry, "title", "name")
    if not title:
        return ""
    return f"film-{_movie_slugify(title)}"


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
        "id": _movie_id(item),
        "title": _movie_title(item),
        "year": _movie_text(item, "year"),
        "poster": _movie_text(item, "poster", "poster_url", "cover"),
        "status": _movie_text(item, "status", "state", "watch_status"),
        "score": _movie_score(item),
        "type": _movie_text(item, "type", "category", "media_type", default=""),
        "overview": _movie_overview(item),
    }


def _load_movie_entries():
    payload = _load_local_json(EXPORTS_DIR / "movies_export.json")
    entries = _movie_entries_from_payload(payload)
    if entries is None:
        return None
    return entries


def _build_movies_response(limit, offset):
    entries = _load_movie_entries()
    if entries is None:
        return {
            "ok": True,
            "api_version": "v1",
            "items": [],
            "count": 0,
            "total": 0,
            "limit": limit,
            "offset": offset,
            "next_offset": None,
            "has_more": False,
        }

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

    page = _paginate_items(entries, limit, offset)
    items = [_project_movie_item(entry) for entry in page["items"]]
    return {
        "ok": True,
        "api_version": "v1",
        "items": items,
        "count": len(items),
        "total": page["total"],
        "limit": page["limit"],
        "offset": page["offset"],
        "next_offset": page["next_offset"],
        "has_more": page["has_more"],
    }


def _youtube_entries_from_payload(payload):
    entries = []

    def visit(node, group_name="", section_name="", source_hint="unknown"):
        if isinstance(node, list):
            for item in node:
                visit(item, group_name=group_name, section_name=section_name, source_hint=source_hint)
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
                    if group_name and not _youtube_text(augmented, "group", "group_name"):
                        augmented["group"] = group_name
                    if section_name and not _youtube_text(augmented, "section", "section_name"):
                        augmented["section"] = section_name
                    if source_hint == "pockettube":
                        augmented["_youtube_context"] = "pockettube"
                    elif source_hint == "watchlater":
                        augmented["_youtube_context"] = "watchlater"
                    else:
                        explicit_source = _youtube_explicit_source(augmented)
                        if explicit_source != "unknown":
                            augmented["_youtube_context"] = explicit_source
                    entries.append(augmented)

        groups = node.get("groups")
        if isinstance(groups, dict):
            containers_found = True
            for name, group in groups.items():
                next_group = _youtube_text(group, "group_name", "group", default=str(name))
                next_section = _youtube_text(group, "section_name", "section", default="")
                visit(group, group_name=next_group, section_name=next_section, source_hint="pockettube")

        for key in ("watchlater", "watch_later", "watchLater"):
            value = node.get(key)
            if isinstance(value, list):
                containers_found = True
                visit(value, source_hint="watchlater")
            elif isinstance(value, dict):
                containers_found = True
                visit(value, source_hint="watchlater")

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
            if group_name and not _youtube_text(augmented, "group", "group_name"):
                augmented["group"] = group_name
            if section_name and not _youtube_text(augmented, "section", "section_name"):
                augmented["section"] = section_name
            explicit_source = _youtube_explicit_source(augmented)
            if source_hint == "pockettube" or explicit_source == "pockettube":
                augmented["_youtube_context"] = "pockettube"
            elif source_hint == "watchlater" or explicit_source == "watchlater":
                augmented["_youtube_context"] = "watchlater"
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


def _youtube_normalized_text(value):
    text = str(value or "").strip().lower()
    text = text.replace("_", " ").replace("-", " ")
    text = " ".join(text.split())
    return text


def _youtube_text_contains(value, needles):
    text = _youtube_normalized_text(value)
    if not text:
        return False
    for needle in needles:
        if needle in text:
            return True
    return False


def _youtube_has_pockettube_markers(entry):
    item = entry if isinstance(entry, dict) else {}
    if str(item.get("_youtube_context", "")).strip().lower() == "pockettube":
        return True

    for key in ("group_key", "group_name", "group_names", "group", "section_key", "section_name", "section", "source_name", "reason_tags"):
        if _youtube_text_contains(item.get(key), ("pockettube",)):
            return True

    if _youtube_text(item, "group_name", "group", "section_name", "section"):
        return True

    return False


def _youtube_has_watchlater_markers(entry):
    item = entry if isinstance(entry, dict) else {}
    if str(item.get("_youtube_context", "")).strip().lower() == "watchlater":
        return True

    watchlater_fields = (
        "playlist",
        "playlist_title",
        "source",
        "source_name",
        "state_key",
        "watch_key",
        "section_name",
        "section",
        "group_name",
        "group",
        "group_names",
        "reason_tags",
    )
    for key in watchlater_fields:
        if _youtube_text_contains(item.get(key), ("watch later", "watchlater")):
            return True
    return False


def _youtube_explicit_source(entry):
    item = entry if isinstance(entry, dict) else {}
    if _youtube_has_pockettube_markers(item):
        return "pockettube"
    if _youtube_has_watchlater_markers(item):
        return "watchlater"
    return "unknown"


def _youtube_detect_source(entry):
    item = entry if isinstance(entry, dict) else {}
    explicit = str(item.get("_youtube_context", "")).strip().lower()
    if explicit in ("pockettube", "watchlater"):
        return explicit
    return _youtube_explicit_source(item)


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
    if normalized == "last" and _youtube_detect_source(item) == "watchlater":
        return True
    for key in ("section", "section_name", "group", "group_name", "playlist", "playlist_title"):
        if _youtube_text(item, key).strip().lower() == normalized:
            return True
    return False


def _project_youtube_item(entry):
    item = entry if isinstance(entry, dict) else {}
    video_id = _youtube_text(item, "video_id", "videoId", "youtube_id")
    url = _youtube_text(item, "url")
    if not url and video_id:
        url = f"https://www.youtube.com/watch?v={video_id}"

    group_value = _youtube_text(item, "group", "group_name")
    section_value = _youtube_text(item, "section", "section_name")
    playlist_value = _youtube_text(item, "playlist", "playlist_title")

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
        "section": section_value,
        "group": group_value,
        "playlist": playlist_value,
        "source": _youtube_detect_source(item),
    }


def _project_youtube_video_item(entry, section_value=""):
    item = entry if isinstance(entry, dict) else {}
    video_id = _youtube_text(item, "video_id", "videoId", "youtube_id")
    url = _youtube_text(item, "url")
    if not url and video_id:
        url = f"https://www.youtube.com/watch?v={video_id}"

    return {
        "id": _youtube_text(item, "id", default=video_id),
        "title": _youtube_text(item, "title", default="Untitled video"),
        "channel": _youtube_text(item, "channel", "channel_title"),
        "url": url,
        "thumbnail": _youtube_text(item, "thumbnail", "thumbnail_url", "thumb", "thumbnailUrl"),
        "published_at": _youtube_text(item, "published_at", "publishedAt"),
        "duration": _youtube_text(item, "duration", "length"),
        "section": section_value,
    }


def _parse_iso_datetime(value):
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _cache_entry_is_stale(entry, max_age_seconds=24 * 60 * 60):
    item = entry if isinstance(entry, dict) else {}
    timestamp = _parse_iso_datetime(item.get("updated_at", ""))
    if timestamp is None:
        return True
    return (datetime.now(timezone.utc) - timestamp.astimezone(timezone.utc)).total_seconds() > max_age_seconds


def _playlist_id_from_value(value):
    text = str(value or "").strip()
    if not text:
        return ""
    if "://" not in text and "list=" not in text:
        return text
    parsed = urlparse(text)
    query_values = parse_qs(parsed.query).get("list", [])
    for candidate in query_values:
        playlist_id = str(candidate or "").strip()
        if playlist_id:
            return playlist_id
    return ""


def _load_watchlater_playlists():
    payload = _load_local_json(PLAYLISTS_PATH)
    if not isinstance(payload, dict):
        return []

    raw_playlists = payload.get("YouTube Watch Later")
    if not isinstance(raw_playlists, list):
        return []

    playlists = []
    for entry in raw_playlists:
        if not isinstance(entry, dict):
            continue
        playlist_id = _playlist_id_from_value(entry.get("id", "") or entry.get("url", ""))
        if not playlist_id:
            continue
        playlists.append(
            {
                "id": playlist_id,
                "name": str(entry.get("name", "") or entry.get("title", "") or "YouTube Watch Later").strip() or "YouTube Watch Later",
            }
        )
    return playlists


def _load_watchlater_entries():
    state = _load_watchlater_cache_state()
    return state["entries"]


def _load_watchlater_cache_state():
    started_at = time.monotonic()
    base_meta = {
        "data_source": "watchlater_playlist_cache",
        "cache_status": "unavailable",
        "warning": "",
    }

    entries = []
    seen_keys = set()
    section_name = "YouTube Watch Later"
    playlists = _load_watchlater_playlists()
    playlist_ids = []
    stale_playlist_count = 0
    cached_playlist_count = 0
    missing_playlist_ids = []

    cache_data = _load_local_json(CACHE_DATA_PATH)
    playlist_cache = cache_data.get("youtube_playlists", {}) if isinstance(cache_data, dict) else {}
    if not isinstance(playlist_cache, dict):
        playlist_cache = {}

    for playlist in playlists:
        if not isinstance(playlist, dict):
            continue
        playlist_id = str(playlist.get("id", "") or "").strip()
        if not playlist_id:
            continue
        if playlist_id not in playlist_ids:
            playlist_ids.append(playlist_id)

        playlist_label = str(playlist.get("name", "") or playlist.get("title", "") or section_name).strip() or section_name
        playlist_entry = playlist_cache.get(playlist_id)
        if not isinstance(playlist_entry, dict):
            missing_playlist_ids.append(playlist_id)
            continue

        cached_videos = playlist_entry.get("data", [])
        if not isinstance(cached_videos, list):
            missing_playlist_ids.append(playlist_id)
            continue

        cached_playlist_count += 1
        if _cache_entry_is_stale(playlist_entry):
            stale_playlist_count += 1

        videos = [dict(item) for item in cached_videos if isinstance(item, dict)]

        for item in videos:
            if not isinstance(item, dict):
                continue

            playlist_item_id = _youtube_text(item, "playlist_item_id")
            video_id = _youtube_text(item, "video_id", "videoId", "youtube_id")
            dedupe_key = playlist_item_id or f"{playlist_id}:{video_id}"
            if dedupe_key and dedupe_key in seen_keys:
                continue
            if dedupe_key:
                seen_keys.add(dedupe_key)

            augmented = dict(item)
            augmented["_youtube_context"] = "watchlater"
            augmented["section"] = _youtube_text(augmented, "section", default=section_name) or section_name
            augmented["section_name"] = _youtube_text(augmented, "section_name", default=section_name) or section_name
            augmented["playlist"] = _youtube_text(augmented, "playlist", default=playlist_label) or playlist_label
            augmented["playlist_title"] = _youtube_text(augmented, "playlist_title", default=playlist_label) or playlist_label
            augmented["channel_title"] = _youtube_text(augmented, "channel_title", "channel_name")
            augmented["thumbnail"] = _youtube_text(augmented, "thumbnail", "thumbnail_url", "thumb")
            augmented["source"] = "watchlater"
            entries.append(augmented)

    meta = dict(base_meta)
    meta.update(
        {
            "playlist_count": len(playlist_ids),
            "cached_playlist_count": cached_playlist_count,
            "missing_playlist_count": len(missing_playlist_ids),
            "elapsed_ms": round((time.monotonic() - started_at) * 1000, 2),
        }
    )

    if entries or cached_playlist_count:
        meta["cache_status"] = "stale" if stale_playlist_count or missing_playlist_ids else "fresh"
        if meta["cache_status"] == "stale":
            if stale_playlist_count and missing_playlist_ids:
                meta["warning"] = "Returning cached Watch Later data; some playlist caches are stale or missing."
            elif stale_playlist_count:
                meta["warning"] = "Returning stale cached Watch Later data."
            else:
                meta["warning"] = "Returning partial cached Watch Later data; some playlist caches are missing."
        return {"entries": entries, "available": True, "meta": meta}

    meta.update(
        {
            "data_source": "watchlater_cache_unavailable",
            "cache_status": "unavailable",
            "warning": "No cached Watch Later playlist data is available.",
        }
    )
    return {"entries": [], "available": False, "meta": meta}


def _load_youtube_entries_state(source="all"):
    requested_source = _youtube_normalize_source_filter(source)
    include_snapshot_entries = requested_source in {"all", "pockettube"}
    include_watchlater_entries = requested_source in {"all", "watchlater"}

    payload = _load_local_json(YOUTUBE_LATEST_SNAPSHOT_PATH) if include_snapshot_entries else None
    snapshot_entries = _youtube_entries_from_payload(payload) if payload is not None else []
    watchlater_state = (
        _load_watchlater_cache_state()
        if include_watchlater_entries
        else {"entries": [], "available": False, "meta": {}}
    )
    watchlater_entries = watchlater_state["entries"]

    if payload is None and not watchlater_state["available"] and not watchlater_entries:
        return {"entries": None, "watchlater_meta": watchlater_state["meta"]}

    return {
        "entries": snapshot_entries + watchlater_entries,
        "watchlater_meta": watchlater_state["meta"],
    }


def _load_youtube_entries():
    state = _load_youtube_entries_state()
    return state["entries"]


def _youtube_search_values(entry):
    item = entry if isinstance(entry, dict) else {}
    return [
        _youtube_text(item, "title"),
        _youtube_text(item, "channel", "channel_title", "channel_name"),
        _youtube_text(item, "section", "section_name"),
        _youtube_text(item, "group", "group_name"),
        _youtube_text(item, "playlist", "playlist_title"),
        _youtube_text(item, "url"),
        _youtube_text(item, "video_id", "videoId", "youtube_id", "id"),
    ]


def _build_youtube_response(limit, offset, source="all", section="", query=""):
    started_at = time.monotonic()
    requested_source = _youtube_normalize_source_filter(source)
    youtube_state = _load_youtube_entries_state(requested_source)
    entries = youtube_state["entries"]
    requested_section = _youtube_text({"section": section}, "section")
    normalized_query = _normalize_search_query(query)
    if entries is None:
        page = _paginate_items([], limit, offset)
        meta = {
            "source": requested_source,
            "section": requested_section,
            "search_query": normalized_query,
            "count": page["count"],
            "total": page["total"],
            "limit": page["limit"],
            "offset": page["offset"],
            "has_more": page["has_more"],
            "next_offset": page["next_offset"],
            "elapsed_ms": round((time.monotonic() - started_at) * 1000, 2),
        }
        if requested_source == "watchlater":
            meta.update(youtube_state["watchlater_meta"])
        return {
            "ok": True,
            "api_version": "v1",
            "items": [],
            "count": page["count"],
            "total": page["total"],
            "limit": page["limit"],
            "offset": page["offset"],
            "has_more": page["has_more"],
            "next_offset": page["next_offset"],
            "meta": meta,
        }

    entries = [
        entry
        for entry in entries
        if _youtube_matches_requested_source(entry, requested_source)
        and _youtube_matches_section(entry, requested_section)
    ]

    if normalized_query:
        entries = [entry for entry in entries if _matches_search_query(_youtube_search_values(entry), normalized_query)]

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

    page = _paginate_items(entries, limit, offset)
    items = [_project_youtube_item(entry) for entry in page["items"]]
    meta = {
        "source": requested_source,
        "section": requested_section,
        "search_query": normalized_query,
        "count": page["count"],
        "total": page["total"],
        "limit": page["limit"],
        "offset": page["offset"],
        "has_more": page["has_more"],
        "next_offset": page["next_offset"],
        "elapsed_ms": round((time.monotonic() - started_at) * 1000, 2),
    }
    if requested_source == "watchlater":
        meta.update(youtube_state["watchlater_meta"])
    return {
        "ok": True,
        "api_version": "v1",
        "items": items,
        "count": page["count"],
        "total": page["total"],
        "limit": page["limit"],
        "offset": page["offset"],
        "has_more": page["has_more"],
        "next_offset": page["next_offset"],
        "meta": meta,
    }


def _youtube_exact_section_value(entry):
    item = entry if isinstance(entry, dict) else {}
    for key in ("section", "section_name", "group", "group_name", "playlist", "playlist_title"):
        text = _youtube_text(item, key)
        if text:
            return text
    return ""


def _youtube_matches_exact_section(entry, section_name):
    requested = _youtube_text({"section": section_name}, "section")
    if not requested:
        return True

    item = entry if isinstance(entry, dict) else {}
    if requested.strip().lower() == "last" and _youtube_detect_source(item) == "watchlater":
        return True
    for key in ("section", "section_name", "group", "group_name", "playlist", "playlist_title"):
        if _youtube_text(item, key) == requested:
            return True
    return False


def _build_youtube_videos_response(limit, offset, section=""):
    entries = _load_youtube_entries()
    requested_section = _youtube_text({"section": section}, "section")
    if entries is None:
        page = _paginate_items([], limit, offset)
        return {
            "ok": True,
            "api_version": "v1",
            "section": requested_section,
            "count": page["count"],
            "total": page["total"],
            "limit": page["limit"],
            "offset": page["offset"],
            "has_more": page["has_more"],
            "next_offset": page["next_offset"],
            "items": [],
        }

    if requested_section:
        entries = [entry for entry in entries if _youtube_matches_exact_section(entry, requested_section)]

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

    page = _paginate_items(entries, limit, offset)
    items = [
        _project_youtube_video_item(entry, section_value=requested_section or _youtube_exact_section_value(entry))
        for entry in page["items"]
    ]
    return {
        "ok": True,
        "api_version": "v1",
        "section": requested_section,
        "count": page["count"],
        "total": page["total"],
        "limit": page["limit"],
        "offset": page["offset"],
        "has_more": page["has_more"],
        "next_offset": page["next_offset"],
        "items": items,
    }


def _youtube_sections_from_entries(entries):
    counts = {}

    for entry in entries:
        source = _youtube_detect_source(entry)
        if source == "watchlater":
            key = "watchlater"
            label = "Watch Later"
        else:
            label = _youtube_text(entry, "section_name", "section", "group_name", "group", "playlist_title", "playlist")
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


def _build_home_response():
    reading_payload = _load_local_json(READING_DATA_PATH)
    movie_entries = _load_movie_entries()
    chess_payload = _load_local_json(CHESS_DATA_PATH)
    book_entries = _load_book_entries()
    youtube_entries = _load_youtube_entries()

    sections = [
        _section_payload(key="movies", label="Movies", count=len(movie_entries) if movie_entries is not None else None, href="/api/v1/movies"),
        _section_payload(
            key="youtube",
            label="YouTube",
            count=len(youtube_entries) if youtube_entries is not None else None,
            href="/api/v1/youtube/sections",
        ),
        _section_payload(
            key="articles",
            label="Articles",
            count=_count_list_items(reading_payload, "entries"),
            href="/api/v1/articles",
        ),
        _section_payload(key="books", label="Books", count=len(book_entries) if book_entries is not None else None, href="/api/v1/books"),
        _section_payload(
            key="chess",
            label="Chess",
            count=_count_list_items(chess_payload, "games"),
            href="/api/v1/chess/home",
        ),
    ]

    return {
        "ok": True,
        "app_name": "Dragon",
        "service": "dragon",
        "api_version": "v1",
        "server_time": _server_time_iso(),
        "sections": sections,
    }


def _build_youtube_sections_response():
    entries = _load_youtube_entries()
    if entries is None:
        return {"ok": True, "api_version": "v1", "sections": []}
    return {
        "ok": True,
        "api_version": "v1",
        "sections": _youtube_sections_from_entries(entries),
    }


def _snapshot_warning(message):
    text = str(message or "").strip()
    return text if text else ""


def build_dragon_core_snapshot():
    warnings = []

    home_response = _build_home_response()
    articles_response = _build_articles_response(DRAGON_CORE_SNAPSHOT_LIMITS["articles"])
    books_response = _build_books_response(DRAGON_CORE_SNAPSHOT_LIMITS["books"], 0)
    movies_response = _build_movies_response(DRAGON_CORE_SNAPSHOT_LIMITS["movies"], 0)
    youtube_videos_response = _build_youtube_response(DRAGON_CORE_SNAPSHOT_LIMITS["youtube_videos"], 0)
    youtube_sections_response = _build_youtube_sections_response()

    if _load_article_entries() is None:
        warnings.append(_snapshot_warning("Articles source missing or malformed."))
    if _load_book_entries() is None:
        warnings.append(_snapshot_warning("Books source missing or malformed."))
    if _load_movie_entries() is None:
        warnings.append(_snapshot_warning("Movies source missing or malformed."))

    youtube_state = _load_youtube_entries_state("all")
    if youtube_state.get("entries") is None:
        warnings.append(_snapshot_warning("YouTube sources missing or malformed."))
    youtube_warning = _snapshot_warning((youtube_state.get("watchlater_meta", {}) or {}).get("warning", ""))
    if youtube_warning:
        warnings.append(youtube_warning)

    return {
        "schema_version": DRAGON_CORE_SNAPSHOT_SCHEMA_VERSION,
        "generated_at": _server_time_iso(),
        "producer": {
            "kind": "flask_dashboard",
            "version": "v1",
            "source": "local_exports_and_snapshots",
        },
        "status": {
            "partial": bool(warnings),
            "warnings": warnings,
        },
        "home": {
            "app_name": home_response["app_name"],
            "service": home_response["service"],
            "sections": home_response["sections"],
        },
        "books": {
            "total": len(books_response.get("items", []) or []),
            "items": books_response.get("items", []) or [],
        },
        "articles": {
            "total": len(articles_response.get("items", []) or []),
            "items": articles_response.get("items", []) or [],
        },
        "movies": {
            "total": len(movies_response.get("items", []) or []),
            "items": movies_response.get("items", []) or [],
        },
        "youtube": {
            "sections": youtube_sections_response.get("sections", []) or [],
            "videos": youtube_videos_response.get("items", []) or [],
        },
    }


def export_dragon_core_snapshot(output_path: Path | None = None):
    snapshot = build_dragon_core_snapshot()
    destination = Path(output_path or DRAGON_CORE_SNAPSHOT_PATH)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False), encoding="utf-8")
    return {
        "output_path": str(destination),
        "books_count": snapshot["books"]["total"],
        "articles_count": snapshot["articles"]["total"],
        "movies_count": snapshot["movies"]["total"],
        "youtube_sections_count": len(snapshot["youtube"]["sections"]),
        "youtube_videos_count": len(snapshot["youtube"]["videos"]),
        "warnings": list(snapshot["status"]["warnings"]),
        "snapshot": snapshot,
    }


@api_v1_bp.get("/api/v1/home")
def api_v1_home():
    return jsonify(_build_home_response())


@api_v1_bp.get("/api/v1/articles")
def api_v1_articles():
    limit = _normalize_limit(request.args.get("limit", 20))
    return jsonify(_build_articles_response(limit))


@api_v1_bp.get("/api/v1/articles/<article_id>")
def api_v1_article_detail(article_id):
    entry = _find_article_entry(article_id)
    if entry is None:
        return jsonify({"ok": False, "api_version": "v1", "error": "Article not found."}), 404
    return jsonify(
        {
            "ok": True,
            "api_version": "v1",
            "item": _project_article_detail_item(entry),
        }
    )


@api_v1_bp.get("/api/v1/books")
def api_v1_books():
    limit = _normalize_limit(request.args.get("limit", 50), default=50)
    offset = _normalize_offset(request.args.get("offset", 0))
    query = request.args.get("q", "")
    return jsonify(_build_books_response(limit, offset, query=query))


@api_v1_bp.get("/api/v1/movies")
def api_v1_movies():
    limit = _normalize_limit(request.args.get("limit", 20))
    offset = _normalize_offset(request.args.get("offset", 0))
    return jsonify(_build_movies_response(limit, offset))


@api_v1_bp.get("/api/v1/youtube")
def api_v1_youtube():
    limit = _normalize_limit(request.args.get("limit", 50), default=50)
    offset = _normalize_offset(request.args.get("offset", 0))
    source = request.args.get("source", "all")
    section = request.args.get("section", "")
    query = request.args.get("q", "")
    return jsonify(_build_youtube_response(limit, offset, source=source, section=section, query=query))


@api_v1_bp.get("/api/v1/youtube/videos")
def api_v1_youtube_videos():
    limit = _normalize_limit(request.args.get("limit", 50), default=50, maximum=200)
    offset = _normalize_offset(request.args.get("offset", 0))
    section = request.args.get("section", "")
    return jsonify(_build_youtube_videos_response(limit, offset, section=section))


@api_v1_bp.get("/api/v1/youtube/sections")
def api_v1_youtube_sections():
    return jsonify(_build_youtube_sections_response())


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
