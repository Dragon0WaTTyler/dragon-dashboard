from __future__ import annotations

import math
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from dragon.cache import load_json_file, save_json_file


WATCH_PROGRESS_MIN_SECONDS = 5.0
WATCH_PROGRESS_COMPLETE_RATIO = 0.9
WATCH_PROGRESS_RESUME_PADDING_SECONDS = 10.0


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def _normalized_text(value: Any) -> str:
    return str(value or "").strip()


def _parse_iso_timestamp(value: Any) -> datetime | None:
    raw_value = _normalized_text(value)
    if not raw_value:
        return None
    try:
        return datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _format_seconds_label(value: Any) -> str:
    total_seconds = max(int(_safe_float(value) or 0), 0)
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    if hours > 0:
        return f"{hours}:{minutes:02d}:{seconds:02d}"
    return f"{minutes:02d}:{seconds:02d}"


class MovieWatchProgressService:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.lock = threading.Lock()

    def _default_payload(self) -> dict[str, Any]:
        return {"entries": {}}

    def _load(self) -> dict[str, Any]:
        payload = load_json_file(self.path, self._default_payload())
        if not isinstance(payload, dict):
            payload = self._default_payload()
        entries = payload.get("entries")
        if not isinstance(entries, dict):
            payload["entries"] = {}
        return payload

    def _save(self, payload: Mapping[str, Any]) -> bool:
        return bool(save_json_file(self.path, dict(payload)))

    def build_movie_key(self, *, movie_id: Any = "", tmdb_id: Any = "", title: Any = "") -> str:
        movie_text = _normalized_text(movie_id)
        if movie_text:
            return movie_text
        tmdb_text = _normalized_text(tmdb_id)
        if tmdb_text:
            return f"tmdb:{tmdb_text}"
        title_text = _normalized_text(title).lower()
        if title_text:
            return f"title:{title_text}"
        return ""

    def load_progress(self, *, movie_id: Any = "", tmdb_id: Any = "", title: Any = "") -> dict[str, Any]:
        key = self.build_movie_key(movie_id=movie_id, tmdb_id=tmdb_id, title=title)
        with self.lock:
            payload = self._load()
            entry = dict((payload.get("entries") or {}).get(key) or {}) if key else {}
        return self._public_payload(key=key, entry=entry)

    def list_progress(self, *, include_completed: bool = True, only_resumable: bool = False, limit: int | None = None) -> list[dict[str, Any]]:
        with self.lock:
            payload = self._load()
            raw_entries = list((payload.get("entries") or {}).items())

        items: list[dict[str, Any]] = []
        for key, entry in raw_entries:
            public_entry = self._public_payload(key=str(key or ""), entry=dict(entry or {}))
            if not public_entry.get("has_progress"):
                continue
            if not include_completed and public_entry.get("completed"):
                continue
            if only_resumable and not public_entry.get("resume_available"):
                continue
            items.append(public_entry)

        items.sort(
            key=lambda item: _parse_iso_timestamp(item.get("updated_at")) or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        if limit is not None:
            return items[: max(int(limit), 0)]
        return items

    def list_continue_watching(self, *, limit: int | None = None) -> list[dict[str, Any]]:
        return self.list_progress(include_completed=False, only_resumable=True, limit=limit)

    def save_progress(self, progress: Mapping[str, Any] | None) -> tuple[dict[str, Any], int]:
        payload = dict(progress or {})
        key = self.build_movie_key(
            movie_id=payload.get("movie_id"),
            tmdb_id=payload.get("tmdb_id"),
            title=payload.get("title"),
        )
        if not key:
            return {"ok": False, "error": "movie_id, tmdb_id, or title is required", "code": "invalid_movie_id"}, 400

        current_time = _safe_float(payload.get("current_time"))
        duration = _safe_float(payload.get("duration"))
        if current_time is None or duration is None or duration <= 0 or current_time < 0:
            return {"ok": False, "error": "current_time and duration must be valid positive numbers", "code": "invalid_progress"}, 400

        normalized_duration = max(duration, 0.0)
        normalized_current_time = min(max(current_time, 0.0), normalized_duration)
        explicit_completed = bool(payload.get("completed"))
        completed = bool(
            explicit_completed
            or (normalized_duration > 0 and (normalized_current_time / normalized_duration) >= WATCH_PROGRESS_COMPLETE_RATIO)
        )

        title_text = _normalized_text(payload.get("title"))
        tmdb_text = _normalized_text(payload.get("tmdb_id"))
        if normalized_current_time < WATCH_PROGRESS_MIN_SECONDS and not completed:
            public = self.load_progress(movie_id=payload.get("movie_id"), tmdb_id=tmdb_text, title=title_text)
            public.update(
                {
                    "ok": True,
                    "saved": False,
                    "ignored": True,
                    "completed": False,
                    "local_state": "in_progress" if public.get("has_progress") else "not_started",
                }
            )
            return public, 200

        entry = {
            "movie_id": _normalized_text(payload.get("movie_id")),
            "tmdb_id": tmdb_text,
            "title": title_text,
            "current_time": normalized_duration if completed else normalized_current_time,
            "duration": normalized_duration,
            "completed": completed,
            "local_state": "watched" if completed else "in_progress",
            "updated_at": _utc_now_iso(),
        }

        with self.lock:
            stored = self._load()
            entries = dict(stored.get("entries") or {})
            entries[key] = entry
            stored["entries"] = entries
            self._save(stored)

        public = self._public_payload(key=key, entry=entry)
        public.update({"ok": True, "saved": True, "ignored": False})
        return public, 200

    def _public_payload(self, *, key: str, entry: Mapping[str, Any] | None) -> dict[str, Any]:
        item = dict(entry or {})
        current_time = _safe_float(item.get("current_time")) or 0.0
        duration = _safe_float(item.get("duration")) or 0.0
        completed = bool(item.get("completed"))
        resume_time = 0.0
        resume_available = False
        if not completed and duration > 0 and current_time >= WATCH_PROGRESS_MIN_SECONDS:
            resume_time = min(current_time, max(duration - WATCH_PROGRESS_RESUME_PADDING_SECONDS, 0.0))
            resume_available = resume_time >= WATCH_PROGRESS_MIN_SECONDS
        progress_percent = 0.0
        if duration > 0:
            progress_percent = min(max((current_time / duration) * 100.0, 0.0), 100.0)
        return {
            "ok": True,
            "movie_id": _normalized_text(item.get("movie_id") or key),
            "tmdb_id": _normalized_text(item.get("tmdb_id")),
            "title": _normalized_text(item.get("title")),
            "current_time": current_time,
            "duration": duration,
            "completed": completed,
            "local_state": _normalized_text(item.get("local_state") or ("watched" if completed else "")),
            "resume_time": resume_time,
            "resume_available": resume_available,
            "resume_label": f"Continue from {_format_seconds_label(resume_time)}" if resume_available else "",
            "progress_percent": progress_percent,
            "progress_percent_label": f"{int(round(progress_percent))}%" if duration > 0 else "",
            "current_time_label": _format_seconds_label(current_time) if current_time > 0 else "",
            "duration_label": _format_seconds_label(duration) if duration > 0 else "",
            "has_progress": bool(item),
            "updated_at": _normalized_text(item.get("updated_at")),
        }
