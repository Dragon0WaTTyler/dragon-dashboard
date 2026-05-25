from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

from dragon.cache import load_json_file, save_json_file
from dragon.paths import CACHE_DIR


DEFAULT_EXPERIMENTAL_SESSION_PAYLOAD = {
    "version": 1,
    "sessions": {},
}


class ExperimentalSessionStore:
    def __init__(self, path: Path | None = None) -> None:
        self.path = Path(path or (CACHE_DIR / "magnets" / "experimental_sessions.json"))
        self._lock = threading.Lock()

    def list_sessions(self) -> list[dict[str, Any]]:
        with self._lock:
            payload = self._load()
        sessions = list((payload.get("sessions") or {}).values())
        sessions.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
        return sessions

    def get_session(self, session_id: str) -> dict[str, Any] | None:
        normalized_id = str(session_id or "").strip()
        if not normalized_id:
            return None
        with self._lock:
            payload = self._load()
            session = (payload.get("sessions") or {}).get(normalized_id)
        return dict(session) if isinstance(session, dict) else None

    def save_session(self, session: dict[str, Any]) -> dict[str, Any]:
        session_payload = dict(session or {})
        session_id = str(session_payload.get("session_id") or "").strip()
        if not session_id:
            raise ValueError("session_id is required")
        with self._lock:
            payload = self._load()
            payload["sessions"][session_id] = session_payload
            save_json_file(self.path, payload)
            return dict(payload["sessions"][session_id])

    def _load(self) -> dict[str, Any]:
        payload = load_json_file(self.path, DEFAULT_EXPERIMENTAL_SESSION_PAYLOAD)
        if not isinstance(payload, dict):
            payload = {}
        sessions = payload.get("sessions")
        if not isinstance(sessions, dict):
            sessions = {}
        normalized_sessions = {
            str(key or "").strip(): dict(value)
            for key, value in sessions.items()
            if str(key or "").strip() and isinstance(value, dict)
        }
        return {
            "version": 1,
            "sessions": normalized_sessions,
        }
