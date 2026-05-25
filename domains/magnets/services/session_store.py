from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

from dragon.cache import load_json_file, save_json_file
from dragon.paths import CACHE_DIR

from ..sessions import StreamSession, normalize_session_state

DEFAULT_SESSION_PAYLOAD = {
    "version": 1,
    "sessions": {},
}


class StreamSessionStore:
    def __init__(self, path: Path | None = None) -> None:
        self.path = Path(path or (CACHE_DIR / "magnets" / "sessions.json"))
        self._lock = threading.Lock()

    def list_sessions(self) -> list[dict[str, Any]]:
        with self._lock:
            payload = self._load()
        sessions = list((payload.get("sessions") or {}).values())
        sessions.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
        return sessions

    def get_session(self, session_id: str) -> dict[str, Any] | None:
        normalized_session_id = str(session_id or "").strip()
        if not normalized_session_id:
            return None
        with self._lock:
            payload = self._load()
            session = (payload.get("sessions") or {}).get(normalized_session_id)
            return dict(session) if isinstance(session, dict) else None

    def save_session(self, session: StreamSession | dict[str, Any]) -> dict[str, Any]:
        session_payload = session.to_dict() if isinstance(session, StreamSession) else dict(session or {})
        session_id = str(session_payload.get("session_id") or "").strip()
        if not session_id:
            raise ValueError("session_id is required")
        with self._lock:
            payload = self._load()
            payload["sessions"][session_id] = self._normalize_session(session_payload)
            save_json_file(self.path, payload)
            return dict(payload["sessions"][session_id])

    def delete_session(self, session_id: str) -> bool:
        normalized_session_id = str(session_id or "").strip()
        if not normalized_session_id:
            return False
        with self._lock:
            payload = self._load()
            sessions = payload.get("sessions") or {}
            if normalized_session_id not in sessions:
                return False
            sessions.pop(normalized_session_id, None)
            return save_json_file(self.path, payload)

    def _load(self) -> dict[str, Any]:
        payload = load_json_file(self.path, DEFAULT_SESSION_PAYLOAD)
        if not isinstance(payload, dict):
            payload = {}
        sessions_payload = payload.get("sessions")
        normalized_sessions: dict[str, dict[str, Any]] = {}
        if isinstance(sessions_payload, dict):
            items = sessions_payload.items()
        else:
            items = []
            if isinstance(sessions_payload, list):
                items = (
                    (str(item.get("session_id") or "").strip(), item)
                    for item in sessions_payload
                    if isinstance(item, dict)
                )
        for session_id, session in items:
            normalized_id = str(session_id or "").strip()
            if not normalized_id or not isinstance(session, dict):
                continue
            normalized_sessions[normalized_id] = self._normalize_session(session)
        return {
            "version": 1,
            "sessions": normalized_sessions,
        }

    def _normalize_session(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = StreamSession(
            session_id=str(payload.get("session_id") or "").strip(),
            movie_id=str(payload.get("movie_id") or "").strip(),
            source_fingerprint=str(payload.get("source_fingerprint") or "").strip(),
            handoff_mode=str(payload.get("handoff_mode") or "").strip(),
            preferred_runtime=str(payload.get("preferred_runtime") or "").strip(),
            session_state=normalize_session_state(payload.get("session_state")),
            compatibility_snapshot=dict(payload.get("compatibility_snapshot") or {}),
            created_at=str(payload.get("created_at") or "").strip(),
            updated_at=str(payload.get("updated_at") or "").strip(),
            runtime_intent=str(payload.get("runtime_intent") or "").strip(),
            admission_policy=dict(payload.get("admission_policy") or {}),
            movie_title=str(payload.get("movie_title") or "").strip(),
            failure_reason=str(payload.get("failure_reason") or "").strip(),
        )
        if not normalized.created_at:
            normalized.created_at = normalized.updated_at or ""
        if not normalized.updated_at:
            normalized.updated_at = normalized.created_at or ""
        return normalized.to_dict()
