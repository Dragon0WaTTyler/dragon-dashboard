from __future__ import annotations

import hashlib
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping


def build_browser_runtime_session(
    *,
    linked_stream_runtime_id: str,
    playback_session_id: str,
    runtime_state: str,
    capability_snapshot: Mapping[str, Any] | None,
    bootstrap_summary: Mapping[str, Any] | None,
    execution_state: str = "",
    execution_metrics: Mapping[str, Any] | None = None,
    execution_timeline: Mapping[str, Any] | None = None,
    simulated_runtime_health: str = "",
    recovery_path: Mapping[str, Any] | None = None,
    execution_events: list[Mapping[str, Any]] | None = None,
    coordination_state: str = "",
    coordination_metrics: Mapping[str, Any] | None = None,
    ttl_minutes: int = 20,
) -> dict[str, Any]:
    created_at = _now_iso8601()
    expires_at = _expires_at_iso8601(ttl_minutes)
    return {
        "runtime_session_id": _runtime_session_id(linked_stream_runtime_id, playback_session_id),
        "linked_stream_runtime_id": str(linked_stream_runtime_id or "").strip(),
        "playback_session_id": str(playback_session_id or "").strip(),
        "runtime_state": str(runtime_state or "").strip(),
        "capability_snapshot": dict(capability_snapshot or {}),
        "bootstrap_summary": dict(bootstrap_summary or {}),
        "execution_state": str(execution_state or "").strip(),
        "execution_metrics": dict(execution_metrics or {}),
        "execution_timeline": dict(execution_timeline or {}),
        "simulated_runtime_health": str(simulated_runtime_health or "").strip(),
        "recovery_path": dict(recovery_path or {}),
        "execution_events": [dict(item) for item in (execution_events or []) if isinstance(item, Mapping)],
        "coordination_state": str(coordination_state or "").strip(),
        "coordination_metrics": dict(coordination_metrics or {}),
        "created_at": created_at,
        "expires_at": expires_at,
    }


class InMemoryBrowserRuntimeSessionRegistry:
    def __init__(self) -> None:
        self._sessions: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()

    def create(self, session: Mapping[str, Any]) -> dict[str, Any]:
        payload = _normalize_session(session)
        runtime_session_id = str(payload.get("runtime_session_id") or "").strip()
        if not runtime_session_id:
            raise ValueError("runtime_session_id is required")
        with self._lock:
            self._sessions[runtime_session_id] = payload
            return dict(payload)

    def get(self, runtime_session_id: str) -> dict[str, Any] | None:
        with self._lock:
            session = self._sessions.get(str(runtime_session_id or "").strip())
            return dict(session) if isinstance(session, dict) else None

    def update(self, runtime_session_id: str, updates: Mapping[str, Any]) -> dict[str, Any] | None:
        normalized_id = str(runtime_session_id or "").strip()
        if not normalized_id:
            return None
        with self._lock:
            current = self._sessions.get(normalized_id)
            if not isinstance(current, dict):
                return None
            payload = dict(current)
            payload.update(dict(updates or {}))
            self._sessions[normalized_id] = _normalize_session(payload)
            return dict(self._sessions[normalized_id])

    def expire(self, runtime_session_id: str) -> dict[str, Any] | None:
        return self.update(runtime_session_id, {"runtime_state": "expired"})

    def list(self) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(item) for item in self._sessions.values()]


_DEFAULT_REGISTRY = InMemoryBrowserRuntimeSessionRegistry()


def get_browser_runtime_session_registry() -> InMemoryBrowserRuntimeSessionRegistry:
    return _DEFAULT_REGISTRY


def _runtime_session_id(linked_stream_runtime_id: str, playback_session_id: str) -> str:
    payload = "|".join([str(linked_stream_runtime_id or "").strip(), str(playback_session_id or "").strip()]) or "browser-runtime-session"
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:20]


def _normalize_session(session: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "runtime_session_id": str(session.get("runtime_session_id") or "").strip(),
        "linked_stream_runtime_id": str(session.get("linked_stream_runtime_id") or "").strip(),
        "playback_session_id": str(session.get("playback_session_id") or "").strip(),
        "runtime_state": str(session.get("runtime_state") or "").strip(),
        "capability_snapshot": dict(session.get("capability_snapshot") or {}),
        "bootstrap_summary": dict(session.get("bootstrap_summary") or {}),
        "execution_state": str(session.get("execution_state") or "").strip(),
        "execution_metrics": dict(session.get("execution_metrics") or {}),
        "execution_timeline": dict(session.get("execution_timeline") or {}),
        "simulated_runtime_health": str(session.get("simulated_runtime_health") or "").strip(),
        "recovery_path": dict(session.get("recovery_path") or {}),
        "execution_events": [dict(item) for item in session.get("execution_events") or [] if isinstance(item, Mapping)],
        "coordination_state": str(session.get("coordination_state") or "").strip(),
        "coordination_metrics": dict(session.get("coordination_metrics") or {}),
        "created_at": str(session.get("created_at") or _now_iso8601()),
        "expires_at": str(session.get("expires_at") or _expires_at_iso8601(20)),
    }


def _now_iso8601() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _expires_at_iso8601(ttl_minutes: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(minutes=max(int(ttl_minutes or 0), 1))).replace(microsecond=0).isoformat().replace("+00:00", "Z")
