from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping


def build_runtime_event(
    event_type: str,
    *,
    runtime_id: str = "",
    session_id: str = "",
    runtime_state: str = "",
    runtime_mode: str = "",
    details: Mapping[str, Any] | None = None,
    timestamp: str | None = None,
) -> dict[str, Any]:
    return {
        "event_type": str(event_type or "").strip(),
        "runtime_id": str(runtime_id or "").strip(),
        "session_id": str(session_id or "").strip(),
        "runtime_state": str(runtime_state or "").strip(),
        "runtime_mode": str(runtime_mode or "").strip(),
        "details": dict(details or {}),
        "timestamp": str(timestamp or _now_iso8601()),
    }


def append_runtime_event(events: list[Mapping[str, Any]] | None, event: Mapping[str, Any]) -> list[dict[str, Any]]:
    payload = [dict(item) for item in (events or []) if isinstance(item, Mapping)]
    payload.append(dict(event or {}))
    return payload


def _now_iso8601() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
