from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping


COORDINATION_EVENT_TYPES = {
    "runtime_negotiated",
    "runtime_rejected",
    "adaptation_triggered",
    "runtime_rebalanced",
    "fallback_escalated",
    "recovery_negotiated",
    "coordination_failed",
}


def build_coordination_event(
    event_type: str,
    *,
    coordination_state: str = "",
    details: Mapping[str, Any] | None = None,
    event_order: int = 0,
) -> dict[str, Any]:
    normalized_type = str(event_type or "").strip()
    if normalized_type not in COORDINATION_EVENT_TYPES:
        normalized_type = "coordination_failed"
    return {
        "event_type": normalized_type,
        "coordination_state": str(coordination_state or "").strip(),
        "event_order": int(event_order or 0),
        "timestamp": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "details": dict(details or {}),
    }


def append_coordination_event(events: list[Mapping[str, Any]] | None, event: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    payload = [dict(item) for item in (events or []) if isinstance(item, Mapping)]
    if isinstance(event, Mapping):
        payload.append(dict(event))
    return payload
