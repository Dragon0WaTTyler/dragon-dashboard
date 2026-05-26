from __future__ import annotations

from typing import Any, Mapping


EXECUTION_EVENT_TYPES = {
    "runtime_bootstrap_started",
    "transport_prepared",
    "startup_degraded",
    "runtime_recovered",
    "runtime_fallback_selected",
    "runtime_failed",
    "runtime_completed",
}


def build_execution_event(
    event_type: str,
    *,
    event_order: int,
    execution_state: str = "",
    details: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    normalized = str(event_type or "").strip()
    if normalized not in EXECUTION_EVENT_TYPES:
        raise ValueError(f"Unsupported execution event type: {normalized}")
    return {
        "event_type": normalized,
        "event_order": int(event_order),
        "execution_state": str(execution_state or "").strip(),
        "details": dict(details or {}),
    }


def append_execution_event(events: list[Mapping[str, Any]] | None, event: Mapping[str, Any]) -> list[dict[str, Any]]:
    payload = [dict(item) for item in (events or []) if isinstance(item, Mapping)]
    payload.append(dict(event or {}))
    return payload
