from __future__ import annotations

from typing import Any


def build_ecosystem_events(
    *,
    balance_state: str = "",
    pressure_direction: str = "",
    equilibrium_state: str = "",
    topology: str = "",
    degradation_current: str = "",
    previous_balance_state: str = "",
    previous_topology: str = "",
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    if previous_balance_state and previous_balance_state != balance_state:
        events.append({"event": "ecosystem_shift_detected", "from": previous_balance_state, "to": balance_state})
        if balance_state in {"balanced", "resilience_stable"}:
            events.append({"event": "ecosystem_balance_recovered", "balance_state": balance_state})
    if pressure_direction == "escalating":
        events.append({"event": "pressure_escalation_detected"})
    if equilibrium_state == "equilibrium_fragmented":
        events.append({"event": "equilibrium_fragmented"})
    if previous_topology and previous_topology != topology:
        events.append({"event": "resilience_topology_changed", "from": previous_topology, "to": topology})
    if degradation_current in {"cascading_degradation", "fallback_propagation"}:
        events.append({"event": "degradation_current_elevated", "current": degradation_current})
    return events
