from __future__ import annotations

from typing import Any


def build_cinematic_events(
    *,
    direction_style: str = "",
    pacing: str = "",
    immersion_state: str = "",
    atmosphere: str = "",
    balance_state: str = "",
    runtime_polish: int = 0,
    previous_direction: str = "",
    previous_balance_state: str = "",
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    if previous_direction and previous_direction != direction_style:
        events.append({"event": "cinematic_shift_detected", "from": previous_direction, "to": direction_style})
    if pacing in {"volatile_pacing", "recovery_pacing"}:
        events.append({"event": "pacing_fragmented", "pacing": pacing})
    if immersion_state in {"degraded_immersion", "fragile_immersion"}:
        events.append({"event": "immersion_reduced", "immersion_state": immersion_state})
    if atmosphere in {"calm_atmosphere", "resilient_atmosphere"}:
        events.append({"event": "atmosphere_stabilized", "atmosphere": atmosphere})
    if previous_balance_state and previous_balance_state != balance_state and balance_state in {"balanced_cinema", "resilient_cinema"}:
        events.append({"event": "cinematic_balance_recovered", "balance_state": balance_state})
    if runtime_polish >= 82:
        events.append({"event": "runtime_polish_elevated", "runtime_polish": runtime_polish})
    return events
