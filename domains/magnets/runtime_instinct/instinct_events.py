from __future__ import annotations

from typing import Any


def build_instinct_events(
    *,
    stabilization_state: str = "",
    fallback_state: str = "",
    continuity_state: str = "",
    resilience_state: str = "",
    survival_state: str = "",
    cinematic_state: str = "",
    instinct_integrity: int = 0,
    previous_stabilization_state: str = "",
    previous_survival_state: str = "",
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    if stabilization_state in {"strong_stabilization", "resilient_stabilization"}:
        events.append({"event": "stabilization_instinct_elevated", "stabilization_state": stabilization_state})
    if fallback_state in {"fallback_aggressive", "fallback_recovery"}:
        events.append({"event": "fallback_reflex_triggered", "fallback_state": fallback_state})
    if continuity_state == "continuity_fragmented":
        events.append({"event": "continuity_instinct_fragmented", "continuity_state": continuity_state})
    if resilience_state in {"resilience_recovering", "resilience_balanced"}:
        events.append({"event": "resilience_instinct_recovered", "resilience_state": resilience_state})
    if previous_survival_state and previous_survival_state != survival_state and survival_state in {"survival_stable", "survival_resilient"}:
        events.append({"event": "orchestration_survival_stabilized", "survival_state": survival_state})
    if cinematic_state in {"cinematic_preserving", "cinematic_resilient"}:
        events.append({"event": "cinematic_instinct_preserved", "cinematic_state": cinematic_state})
    if previous_stabilization_state and previous_stabilization_state != stabilization_state:
        events.append({"event": "stabilization_instinct_shifted", "from": previous_stabilization_state, "to": stabilization_state})
    if instinct_integrity >= 84:
        events.append({"event": "instinct_integrity_hardened", "instinct_integrity": instinct_integrity})
    return events
