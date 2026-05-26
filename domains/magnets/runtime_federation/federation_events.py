from __future__ import annotations

from typing import Any


def build_federation_events(
    *,
    phase_transition: str,
    orchestration_unity: str,
    continuity_projection: str,
    cinematic_runtime_state: str,
    instability_layers: list[str],
    previous_phase_transition: str = "",
) -> list[dict[str, Any]]:
    events = [
        {
            "event_type": "federation_synthesized",
            "phase_transition": phase_transition,
            "orchestration_unity": orchestration_unity,
            "continuity_projection": continuity_projection,
            "cinematic_runtime_state": cinematic_runtime_state,
        }
    ]
    if instability_layers:
        events.append(
            {
                "event_type": "instability_convergence_detected",
                "layers": list(instability_layers),
            }
        )
    if previous_phase_transition and previous_phase_transition != phase_transition:
        events.append(
            {
                "event_type": "runtime_phase_transition_changed",
                "from_phase": previous_phase_transition,
                "to_phase": phase_transition,
            }
        )
    return events
