from __future__ import annotations

from typing import Any


def build_symbiosis_events(
    *,
    symbiotic_phase: str = "",
    runtime_coexistence: str = "",
    cooperative_runtime_state: str = "",
    dependency_stress: int = 0,
    systemic_runtime_health: str = "",
    previous_phase: str = "",
) -> list[dict[str, Any]]:
    events = [
        {
            "event_type": "symbiosis_synthesized",
            "symbiotic_phase": str(symbiotic_phase or "measured_symbiosis"),
            "runtime_coexistence": str(runtime_coexistence or "measured_coexistence"),
            "cooperative_runtime_state": str(cooperative_runtime_state or "adaptive_coexistence"),
        }
    ]
    if previous_phase and previous_phase != symbiotic_phase:
        events.append(
            {
                "event_type": "symbiotic_phase_shift",
                "from_phase": str(previous_phase),
                "to_phase": str(symbiotic_phase),
            }
        )
    if dependency_stress >= 68:
        events.append(
            {
                "event_type": "dependency_stress_elevated",
                "dependency_stress": int(dependency_stress or 0),
                "systemic_runtime_health": str(systemic_runtime_health or "pressured"),
            }
        )
    return events
