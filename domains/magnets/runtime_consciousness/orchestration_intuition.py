from __future__ import annotations


def build_orchestration_intuition(
    *,
    degradation_risk: int,
    runtime_resilience: int,
    pressure_direction: str,
    cinematic_direction: str,
    continuity_awareness: str,
) -> dict[str, str]:
    if degradation_risk >= 75:
        state = "degradation_intuition"
    elif "fallback" in pressure_direction or continuity_awareness == "fragmented_awareness":
        state = "fallback_intuition"
    elif runtime_resilience >= 76:
        state = "resilience_intuition"
    elif "cinematic" in cinematic_direction:
        state = "cinematic_intuition"
    else:
        state = "stabilization_intuition"
    return {
        "state": state,
        "intuition_anchor": state,
    }
