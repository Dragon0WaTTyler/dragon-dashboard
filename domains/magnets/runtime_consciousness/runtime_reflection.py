from __future__ import annotations


def build_runtime_reflection(
    *,
    awareness_state: str,
    degradation_risk: int,
    runtime_resilience: int,
    cinematic_direction: str,
    adaptation_pressure: int,
) -> dict[str, int | str]:
    if degradation_risk >= 74:
        state = "degraded_reflection"
    elif runtime_resilience >= 78 and adaptation_pressure <= 34:
        state = "resilient_reflection"
    elif "cinematic" in cinematic_direction or awareness_state == "cinematic_awareness":
        state = "cinematic_reflection"
    elif adaptation_pressure >= 56:
        state = "adaptive_reflection"
    else:
        state = "stable_reflection"
    strength = max(0, min(100, round(((100 - degradation_risk) * 0.36) + (runtime_resilience * 0.34) + (max(0, 100 - adaptation_pressure) * 0.3))))
    return {
        "state": state,
        "reflection_strength": strength,
    }
