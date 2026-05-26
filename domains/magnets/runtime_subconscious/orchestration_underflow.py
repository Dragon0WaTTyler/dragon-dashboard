from __future__ import annotations

from typing import Any

from .subconscious_metrics import clamp


def build_orchestration_underflow(
    *,
    pressure_direction: str = "",
    pressure_score: int = 0,
    degradation_risk: int = 0,
    fallback_intensity: int = 0,
    runtime_resilience: int = 0,
) -> dict[str, Any]:
    underflow_strength = clamp(
        ((100 - pressure_score) * 0.18)
        + ((100 - degradation_risk) * 0.26)
        + ((100 - fallback_intensity) * 0.16)
        + (runtime_resilience * 0.4)
    )
    if degradation_risk >= 78 or fallback_intensity >= 76:
        state = "fragmented_underflow"
    elif pressure_direction == "rising" or pressure_score >= 68:
        state = "pressured_underflow"
    elif runtime_resilience >= 82:
        state = "resilient_underflow"
    elif pressure_direction == "steady" and degradation_risk <= 30:
        state = "calm_underflow"
    else:
        state = "adaptive_underflow"
    return {
        "state": state,
        "underflow_strength": underflow_strength,
    }
