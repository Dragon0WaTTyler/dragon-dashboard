from __future__ import annotations

from typing import Any

from .cinematic_profiles import clamp


def build_dramatic_tension(
    *,
    pressure_score: int = 0,
    degradation_risk: int = 0,
    adaptation_pressure: int = 0,
    fallback_probability: float = 0.0,
) -> dict[str, Any]:
    tension_score = clamp((pressure_score * 0.36) + (degradation_risk * 0.28) + (adaptation_pressure * 0.24) + (fallback_probability * 100 * 0.18))
    if degradation_risk >= 76:
        tension = "degradation_tension"
    elif adaptation_pressure >= 70:
        tension = "escalating_tension"
    elif pressure_score >= 66:
        tension = "constrained_tension"
    elif fallback_probability <= 0.24:
        tension = "stable_tension"
    else:
        tension = "resilience_tension"
    return {
        "tension": tension,
        "tension_score": tension_score,
        "pressure_band": "high" if tension_score >= 70 else "moderate" if tension_score >= 42 else "low",
    }
