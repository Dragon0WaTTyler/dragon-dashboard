from __future__ import annotations

from typing import Any

from .cinematic_profiles import clamp


def build_runtime_atmosphere(
    *,
    pressure_direction: str = "",
    climate: str = "",
    immersion_strength: int = 0,
    runtime_resilience: int = 0,
    degradation_risk: int = 0,
) -> dict[str, Any]:
    integrity = clamp((immersion_strength * 0.4) + (runtime_resilience * 0.3) + ((100 - degradation_risk) * 0.3))
    if degradation_risk >= 74:
        atmosphere = "degraded_atmosphere"
    elif pressure_direction == "escalating":
        atmosphere = "tense_atmosphere"
    elif runtime_resilience >= 76:
        atmosphere = "resilient_atmosphere"
    elif "adaptive" in str(climate or ""):
        atmosphere = "adaptive_atmosphere"
    elif immersion_strength >= 72:
        atmosphere = "cinematic_atmosphere"
    else:
        atmosphere = "calm_atmosphere"
    return {
        "atmosphere": atmosphere,
        "atmosphere_integrity": integrity,
        "atmosphere_bias": "tension_bias" if "tense" in atmosphere or "degraded" in atmosphere else "immersion_bias",
    }
