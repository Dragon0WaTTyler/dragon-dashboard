from __future__ import annotations

from typing import Any

from .cinematic_profiles import clamp, preferred_cinematic_runtime


def build_immersion_state(
    *,
    playback_runtime: str = "",
    startup_confidence: str = "",
    degradation_risk: int = 0,
    runtime_resilience: int = 0,
    continuity_confidence: int = 0,
) -> dict[str, Any]:
    confidence_bonus = {"high": 14, "medium": 4, "low": -8}.get(str(startup_confidence or "").strip(), 0)
    runtime_bonus = 8 if preferred_cinematic_runtime(playback_runtime) else -4
    strength = clamp(44 + confidence_bonus + runtime_bonus + (runtime_resilience * 0.24) + (continuity_confidence * 0.18) - (degradation_risk * 0.28))
    if strength >= 78:
        state = "immersive"
    elif strength >= 66:
        state = "resilient_immersion"
    elif strength >= 48:
        state = "partially_immersive"
    elif degradation_risk >= 72:
        state = "degraded_immersion"
    else:
        state = "fragile_immersion"
    return {
        "state": state,
        "immersion_strength": strength,
        "immersion_stability": clamp((runtime_resilience + continuity_confidence + max(0, 100 - degradation_risk)) / 3),
    }
