from __future__ import annotations

from typing import Any

from .instinct_metrics import clamp


def build_cinematic_instinct(
    *,
    cinematic_quality: int = 0,
    cinematic_direction: str = "",
    continuity_style: str = "",
    fallback_intensity: int = 0,
    immersion_state: str = "",
) -> dict[str, Any]:
    preservation = clamp(
        (cinematic_quality * 0.48)
        + (max(0, 100 - fallback_intensity) * 0.18)
        + (16 if "cinematic" in cinematic_direction else 0)
        + (10 if "preserving" in continuity_style or "resilient" in continuity_style else 0)
        + (8 if "immersive" in immersion_state else 0)
    )
    if fallback_intensity >= 78 or "constrained" in cinematic_direction:
        state = "cinematic_constrained"
    elif "recover" in cinematic_direction or "fragile" in cinematic_direction:
        state = "cinematic_recovering"
    elif preservation >= 78 and "cinematic" in cinematic_direction:
        state = "cinematic_preserving"
    elif preservation >= 70:
        state = "cinematic_resilient"
    else:
        state = "cinematic_balanced"
    return {
        "state": state,
        "cinematic_preservation": preservation,
    }
