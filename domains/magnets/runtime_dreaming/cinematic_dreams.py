from __future__ import annotations

from typing import Any

from .dream_metrics import clamp


def build_cinematic_dreams(
    *,
    cinematic_quality: int = 0,
    cinematic_underflow: str = "",
    cinematic_instinct: str = "",
    cinematic_direction: str = "",
) -> dict[str, Any]:
    strength = clamp(
        (cinematic_quality * 0.46)
        + (16 if "stable" in cinematic_underflow or "resilient" in cinematic_underflow else 6)
        + (14 if cinematic_instinct in {"cinematic_preserving", "cinematic_resilient"} else 6)
        + (12 if "cinematic" in cinematic_direction else 0)
    )
    if "fragile" in cinematic_underflow or "constrained" in cinematic_direction:
        state = "fragmented_cinema_dream"
    elif strength >= 78 and cinematic_underflow == "cinematic_underflow_stable":
        state = "immersive_dream"
    elif "resilient" in cinematic_underflow:
        state = "resilient_cinema_dream"
    elif "stable" in cinematic_underflow:
        state = "stabilized_cinema_dream"
    else:
        state = "adaptive_cinema_dream"
    return {"state": state, "cinematic_projection_strength": strength}
