from __future__ import annotations

from typing import Any

from .cinematic_profiles import clamp


def build_continuity_cinema(
    *,
    continuity_state: str = "",
    continuity_confidence: int = 0,
    switch_frequency: int = 0,
    drift_score: int = 0,
) -> dict[str, Any]:
    strength = clamp(continuity_confidence - (switch_frequency * 9) - (drift_score * 0.18))
    if continuity_state == "stable" and strength >= 72:
        state = "preserved_continuity"
    elif switch_frequency >= 3:
        state = "fragmented_continuity"
    elif strength >= 64:
        state = "resilient_continuity"
    elif strength >= 46:
        state = "adaptive_continuity"
    else:
        state = "unstable_continuity"
    return {
        "continuity": state,
        "continuity_strength": strength,
        "fragmentation_risk": clamp(100 - strength),
    }
