from __future__ import annotations

from typing import Any

from .cinematic_profiles import clamp


def build_cinematic_balance(
    *,
    immersion_strength: int = 0,
    tension_score: int = 0,
    continuity_strength: int = 0,
    atmosphere_integrity: int = 0,
    equilibrium_state: str = "",
) -> dict[str, Any]:
    balance_score = clamp((immersion_strength + continuity_strength + atmosphere_integrity + max(0, 100 - tension_score)) / 4)
    if equilibrium_state == "equilibrium_fragmented" or balance_score <= 42:
        state = "degraded_cinema"
    elif tension_score >= 74:
        state = "pressure_heavy_cinema"
    elif continuity_strength >= 70 and atmosphere_integrity >= 70:
        state = "balanced_cinema"
    elif immersion_strength >= 66:
        state = "resilient_cinema"
    else:
        state = "adaptive_cinema"
    return {
        "balance_state": state,
        "balance_score": balance_score,
    }
