from __future__ import annotations

from typing import Any

from .cinematic_profiles import clamp


def build_scene_energy(
    *,
    tension_score: int = 0,
    adaptation_pressure: int = 0,
    pacing: str = "",
    runtime_resilience: int = 0,
) -> dict[str, Any]:
    energy_score = clamp((tension_score * 0.46) + (adaptation_pressure * 0.24) + (runtime_resilience * 0.18) + (8 if "cinematic" in pacing else 0))
    if tension_score >= 74:
        energy = "escalating_energy"
    elif adaptation_pressure >= 66:
        energy = "adaptive_energy"
    elif runtime_resilience <= 46:
        energy = "degraded_energy"
    elif "constrained" in pacing or "recovery" in pacing:
        energy = "constrained_energy"
    else:
        energy = "stable_energy"
    return {
        "energy": energy,
        "energy_score": energy_score,
    }
