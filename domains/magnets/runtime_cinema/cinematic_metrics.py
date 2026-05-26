from __future__ import annotations

from typing import Any

from .cinematic_profiles import clamp


def build_cinematic_metrics(
    *,
    immersion_strength: int = 0,
    pacing_stability: int = 0,
    atmosphere_integrity: int = 0,
    continuity_strength: int = 0,
    tension_score: int = 0,
    runtime_polish: int = 0,
    cinematic_balance_score: int = 0,
) -> dict[str, Any]:
    cinematic_quality = clamp(
        (immersion_strength * 0.18)
        + (pacing_stability * 0.16)
        + (atmosphere_integrity * 0.16)
        + (continuity_strength * 0.16)
        + (runtime_polish * 0.18)
        + (cinematic_balance_score * 0.16)
    )
    return {
        "cinematic_quality": cinematic_quality,
        "immersion_strength": clamp(immersion_strength),
        "pacing_stability": clamp(pacing_stability),
        "atmosphere_integrity": clamp(atmosphere_integrity),
        "continuity_strength": clamp(continuity_strength),
        "cinematic_pressure": clamp(tension_score),
        "runtime_polish": clamp(runtime_polish),
        "orchestration_beauty": clamp((runtime_polish + atmosphere_integrity + continuity_strength + cinematic_balance_score) / 4),
    }
