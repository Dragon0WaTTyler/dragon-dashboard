from __future__ import annotations

from typing import Any


def clamp(value: int | float, low: int = 0, high: int = 100) -> int:
    return max(low, min(high, int(round(value))))


def build_instinct_metrics(
    *,
    stabilization_strength: int = 0,
    resilience_strength: int = 0,
    fallback_intensity: int = 0,
    continuity_preservation: int = 0,
    cinematic_preservation: int = 0,
    orchestration_survival_score: int = 0,
    equilibrium_resilience: int = 0,
) -> dict[str, Any]:
    instinct_integrity = clamp(
        (stabilization_strength * 0.18)
        + (resilience_strength * 0.16)
        + ((100 - fallback_intensity) * 0.12)
        + (continuity_preservation * 0.16)
        + (cinematic_preservation * 0.14)
        + (orchestration_survival_score * 0.14)
        + (equilibrium_resilience * 0.1)
    )
    return {
        "instinct_integrity": instinct_integrity,
        "stabilization_strength": clamp(stabilization_strength),
        "resilience_strength": clamp(resilience_strength),
        "fallback_intensity": clamp(fallback_intensity),
        "continuity_preservation": clamp(continuity_preservation),
        "cinematic_preservation": clamp(cinematic_preservation),
        "orchestration_survival_score": clamp(orchestration_survival_score),
        "equilibrium_resilience": clamp(equilibrium_resilience),
    }
