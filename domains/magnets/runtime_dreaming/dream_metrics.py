from __future__ import annotations

from typing import Any


def clamp(value: int | float, low: int = 0, high: int = 100) -> int:
    return max(low, min(high, int(round(value))))


def build_dream_metrics(
    *,
    cinematic_projection_strength: int = 0,
    latent_projection_stability: int = 0,
    dormant_pathway_strength: int = 0,
    adaptive_dreaming_strength: int = 0,
    runtime_mirroring_integrity: int = 0,
    continuity_projection_strength: int = 0,
    orchestration_dream_balance: int = 0,
) -> dict[str, Any]:
    dreaming_integrity = clamp(
        (cinematic_projection_strength * 0.18)
        + (latent_projection_stability * 0.18)
        + (dormant_pathway_strength * 0.14)
        + (adaptive_dreaming_strength * 0.16)
        + (runtime_mirroring_integrity * 0.12)
        + (continuity_projection_strength * 0.12)
        + (orchestration_dream_balance * 0.1)
    )
    return {
        "dreaming_integrity": dreaming_integrity,
        "cinematic_projection_strength": clamp(cinematic_projection_strength),
        "latent_projection_stability": clamp(latent_projection_stability),
        "dormant_pathway_strength": clamp(dormant_pathway_strength),
        "adaptive_dreaming_strength": clamp(adaptive_dreaming_strength),
        "runtime_mirroring_integrity": clamp(runtime_mirroring_integrity),
        "continuity_projection_strength": clamp(continuity_projection_strength),
        "orchestration_dream_balance": clamp(orchestration_dream_balance),
    }
