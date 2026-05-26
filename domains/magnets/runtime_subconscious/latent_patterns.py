from __future__ import annotations

from typing import Any

from .subconscious_metrics import clamp


def build_latent_patterns(
    *,
    instinct_integrity: int = 0,
    awareness_integrity: int = 0,
    cinematic_quality: int = 0,
    degradation_risk: int = 0,
    continuity_confidence: int = 0,
) -> dict[str, Any]:
    latent_stability = clamp(
        (instinct_integrity * 0.32)
        + (awareness_integrity * 0.24)
        + (cinematic_quality * 0.18)
        + ((100 - degradation_risk) * 0.16)
        + (continuity_confidence * 0.1)
    )
    if degradation_risk >= 78:
        pattern = "latent_fragmentation"
    elif continuity_confidence <= 42:
        pattern = "latent_recovery"
    elif cinematic_quality >= 82 and instinct_integrity >= 74:
        pattern = "latent_cinematic_preservation"
    elif instinct_integrity >= 80 and awareness_integrity >= 74:
        pattern = "latent_stabilization"
    else:
        pattern = "latent_resilience"
    return {
        "pattern": pattern,
        "latent_stability": latent_stability,
    }
