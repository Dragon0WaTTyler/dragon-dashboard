from __future__ import annotations

from typing import Any

from .instinct_metrics import clamp


def build_stabilization_instinct(
    *,
    stability_score: int = 0,
    degradation_risk: int = 0,
    continuity_confidence: int = 0,
    awareness_integrity: int = 0,
    pressure_score: int = 0,
) -> dict[str, Any]:
    strength = clamp(
        (stability_score * 0.34)
        + ((100 - degradation_risk) * 0.28)
        + (continuity_confidence * 0.16)
        + (awareness_integrity * 0.14)
        + ((100 - pressure_score) * 0.08)
    )
    if degradation_risk >= 76 or stability_score <= 36:
        state = "degraded_stabilization"
    elif continuity_confidence <= 44 or pressure_score >= 72:
        state = "fragmented_stabilization"
    elif strength >= 76 and stability_score >= 80 and degradation_risk <= 30:
        state = "strong_stabilization"
    elif awareness_integrity >= 72 and continuity_confidence >= 68:
        state = "resilient_stabilization"
    else:
        state = "adaptive_stabilization"
    return {
        "state": state,
        "stabilization_strength": strength,
    }
