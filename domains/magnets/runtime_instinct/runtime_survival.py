from __future__ import annotations

from typing import Any

from .instinct_metrics import clamp


def build_runtime_survival(
    *,
    stabilization_strength: int = 0,
    resilience_strength: int = 0,
    fallback_intensity: int = 0,
    continuity_preservation: int = 0,
    degradation_risk: int = 0,
) -> dict[str, Any]:
    score = clamp(
        (stabilization_strength * 0.24)
        + (resilience_strength * 0.26)
        + (continuity_preservation * 0.18)
        + ((100 - fallback_intensity) * 0.14)
        + ((100 - degradation_risk) * 0.18)
    )
    if degradation_risk >= 78 or score <= 38:
        state = "survival_fragile"
    elif fallback_intensity >= 72 or continuity_preservation <= 44:
        state = "survival_recovering"
    elif resilience_strength >= 80 and stabilization_strength >= 76:
        state = "survival_resilient"
    elif score >= 72:
        state = "survival_stable"
    else:
        state = "survival_adaptive"
    return {
        "state": state,
        "orchestration_survival_score": score,
    }
