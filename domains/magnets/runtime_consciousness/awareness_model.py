from __future__ import annotations


def build_awareness_state(
    *,
    runtime_resilience: int,
    degradation_risk: int,
    continuity_confidence: int,
    cinematic_quality: int,
    identity_confidence: int,
) -> dict[str, int | str]:
    integrity = max(
        0,
        min(
            100,
            round(
                (runtime_resilience * 0.28)
                + ((100 - degradation_risk) * 0.24)
                + (continuity_confidence * 0.22)
                + (cinematic_quality * 0.14)
                + (identity_confidence * 0.12)
            ),
        ),
    )
    if degradation_risk >= 75:
        state = "degraded_awareness"
    elif continuity_confidence <= 44:
        state = "fragmented_awareness"
    elif cinematic_quality >= 82 and runtime_resilience >= 72:
        state = "cinematic_awareness"
    elif runtime_resilience >= 78 and degradation_risk <= 34:
        state = "resilient_awareness"
    elif identity_confidence >= 70 or continuity_confidence >= 68:
        state = "adaptive_awareness"
    else:
        state = "stable_awareness"
    return {
        "state": state,
        "awareness_integrity": integrity,
    }
