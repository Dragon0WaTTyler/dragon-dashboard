from __future__ import annotations


def build_runtime_presence(
    *,
    awareness_integrity: int,
    continuity_score: int,
    runtime_resilience: int,
    cinematic_quality: int,
    degradation_risk: int,
) -> dict[str, int | str]:
    score = max(0, min(100, round((awareness_integrity * 0.28) + (continuity_score * 0.24) + (runtime_resilience * 0.24) + (cinematic_quality * 0.14) + ((100 - degradation_risk) * 0.1))))
    if degradation_risk >= 76:
        state = "degraded_presence"
    elif cinematic_quality >= 84 and awareness_integrity >= 74:
        state = "cinematic_presence"
    elif runtime_resilience >= 78 and continuity_score >= 72:
        state = "resilient_presence"
    elif awareness_integrity >= 64:
        state = "strong_presence"
    else:
        state = "adaptive_presence"
    return {
        "state": state,
        "runtime_presence_score": score,
    }
