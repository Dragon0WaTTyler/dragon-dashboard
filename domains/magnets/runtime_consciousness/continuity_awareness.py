from __future__ import annotations


def build_continuity_awareness(
    *,
    continuity_state: str,
    continuity_confidence: int,
    switch_frequency: int,
    drift_score: int,
) -> dict[str, int | str]:
    if continuity_confidence >= 76 and switch_frequency <= 1 and drift_score <= 18:
        state = "preserved_awareness"
    elif switch_frequency >= 4 or drift_score >= 58:
        state = "fragmented_awareness"
    elif continuity_confidence <= 38:
        state = "unstable_awareness"
    elif "resilient" in continuity_state or continuity_confidence >= 68:
        state = "resilient_awareness"
    else:
        state = "adaptive_awareness"
    score = max(0, min(100, round((continuity_confidence * 0.7) + ((100 - min(drift_score, 100)) * 0.2) + (max(0, 5 - switch_frequency) * 6))))
    return {
        "state": state,
        "continuity_awareness_score": score,
    }
