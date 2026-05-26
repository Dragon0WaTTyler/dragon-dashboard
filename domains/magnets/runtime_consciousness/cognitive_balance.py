from __future__ import annotations


def build_cognitive_balance(
    *,
    awareness_integrity: int,
    pressure_score: int,
    continuity_score: int,
    reflection_strength: int,
) -> dict[str, int | str]:
    stability = max(0, min(100, round((awareness_integrity * 0.32) + ((100 - pressure_score) * 0.18) + (continuity_score * 0.24) + (reflection_strength * 0.26))))
    if pressure_score >= 74 and (continuity_score <= 52 or awareness_integrity <= 54):
        state = "fragmented_cognition"
    elif pressure_score >= 62:
        state = "pressured_cognition"
    elif continuity_score >= 72 and reflection_strength >= 70:
        state = "resilient_cognition"
    elif awareness_integrity >= 70:
        state = "balanced_cognition"
    else:
        state = "adaptive_cognition"
    return {
        "state": state,
        "cognitive_stability": stability,
    }
