from __future__ import annotations

from typing import Any

from .instinct_metrics import clamp


def build_continuity_instinct(
    *,
    continuity_state: str = "",
    continuity_confidence: int = 0,
    switch_frequency: int = 0,
    drift_score: int = 0,
    continuity_awareness: str = "",
) -> dict[str, Any]:
    preservation = clamp(
        (continuity_confidence * 0.48)
        + (max(0, 100 - (switch_frequency * 16)) * 0.18)
        + (max(0, 100 - drift_score) * 0.22)
        + (12 if "resilient" in continuity_awareness else 6 if "adaptive" in continuity_awareness else 0)
    )
    if "fragmented" in continuity_state or switch_frequency >= 4 or drift_score >= 60:
        state = "continuity_fragmented"
    elif "recover" in continuity_state or "recover" in continuity_awareness:
        state = "continuity_recovering"
    elif continuity_confidence >= 78 and drift_score <= 22:
        state = "continuity_preserving"
    elif continuity_confidence >= 68:
        state = "continuity_resilient"
    else:
        state = "continuity_adaptive"
    return {
        "state": state,
        "continuity_preservation": preservation,
    }
