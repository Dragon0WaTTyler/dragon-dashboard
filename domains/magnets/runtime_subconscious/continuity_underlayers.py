from __future__ import annotations

from typing import Any

from .subconscious_metrics import clamp


def build_continuity_underlayers(
    *,
    continuity_awareness: str = "",
    continuity_instinct: str = "",
    switch_frequency: int = 0,
    drift_score: int = 0,
    cinematic_quality: int = 0,
) -> dict[str, Any]:
    strength = clamp(
        (max(0, 100 - (switch_frequency * 15)) * 0.24)
        + (max(0, 100 - drift_score) * 0.26)
        + (18 if "resilient" in continuity_awareness else 8 if "adaptive" in continuity_awareness else 0)
        + (14 if "preserving" in continuity_instinct or "resilient" in continuity_instinct else 0)
        + (cinematic_quality * 0.18)
    )
    if "fragmented" in continuity_instinct or switch_frequency >= 4 or drift_score >= 60:
        state = "fragmented_underlayers"
    elif cinematic_quality >= 82 and "resilient" in continuity_awareness:
        state = "cinematic_underlayers"
    elif "resilient" in continuity_instinct:
        state = "resilient_underlayers"
    elif "preserving" in continuity_instinct:
        state = "preserved_underlayers"
    else:
        state = "adaptive_underlayers"
    return {
        "state": state,
        "underlayer_strength": strength,
    }
