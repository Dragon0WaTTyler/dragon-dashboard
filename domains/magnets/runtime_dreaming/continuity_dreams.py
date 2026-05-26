from __future__ import annotations

from typing import Any

from .dream_metrics import clamp


def build_continuity_dreams(
    *,
    continuity_underlayers: str = "",
    continuity_instinct: str = "",
    continuity_confidence: int = 0,
    switch_frequency: int = 0,
) -> dict[str, Any]:
    strength = clamp(
        (continuity_confidence * 0.44)
        + (18 if continuity_underlayers in {"preserved_underlayers", "resilient_underlayers", "cinematic_underlayers"} else 8)
        + (14 if "preserving" in continuity_instinct or "resilient" in continuity_instinct else 6)
        + max(0, 20 - switch_frequency * 4)
    )
    if continuity_underlayers == "fragmented_underlayers":
        state = "continuity_fragmentation"
    elif "recover" in continuity_instinct:
        state = "continuity_recovery"
    elif continuity_underlayers in {"preserved_underlayers", "cinematic_underlayers"}:
        state = "continuity_preservation"
    elif continuity_confidence >= 68:
        state = "continuity_balance"
    else:
        state = "continuity_adaptation"
    return {"state": state, "continuity_projection_strength": strength}
