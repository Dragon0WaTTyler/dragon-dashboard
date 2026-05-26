from __future__ import annotations

from typing import Any

from .dream_metrics import clamp


def build_runtime_mirroring(
    *,
    orchestration_vision: str = "",
    cinematic_dream: str = "",
    continuity_dream: str = "",
    hidden_equilibrium: str = "",
) -> dict[str, Any]:
    integrity = clamp(
        (18 if orchestration_vision in {"stabilization_vision", "cinematic_preservation_vision"} else 10)
        + (18 if cinematic_dream in {"immersive_dream", "stabilized_cinema_dream", "resilient_cinema_dream"} else 8)
        + (18 if continuity_dream in {"continuity_preservation", "continuity_balance"} else 8)
        + (18 if hidden_equilibrium in {"hidden_balance", "hidden_resilience"} else 8)
        + 18
    )
    if cinematic_dream in {"immersive_dream", "stabilized_cinema_dream"}:
        state = "cinematic_mirroring"
    elif continuity_dream == "continuity_preservation":
        state = "continuity_mirroring"
    elif hidden_equilibrium == "hidden_fragmentation":
        state = "fragmented_mirroring"
    elif orchestration_vision == "stabilization_vision":
        state = "stabilized_mirroring"
    else:
        state = "resilient_mirroring"
    return {"state": state, "runtime_mirroring_integrity": integrity}
