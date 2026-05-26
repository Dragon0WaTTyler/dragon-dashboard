from __future__ import annotations

from typing import Any

from .dream_metrics import clamp


def build_adaptive_dreaming(
    *,
    adaptive_state: str = "",
    continuity_dream: str = "",
    cinematic_dream: str = "",
    latent_projection: str = "",
) -> dict[str, Any]:
    strength = clamp(
        (20 if adaptive_state in {"silent_stabilization", "silent_resilience", "silent_equilibrium"} else 10)
        + (16 if continuity_dream in {"continuity_recovery", "continuity_balance"} else 8)
        + (16 if cinematic_dream in {"resilient_cinema_dream", "adaptive_cinema_dream"} else 8)
        + (16 if latent_projection in {"latent_equilibrium_projection", "latent_resilience_projection"} else 8)
        + 20
    )
    if adaptive_state == "silent_recovery" or continuity_dream in {"continuity_recovery", "continuity_fragmentation"}:
        state = "adaptive_continuity_repair"
    elif cinematic_dream in {"immersive_dream", "resilient_cinema_dream"}:
        state = "adaptive_cinematic_preservation"
    elif latent_projection == "latent_fragmentation_projection":
        state = "adaptive_fragmentation_control"
    elif latent_projection == "latent_resilience_projection":
        state = "adaptive_resilience"
    else:
        state = "adaptive_equilibrium"
    return {"state": state, "adaptive_dreaming_strength": strength}
