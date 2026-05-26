from __future__ import annotations

from typing import Any


def build_stabilization_dreams(
    *,
    stabilization_state: str = "",
    latent_projection: str = "",
    cinematic_dream: str = "",
    hidden_equilibrium: str = "",
) -> dict[str, Any]:
    if cinematic_dream == "stabilized_cinema_dream":
        state = "cinematic_stabilization"
    elif hidden_equilibrium == "hidden_resilience":
        state = "equilibrium_stabilization"
    elif stabilization_state in {"strong_stabilization", "resilient_stabilization"}:
        state = "resilient_stabilization"
    elif latent_projection == "latent_equilibrium_projection":
        state = "soft_stabilization"
    else:
        state = "adaptive_stabilization"
    return {"state": state, "stabilization_path": "deterministic_stabilization_dream"}
