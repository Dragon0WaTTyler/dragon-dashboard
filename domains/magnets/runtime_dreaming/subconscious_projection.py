from __future__ import annotations

from typing import Any


def build_subconscious_projection(
    *,
    hidden_equilibrium: str = "",
    latent_pattern: str = "",
    cinematic_underflow: str = "",
    residue_pattern: str = "",
) -> dict[str, Any]:
    if hidden_equilibrium == "hidden_fragmentation" or residue_pattern == "degradation_residue":
        state = "subconscious_fragmentation_projection"
    elif cinematic_underflow in {"cinematic_underflow_stable", "cinematic_underflow_resilient"}:
        state = "subconscious_cinematic_projection"
    elif latent_pattern in {"latent_stabilization", "latent_resilience"}:
        state = "subconscious_resilience_projection"
    elif latent_pattern == "latent_recovery":
        state = "subconscious_recovery_projection"
    else:
        state = "subconscious_equilibrium_projection"
    return {"state": state, "projection_state": "deterministic_subconscious_projection"}
