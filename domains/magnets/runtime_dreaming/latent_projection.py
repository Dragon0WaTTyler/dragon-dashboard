from __future__ import annotations

from typing import Any

from .dream_metrics import clamp


def build_latent_projection(
    *,
    latent_pattern: str = "",
    hidden_equilibrium: str = "",
    dreaming_pressure: int = 0,
    dormant_resilience: str = "",
) -> dict[str, Any]:
    stability = clamp(
        (18 if latent_pattern in {"latent_stabilization", "latent_resilience", "latent_cinematic_preservation"} else 8)
        + (16 if hidden_equilibrium in {"hidden_balance", "hidden_resilience"} else 6)
        + (16 if dormant_resilience in {"dormant_resilient", "dormant_balanced"} else 8)
        + max(0, 50 - int(dreaming_pressure * 0.3))
    )
    if latent_pattern == "latent_recovery":
        state = "latent_recovery_projection"
    elif latent_pattern == "latent_fragmentation" or hidden_equilibrium == "hidden_fragmentation":
        state = "latent_fragmentation_projection"
    elif hidden_equilibrium in {"hidden_balance", "hidden_adaptation"}:
        state = "latent_equilibrium_projection"
    elif dormant_resilience in {"dormant_resilient", "dormant_balanced"}:
        state = "latent_resilience_projection"
    else:
        state = "latent_stability_projection"
    return {"state": state, "latent_projection_stability": stability}
