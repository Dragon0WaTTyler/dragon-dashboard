from __future__ import annotations

from typing import Any


def build_subconscious_forecast(
    *,
    latent_pattern: str = "",
    hidden_equilibrium_state: str = "",
    dormant_resilience_state: str = "",
    cinematic_underflow_state: str = "",
    continuity_underlayers_state: str = "",
    residue_pattern: str = "",
) -> dict[str, Any]:
    if latent_pattern == "latent_stabilization":
        forecast = "latent_stabilization_convergence"
    elif hidden_equilibrium_state == "hidden_fragmentation" or residue_pattern == "degradation_residue":
        forecast = "subconscious_fragmentation_risk"
    elif dormant_resilience_state in {"dormant_recovering", "dormant_balanced"}:
        forecast = "resilience_awakening"
    elif cinematic_underflow_state in {"cinematic_underflow_fragile", "cinematic_underflow_adaptive"}:
        forecast = "cinematic_underflow_stabilization"
    elif continuity_underlayers_state == "fragmented_underlayers":
        forecast = "continuity_recovery_drift"
    else:
        forecast = "equilibrium_convergence"
    return {
        "forecast": forecast,
        "forecast_bias": "recovery" if forecast in {"subconscious_fragmentation_risk", "resilience_awakening", "cinematic_underflow_stabilization", "continuity_recovery_drift"} else "stability",
    }
