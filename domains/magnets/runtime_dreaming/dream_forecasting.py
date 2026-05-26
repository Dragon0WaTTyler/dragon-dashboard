from __future__ import annotations

from typing import Any


def build_dream_forecast(
    *,
    cinematic_dream: str = "",
    stabilization_dream: str = "",
    latent_projection: str = "",
    dormant_pathway: str = "",
    adaptive_dreaming: str = "",
    orchestration_vision: str = "",
) -> dict[str, Any]:
    if cinematic_dream in {"immersive_dream", "stabilized_cinema_dream", "resilient_cinema_dream"}:
        forecast = "cinematic_convergence"
    elif stabilization_dream in {"resilient_stabilization", "equilibrium_stabilization"}:
        forecast = "stabilization_convergence"
    elif latent_projection == "latent_fragmentation_projection":
        forecast = "fragmentation_drift"
    elif dormant_pathway in {"dormant_resilience_path", "dormant_recovery_path"}:
        forecast = "dormant_resilience_awakening"
    elif adaptive_dreaming == "adaptive_continuity_repair":
        forecast = "adaptive_continuity_evolution"
    else:
        forecast = "orchestration_recovery_projection"
    return {
        "forecast": forecast,
        "forecast_bias": "stability" if forecast in {"cinematic_convergence", "stabilization_convergence"} else "recovery",
    }
