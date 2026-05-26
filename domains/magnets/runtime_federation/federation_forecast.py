from __future__ import annotations

from typing import Any


def build_federation_forecast(
    *,
    phase_transition: str,
    continuity_projection: str,
    cinematic_runtime_state: str,
    pressure: int,
    resilience: int,
    divergence: int,
) -> dict[str, Any]:
    if phase_transition == "volatile_stabilization":
        forecast = "stabilization_with_guardrails"
    elif divergence >= 58:
        forecast = "divergence_reduction_required"
    elif resilience >= 72 and pressure <= 42:
        forecast = "continuity_holding"
    else:
        forecast = "adaptive_rebalancing"

    risk = "elevated" if pressure >= 65 or divergence >= 55 else "moderate" if pressure >= 42 else "contained"
    return {
        "forecast": forecast,
        "forecast_risk": risk,
        "continuity_projection": continuity_projection,
        "cinematic_runtime_state": cinematic_runtime_state,
    }
