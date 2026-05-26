from __future__ import annotations

from typing import Any


def build_cinematic_forecast(
    *,
    pacing: str = "",
    immersion_state: str = "",
    atmosphere: str = "",
    tension: str = "",
    continuity: str = "",
    balance_state: str = "",
) -> dict[str, Any]:
    if "degraded" in immersion_state:
        forecast = "immersion_degradation"
    elif tension == "escalating_tension":
        forecast = "tension_escalation"
    elif "fragmented" in continuity:
        forecast = "continuity_hardening"
    elif balance_state in {"degraded_cinema", "pressure_heavy_cinema"}:
        forecast = "cinematic_recovery"
    elif atmosphere in {"calm_atmosphere", "resilient_atmosphere"} and pacing in {"smooth_pacing", "cinematic_pacing"}:
        forecast = "pacing_stabilization"
    else:
        forecast = "atmosphere_stabilization"
    return {
        "forecast": forecast,
        "forecast_bias": "stability" if forecast in {"pacing_stabilization", "atmosphere_stabilization"} else "recovery",
    }
