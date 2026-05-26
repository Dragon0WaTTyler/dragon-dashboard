from __future__ import annotations

from typing import Any


def build_instinct_forecast(
    *,
    stabilization_state: str = "",
    fallback_state: str = "",
    resilience_state: str = "",
    continuity_state: str = "",
    cinematic_state: str = "",
    equilibrium_state: str = "",
) -> dict[str, Any]:
    if stabilization_state in {"degraded_stabilization", "fragmented_stabilization"}:
        forecast = "stabilization_hardening"
    elif fallback_state in {"fallback_aggressive", "fallback_recovery"}:
        forecast = "fallback_escalation"
    elif continuity_state in {"continuity_recovering", "continuity_fragmented"}:
        forecast = "continuity_recovery"
    elif cinematic_state in {"cinematic_constrained", "cinematic_recovering"}:
        forecast = "cinematic_stabilization"
    elif equilibrium_state in {"equilibrium_adaptive", "equilibrium_recovering"}:
        forecast = "equilibrium_adaptation"
    else:
        forecast = "resilience_convergence"
    return {
        "forecast": forecast,
        "forecast_bias": "recovery" if "recovery" in forecast or "escalation" in forecast or "hardening" in forecast else "stability",
    }
