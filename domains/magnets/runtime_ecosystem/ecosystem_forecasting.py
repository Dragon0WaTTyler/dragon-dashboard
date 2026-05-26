from __future__ import annotations

from typing import Any


def build_ecosystem_forecast(
    *,
    pressure_score: int = 0,
    pressure_direction: str = "",
    degradation_current: str = "",
    equilibrium_state: str = "",
    topology: str = "",
    climate: str = "",
) -> dict[str, Any]:
    if degradation_current in {"cascading_degradation", "fallback_propagation"} or pressure_score >= 76:
        forecast = "degradation_spread"
    elif equilibrium_state == "resilience_equilibrium" and topology == "distributed_resilience":
        forecast = "resilience_convergence"
    elif climate == "calm_climate":
        forecast = "future_stability"
    elif pressure_direction == "escalating":
        forecast = "pressure_escalation"
    elif equilibrium_state == "equilibrium_stable":
        forecast = "orchestration_stabilization"
    else:
        forecast = "equilibrium_hardening"
    confidence = 84 if forecast in {"future_stability", "degradation_spread"} else 72
    return {
        "forecast": forecast,
        "forecast_confidence": confidence,
        "forecast_risk": "high" if forecast in {"degradation_spread", "pressure_escalation"} else "low" if forecast in {"future_stability", "resilience_convergence"} else "moderate",
    }
