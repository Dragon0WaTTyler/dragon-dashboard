from __future__ import annotations

from typing import Any


def build_degradation_currents(
    *,
    degradation_risk: int = 0,
    fallback_probability: float = 0.0,
    adaptation_pressure: int = 0,
    runtime_resilience: int = 0,
) -> dict[str, Any]:
    if degradation_risk >= 78 and fallback_probability >= 0.68:
        current = "cascading_degradation"
    elif degradation_risk >= 66 and runtime_resilience <= 54:
        current = "fallback_propagation"
    elif runtime_resilience >= 72 and adaptation_pressure <= 48:
        current = "adaptive_containment"
    elif degradation_risk >= 52:
        current = "localized_degradation"
    else:
        current = "instability_drift"
    return {
        "current": current,
        "propagation_risk": max(0, min(100, int(round((degradation_risk + int(round(fallback_probability * 100)) + adaptation_pressure) / 3)))),
    }
