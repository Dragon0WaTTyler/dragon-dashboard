from __future__ import annotations

from typing import Any


def build_orchestration_climate(
    *,
    pressure_direction: str = "",
    pressure_score: int = 0,
    degradation_risk: int = 0,
    runtime_resilience: int = 0,
    balance_state: str = "",
) -> dict[str, Any]:
    if degradation_risk >= 72 or pressure_direction == "escalating":
        climate = "degraded_climate"
    elif runtime_resilience >= 74 and balance_state in {"balanced", "resilience_stable"}:
        climate = "resilient_climate"
    elif pressure_score >= 62:
        climate = "volatile_climate"
    elif balance_state == "fallback_dominant":
        climate = "constrained_climate"
    elif balance_state == "adaptation_fragmented":
        climate = "adaptive_climate"
    else:
        climate = "calm_climate"
    return {
        "climate": climate,
        "climate_stability": max(0, min(100, int(round((100 - pressure_score + runtime_resilience) / 2)))),
    }
