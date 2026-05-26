from __future__ import annotations

from typing import Any


def build_adaptive_equilibrium(
    *,
    runtime_resilience: int = 0,
    adaptation_pressure: int = 0,
    degradation_risk: int = 0,
    balance_state: str = "",
) -> dict[str, Any]:
    if balance_state == "degradation_heavy" or degradation_risk >= 74:
        state = "degradation_equilibrium"
    elif adaptation_pressure >= 66:
        state = "equilibrium_fragmented"
    elif runtime_resilience >= 74 and degradation_risk <= 44:
        state = "resilience_equilibrium"
    elif balance_state == "fallback_dominant":
        state = "fallback_equilibrium"
    else:
        state = "equilibrium_stable"
    strength = max(0, min(100, int(round((runtime_resilience + (100 - adaptation_pressure) + (100 - degradation_risk)) / 3))))
    return {
        "equilibrium_state": state,
        "equilibrium_strength": strength,
    }
