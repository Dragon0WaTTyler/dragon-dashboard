from __future__ import annotations

from typing import Any

from .instinct_metrics import clamp


def build_equilibrium_instinct(
    *,
    equilibrium_state: str = "",
    balance_state: str = "",
    pressure_direction: str = "",
    runtime_resilience: int = 0,
    degradation_risk: int = 0,
) -> dict[str, Any]:
    resilience = clamp(
        (runtime_resilience * 0.42)
        + ((100 - degradation_risk) * 0.24)
        + (18 if "stable" in equilibrium_state else 8 if "adaptive" in equilibrium_state else 0)
        + (10 if "balanced" in balance_state or "resilient" in balance_state else 0)
        + (6 if pressure_direction == "steady" else 0)
    )
    if "fragmented" in equilibrium_state or degradation_risk >= 76:
        state = "equilibrium_fragmented"
    elif "recover" in equilibrium_state:
        state = "equilibrium_recovering"
    elif pressure_direction == "rising":
        state = "equilibrium_adaptive"
    elif resilience >= 82:
        state = "equilibrium_resilient"
    else:
        state = "equilibrium_preserving"
    return {
        "state": state,
        "equilibrium_resilience": resilience,
    }
