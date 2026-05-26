from __future__ import annotations

from typing import Any

from .instinct_metrics import clamp


def build_resilience_instinct(
    *,
    runtime_resilience: int = 0,
    resilience_topology: str = "",
    survival_state: str = "",
    degradation_risk: int = 0,
    fallback_pressure: int = 0,
) -> dict[str, Any]:
    strength = clamp(
        (runtime_resilience * 0.42)
        + ((100 - degradation_risk) * 0.24)
        + ((100 - fallback_pressure) * 0.18)
        + (12 if "distributed" in resilience_topology else 0)
        + (8 if "resilient" in survival_state else 0)
    )
    if runtime_resilience >= 82 and degradation_risk <= 34:
        state = "resilience_preserving"
    elif runtime_resilience >= 72:
        state = "resilience_balanced"
    elif "recover" in survival_state or fallback_pressure >= 66:
        state = "resilience_recovering"
    elif degradation_risk >= 74:
        state = "resilience_fragile"
    else:
        state = "resilience_adaptive"
    return {
        "state": state,
        "resilience_strength": strength,
    }
