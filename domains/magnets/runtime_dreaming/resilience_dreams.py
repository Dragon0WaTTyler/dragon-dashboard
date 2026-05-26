from __future__ import annotations

from typing import Any


def build_resilience_dreams(
    *,
    dormant_resilience: str = "",
    runtime_resilience: int = 0,
    degradation_risk: int = 0,
) -> dict[str, Any]:
    if degradation_risk >= 78:
        state = "dormant_resilience_fragility"
    elif dormant_resilience == "dormant_recovering":
        state = "dormant_resilience_recovery"
    elif dormant_resilience == "dormant_resilient":
        state = "dormant_resilience_growth"
    elif runtime_resilience >= 68:
        state = "dormant_resilience_balance"
    else:
        state = "dormant_resilience_adaptation"
    return {"state": state, "resilience_trajectory": "deterministic_resilience_dream"}
