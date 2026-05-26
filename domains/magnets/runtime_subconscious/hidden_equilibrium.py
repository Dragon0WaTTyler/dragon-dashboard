from __future__ import annotations

from typing import Any

from .subconscious_metrics import clamp


def build_hidden_equilibrium(
    *,
    equilibrium_state: str = "",
    balance_state: str = "",
    underflow_state: str = "",
    instinct_integrity: int = 0,
    degradation_risk: int = 0,
) -> dict[str, Any]:
    strength = clamp(
        (instinct_integrity * 0.34)
        + ((100 - degradation_risk) * 0.22)
        + (16 if "stable" in equilibrium_state else 8 if "adaptive" in equilibrium_state else 0)
        + (14 if "balanced" in balance_state or "resilient" in balance_state else 0)
        + (10 if underflow_state in {"calm_underflow", "resilient_underflow"} else 0)
    )
    if "fragmented" in equilibrium_state or underflow_state == "fragmented_underflow":
        state = "hidden_fragmentation"
    elif "recover" in equilibrium_state:
        state = "hidden_recovery"
    elif underflow_state == "adaptive_underflow":
        state = "hidden_adaptation"
    elif strength >= 78:
        state = "hidden_resilience"
    else:
        state = "hidden_balance"
    return {
        "state": state,
        "hidden_equilibrium_strength": strength,
    }
