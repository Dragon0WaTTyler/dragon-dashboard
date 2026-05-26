from __future__ import annotations

from typing import Any

from .subconscious_metrics import clamp


def build_dormant_resilience(
    *,
    runtime_resilience: int = 0,
    resilience_topology: str = "",
    latent_pattern: str = "",
    survival_state: str = "",
    degradation_risk: int = 0,
) -> dict[str, Any]:
    strength = clamp(
        (runtime_resilience * 0.44)
        + ((100 - degradation_risk) * 0.22)
        + (12 if "distributed" in resilience_topology else 0)
        + (10 if "stable" in survival_state or "resilient" in survival_state else 0)
        + (8 if latent_pattern in {"latent_stabilization", "latent_resilience"} else 0)
    )
    if degradation_risk >= 78:
        state = "dormant_fragmented"
    elif "recover" in survival_state or latent_pattern == "latent_recovery":
        state = "dormant_recovering"
    elif strength >= 82:
        state = "dormant_resilient"
    elif runtime_resilience >= 68:
        state = "dormant_balanced"
    else:
        state = "dormant_adaptive"
    return {
        "state": state,
        "dormant_resilience_strength": strength,
    }
