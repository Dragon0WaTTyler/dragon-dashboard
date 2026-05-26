from __future__ import annotations

from typing import Any


def build_resilience_topology(
    *,
    runtime_resilience: int = 0,
    fallback_probability: float = 0.0,
    degradation_risk: int = 0,
    adaptation_pressure: int = 0,
) -> dict[str, Any]:
    if runtime_resilience >= 78 and fallback_probability <= 0.28:
        topology = "distributed_resilience"
    elif fallback_probability >= 0.62:
        topology = "fallback_resilience"
    elif adaptation_pressure >= 60:
        topology = "adaptive_resilience"
    elif degradation_risk >= 72:
        topology = "fragile_resilience"
    else:
        topology = "concentrated_resilience"
    return {
        "topology": topology,
        "resilience_distribution": max(0, min(100, int(round((runtime_resilience + (100 - degradation_risk)) / 2)))),
    }
