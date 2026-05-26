from __future__ import annotations

from typing import Any


def build_cinematic_direction(
    *,
    authority_state: str = "",
    pressure_direction: str = "",
    equilibrium_state: str = "",
    topology: str = "",
    archetype: str = "",
    degradation_risk: int = 0,
    runtime_resilience: int = 0,
) -> dict[str, Any]:
    if authority_state == "guarded" and degradation_risk >= 72:
        style = "cinematic_constrained"
    elif equilibrium_state == "equilibrium_fragmented" or pressure_direction == "escalating":
        style = "cinematic_fragile"
    elif degradation_risk >= 76:
        style = "cinematic_recovery"
    elif "cinematic" in str(archetype or "") and pressure_direction != "escalating" and degradation_risk <= 42:
        style = "cinematic_stable"
    elif runtime_resilience >= 76 and topology == "distributed_resilience":
        style = "cinematic_resilient"
    elif authority_state == "approved":
        style = "cinematic_balanced"
    else:
        style = "cinematic_adaptive"
    return {
        "style": style,
        "direction_bias": "resilience_bias" if "resilient" in style else "stability_bias" if "stable" in style or "balanced" in style else "containment_bias",
    }
