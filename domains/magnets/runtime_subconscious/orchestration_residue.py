from __future__ import annotations

from typing import Any

from .subconscious_metrics import clamp


def build_orchestration_residue(
    *,
    fallback_state: str = "",
    resilience_state: str = "",
    cinematic_state: str = "",
    degradation_risk: int = 0,
    hidden_equilibrium_state: str = "",
) -> dict[str, Any]:
    density = clamp(
        (degradation_risk * 0.34)
        + (18 if "fallback" in fallback_state else 0)
        + (14 if "recover" in resilience_state else 0)
        + (12 if "recover" in cinematic_state or "constrained" in cinematic_state else 0)
        + (10 if hidden_equilibrium_state == "hidden_fragmentation" else 0)
    )
    if fallback_state in {"fallback_aggressive", "fallback_recovery"}:
        pattern = "fallback_residue"
    elif degradation_risk >= 76 or hidden_equilibrium_state == "hidden_fragmentation":
        pattern = "degradation_residue"
    elif "recover" in cinematic_state or "constrained" in cinematic_state:
        pattern = "cinematic_residue"
    elif "resilience" in resilience_state or "balanced" in resilience_state:
        pattern = "resilience_residue"
    else:
        pattern = "equilibrium_residue"
    return {
        "pattern": pattern,
        "orchestration_residue_density": density,
    }
