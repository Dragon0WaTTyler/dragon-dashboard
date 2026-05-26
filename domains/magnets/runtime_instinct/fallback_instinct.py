from __future__ import annotations

from typing import Any

from .instinct_metrics import clamp


def build_fallback_instinct(
    *,
    fallback_strategy: str = "",
    fallback_probability: float = 0.0,
    degradation_risk: int = 0,
    startup_confidence: str = "",
    authority_state: str = "",
) -> dict[str, Any]:
    intensity = clamp(
        (float(fallback_probability or 0.0) * 100 * 0.48)
        + (degradation_risk * 0.26)
        + (18 if startup_confidence == "low" else 8 if startup_confidence == "medium" else 0)
        + (8 if "fallback" in fallback_strategy else 0)
        + (6 if authority_state in {"constrained", "fallback_only"} else 0)
    )
    if "recovery" in fallback_strategy or (degradation_risk >= 72 and (startup_confidence == "low" or float(fallback_probability or 0.0) >= 0.55)):
        state = "fallback_recovery"
    elif intensity >= 78 or float(fallback_probability or 0.0) >= 0.7:
        state = "fallback_aggressive"
    elif startup_confidence == "low":
        state = "fallback_protective"
    elif intensity <= 26:
        state = "fallback_minimal"
    else:
        state = "fallback_balanced"
    return {
        "state": state,
        "fallback_intensity": intensity,
    }
