from __future__ import annotations

from typing import Any


def build_symbiosis_projection(
    *,
    symbiotic_phase: str = "",
    systemic_runtime_health: str = "",
    dependency_stress: int = 0,
    recovery_cohesion: str = "",
    fragmentation: int = 0,
    prior_phase: str = "",
) -> dict[str, Any]:
    if fragmentation >= 68 or dependency_stress >= 78:
        forecast = "systemic_fragmentation_risk"
    elif recovery_cohesion in {"stable", "recovering"} and systemic_runtime_health == "recovering":
        forecast = "cooperative_recovery_strengthening"
    elif symbiotic_phase == "strained_mutualism":
        forecast = "pressured_coexistence_stabilizing" if prior_phase == "fractured_symbiosis" else "dependency_pressure_persisting"
    elif symbiotic_phase == "adaptive_mutualism":
        forecast = "adaptive_mutualism_holding"
    else:
        forecast = "measured_symbiosis_projection"
    return {
        "forecast": forecast,
        "projection_bias": "recovery" if "recover" in forecast else "stabilization" if "stabil" in forecast else "containment",
    }
