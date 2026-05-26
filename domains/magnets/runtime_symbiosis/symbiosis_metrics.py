from __future__ import annotations

from typing import Any


def build_symbiosis_metrics(
    *,
    symbiosis_stability: int = 0,
    symbiosis_alignment: int = 0,
    symbiosis_integrity: int = 0,
    symbiosis_pressure: int = 0,
    symbiosis_mutualism: int = 0,
    symbiosis_fragmentation: int = 0,
    dependency_stress: int = 0,
    systemic_runtime_health_index: int = 0,
    recovery_cohesion_score: int = 0,
) -> dict[str, Any]:
    return {
        "symbiosis_stability": _clamp(symbiosis_stability),
        "symbiosis_alignment": _clamp(symbiosis_alignment),
        "symbiosis_integrity": _clamp(symbiosis_integrity),
        "symbiosis_pressure": _clamp(symbiosis_pressure),
        "symbiosis_mutualism": _clamp(symbiosis_mutualism),
        "symbiosis_fragmentation": _clamp(symbiosis_fragmentation),
        "dependency_stress": _clamp(dependency_stress),
        "systemic_runtime_health_index": _clamp(systemic_runtime_health_index),
        "recovery_cohesion_score": _clamp(recovery_cohesion_score),
    }


def _clamp(value: int) -> int:
    return max(0, min(100, int(value or 0)))
