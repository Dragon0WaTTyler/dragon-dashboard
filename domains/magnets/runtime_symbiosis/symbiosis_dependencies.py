from __future__ import annotations

from typing import Any


def build_symbiosis_dependencies(
    *,
    federation_divergence: int = 0,
    resonance_fragmentation: int = 0,
    resonance_pressure: int = 0,
    temporal_pressure: int = 0,
    ecosystem_pressure: int = 0,
    ecosystem_degradation: int = 0,
    recovery_velocity: str = "",
) -> dict[str, Any]:
    recovery_offset = 14 if recovery_velocity == "strong" else 8 if recovery_velocity in {"adaptive", "improving"} else 0
    dependency_stress = _clamp(
        int(
            round(
                (federation_divergence * 0.24)
                + (resonance_fragmentation * 0.22)
                + (resonance_pressure * 0.18)
                + (temporal_pressure * 0.16)
                + (ecosystem_pressure * 0.12)
                + (ecosystem_degradation * 0.14)
                - recovery_offset
            )
        )
    )
    if dependency_stress >= 72:
        dependency_state = "elevated_dependency_stress"
    elif dependency_stress >= 48:
        dependency_state = "managed_dependency_pressure"
    else:
        dependency_state = "balanced_dependencies"
    return {
        "dependency_stress": dependency_stress,
        "dependency_state": dependency_state,
        "shared_stabilization_bias": "recovery_weighted" if recovery_offset >= 8 else "pressure_weighted",
    }


def _clamp(value: int) -> int:
    return max(0, min(100, int(value or 0)))
