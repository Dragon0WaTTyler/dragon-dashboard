from __future__ import annotations

from typing import Any


def build_symbiosis_recovery(
    *,
    temporal_recovery_score: int = 0,
    resonance_recovery_score: int = 0,
    federation_resilience: int = 0,
    ecosystem_stability: int = 0,
    dependency_stress: int = 0,
    temporal_recovery_velocity: str = "",
) -> dict[str, Any]:
    recovery_cohesion_score = _clamp(
        int(
            round(
                (temporal_recovery_score * 0.28)
                + (resonance_recovery_score * 0.26)
                + (federation_resilience * 0.2)
                + (ecosystem_stability * 0.14)
                + ((100 - dependency_stress) * 0.12)
            )
        )
    )
    if recovery_cohesion_score >= 72:
        recovery_cohesion = "stable"
    elif recovery_cohesion_score >= 48:
        recovery_cohesion = "recovering"
    else:
        recovery_cohesion = "fragile"
    if temporal_recovery_velocity == "strong" or recovery_cohesion_score >= 74:
        velocity = "strong"
    elif temporal_recovery_velocity in {"adaptive", "improving"} or recovery_cohesion_score >= 52:
        velocity = "improving"
    else:
        velocity = "guarded"
    return {
        "recovery_cohesion_score": recovery_cohesion_score,
        "recovery_cohesion": recovery_cohesion,
        "cooperative_recovery_velocity": velocity,
        "recovery_mode": "shared_stabilization" if dependency_stress >= 56 else "continuity_preservation",
    }


def _clamp(value: int) -> int:
    return max(0, min(100, int(value or 0)))
