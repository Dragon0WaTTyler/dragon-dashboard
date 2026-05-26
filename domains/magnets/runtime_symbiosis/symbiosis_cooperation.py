from __future__ import annotations

from typing import Any


def build_symbiosis_cooperation(
    *,
    federation_coherence: int = 0,
    federation_resilience: int = 0,
    resonance_cohesion: int = 0,
    temporal_stability: int = 0,
    recovery_score: int = 0,
    ecosystem_stability: int = 0,
    dependency_stress: int = 0,
) -> dict[str, Any]:
    cooperation_score = _clamp(
        int(
            round(
                (federation_coherence * 0.2)
                + (federation_resilience * 0.16)
                + (resonance_cohesion * 0.2)
                + (temporal_stability * 0.16)
                + (recovery_score * 0.16)
                + (ecosystem_stability * 0.12)
                - (dependency_stress * 0.16)
            )
        )
    )
    if dependency_stress >= 72 or recovery_score >= 72:
        cooperative_runtime_state = "adaptive_shared_recovery"
    elif cooperation_score >= 72:
        cooperative_runtime_state = "stable_mutual_support"
    elif cooperation_score >= 48:
        cooperative_runtime_state = "adaptive_coexistence"
    else:
        cooperative_runtime_state = "limited_cooperation"
    return {
        "cooperation_score": cooperation_score,
        "cooperation_state": "cooperative" if cooperation_score >= 60 else "guarded",
        "cooperative_runtime_state": cooperative_runtime_state,
    }


def _clamp(value: int) -> int:
    return max(0, min(100, int(value or 0)))
