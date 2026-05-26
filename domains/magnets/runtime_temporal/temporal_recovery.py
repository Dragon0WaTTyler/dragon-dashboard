from __future__ import annotations


def build_temporal_recovery(
    *,
    temporal_pressure: int,
    continuity_decay_rate: int,
    federation_resilience: int,
    consciousness_clarity: int,
    dreaming_integrity: int,
    continuity_persistence: int,
) -> dict[str, object]:
    recovery_score = max(
        0,
        min(
            100,
            int(
                round(
                    (federation_resilience * 0.32)
                    + (consciousness_clarity * 0.22)
                    + (dreaming_integrity * 0.18)
                    + (continuity_persistence * 0.18)
                    - (temporal_pressure * 0.12)
                    - (continuity_decay_rate * 0.08)
                )
            ),
        ),
    )
    if recovery_score >= 72:
        velocity = "strong"
    elif recovery_score >= 54:
        velocity = "adaptive"
    elif recovery_score >= 36:
        velocity = "guarded"
    else:
        velocity = "weak"
    return {
        "state": "recovery_vector",
        "adaptive_recovery_velocity": velocity,
        "recovery_score": recovery_score,
    }
