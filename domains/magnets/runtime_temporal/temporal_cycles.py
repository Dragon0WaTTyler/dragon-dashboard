from __future__ import annotations


def build_runtime_cycle_phase(
    *,
    temporal_pressure: int,
    temporal_stability: int,
    temporal_momentum: int,
    continuity_decay_rate: int,
    recovery_score: int,
    focused_consciousness: bool,
    unstable_subconscious: bool,
    optimistic_dreaming: bool,
) -> dict[str, object]:
    if temporal_pressure >= 68 and unstable_subconscious and recovery_score >= 52:
        phase = "adaptive_transition"
    elif continuity_decay_rate >= 66 and temporal_stability <= 45:
        phase = "continuity_fracture"
    elif temporal_stability >= 72 and temporal_momentum >= 60:
        phase = "stable_progression"
    elif recovery_score >= 64 and optimistic_dreaming:
        phase = "recovery_resonance"
    elif temporal_pressure >= 60:
        phase = "strained_balancing"
    else:
        phase = "measured_continuity"

    if continuity_decay_rate >= 66 or unstable_subconscious:
        velocity = "accelerating" if recovery_score >= 48 else "destabilizing"
    elif temporal_momentum >= 72 and focused_consciousness:
        velocity = "accelerating"
    elif temporal_pressure <= 34 and temporal_stability >= 68:
        velocity = "steady"
    else:
        velocity = "modulating"

    phase_confidence = max(
        0,
        min(
            100,
            int(
                round(
                    (temporal_stability * 0.34)
                    + (temporal_momentum * 0.22)
                    + (recovery_score * 0.2)
                    - (continuity_decay_rate * 0.18)
                    - (temporal_pressure * 0.08)
                )
            ),
        ),
    )
    return {
        "phase": phase,
        "orchestration_phase_velocity": velocity,
        "phase_confidence": phase_confidence,
    }
