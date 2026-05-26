from __future__ import annotations


def build_resonance_harmony(
    *,
    federation_harmony: int,
    cinematic_quality: int,
    cinematic_immersion: int,
    consciousness_clarity: int,
    instinct_pressure: int,
    temporal_stability: int,
    resonance_fragmentation: int,
) -> dict[str, object]:
    emotional_cadence = max(
        0,
        min(
            100,
            int(
                round(
                    (federation_harmony * 0.24)
                    + (cinematic_quality * 0.24)
                    + (cinematic_immersion * 0.22)
                    + (consciousness_clarity * 0.18)
                    + (temporal_stability * 0.12)
                    - (instinct_pressure * 0.08)
                    - (resonance_fragmentation * 0.12)
                )
            ),
        ),
    )
    if emotional_cadence >= 74 and resonance_fragmentation < 36:
        cinematic_resonance = "deep_cinematic_harmony"
    elif emotional_cadence >= 62 and resonance_fragmentation < 52:
        cinematic_resonance = "emotionally_stable_resonance"
    elif emotional_cadence >= 50:
        cinematic_resonance = "emotionally_stable_but_fragmenting"
    else:
        cinematic_resonance = "cinematic_resonance_disrupted"
    return {
        "harmony_state": "harmonic_convergence" if emotional_cadence >= 68 else "harmonic_balancing",
        "cinematic_resonance": cinematic_resonance,
        "emotional_cadence": emotional_cadence,
        "harmony_bias": "recovery" if instinct_pressure < 56 and temporal_stability >= 58 else "stabilization",
    }
