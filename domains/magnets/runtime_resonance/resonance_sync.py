from __future__ import annotations


def build_resonance_sync(
    *,
    temporal_stability: int,
    temporal_alignment: int,
    federation_alignment: int,
    federation_coherence: int,
    consciousness_clarity: int,
    cinematic_quality: int,
    resonance_pressure: int,
    resonance_fragmentation: int,
) -> dict[str, object]:
    sync_strength = max(
        0,
        min(
            100,
            int(
                round(
                    (temporal_stability * 0.18)
                    + (temporal_alignment * 0.16)
                    + (federation_alignment * 0.18)
                    + (federation_coherence * 0.16)
                    + (consciousness_clarity * 0.16)
                    + (cinematic_quality * 0.16)
                    - (resonance_pressure * 0.18)
                    - (resonance_fragmentation * 0.22)
                )
            ),
        ),
    )
    sync_drift = max(0, min(100, int(round(100 - sync_strength + (resonance_fragmentation * 0.2)))))
    if sync_strength >= 74 and sync_drift < 28:
        orchestration_resonance = "high"
    elif sync_strength >= 56 and sync_drift < 46:
        orchestration_resonance = "moderate"
    else:
        orchestration_resonance = "strained"
    if sync_drift >= 64:
        sync_state = "drifting"
    elif sync_drift >= 42:
        sync_state = "pressured"
    else:
        sync_state = "synchronized"
    return {
        "sync_state": sync_state,
        "sync_strength": sync_strength,
        "sync_drift": sync_drift,
        "orchestration_resonance": orchestration_resonance,
    }
