from __future__ import annotations


def build_resonance_interference(
    *,
    temporal_pressure: int,
    federation_divergence: int,
    instinct_pressure: int,
    subconscious_integrity: int,
    consciousness_clarity: int,
    temporal_stability: int,
) -> dict[str, object]:
    fragmentation = max(
        0,
        min(
            100,
            int(
                round(
                    (temporal_pressure * 0.28)
                    + (federation_divergence * 0.3)
                    + (instinct_pressure * 0.2)
                    + ((100 - subconscious_integrity) * 0.12)
                    + ((100 - temporal_stability) * 0.1)
                    - (consciousness_clarity * 0.08)
                )
            ),
        ),
    )
    if fragmentation >= 70:
        state = "resonance_fragmented"
    elif fragmentation >= 48:
        state = "resonance_interference_detected"
    else:
        state = "resonance_interference_managed"
    vectors: list[str] = []
    if temporal_pressure >= 60:
        vectors.append("temporal_pressure")
    if federation_divergence >= 54:
        vectors.append("federation_divergence")
    if instinct_pressure >= 58:
        vectors.append("instinct_pressure")
    if subconscious_integrity <= 48:
        vectors.append("subconscious_instability")
    if not vectors:
        vectors.append("minor_runtime_variance")
    return {
        "interference_state": state,
        "fragmentation_index": fragmentation,
        "interference_vectors": vectors,
    }
