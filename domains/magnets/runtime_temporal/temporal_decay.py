from __future__ import annotations


def build_temporal_decay(
    *,
    temporal_pressure: int,
    federation_divergence: int,
    subconscious_integrity: int,
    continuity_persistence: int,
    prior_decay_rate: int,
) -> dict[str, object]:
    decay_rate = max(
        0,
        min(
            100,
            int(
                round(
                    (temporal_pressure * 0.34)
                    + (federation_divergence * 0.26)
                    + ((100 - subconscious_integrity) * 0.2)
                    + ((100 - continuity_persistence) * 0.2)
                    + (prior_decay_rate * 0.1)
                )
            ),
        ),
    )
    if decay_rate >= 72:
        decay_state = "critical_decay"
    elif decay_rate >= 56:
        decay_state = "elevated_decay"
    elif decay_rate >= 34:
        decay_state = "managed_decay"
    else:
        decay_state = "minimal_decay"
    return {
        "state": decay_state,
        "continuity_decay_rate": decay_rate,
    }
