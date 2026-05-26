from __future__ import annotations


def build_temporal_metrics(
    *,
    temporal_stability: int,
    temporal_momentum: int,
    temporal_pressure: int,
    temporal_alignment: int,
    temporal_integrity: int,
    continuity_decay_rate: int,
    continuity_persistence: int,
    recovery_score: int,
    rhythm_strength: int,
    adaptive_temporal_balance: int,
) -> dict[str, int]:
    forecast_confidence = max(
        0,
        min(
            100,
            int(round((temporal_stability * 0.25) + (temporal_alignment * 0.2) + (recovery_score * 0.2) + (rhythm_strength * 0.15) + ((100 - continuity_decay_rate) * 0.2))),
        ),
    )
    return {
        "temporal_stability": temporal_stability,
        "temporal_momentum": temporal_momentum,
        "temporal_pressure": temporal_pressure,
        "temporal_alignment": temporal_alignment,
        "temporal_integrity": temporal_integrity,
        "continuity_decay_rate": continuity_decay_rate,
        "continuity_persistence": continuity_persistence,
        "recovery_score": recovery_score,
        "rhythm_strength": rhythm_strength,
        "adaptive_temporal_balance": adaptive_temporal_balance,
        "forecast_confidence": forecast_confidence,
    }
