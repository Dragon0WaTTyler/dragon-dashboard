from __future__ import annotations


def build_resonance_metrics(
    *,
    resonance_stability: int,
    resonance_alignment: int,
    resonance_integrity: int,
    resonance_pressure: int,
    resonance_fragmentation: int,
    resonance_cohesion: int,
    sync_drift: int,
    runtime_harmony_index: int,
    adaptive_sync_balance: int,
) -> dict[str, int]:
    return {
        "resonance_stability": int(resonance_stability or 0),
        "resonance_alignment": int(resonance_alignment or 0),
        "resonance_integrity": int(resonance_integrity or 0),
        "resonance_pressure": int(resonance_pressure or 0),
        "resonance_fragmentation": int(resonance_fragmentation or 0),
        "resonance_cohesion": int(resonance_cohesion or 0),
        "sync_drift": int(sync_drift or 0),
        "runtime_harmony_index": int(runtime_harmony_index or 0),
        "adaptive_sync_balance": int(adaptive_sync_balance or 0),
    }
