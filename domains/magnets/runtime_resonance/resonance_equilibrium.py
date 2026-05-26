from __future__ import annotations


def build_resonance_equilibrium(
    *,
    resonance_stability: int,
    resonance_pressure: int,
    resonance_fragmentation: int,
    sync_drift: int,
    adaptive_sync_balance: int,
) -> dict[str, object]:
    equilibrium_pressure = max(
        0,
        min(
            100,
            int(round((resonance_pressure * 0.42) + (resonance_fragmentation * 0.26) + (sync_drift * 0.2) - (adaptive_sync_balance * 0.12))),
        ),
    )
    if resonance_stability >= 70 and equilibrium_pressure < 40:
        harmonic_runtime_state = "stable_resonant_equilibrium"
    elif resonance_stability >= 50 and equilibrium_pressure < 62:
        harmonic_runtime_state = "adaptive_resonant_equilibrium"
    else:
        harmonic_runtime_state = "unstable_resonant_equilibrium"
    return {
        "equilibrium_state": harmonic_runtime_state,
        "equilibrium_pressure": equilibrium_pressure,
        "equilibrium_bias": "recovery" if adaptive_sync_balance >= 58 else "containment",
    }
