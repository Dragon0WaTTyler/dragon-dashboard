from __future__ import annotations


def build_resonance_projection(
    *,
    resonance_phase: str,
    sync_drift: int,
    resonance_fragmentation: int,
    recovery_velocity: str,
    harmonic_runtime_state: str,
    cinematic_resonance: str,
    prior_phase: str,
) -> dict[str, object]:
    if sync_drift >= 64 or resonance_fragmentation >= 68:
        forecast = "fragmentation_risk_rising"
    elif "strong" in str(recovery_velocity or "") or "recover" in str(cinematic_resonance or ""):
        forecast = "adaptive_resonance_recovery"
    elif "strained" in str(resonance_phase or ""):
        forecast = "strained_harmony_holding"
    else:
        forecast = "stable_harmonic_projection"
    return {
        "forecast": forecast,
        "drift_projection": "elevated" if sync_drift >= 52 else "managed",
        "equilibrium_projection": str(harmonic_runtime_state or "measured_harmonic_balance"),
        "previous_phase": str(prior_phase or ""),
    }
