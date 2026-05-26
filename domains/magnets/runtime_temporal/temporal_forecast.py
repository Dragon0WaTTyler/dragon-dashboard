from __future__ import annotations


def build_temporal_forecast(
    *,
    runtime_cycle_phase: str,
    runtime_rhythm_state: str,
    cinematic_temporal_flow: str,
    continuity_decay_rate: int,
    adaptive_recovery_velocity: str,
    temporal_alignment: int,
) -> dict[str, object]:
    if continuity_decay_rate >= 56 and adaptive_recovery_velocity in {"strong", "adaptive"}:
        projection = "recovering_temporal_arc"
    elif continuity_decay_rate >= 68:
        projection = "guarded_temporal_contraction"
    elif temporal_alignment >= 70 and "steady" in str(cinematic_temporal_flow or ""):
        projection = "stable_temporal_expansion"
    else:
        projection = "measured_future_shaping"
    return {
        "forecast": projection,
        "projected_phase": str(runtime_cycle_phase or "measured_continuity"),
        "projected_rhythm": str(runtime_rhythm_state or "measured_pacing"),
        "cinematic_flow_projection": str(cinematic_temporal_flow or "steady_cinematic_flow"),
    }
