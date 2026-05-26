from __future__ import annotations


def build_temporal_events(
    *,
    runtime_cycle_phase: str,
    runtime_rhythm_state: str,
    cinematic_temporal_flow: str,
    continuity_decay_rate: int,
    adaptive_recovery_velocity: str,
    previous_phase: str,
) -> list[dict[str, object]]:
    events = [
        {
            "event_type": "temporal_synthesized",
            "runtime_cycle_phase": str(runtime_cycle_phase or "measured_continuity"),
            "runtime_rhythm_state": str(runtime_rhythm_state or "measured_pacing"),
            "cinematic_temporal_flow": str(cinematic_temporal_flow or "steady_cinematic_flow"),
            "continuity_decay_rate": int(continuity_decay_rate or 0),
            "adaptive_recovery_velocity": str(adaptive_recovery_velocity or "guarded"),
        }
    ]
    if previous_phase and previous_phase != runtime_cycle_phase:
        events.append(
            {
                "event_type": "temporal_phase_shift",
                "previous_phase": str(previous_phase),
                "next_phase": str(runtime_cycle_phase or "measured_continuity"),
            }
        )
    return events
