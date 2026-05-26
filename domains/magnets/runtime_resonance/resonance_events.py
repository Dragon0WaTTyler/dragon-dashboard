from __future__ import annotations


def build_resonance_events(
    *,
    resonance_phase: str,
    harmonic_runtime_state: str,
    cinematic_resonance: str,
    sync_drift: int,
    resonance_fragmentation: int,
    previous_phase: str,
) -> list[dict[str, object]]:
    events = [
        {
            "event_type": "resonance_synthesized",
            "resonance_phase": str(resonance_phase or "measured_resonance"),
            "harmonic_runtime_state": str(harmonic_runtime_state or "measured_harmonic_balance"),
            "cinematic_resonance": str(cinematic_resonance or "measured_cinematic_resonance"),
            "sync_drift": int(sync_drift or 0),
            "resonance_fragmentation": int(resonance_fragmentation or 0),
        }
    ]
    if previous_phase and previous_phase != resonance_phase:
        events.append(
            {
                "event_type": "resonance_phase_shift",
                "previous_phase": str(previous_phase),
                "next_phase": str(resonance_phase or "measured_resonance"),
            }
        )
    return events
