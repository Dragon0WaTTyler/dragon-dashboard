from __future__ import annotations


def build_temporal_rhythm(
    *,
    runtime_cycle_phase: str,
    temporal_momentum: int,
    temporal_pressure: int,
    continuity_decay_rate: int,
    cinematic_runtime_state: str,
    consciousness_focus: str,
    dream_forecast: str,
) -> dict[str, object]:
    cinematic_text = str(cinematic_runtime_state or "").lower()
    focus_text = str(consciousness_focus or "").lower()
    dream_text = str(dream_forecast or "").lower()
    phase_text = str(runtime_cycle_phase or "").lower()
    if "transition" in phase_text or continuity_decay_rate >= 62:
        rhythm_state = "adaptive_pulse"
    elif temporal_pressure >= 65:
        rhythm_state = "compressed_rhythm"
    elif temporal_momentum >= 66 and "focus" in focus_text:
        rhythm_state = "driven_continuity"
    else:
        rhythm_state = "measured_pacing"

    if continuity_decay_rate >= 62 and temporal_momentum >= 48:
        cinematic_flow = "unstable_but_recovering"
    elif "unstable" in cinematic_text or temporal_pressure >= 70:
        cinematic_flow = "fractured_cinematic_flow"
    elif temporal_momentum >= 68 and ("optim" in dream_text or "recovery" in dream_text):
        cinematic_flow = "ascending_cinematic_flow"
    else:
        cinematic_flow = "steady_cinematic_flow"

    rhythm_strength = max(
        0,
        min(
            100,
            int(round((temporal_momentum * 0.45) + ((100 - temporal_pressure) * 0.25) + ((100 - continuity_decay_rate) * 0.3))),
        ),
    )
    return {
        "rhythm_state": rhythm_state,
        "cinematic_temporal_flow": cinematic_flow,
        "rhythm_strength": rhythm_strength,
    }
