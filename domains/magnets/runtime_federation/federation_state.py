from __future__ import annotations

from typing import Any, Mapping


def build_federation_state(
    *,
    coherence: int,
    harmony: int,
    pressure: int,
    integrity: int,
    resilience: int,
    alignment: int,
    divergence: int,
    instability_layers: list[str],
    restrictive_authority: bool,
    degraded_ecosystem: bool,
    optimistic_dreaming: bool,
    focused_consciousness: bool,
) -> dict[str, Any]:
    if divergence >= 55 or len(instability_layers) >= 4:
        state = "federation_fragmenting"
    elif coherence >= 72 and harmony >= 70 and pressure <= 42:
        state = "federation_convergent"
    elif pressure >= 65 or restrictive_authority or degraded_ecosystem:
        state = "federation_stabilizing"
    else:
        state = "federation_balancing"

    if instability_layers and optimistic_dreaming and restrictive_authority:
        phase_transition = "volatile_stabilization"
    elif divergence >= 60:
        phase_transition = "divergent_realignment"
    elif coherence >= 75 and harmony >= 72:
        phase_transition = "harmonic_continuation"
    elif pressure >= 58:
        phase_transition = "pressure_adaptation"
    else:
        phase_transition = "steady_continuity"

    if alignment >= 74 and divergence <= 40:
        unity = "high"
    elif alignment >= 38:
        unity = "moderate"
    else:
        unity = "low"

    if pressure >= 70:
        pressure_state = "elevated"
    elif pressure >= 45:
        pressure_state = "moderate"
    else:
        pressure_state = "contained"

    anchors = [state]
    if focused_consciousness:
        anchors.append("focused_consciousness")
    if optimistic_dreaming:
        anchors.append("optimistic_projection")
    if degraded_ecosystem:
        anchors.append("ecosystem_drag")

    return {
        "state": state,
        "phase_transition": phase_transition,
        "orchestration_unity": unity,
        "pressure_state": pressure_state,
        "instability_layers": list(instability_layers),
        "stability_anchors": anchors,
        "coherence": coherence,
        "harmony": harmony,
        "integrity": integrity,
        "resilience": resilience,
        "alignment": alignment,
        "divergence": divergence,
    }
