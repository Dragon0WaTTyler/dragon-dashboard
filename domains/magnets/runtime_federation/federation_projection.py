from __future__ import annotations

from typing import Any


def build_federation_projection(
    *,
    phase_transition: str,
    orchestration_unity: str,
    coherence: int,
    resilience: int,
    pressure: int,
    optimistic_dreaming: bool,
    degraded_ecosystem: bool,
) -> dict[str, Any]:
    if phase_transition == "volatile_stabilization":
        continuity_projection = "adaptive_recovery"
    elif orchestration_unity == "high" and coherence >= 72:
        continuity_projection = "cinematic_continuation"
    elif degraded_ecosystem or pressure >= 65:
        continuity_projection = "guarded_rebalancing"
    elif optimistic_dreaming and resilience >= 66:
        continuity_projection = "optimistic_stabilization"
    else:
        continuity_projection = "measured_continuity"

    if pressure >= 62 and orchestration_unity != "high":
        cinematic_runtime_state = "unstable_cinematic_transition"
    elif coherence >= 74 and resilience >= 72:
        cinematic_runtime_state = "cinematic_runtime_harmony"
    elif degraded_ecosystem:
        cinematic_runtime_state = "degraded_cinematic_recovery"
    else:
        cinematic_runtime_state = "adaptive_cinematic_balance"

    if orchestration_unity == "high":
        trajectory = "convergent"
    elif pressure >= 58 or degraded_ecosystem:
        trajectory = "recovering"
    else:
        trajectory = "adaptive"

    return {
        "continuity_projection": continuity_projection,
        "cinematic_runtime_state": cinematic_runtime_state,
        "trajectory": trajectory,
        "projection_confidence": max(0, min(100, int(round((coherence * 0.55) + (resilience * 0.45) - (pressure * 0.2))))),
    }
