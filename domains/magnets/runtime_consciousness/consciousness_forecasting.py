from __future__ import annotations


def build_consciousness_forecast(
    *,
    awareness_state: str,
    cognitive_balance: str,
    perception_state: str,
    continuity_awareness: str,
    focus: str,
    reflection_state: str,
) -> dict[str, str]:
    if cognitive_balance == "fragmented_cognition":
        forecast = "cognitive_fragmentation"
    elif perception_state == "fragmented_perception":
        forecast = "perception_recovery"
    elif continuity_awareness == "unstable_awareness":
        forecast = "continuity_hardening"
    elif focus in {"resilience_focus", "equilibrium_focus"}:
        forecast = "orchestration_focus_convergence"
    elif "adaptive" in reflection_state:
        forecast = "reflection_stabilization"
    else:
        forecast = "awareness_stabilization"
    return {
        "forecast": forecast,
        "trajectory": forecast,
    }
