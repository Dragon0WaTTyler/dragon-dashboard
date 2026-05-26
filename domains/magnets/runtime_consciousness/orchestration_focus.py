from __future__ import annotations


def build_orchestration_focus(
    *,
    awareness_state: str,
    intuition_state: str,
    pressure_score: int,
    cinematic_quality: int,
    runtime_resilience: int,
) -> dict[str, int | str]:
    if pressure_score >= 72:
        focus = "degradation_focus"
    elif intuition_state == "stabilization_intuition":
        focus = "stabilization_focus"
    elif runtime_resilience >= 76:
        focus = "resilience_focus"
    elif awareness_state == "cinematic_awareness" or cinematic_quality >= 82:
        focus = "cinematic_focus"
    else:
        focus = "equilibrium_focus"
    strength = max(0, min(100, round((runtime_resilience * 0.35) + ((100 - pressure_score) * 0.3) + (cinematic_quality * 0.2) + (15 if focus in {"resilience_focus", "equilibrium_focus"} else 5))))
    return {
        "focus": focus,
        "orchestration_focus_strength": strength,
    }
