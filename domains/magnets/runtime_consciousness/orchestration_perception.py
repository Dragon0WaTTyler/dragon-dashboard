from __future__ import annotations


def build_orchestration_perception(
    *,
    awareness_state: str,
    reflection_state: str,
    continuity_awareness: str,
    runtime_presence: str,
    degradation_risk: int,
) -> dict[str, int | str]:
    if degradation_risk >= 74:
        state = "fragmented_perception"
    elif awareness_state == "cinematic_awareness" and runtime_presence == "cinematic_presence":
        state = "cinematic_perception"
    elif "resilient" in reflection_state or continuity_awareness == "resilient_awareness":
        state = "resilient_perception"
    elif "adaptive" in awareness_state or "adaptive" in reflection_state:
        state = "adaptive_perception"
    else:
        state = "stable_perception"
    integrity = {
        "stable_perception": 82,
        "fragmented_perception": 38,
        "adaptive_perception": 68,
        "resilient_perception": 88,
        "cinematic_perception": 84,
    }[state]
    return {
        "state": state,
        "perception_integrity": integrity,
    }
