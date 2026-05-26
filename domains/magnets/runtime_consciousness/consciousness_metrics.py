from __future__ import annotations


def build_consciousness_metrics(
    *,
    awareness_integrity: int,
    focus_strength: int,
    cognitive_stability: int,
    runtime_presence_score: int,
    perception_integrity: int,
    reflection_strength: int,
    continuity_awareness_score: int,
) -> dict[str, int]:
    orchestration_clarity = max(
        0,
        min(
            100,
            round(
                (awareness_integrity * 0.16)
                + (focus_strength * 0.15)
                + (cognitive_stability * 0.18)
                + (runtime_presence_score * 0.14)
                + (perception_integrity * 0.14)
                + (reflection_strength * 0.11)
                + (continuity_awareness_score * 0.12)
            ),
        ),
    )
    return {
        "awareness_integrity": awareness_integrity,
        "orchestration_focus_strength": focus_strength,
        "cognitive_stability": cognitive_stability,
        "runtime_presence_score": runtime_presence_score,
        "perception_integrity": perception_integrity,
        "reflection_strength": reflection_strength,
        "continuity_awareness_score": continuity_awareness_score,
        "orchestration_clarity": orchestration_clarity,
    }
