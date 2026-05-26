from __future__ import annotations


def build_awareness_pressure(
    *,
    degradation_risk: int,
    pressure_score: int,
    adaptation_pressure: int,
    cinematic_quality: int,
    continuity_confidence: int,
) -> dict[str, int | str]:
    escalation = max(0, min(100, round((degradation_risk * 0.45) + (pressure_score * 0.35) + (adaptation_pressure * 0.2))))
    continuity = max(0, min(100, round(((100 - continuity_confidence) * 0.7) + (adaptation_pressure * 0.3))))
    resilience = max(0, min(100, round((max(0, 100 - degradation_risk) * 0.5) + (max(0, 100 - pressure_score) * 0.5))))
    cinematic = max(0, min(100, round((cinematic_quality * 0.6) + (max(0, 100 - degradation_risk) * 0.4))))
    return {
        "pressure_state": "elevated_awareness_pressure" if escalation >= 60 or continuity >= 58 else "stable_awareness_pressure",
        "escalation_pressure": escalation,
        "continuity_pressure": continuity,
        "resilience_pressure": resilience,
        "degradation_pressure": max(0, min(100, degradation_risk)),
        "cinematic_pressure": cinematic,
    }
