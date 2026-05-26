from __future__ import annotations

from typing import Any, Mapping


def build_orchestration_pressure(
    *,
    selected_source: Mapping[str, Any] | None = None,
    degradation_risk: int = 0,
    runtime_resilience: int = 0,
    coordination_confidence: int = 0,
    prediction_confidence: int = 0,
    fallback_probability: float = 0.0,
    adaptation_pressure: int = 0,
) -> dict[str, Any]:
    source = dict(selected_source or {})
    bandwidth_pressure = 66 if bool(source.get("high_bandwidth_required")) else 28
    fallback_pressure = max(0, min(100, int(round(fallback_probability * 100))))
    instability_pressure = max(0, min(100, degradation_risk))
    coordination_pressure = max(0, min(100, 100 - coordination_confidence))
    confidence_pressure = max(0, min(100, 100 - prediction_confidence))
    adaptation_pressure_value = max(0, min(100, adaptation_pressure))
    pressure_score = int(
        round(
            (
                bandwidth_pressure
                + fallback_pressure
                + instability_pressure
                + coordination_pressure
                + confidence_pressure
                + adaptation_pressure_value
                + max(0, min(100, 100 - runtime_resilience))
            )
            / 7
        )
    )
    return {
        "pressure_score": pressure_score,
        "pressure_direction": _pressure_direction(pressure_score, fallback_pressure, instability_pressure),
        "escalation_tendency": _escalation_tendency(pressure_score, fallback_pressure, instability_pressure, adaptation_pressure_value),
        "pressure_components": {
            "bandwidth_pressure": bandwidth_pressure,
            "fallback_pressure": fallback_pressure,
            "runtime_instability_pressure": instability_pressure,
            "coordination_pressure": coordination_pressure,
            "confidence_pressure": confidence_pressure,
            "adaptation_pressure": adaptation_pressure_value,
        },
    }


def _pressure_direction(score: int, fallback_pressure: int, instability_pressure: int) -> str:
    if score >= 70 or instability_pressure >= 76:
        return "escalating"
    if fallback_pressure <= 32 and score <= 42:
        return "recovering"
    return "steady"


def _escalation_tendency(score: int, fallback_pressure: int, instability_pressure: int, adaptation_pressure: int) -> str:
    if score >= 76 or (fallback_pressure >= 70 and instability_pressure >= 70):
        return "high_escalation"
    if score >= 56 or adaptation_pressure >= 56:
        return "watchful_escalation"
    return "contained"
