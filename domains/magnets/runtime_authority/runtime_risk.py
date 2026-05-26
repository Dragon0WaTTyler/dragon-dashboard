from __future__ import annotations

from typing import Any, Mapping


def build_runtime_risk(
    *,
    execution_metrics: Mapping[str, Any] | None = None,
    execution_timeline: Mapping[str, Any] | None = None,
    coordination_metrics: Mapping[str, Any] | None = None,
    orchestration_forecast: Mapping[str, Any] | None = None,
    stability_guard: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    execution = dict(execution_metrics or {})
    timeline = dict(execution_timeline or {})
    coordination = dict(coordination_metrics or {})
    forecast = dict(orchestration_forecast or {})
    stability = dict(stability_guard or {})

    score = 0
    score += int(execution.get("degradation_risk", 0) or 0) * 0.45
    score += float(timeline.get("fallback_probability", 0) or 0) * 100 * 0.35
    score += max(0, 100 - int(coordination.get("coordination_confidence", 0) or 0)) * 0.2
    if str(forecast.get("forecast_risk") or "") == "high":
        score += 12
    if str(stability.get("stability_state") or "") in {"guarded", "intervening"}:
        score += 14

    risk_score = max(0, min(100, int(round(score))))
    if risk_score >= 80:
        risk_state = "orchestration_hazard"
    elif risk_score >= 68:
        risk_state = "unstable_runtime"
    elif risk_score >= 55:
        risk_state = "fallback_sensitive"
    elif risk_score >= 45:
        risk_state = "recovery_fragile"
    elif risk_score >= 30:
        risk_state = "elevated_risk"
    else:
        risk_state = "low_risk"

    reasons: list[str] = []
    if int(execution.get("degradation_risk", 0) or 0) >= 55:
        reasons.append("execution_degradation_risk_elevated")
    if float(timeline.get("fallback_probability", 0) or 0) >= 0.45:
        reasons.append("fallback_probability_elevated")
    if int(coordination.get("coordination_confidence", 0) or 0) < 60:
        reasons.append("coordination_confidence_suppressed")
    if str(stability.get("guard_intervention") or "") != "none":
        reasons.append("stability_guard_active")

    return {
        "risk_state": risk_state,
        "risk_score": risk_score,
        "risk_reasons": reasons,
    }
