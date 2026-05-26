from __future__ import annotations

from typing import Any, Mapping


def govern_confidence(
    *,
    confidence_evolution: Mapping[str, Any] | None = None,
    runtime_predictions: Mapping[str, Any] | None = None,
    runtime_learning: Mapping[str, Any] | None = None,
    runtime_risk: Mapping[str, Any] | None = None,
    stability_guard: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    confidence = dict(confidence_evolution or {})
    prediction = dict(runtime_predictions or {})
    learning = dict(runtime_learning or {})
    risk = dict(runtime_risk or {})
    stability = dict(stability_guard or {})

    stages = [dict(item) for item in confidence.get("stages") or [] if isinstance(item, Mapping)]
    baseline = int((stages[-1] if stages else {}).get("confidence", 38) or 38)
    adjusted = baseline
    actions: list[str] = []

    if str(risk.get("risk_state") or "") in {"fallback_sensitive", "unstable_runtime", "orchestration_hazard"}:
        adjusted -= 10
        actions.append("confidence_suppressed")
    if str(stability.get("stability_state") or "") in {"guarded", "intervening"}:
        adjusted -= 8
        actions.append("volatility_reduction")
    if int(prediction.get("prediction_confidence", 0) or 0) >= 84 and str(risk.get("risk_state") or "") not in {"low_risk", "elevated_risk"}:
        adjusted = min(adjusted, 68)
        actions.append("overconfidence_prevention")
    if int(learning.get("fallback_trust_adjustment", 0) or 0) > 0 and adjusted < baseline:
        adjusted += 4
        actions.append("confidence_recovery")

    regulated = max(0, min(100, adjusted))
    return {
        "regulated_confidence_score": regulated,
        "regulated_confidence_label": _label(regulated),
        "governance_actions": actions,
        "suppressed": regulated < baseline,
        "confidence_delta_applied": regulated - baseline,
    }


def _label(score: int) -> str:
    if score >= 75:
        return "high"
    if score >= 55:
        return "medium"
    return "low"
