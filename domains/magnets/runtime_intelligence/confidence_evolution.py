from __future__ import annotations

from typing import Any, Mapping


def build_confidence_evolution(
    *,
    current_context: Mapping[str, Any] | None = None,
    runtime_predictions: Mapping[str, Any] | None = None,
    runtime_learning: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    context = dict(current_context or {})
    execution_metrics = dict(context.get("execution_metrics") or {})
    coordination_metrics = dict(context.get("coordination_metrics") or {})
    prediction = dict(runtime_predictions or {})
    learning = dict(runtime_learning or {})
    before = _confidence_from_label(str(context.get("startup_confidence") or "low"))
    after_execution = max(0, min(100, int(round((before + int(execution_metrics.get("runtime_confidence", before) or before)) / 2))))
    after_coordination = max(0, min(100, int(round((after_execution + int(coordination_metrics.get("coordination_confidence", after_execution) or after_execution)) / 2))))
    after_degradation = max(0, min(100, after_coordination - int(execution_metrics.get("degradation_risk", 0) or 0) // 4))
    after_fallback = max(0, min(100, after_degradation + int(learning.get("fallback_trust_adjustment", 0) or 0)))
    stages = [
        {"stage": "before_negotiation", "confidence": before},
        {"stage": "after_execution_simulation", "confidence": after_execution},
        {"stage": "after_coordination", "confidence": after_coordination},
        {"stage": "after_degradation", "confidence": after_degradation},
        {"stage": "after_fallback", "confidence": after_fallback},
    ]
    delta = after_fallback - before
    return {
        "stages": stages,
        "confidence_delta": delta,
        "confidence_direction": "up" if delta > 0 else ("down" if delta < 0 else "steady"),
        "confidence_stability": _stability_band(stages),
        "prediction_alignment": int(prediction.get("prediction_confidence", 0) or 0),
    }


def _confidence_from_label(label: str) -> int:
    normalized = str(label or "").strip().lower()
    if normalized == "high":
        return 82
    if normalized == "medium":
        return 64
    return 38


def _stability_band(stages: list[dict[str, Any]]) -> str:
    values = [int(item.get("confidence", 0) or 0) for item in stages]
    spread = max(values) - min(values)
    if spread <= 10:
        return "stable"
    if spread <= 24:
        return "adaptive"
    return "volatile"
