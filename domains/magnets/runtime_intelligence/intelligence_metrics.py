from __future__ import annotations

from typing import Any, Mapping


def build_intelligence_metrics(
    *,
    memory_summary: Mapping[str, Any] | None = None,
    runtime_predictions: Mapping[str, Any] | None = None,
    confidence_evolution: Mapping[str, Any] | None = None,
    runtime_reputation: Mapping[str, Any] | None = None,
    adaptation_history: Mapping[str, Any] | None = None,
    orchestration_forecast: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    memory = dict(memory_summary or {})
    prediction = dict(runtime_predictions or {})
    confidence = dict(confidence_evolution or {})
    forecast = dict(orchestration_forecast or {})
    runtime_profiles = dict(dict(runtime_reputation or {}).get("runtime_profiles") or {})
    trust_scores = [int(dict(item).get("orchestration_trust", 60) or 60) for item in runtime_profiles.values()]
    average_trust = int(round(sum(trust_scores) / len(trust_scores))) if trust_scores else 60
    switch_frequency = int(dict(adaptation_history or {}).get("switch_frequency", 0) or 0)
    predictive_accuracy = max(35, min(95, int(round((float(memory.get("recovery_success_rate", 0) or 0) * 45) + (int(prediction.get("prediction_confidence", 0) or 0) * 0.5)))))
    learning_score = max(20, min(96, int(round((1 - float(memory.get("runtime_instability", 0) or 0)) * 60 + (float(memory.get("total_observations", 0) or 0) * 2)))))
    runtime_memory_confidence = max(25, min(95, int(round((float(memory.get("total_observations", 0) or 0) * 6) + (1 - float(memory.get("fallback_frequency", 0) or 0)) * 35))))
    fallback_intelligence = max(20, min(95, int(round((float(memory.get("recovery_success_rate", 0) or 0) * 55) + (0 if forecast.get("forecast_risk") == "high" else 20)))))
    runtime_trust_score = max(20, min(95, int(round((average_trust + int(prediction.get("prediction_confidence", 0) or 0)) / 2))))
    adaptation_efficiency = max(20, min(95, int(round((int(confidence.get("prediction_alignment", 0) or 0) * 0.6) + max(0, 25 - switch_frequency)))))
    return {
        "predictive_accuracy": predictive_accuracy,
        "orchestration_learning_score": learning_score,
        "runtime_memory_confidence": runtime_memory_confidence,
        "fallback_intelligence": fallback_intelligence,
        "runtime_trust_score": runtime_trust_score,
        "adaptation_efficiency": adaptation_efficiency,
    }
