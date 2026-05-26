from __future__ import annotations

from typing import Any, Mapping


def build_intelligence_events(
    *,
    memory_summary: Mapping[str, Any] | None = None,
    historical_patterns: list[Mapping[str, Any]] | None = None,
    runtime_predictions: Mapping[str, Any] | None = None,
    confidence_evolution: Mapping[str, Any] | None = None,
    adaptation_history: Mapping[str, Any] | None = None,
    orchestration_forecast: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    memory = dict(memory_summary or {})
    prediction = dict(runtime_predictions or {})
    confidence = dict(confidence_evolution or {})
    adaptation = dict(adaptation_history or {})
    forecast = dict(orchestration_forecast or {})

    for pattern in historical_patterns or []:
        pattern_payload = dict(pattern or {})
        events.append({"event_type": "runtime_pattern_detected", "details": pattern_payload})
    if int(confidence.get("confidence_delta", 0) or 0):
        events.append({"event_type": "confidence_shifted", "details": {"direction": confidence.get("confidence_direction"), "delta": confidence.get("confidence_delta")}})
    if str(prediction.get("predicted_outcome") or "") in {"likely_runtime_failure", "likely_mobile_instability"}:
        events.append({"event_type": "instability_predicted", "details": {"predicted_outcome": prediction.get("predicted_outcome")}})
    if str(forecast.get("forecast") or "").startswith(("high_probability_of_external_fallback", "mobile_runtime_likely_unstable", "cinematic_runtime_risk_elevated")):
        events.append({"event_type": "fallback_forecasted", "details": {"forecast": forecast.get("forecast")}})
    if int(adaptation.get("switch_frequency", 0) or 0) > 0:
        events.append({"event_type": "adaptation_chain_detected", "details": {"switch_frequency": adaptation.get("switch_frequency")}})
    events.append({"event_type": "orchestration_memory_updated", "details": {"total_observations": memory.get("total_observations", 0)}})
    return events
