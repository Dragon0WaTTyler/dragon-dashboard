from __future__ import annotations

from typing import Any, Mapping


def forecast_capability_feasibility(
    *,
    runtime_predictions: Mapping[str, Any] | None = None,
    network_profile: Mapping[str, Any] | None = None,
    resource_state: Mapping[str, Any] | None = None,
    thermal_profile: Mapping[str, Any] | None = None,
    compatibility: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    predictions = dict(runtime_predictions or {})
    network = dict(network_profile or {})
    resource = dict(resource_state or {})
    thermal = dict(thermal_profile or {})
    compatibility_map = dict(compatibility or {})

    escalation = 0.16
    escalation += float(network.get("degradation_probability", 0) or 0.0) * 0.35
    escalation += int(resource.get("resource_pressure_score", 0) or 0) / 220
    escalation += float(thermal.get("thermal_throttling_likelihood", 0) or 0.0) * 0.25
    if str(predictions.get("predicted_outcome") or "") in {"likely_external_fallback", "likely_runtime_failure"}:
        escalation += 0.18
    if bool(compatibility_map.get("conflicts")):
        escalation += 0.12
    escalation = round(min(1.0, escalation), 4)

    return {
        "degradation_escalation": escalation,
        "stability_collapse_probability": round(min(1.0, escalation + 0.12), 4),
        "future_fallback_likelihood": round(min(1.0, escalation + float(network.get("fallback_sensitivity", 0) or 0.0) * 0.4), 4),
        "runtime_exhaustion_risk": "high" if escalation >= 0.7 else "medium" if escalation >= 0.42 else "low",
        "orchestration_sustainability": "fragile" if escalation >= 0.7 else "guarded" if escalation >= 0.42 else "sustainable",
    }
