from __future__ import annotations

from typing import Any, Mapping


def build_identity_forecast(
    *,
    continuity_state: Mapping[str, Any] | None = None,
    behavioral_drift: Mapping[str, Any] | None = None,
    preference_evolution: Mapping[str, Any] | None = None,
    identity_metrics: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    continuity = dict(continuity_state or {})
    drift = dict(behavioral_drift or {})
    preferences = dict(preference_evolution or {})
    metrics = dict(identity_metrics or {})
    continuity_confidence = int(continuity.get("continuity_confidence", 0) or 0)
    drift_state = str(drift.get("drift_state") or "stability_bias")
    browser_preference = str(preferences.get("browser_preference") or "external_runtime")
    if drift_state == "stronger_fallback_dependency":
        trajectory = "fallback_dependency_growth"
    elif continuity_confidence >= 72:
        trajectory = "orchestration_stabilization"
    elif browser_preference == "browser_runtime":
        trajectory = "confidence_stabilization_trajectory"
    else:
        trajectory = "adaptation_hardening"
    return {
        "forecast": trajectory,
        "forecast_confidence": int(metrics.get("runtime_identity_confidence", 0) or 0),
        "likely_drift": drift_state,
        "stabilization_outlook": "stable" if continuity_confidence >= 70 else "evolving",
    }
