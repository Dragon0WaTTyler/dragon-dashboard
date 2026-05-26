from __future__ import annotations

from typing import Any, Mapping


def build_network_profile(
    *,
    selected_source: Mapping[str, Any] | None = None,
    capability_snapshot: Mapping[str, Any] | None = None,
    execution_timeline: Mapping[str, Any] | None = None,
    runtime_predictions: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    source = dict(selected_source or {})
    capability = dict(capability_snapshot or {})
    timeline = dict(execution_timeline or {})
    predictions = dict(runtime_predictions or {})
    bandwidth_class = str(source.get("bandwidth_class") or capability.get("bandwidth_class") or "balanced").strip().lower()
    startup_risk = str(source.get("startup_risk") or capability.get("startup_risk") or "low").strip().lower()
    fallback_probability = float(timeline.get("fallback_probability", 0) or 0.0)
    prediction = str(predictions.get("predicted_outcome") or "").strip().lower()

    profile = "balanced_connection"
    if bandwidth_class == "high" and fallback_probability >= 0.45:
        profile = "volatile_network"
    elif bandwidth_class == "high":
        profile = "constrained_wifi"
    elif bandwidth_class == "medium" and startup_risk in {"medium", "high"}:
        profile = "recovery_sensitive"
    elif bandwidth_class == "low" and fallback_probability < 0.24:
        profile = "high_bandwidth"
    elif startup_risk == "high":
        profile = "unstable_mobile"

    degradation_probability = 0.18
    degradation_probability += 0.34 if bandwidth_class == "high" else 0.18 if bandwidth_class == "medium" else 0.06
    degradation_probability += 0.22 if startup_risk == "high" else 0.1 if startup_risk == "medium" else 0.0
    degradation_probability += min(fallback_probability, 0.35)
    degradation_probability += 0.12 if prediction in {"likely_external_fallback", "likely_runtime_failure"} else 0.0
    degradation_probability = round(min(1.0, degradation_probability), 4)

    buffering_likelihood = round(min(1.0, degradation_probability + (0.18 if bandwidth_class == "high" else 0.0)), 4)
    startup_risk_score = round(min(1.0, degradation_probability + 0.08), 4)
    fallback_sensitivity = round(min(1.0, fallback_probability + degradation_probability / 2), 4)
    stability = "stable"
    if degradation_probability >= 0.72:
        stability = "volatile"
    elif degradation_probability >= 0.46:
        stability = "guarded"

    return {
        "profile": profile,
        "network_stability": stability,
        "degradation_probability": degradation_probability,
        "startup_risk": startup_risk_score,
        "buffering_likelihood": buffering_likelihood,
        "fallback_sensitivity": fallback_sensitivity,
        "bandwidth_compatibility": bandwidth_class,
    }
