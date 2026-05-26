from __future__ import annotations

from typing import Any, Mapping


def forecast_runtime_behavior(
    *,
    runtime_predictions: Mapping[str, Any] | None = None,
    historical_patterns: list[Mapping[str, Any]] | None = None,
    runtime_reputation: Mapping[str, Any] | None = None,
    confidence_evolution: Mapping[str, Any] | None = None,
    current_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    prediction = dict(runtime_predictions or {})
    confidence = dict(confidence_evolution or {})
    context = dict(current_context or {})
    profile = str(context.get("runtime_profile") or "").strip() or "browser_balanced"
    profile_reputation = dict((dict(runtime_reputation or {}).get("runtime_profiles") or {}).get(profile) or {})
    risk = "moderate"
    recommendation = "balanced_profile_recommended"
    reasons = list(prediction.get("prediction_reasoning") or [])
    outcome = str(prediction.get("predicted_outcome") or "").strip()

    if outcome == "likely_external_fallback":
        risk = "high"
        recommendation = "high_probability_of_external_fallback"
    elif outcome == "likely_mobile_instability":
        risk = "high"
        recommendation = "mobile_runtime_likely_unstable"
    elif outcome == "likely_runtime_failure":
        risk = "high"
        recommendation = "cinematic_runtime_risk_elevated"
    elif outcome == "likely_stable_browser_runtime":
        risk = "low"
        recommendation = "stable_browser_runtime_expected"

    if int(profile_reputation.get("stability_reputation", 0) or 0) >= 78 and risk != "high":
        recommendation = "balanced_profile_recommended" if profile == "browser_balanced" else recommendation
    if str(confidence.get("confidence_direction") or "") == "down":
        risk = "high" if risk == "moderate" else risk
        reasons.append("confidence evolution is trending downward")
    for pattern in historical_patterns or []:
        pattern_type = str(dict(pattern).get("pattern_type") or "")
        if pattern_type and pattern_type not in reasons and "downgrade" in pattern_type:
            reasons.append(pattern_type.replace("_", " "))

    return {
        "forecast": recommendation,
        "forecast_confidence": max(1, int(prediction.get("prediction_confidence", 0) or 0)),
        "forecast_risk": risk,
        "recommended_profile": "browser_balanced" if risk == "high" else profile,
        "forecast_reasoning": reasons[:4],
    }
