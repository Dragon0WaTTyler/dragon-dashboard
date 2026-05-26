from __future__ import annotations

from typing import Any, Mapping


def summarize_execution_metrics(
    *,
    capability_snapshot: Mapping[str, Any] | None = None,
    playback_readiness: str = "",
    transport_descriptor: Mapping[str, Any] | None = None,
    guardrails: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    capability = dict(capability_snapshot or {})
    transport = dict(transport_descriptor or {})
    guardrail_payload = dict(guardrails or {})

    startup_score = 88
    if str(playback_readiness or "").strip() == "browser_deferred":
        startup_score -= 30
    if str(capability.get("startup_viability") or "") == "fragile":
        startup_score -= 24
    if str(transport.get("startup_behavior") or "") == "buffer_sensitive":
        startup_score -= 12

    stability_score = 86
    if str(transport.get("runtime_pressure") or "") == "high":
        stability_score -= 26
    elif str(transport.get("runtime_pressure") or "") == "medium":
        stability_score -= 14
    if str(capability.get("mobile_runtime_risk") or "") == "high":
        stability_score -= 14

    browser_safety_score = 90
    safety_class = str(capability.get("browser_safety_class") or "unknown")
    if safety_class == "limited":
        browser_safety_score -= 26
    elif safety_class == "unsafe":
        browser_safety_score -= 58

    degradation_risk = 18
    degradation_likelihood = str(transport.get("degradation_likelihood") or "")
    if degradation_likelihood == "medium":
        degradation_risk = 44
    elif degradation_likelihood == "high":
        degradation_risk = 72

    fallback_pressure = 8
    if guardrail_payload.get("rejected"):
        fallback_pressure = 88
    elif str(transport.get("transport_class") or "") in {"browser_heavy", "mobile_limited"}:
        fallback_pressure = 52

    runtime_confidence = max(
        0,
        min(
            100,
            int(round((startup_score + stability_score + browser_safety_score) / 3)) - int(round(fallback_pressure / 5)),
        ),
    )

    return {
        "startup_score": max(0, min(100, startup_score)),
        "stability_score": max(0, min(100, stability_score)),
        "browser_safety_score": max(0, min(100, browser_safety_score)),
        "fallback_pressure": max(0, min(100, fallback_pressure)),
        "runtime_confidence": runtime_confidence,
        "degradation_risk": max(0, min(100, degradation_risk)),
    }


def build_runtime_grade(metrics: Mapping[str, Any] | None) -> dict[str, Any]:
    payload = dict(metrics or {})
    confidence = int(payload.get("runtime_confidence") or 0)
    if confidence >= 85:
        grade = "A"
        label = "stable"
    elif confidence >= 70:
        grade = "B"
        label = "guarded"
    elif confidence >= 55:
        grade = "C"
        label = "degraded"
    elif confidence >= 40:
        grade = "D"
        label = "fragile"
    else:
        grade = "F"
        label = "fallback_only"
    return {
        "grade": grade,
        "label": label,
        "score": confidence,
    }
