from __future__ import annotations

from typing import Any, Mapping


def assess_runtime_degradation(
    *,
    capability_snapshot: Mapping[str, Any] | None = None,
    execution_metrics: Mapping[str, Any] | None = None,
    readiness_snapshot: Mapping[str, Any] | None = None,
    runtime_pressure: str = "",
) -> dict[str, Any]:
    capability = dict(capability_snapshot or {})
    metrics = dict(execution_metrics or {})
    readiness = dict(readiness_snapshot or {})
    pressure = str(runtime_pressure or "").strip().lower() or "medium"

    reasons: list[str] = []
    severity = 12
    if str(capability.get("startup_viability") or "") == "fragile":
        reasons.append("startup_viability_fragile")
        severity += 18
    if str(capability.get("memory_risk") or "") == "high":
        reasons.append("memory_pressure_risk")
        severity += 26
    if str(capability.get("browser_risk") or "") == "high":
        reasons.append("browser_instability_detected")
        severity += 24
    if str(capability.get("mobile_runtime_risk") or "") == "high":
        reasons.append("mobile_degradation")
        severity += 18
    if int(metrics.get("degradation_risk") or 0) >= 55:
        reasons.append("startup_overload")
        severity += 22
    if pressure == "high":
        reasons.append("transport_pressure_high")
        severity += 14
    if str(readiness.get("runtime_profile") or "") == "browser_cinematic" and str(capability.get("startup_viability") or "") != "viable":
        reasons.append("heavy_source_downgrade")
        severity += 16

    severity = max(0, min(100, severity))
    if severity >= 75:
        recommendation = "fallback_external"
        urgency = "immediate"
    elif severity >= 52:
        recommendation = "downgrade_mobile_safe"
        urgency = "high"
    elif severity >= 32:
        recommendation = "downgrade_balanced"
        urgency = "guarded"
    else:
        recommendation = "retain_runtime"
        urgency = "low"

    return {
        "degradation_severity": severity,
        "degradation_reasons": reasons,
        "downgrade_recommendation": recommendation,
        "fallback_urgency": urgency,
        "unstable_runtime_detected": severity >= 52,
    }
