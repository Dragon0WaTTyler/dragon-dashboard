from __future__ import annotations

from typing import Any, Mapping


_RUNTIME_BONUSES = {
    "browser_runtime": {"browser_responsiveness": 24, "startup_stability": 18, "fallback_resilience": -10},
    "cinematic_runtime": {"cinematic_quality": 24, "transport_pressure": -12, "startup_stability": -6},
    "external_runtime": {"fallback_resilience": 18, "startup_stability": 12, "browser_responsiveness": -14},
    "mobile_safe_runtime": {"mobile_viability": 24, "cinematic_quality": -8, "transport_pressure": 8},
    "degraded_runtime": {"fallback_resilience": 18, "transport_pressure": 12, "cinematic_quality": -14},
}


def compute_runtime_priority(
    runtime_name: str,
    *,
    capability_snapshot: Mapping[str, Any] | None = None,
    execution_metrics: Mapping[str, Any] | None = None,
    readiness_snapshot: Mapping[str, Any] | None = None,
    runtime_pressure: str = "",
    degradation_risk: int = 0,
) -> dict[str, Any]:
    capability = dict(capability_snapshot or {})
    metrics = dict(execution_metrics or {})
    readiness = dict(readiness_snapshot or {})
    runtime = str(runtime_name or "").strip() or "degraded_runtime"
    pressure = str(runtime_pressure or "").strip().lower() or "medium"
    risk = max(0, min(100, int(degradation_risk or 0)))

    responsiveness = 68
    if str(capability.get("startup_viability") or "") == "viable":
        responsiveness += 16
    elif str(capability.get("startup_viability") or "") == "fragile":
        responsiveness -= 10
    if str(readiness.get("playback_runtime") or "") == "browser_runtime":
        responsiveness += 8

    startup_stability = int(metrics.get("startup_score") or 55)
    transport_pressure = 72 if pressure == "low" else 48 if pressure == "medium" else 22
    mobile_viability = 82 if str(capability.get("mobile_runtime_risk") or "") == "low" else 38
    cinematic_quality = 78 if str(readiness.get("runtime_profile") or "").strip() == "browser_cinematic" else 56
    fallback_resilience = 84 if str(readiness.get("fallback_strategy") or readiness.get("runtime_mode") or "").startswith("external") else 58

    components = {
        "browser_responsiveness": responsiveness,
        "startup_stability": startup_stability,
        "transport_pressure": transport_pressure,
        "mobile_viability": mobile_viability,
        "cinematic_quality": cinematic_quality,
        "fallback_resilience": fallback_resilience,
    }
    for key, delta in _RUNTIME_BONUSES.get(runtime, {}).items():
        components[key] = max(0, min(100, int(components.get(key, 0) + delta)))

    if risk >= 70:
        components["startup_stability"] = max(0, components["startup_stability"] - 18)
        components["transport_pressure"] = max(0, components["transport_pressure"] - 14)
    elif risk >= 45:
        components["startup_stability"] = max(0, components["startup_stability"] - 8)

    total = int(round(sum(components.values()) / len(components)))
    if runtime == "browser_runtime" and str(capability.get("browser_safety_class") or "") == "unsafe":
        total = min(total, 24)
    elif runtime == "browser_runtime" and str(capability.get("browser_safety_class") or "") == "safe" and pressure == "low":
        total = min(100, total + 10)
    if runtime == "mobile_safe_runtime" and components["mobile_viability"] < 40:
        total = min(total, 32)
    return {
        "runtime": runtime,
        "score": max(0, min(100, total)),
        "components": components,
    }


def explain_runtime_priority(priority: Mapping[str, Any] | None) -> list[str]:
    payload = dict(priority or {})
    components = dict(payload.get("components") or {})
    score = int(payload.get("score") or 0)
    runtime = str(payload.get("runtime") or "degraded_runtime")
    reasons = [
        f"{runtime} scored {score} from browser responsiveness {int(components.get('browser_responsiveness') or 0)}.",
        f"Startup stability is {int(components.get('startup_stability') or 0)} and fallback resilience is {int(components.get('fallback_resilience') or 0)}.",
    ]
    if int(components.get("transport_pressure") or 0) < 40:
        reasons.append("Transport pressure reduced runtime priority.")
    if runtime == "cinematic_runtime":
        reasons.append("Cinematic quality was weighted above mobile viability.")
    if runtime == "mobile_safe_runtime":
        reasons.append("Mobile viability was prioritized over cinematic quality.")
    return reasons
