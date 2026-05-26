from __future__ import annotations

from typing import Any, Mapping

from .browser_capabilities import build_browser_capabilities
from .capability_events import build_capability_events
from .capability_forecasting import forecast_capability_feasibility
from .capability_memory import build_capability_memory_summary, load_capability_memory, update_capability_memory
from .capability_metrics import build_capability_metrics
from .compatibility_matrix import evaluate_runtime_compatibility
from .device_profiles import build_device_profile
from .execution_affordances import build_execution_affordances
from .network_profiles import build_network_profile
from .resource_limits import build_resource_limits
from .runtime_feasibility import evaluate_runtime_feasibility
from .thermal_profiles import build_thermal_profile


def build_runtime_capability_engine(
    orchestration: Mapping[str, Any] | None,
    *,
    persist_memory: bool = True,
    memory_path=None,
    timestamp: str = "",
) -> dict[str, Any]:
    payload = dict(orchestration or {})
    capability_memory = load_capability_memory(path=memory_path)
    capability_memory_summary = build_capability_memory_summary(capability_memory, current_context=payload)
    browser = build_browser_capabilities(
        selected_source=payload.get("selected_source"),
        capability_snapshot=payload.get("capability_snapshot") or payload.get("readiness_snapshot"),
        playback_runtime=str(payload.get("playback_runtime") or payload.get("runtime_mode") or ""),
        runtime_profile=str(payload.get("runtime_profile") or ""),
        startup_confidence=str(payload.get("startup_confidence") or ""),
    )
    network = build_network_profile(
        selected_source=payload.get("selected_source"),
        capability_snapshot=payload.get("capability_snapshot") or payload.get("readiness_snapshot"),
        execution_timeline=payload.get("execution_timeline"),
        runtime_predictions=payload.get("runtime_predictions"),
    )
    device = build_device_profile(
        selected_source=payload.get("selected_source"),
        runtime_profile=str(payload.get("runtime_profile") or ""),
        authority_memory_summary=payload.get("authority_memory_summary"),
        execution_metrics=payload.get("execution_metrics"),
        network_profile=network,
    )
    resource = build_resource_limits(
        selected_source=payload.get("selected_source"),
        runtime_profile=str(payload.get("runtime_profile") or ""),
        browser_capabilities=browser,
        execution_metrics=payload.get("execution_metrics"),
    )
    thermal = build_thermal_profile(
        device_profile=device,
        resource_state=resource,
        runtime_profile=str(payload.get("runtime_profile") or ""),
        execution_metrics=payload.get("execution_metrics"),
    )
    affordances = build_execution_affordances(
        selected_source=payload.get("selected_source"),
        runtime_profile=str(payload.get("runtime_profile") or ""),
        device_profile=device,
        network_profile=network,
        resource_state=resource,
        thermal_profile=thermal,
    )
    compatibility = evaluate_runtime_compatibility(
        playback_runtime=str(payload.get("playback_runtime") or payload.get("runtime_mode") or ""),
        runtime_profile=str(payload.get("runtime_profile") or ""),
        selected_source=payload.get("selected_source"),
        browser_capabilities=browser,
        device_profile=device,
        network_profile=network,
        resource_state=resource,
        runtime_affordances=affordances,
    )
    forecast = forecast_capability_feasibility(
        runtime_predictions=payload.get("runtime_predictions"),
        network_profile=network,
        resource_state=resource,
        thermal_profile=thermal,
        compatibility=compatibility,
    )
    feasibility = evaluate_runtime_feasibility(
        approved_runtime=str(payload.get("approved_runtime") or payload.get("playback_runtime") or payload.get("runtime_mode") or ""),
        authority_state=str(payload.get("authority_state") or ""),
        browser_capabilities=browser,
        network_profile=network,
        resource_state=resource,
        thermal_profile=thermal,
        compatibility=compatibility,
        capability_forecast=forecast,
    )
    metrics = build_capability_metrics(
        compatibility=compatibility,
        network_profile=network,
        resource_state=resource,
        capability_forecast=forecast,
        runtime_feasibility=feasibility,
    )
    confidence = _capability_confidence(feasibility, metrics, compatibility)
    warnings = _build_warnings(browser, compatibility, feasibility, resource, thermal)
    degradation_risk = _degradation_risk_label(forecast, metrics)
    result = {
        "capability_state": _capability_state(feasibility, compatibility),
        "runtime_feasibility": str(feasibility.get("runtime_feasibility") or "feasible"),
        "device_profile": device,
        "network_profile": network,
        "resource_state": resource,
        "thermal_profile": thermal,
        "capability_confidence": confidence,
        "capability_warnings": warnings,
        "degradation_risk": degradation_risk,
        "runtime_affordances": affordances,
        "feasible_runtime_modes": list(compatibility.get("feasible_runtime_modes") or ["external_runtime"]),
        "capability_forecast": forecast,
        "capability_metrics": metrics,
        "compatibility_matrix": compatibility,
    }
    result["capability_events"] = build_capability_events(
        runtime_feasibility=feasibility,
        network_profile=network,
        resource_state=resource,
        thermal_profile=thermal,
    )
    if persist_memory:
        result["capability_memory_summary"] = update_capability_memory(payload, result, path=memory_path, timestamp=timestamp)
    else:
        result["capability_memory_summary"] = capability_memory_summary
    return result


def _capability_state(
    feasibility: Mapping[str, Any],
    compatibility: Mapping[str, Any],
) -> str:
    state = str(feasibility.get("runtime_feasibility") or "feasible")
    if state in {"unsafe", "impossible"}:
        return "rejected"
    if state in {"unstable", "constrained"} or bool(list(compatibility.get("conflicts") or [])):
        return "guarded"
    if state == "degraded":
        return "degraded"
    return "approved"


def _capability_confidence(
    feasibility: Mapping[str, Any],
    metrics: Mapping[str, Any],
    compatibility: Mapping[str, Any],
) -> dict[str, Any]:
    score = int(metrics.get("feasibility_score", 0) or 0)
    score -= len(list(compatibility.get("conflicts") or [])) * 8
    state = str(feasibility.get("runtime_feasibility") or "feasible")
    score -= 24 if state in {"unsafe", "impossible"} else 12 if state in {"unstable", "constrained"} else 0
    score = max(0, min(100, score))
    label = "high" if score >= 72 else "medium" if score >= 46 else "low"
    return {"score": score, "label": label}


def _build_warnings(
    browser: Mapping[str, Any],
    compatibility: Mapping[str, Any],
    feasibility: Mapping[str, Any],
    resource: Mapping[str, Any],
    thermal: Mapping[str, Any],
) -> list[str]:
    warnings: list[str] = []
    warnings.extend(str(item) for item in browser.get("warnings") or [] if str(item or "").strip())
    warnings.extend(str(item) for item in compatibility.get("conflicts") or [] if str(item or "").strip())
    if str(resource.get("orchestration_saturation_risk") or "") in {"elevated", "high"}:
        warnings.append("resource_pressure_elevated")
    if str(thermal.get("thermal_state") or "") in {"elevated_thermal_risk", "sustained_runtime_pressure", "mobile_heat_sensitive"}:
        warnings.append("thermal_pressure_elevated")
    if str(feasibility.get("runtime_feasibility") or "") in {"unsafe", "impossible"}:
        warnings.append("runtime_infeasible")
    seen: set[str] = set()
    ordered: list[str] = []
    for warning in warnings:
        normalized = str(warning or "").strip()
        if normalized and normalized not in seen:
            seen.add(normalized)
            ordered.append(normalized)
    return ordered


def _degradation_risk_label(forecast: Mapping[str, Any], metrics: Mapping[str, Any]) -> str:
    pressure = int(metrics.get("degradation_pressure", 0) or 0)
    escalation = float(forecast.get("degradation_escalation", 0) or 0.0)
    if pressure >= 72 or escalation >= 0.72:
        return "high"
    if pressure >= 44 or escalation >= 0.44:
        return "medium"
    return "low"
