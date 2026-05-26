from __future__ import annotations

from typing import Any, Mapping


def build_capability_metrics(
    *,
    compatibility: Mapping[str, Any] | None = None,
    network_profile: Mapping[str, Any] | None = None,
    resource_state: Mapping[str, Any] | None = None,
    capability_forecast: Mapping[str, Any] | None = None,
    runtime_feasibility: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    compatibility_map = dict(compatibility or {})
    network = dict(network_profile or {})
    resource = dict(resource_state or {})
    forecast = dict(capability_forecast or {})
    feasibility = dict(runtime_feasibility or {})

    feasibility_score = 82
    feasibility_score -= len(list(compatibility_map.get("conflicts") or [])) * 16
    feasibility_score -= int(resource.get("resource_pressure_score", 0) or 0) // 4
    feasibility_score -= int(round(float(network.get("degradation_probability", 0) or 0.0) * 30))
    feasibility_score -= 18 if str(feasibility.get("runtime_feasibility") or "") in {"unsafe", "impossible"} else 0
    feasibility_score = max(0, min(100, feasibility_score))

    affordance_score = max(0, min(100, 100 - int(resource.get("resource_pressure_score", 0) or 0)))
    degradation_pressure = max(0, min(100, int(round(float(forecast.get("degradation_escalation", 0) or 0.0) * 100))))
    sustainability = max(0, min(100, 100 - int(round(float(forecast.get("stability_collapse_probability", 0) or 0.0) * 100))))
    stability = max(0, min(100, feasibility_score - len(list(compatibility_map.get("conflicts") or [])) * 6))
    headroom = max(0, min(100, 100 - max(int(resource.get("resource_pressure_score", 0) or 0), degradation_pressure)))

    return {
        "feasibility_score": feasibility_score,
        "runtime_affordance_score": affordance_score,
        "degradation_pressure": degradation_pressure,
        "orchestration_sustainability": sustainability,
        "capability_stability": stability,
        "runtime_headroom": headroom,
    }
