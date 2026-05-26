from __future__ import annotations

from typing import Any, Mapping


def evaluate_runtime_feasibility(
    *,
    approved_runtime: str = "",
    authority_state: str = "",
    browser_capabilities: Mapping[str, Any] | None = None,
    network_profile: Mapping[str, Any] | None = None,
    resource_state: Mapping[str, Any] | None = None,
    thermal_profile: Mapping[str, Any] | None = None,
    compatibility: Mapping[str, Any] | None = None,
    capability_forecast: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    browser = dict(browser_capabilities or {})
    network = dict(network_profile or {})
    resource = dict(resource_state or {})
    thermal = dict(thermal_profile or {})
    compatibility_map = dict(compatibility or {})
    forecast = dict(capability_forecast or {})

    runtime = str(approved_runtime or "").strip() or "external_runtime"
    state = str(authority_state or "").strip().lower()
    conflicts = list(compatibility_map.get("conflicts") or [])
    network_risk = float(network.get("degradation_probability", 0) or 0.0)
    collapse = float(forecast.get("stability_collapse_probability", 0) or 0.0)
    resource_pressure = int(resource.get("resource_pressure_score", 0) or 0)
    thermal_state = str(thermal.get("thermal_state") or "").strip()
    browser_state = str(browser.get("browser_feasibility") or "").strip()

    feasibility = "feasible"
    reasons: list[str] = []

    if state == "blocked" or runtime == "blocked":
        feasibility = "impossible"
        reasons.append("Authority state is blocked before capability finalization.")
    if runtime == "browser_runtime" and browser_state == "rejected":
        feasibility = "unsafe"
        reasons.append("Browser runtime is deterministically rejected for this environment.")
    if conflicts and runtime == "browser_runtime" and feasibility == "feasible":
        feasibility = "constrained"
        reasons.extend(str(item).replace("_", " ") for item in conflicts)
    if network_risk >= 0.72 or collapse >= 0.8:
        feasibility = "unstable" if feasibility not in {"unsafe", "impossible"} else feasibility
        reasons.append("Forecasted runtime collapse probability is elevated.")
    elif network_risk >= 0.48 or resource_pressure >= 68:
        feasibility = "degraded" if feasibility == "feasible" else feasibility
        reasons.append("Resource or network pressure requires degraded orchestration expectations.")
    if thermal_state in {"sustained_runtime_pressure", "mobile_heat_sensitive"} and feasibility == "feasible":
        feasibility = "constrained"
        reasons.append("Thermal shaping indicates sustained runtime pressure.")

    if not reasons:
        reasons.append("Capability signals remain within sustainable feasibility thresholds.")

    return {
        "runtime_feasibility": feasibility,
        "approved_runtime": runtime,
        "feasibility_reasons": reasons,
    }
