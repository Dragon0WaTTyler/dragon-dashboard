from __future__ import annotations

from typing import Any, Mapping


def build_capability_events(
    *,
    runtime_feasibility: Mapping[str, Any] | None = None,
    network_profile: Mapping[str, Any] | None = None,
    resource_state: Mapping[str, Any] | None = None,
    thermal_profile: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    feasibility = dict(runtime_feasibility or {})
    network = dict(network_profile or {})
    resource = dict(resource_state or {})
    thermal = dict(thermal_profile or {})
    events: list[dict[str, Any]] = []

    state = str(feasibility.get("runtime_feasibility") or "").strip()
    if state in {"degraded", "constrained"}:
        events.append(_event("capability_degraded", state=state))
        events.append(_event("runtime_constrained", state=state))
    if state in {"unsafe", "impossible"}:
        events.append(_event("infeasible_runtime_rejected", state=state))
    if float(network.get("degradation_probability", 0) or 0.0) >= 0.5:
        events.append(_event("network_instability_detected", state=str(network.get("network_stability") or "guarded")))
    if int(resource.get("resource_pressure_score", 0) or 0) >= 65:
        events.append(_event("resource_limit_triggered", state=str(resource.get("orchestration_saturation_risk") or "elevated")))
    if str(thermal.get("thermal_state") or "") in {"elevated_thermal_risk", "sustained_runtime_pressure", "mobile_heat_sensitive"}:
        events.append(_event("thermal_risk_elevated", state=str(thermal.get("thermal_state") or "")))
    return events


def _event(event_type: str, *, state: str) -> dict[str, Any]:
    return {
        "event_type": event_type,
        "state": str(state or "").strip(),
        "deterministic": True,
    }
