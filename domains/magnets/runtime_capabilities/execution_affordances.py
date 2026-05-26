from __future__ import annotations

from typing import Any, Mapping


def build_execution_affordances(
    *,
    selected_source: Mapping[str, Any] | None = None,
    runtime_profile: str = "",
    device_profile: Mapping[str, Any] | None = None,
    network_profile: Mapping[str, Any] | None = None,
    resource_state: Mapping[str, Any] | None = None,
    thermal_profile: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    source = dict(selected_source or {})
    device = dict(device_profile or {})
    network = dict(network_profile or {})
    resource = dict(resource_state or {})
    thermal = dict(thermal_profile or {})
    profile = str(runtime_profile or "").strip().lower()
    mobile_friendly = bool(source.get("mobile_friendly"))

    subtitle = "comfortable"
    cinematic = "comfortable"
    aggressive_fallback = "allowed"
    browser_runtime = "sustainable"
    mobile_constraints = "desktop_safe"
    orchestration_overhead = "low"

    if str(resource.get("subtitle_overhead_limit") or "") == "tight":
        subtitle = "constrained"
    if "cinematic" in profile or str(device.get("profile") or "") in {"low_end_mobile", "constrained_runtime"}:
        cinematic = "constrained"
    if float(network.get("fallback_sensitivity", 0) or 0) >= 0.6:
        aggressive_fallback = "discouraged"
    if str(thermal.get("thermal_state") or "") in {"sustained_runtime_pressure", "mobile_heat_sensitive"}:
        browser_runtime = "fragile"
    if mobile_friendly:
        mobile_constraints = "mobile_safe"
    elif "mobile" in str(device.get("profile") or ""):
        mobile_constraints = "mobile_limited"
    if int(resource.get("resource_pressure_score", 0) or 0) >= 66:
        orchestration_overhead = "elevated"

    return {
        "subtitle_rendering_affordability": subtitle,
        "cinematic_runtime_affordability": cinematic,
        "aggressive_fallback_affordability": aggressive_fallback,
        "browser_runtime_sustainability": browser_runtime,
        "mobile_safe_constraints": mobile_constraints,
        "orchestration_overhead": orchestration_overhead,
    }
