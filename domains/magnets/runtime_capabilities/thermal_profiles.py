from __future__ import annotations

from typing import Any, Mapping


def build_thermal_profile(
    *,
    device_profile: Mapping[str, Any] | None = None,
    resource_state: Mapping[str, Any] | None = None,
    runtime_profile: str = "",
    execution_metrics: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    device = dict(device_profile or {})
    resource = dict(resource_state or {})
    execution = dict(execution_metrics or {})
    profile = str(runtime_profile or "").strip().lower()
    device_name = str(device.get("profile") or "").strip().lower()
    pressure_score = int(resource.get("resource_pressure_score", 0) or 0)
    degradation = int(execution.get("degradation_risk", 0) or 0)

    thermal_state = "low_thermal_risk"
    if device_name in {"low_end_mobile", "constrained_runtime"} or pressure_score >= 58:
        thermal_state = "elevated_thermal_risk"
    if pressure_score >= 76 or degradation >= 72:
        thermal_state = "sustained_runtime_pressure"
    if "mobile" in device_name and ("cinematic" in profile or degradation >= 64):
        thermal_state = "mobile_heat_sensitive"

    thermal_score = 18
    thermal_score += 24 if "mobile" in device_name else 8
    thermal_score += pressure_score // 3
    thermal_score += 12 if "cinematic" in profile else 0
    thermal_score = max(0, min(100, thermal_score))

    return {
        "thermal_state": thermal_state,
        "thermal_pressure_score": thermal_score,
        "thermal_throttling_likelihood": round(min(1.0, thermal_score / 100), 4),
    }
