from __future__ import annotations

from typing import Any, Mapping


def build_resource_limits(
    *,
    selected_source: Mapping[str, Any] | None = None,
    runtime_profile: str = "",
    browser_capabilities: Mapping[str, Any] | None = None,
    execution_metrics: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    source = dict(selected_source or {})
    browser = dict(browser_capabilities or {})
    execution = dict(execution_metrics or {})
    profile = str(runtime_profile or "").strip().lower()
    size_gb = float(source.get("size_gb", 0) or 0.0)
    degradation = int(execution.get("degradation_risk", 0) or 0)
    memory_sensitivity = str(browser.get("memory_sensitivity") or "balanced").strip().lower()

    memory_ceiling = "balanced"
    runtime_complexity_ceiling = "standard"
    fallback_depth_ceiling = 2
    subtitle_overhead_limit = "standard"
    orchestration_saturation_risk = "low"

    if size_gb >= 20 or memory_sensitivity == "high" or "cinematic" in profile:
        memory_ceiling = "constrained"
        runtime_complexity_ceiling = "reduced"
        subtitle_overhead_limit = "tight"
    if degradation >= 60:
        fallback_depth_ceiling = 1
        orchestration_saturation_risk = "elevated"
    if degradation >= 78:
        runtime_complexity_ceiling = "minimal"
        orchestration_saturation_risk = "high"

    pressure_score = 24
    pressure_score += 26 if memory_ceiling == "constrained" else 8
    pressure_score += 22 if orchestration_saturation_risk == "high" else 10 if orchestration_saturation_risk == "elevated" else 0
    pressure_score += min(28, max(0, degradation // 3))
    pressure_score = max(0, min(100, pressure_score))

    return {
        "memory_ceiling": memory_ceiling,
        "runtime_complexity_ceiling": runtime_complexity_ceiling,
        "fallback_depth_ceiling": fallback_depth_ceiling,
        "subtitle_overhead_limit": subtitle_overhead_limit,
        "orchestration_saturation_risk": orchestration_saturation_risk,
        "resource_pressure_score": pressure_score,
    }
