from __future__ import annotations

from typing import Any, Mapping


def build_device_profile(
    *,
    selected_source: Mapping[str, Any] | None = None,
    runtime_profile: str = "",
    authority_memory_summary: Mapping[str, Any] | None = None,
    execution_metrics: Mapping[str, Any] | None = None,
    network_profile: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    source = dict(selected_source or {})
    authority_memory = dict(authority_memory_summary or {})
    execution = dict(execution_metrics or {})
    network = dict(network_profile or {})
    resolution = str(source.get("quality_label") or source.get("resolution") or "unknown").strip().lower()
    profile = str(runtime_profile or "").strip().lower()
    mobile_friendly = bool(source.get("mobile_friendly"))
    instability = float(authority_memory.get("blocked_runtime_frequency", 0) or 0)
    degradation = int(execution.get("degradation_risk", 0) or 0)
    fallback_pressure = float(network.get("fallback_sensitivity", 0) or 0)

    profile_id = "desktop_balanced"
    if not mobile_friendly and resolution in {"2160p", "4k"}:
        profile_id = "desktop_high_end"
    if mobile_friendly and resolution in {"720p", "1080p"} and "balanced" in profile:
        profile_id = "balanced_mobile"
    if mobile_friendly and (degradation >= 58 or fallback_pressure >= 0.55):
        profile_id = "constrained_runtime"
    if mobile_friendly and resolution in {"2160p", "4k"}:
        profile_id = "low_end_mobile"
    if instability >= 0.35 or degradation >= 75:
        profile_id = "unstable_environment"

    execution_stability = "stable"
    if degradation >= 70 or instability >= 0.4:
        execution_stability = "fragile"
    elif degradation >= 48:
        execution_stability = "guarded"

    return {
        "profile": profile_id,
        "resolution_class": resolution or "unknown",
        "runtime_mode_family": "browser" if "browser" in profile else "external",
        "execution_stability": execution_stability,
        "fallback_history_pressure": round(instability, 4),
        "authority_memory_pressure": round(float(authority_memory.get("fallback_loop_frequency", 0) or 0), 4),
        "environment_label": profile_id.replace("_", " "),
    }
