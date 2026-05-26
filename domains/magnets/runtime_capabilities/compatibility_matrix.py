from __future__ import annotations

from typing import Any, Mapping


def evaluate_runtime_compatibility(
    *,
    playback_runtime: str = "",
    runtime_profile: str = "",
    selected_source: Mapping[str, Any] | None = None,
    browser_capabilities: Mapping[str, Any] | None = None,
    device_profile: Mapping[str, Any] | None = None,
    network_profile: Mapping[str, Any] | None = None,
    resource_state: Mapping[str, Any] | None = None,
    runtime_affordances: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    source = dict(selected_source or {})
    browser = dict(browser_capabilities or {})
    device = dict(device_profile or {})
    network = dict(network_profile or {})
    resource = dict(resource_state or {})
    affordances = dict(runtime_affordances or {})
    runtime = str(playback_runtime or "").strip().lower()
    profile = str(runtime_profile or "").strip().lower()

    reasons: list[str] = []
    compatible = True
    conflicts: list[str] = []
    feasible_modes = ["external_runtime"]

    if str(browser.get("browser_feasibility") or "") != "rejected":
        feasible_modes.insert(0, "browser_runtime")
    else:
        compatible = runtime != "browser_runtime"
        conflicts.append("browser_runtime_rejected")
        reasons.append("Browser capability shaping rejects the browser runtime path.")

    if str(network.get("network_stability") or "") == "volatile" and runtime == "browser_runtime":
        conflicts.append("volatile_network_browser_conflict")
        reasons.append("Volatile network profile makes browser startup sustainability unlikely.")
    if str(resource.get("runtime_complexity_ceiling") or "") == "minimal" and "cinematic" in profile:
        conflicts.append("cinematic_resource_conflict")
        reasons.append("Runtime complexity ceiling is below cinematic orchestration requirements.")
    if str(device.get("profile") or "") in {"low_end_mobile", "constrained_runtime"} and not bool(source.get("mobile_friendly")):
        conflicts.append("mobile_viability_conflict")
        reasons.append("Current device profile is constrained while the selected source is not mobile friendly.")
    if str(affordances.get("subtitle_rendering_affordability") or "") == "constrained":
        reasons.append("Subtitle rendering is affordable only in constrained mode.")
    if str(browser.get("browser_codec_support") or "") == "degraded":
        reasons.append("Codec feasibility is degraded for deterministic browser execution.")
    if runtime == "browser_runtime" and conflicts:
        compatible = False

    if runtime == "external_runtime" and "browser_runtime" not in feasible_modes:
        reasons.append("External runtime remains the only feasible orchestration-safe mode.")
    elif runtime == "browser_runtime" and compatible:
        reasons.append("Browser runtime remains feasible under current deterministic compatibility shaping.")

    return {
        "compatible": compatible,
        "conflicts": conflicts,
        "reasoning": reasons,
        "runtime_mode_compatibility": "compatible" if compatible else "conflicted",
        "codec_feasibility": str(browser.get("browser_codec_support") or "unknown"),
        "bandwidth_compatibility": str(network.get("bandwidth_compatibility") or "unknown"),
        "browser_device_compatibility": str(browser.get("browser_feasibility") or "unknown"),
        "mobile_viability": "supported" if bool(source.get("mobile_friendly")) else "limited",
        "subtitle_compatibility": str(affordances.get("subtitle_rendering_affordability") or "unknown"),
        "fallback_compatibility": str(affordances.get("aggressive_fallback_affordability") or "unknown"),
        "feasible_runtime_modes": feasible_modes,
    }
