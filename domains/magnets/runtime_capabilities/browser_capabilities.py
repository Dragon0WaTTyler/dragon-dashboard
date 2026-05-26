from __future__ import annotations

from typing import Any, Mapping


def build_browser_capabilities(
    *,
    selected_source: Mapping[str, Any] | None = None,
    capability_snapshot: Mapping[str, Any] | None = None,
    playback_runtime: str = "",
    runtime_profile: str = "",
    startup_confidence: str = "",
) -> dict[str, Any]:
    source = dict(selected_source or {})
    capability = dict(capability_snapshot or {})
    profile = str(runtime_profile or "").strip().lower()
    runtime = str(playback_runtime or "").strip().lower()
    codec = str(source.get("codec") or source.get("video_codec") or "").strip().lower()
    resolution = str(source.get("quality_label") or source.get("resolution") or "").strip().lower()
    source_type = str(source.get("source_type") or source.get("release_type") or "").strip().lower()
    browser_risk = str(capability.get("browser_risk") or "low").strip().lower()
    safety_class = str(capability.get("browser_safety_class") or "safe").strip().lower()
    mobile_risk = str(capability.get("mobile_runtime_risk") or "low").strip().lower()

    codec_support = "supported"
    transport_support = "stable"
    mobile_limits = "desktop_safe"
    autoplay = "requires_user_gesture"
    memory_sensitivity = "balanced"
    iframe_constraints = "standard"
    chromium_stability = "stable"
    firefox_degradation = "low"
    mobile_safari_risk = "low"
    warnings: list[str] = []

    if codec in {"x265", "hevc", "av1"}:
        codec_support = "degraded"
        transport_support = "fragile"
        warnings.append("browser_codec_fragility")
    if safety_class == "unsafe" or browser_risk == "high":
        transport_support = "restricted"
        chromium_stability = "degraded"
        firefox_degradation = "elevated"
        warnings.append("browser_runtime_restricted")
    if resolution in {"2160p", "4k"} or source_type in {"remux", "bluray"}:
        memory_sensitivity = "high"
        iframe_constraints = "high_pressure"
        warnings.append("high_density_playback_pressure")
    if "cinematic" in profile:
        memory_sensitivity = "high" if memory_sensitivity != "high" else memory_sensitivity
        autoplay = "fragile"
    if mobile_risk == "high":
        mobile_limits = "mobile_constrained"
        mobile_safari_risk = "high"
        firefox_degradation = "medium"
        warnings.append("mobile_browser_constraints")
    elif mobile_risk == "medium":
        mobile_limits = "mobile_balanced"
        mobile_safari_risk = "medium"
    if str(startup_confidence or "").strip().lower() == "low":
        chromium_stability = "guarded" if chromium_stability == "stable" else chromium_stability
        warnings.append("low_confidence_browser_boot")
    if runtime == "external_runtime":
        autoplay = "not_applicable"
        iframe_constraints = "not_applicable"

    viability_score = 78
    viability_score -= 26 if safety_class == "unsafe" else 0
    viability_score -= 18 if browser_risk == "high" else 8 if browser_risk == "medium" else 0
    viability_score -= 14 if codec_support == "degraded" else 0
    viability_score -= 12 if memory_sensitivity == "high" else 0
    viability_score -= 10 if mobile_safari_risk == "high" else 4 if mobile_safari_risk == "medium" else 0
    viability_score += 4 if runtime == "browser_runtime" and str(startup_confidence or "").strip().lower() == "high" else 0
    viability_score = max(0, min(100, viability_score))

    feasibility = "viable"
    if viability_score < 28:
        feasibility = "rejected"
    elif viability_score < 45:
        feasibility = "fragile"
    elif viability_score < 62:
        feasibility = "guarded"

    return {
        "browser_codec_support": codec_support,
        "mobile_browser_limits": mobile_limits,
        "autoplay_restrictions": autoplay,
        "memory_sensitivity": memory_sensitivity,
        "transport_support": transport_support,
        "iframe_constraints": iframe_constraints,
        "mobile_safari_risk": mobile_safari_risk,
        "chromium_stability": chromium_stability,
        "firefox_degradation": firefox_degradation,
        "browser_feasibility": feasibility,
        "browser_feasibility_score": viability_score,
        "warnings": warnings,
    }
