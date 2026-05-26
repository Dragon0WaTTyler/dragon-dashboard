from __future__ import annotations

from typing import Any

from .cinematic_profiles import cinematic_profile_bias, clamp, normalized_quality_weight


def build_runtime_aesthetics(
    *,
    quality_label: str = "",
    runtime_profile: str = "",
    stability_score: int = 0,
    continuity_strength: int = 0,
    atmosphere_integrity: int = 0,
    degradation_risk: int = 0,
) -> dict[str, Any]:
    polish = clamp(
        (normalized_quality_weight(quality_label) * 0.22)
        + (stability_score * 0.26)
        + (continuity_strength * 0.24)
        + (atmosphere_integrity * 0.22)
        + cinematic_profile_bias(runtime_profile)
        - (degradation_risk * 0.16)
    )
    if degradation_risk >= 76:
        state = "degraded_runtime"
    elif polish >= 82:
        state = "polished_runtime"
    elif continuity_strength >= 68:
        state = "cinematic_runtime"
    elif stability_score >= 64:
        state = "resilient_runtime"
    else:
        state = "constrained_runtime"
    return {
        "aesthetic_state": state,
        "runtime_polish": polish,
    }
