from __future__ import annotations

from typing import Any

from .cinematic_profiles import cinematic_profile_bias, clamp


def build_runtime_pacing(
    *,
    stability_score: int = 0,
    adaptation_pressure: int = 0,
    fallback_probability: float = 0.0,
    runtime_profile: str = "",
) -> dict[str, Any]:
    volatility = clamp((adaptation_pressure * 0.55) + (fallback_probability * 100 * 0.35) + (100 - stability_score) * 0.25)
    stability = clamp(stability_score + cinematic_profile_bias(runtime_profile) - int(fallback_probability * 18))
    if stability >= 78 and volatility <= 34:
        pacing = "smooth_pacing"
    elif adaptation_pressure >= 68:
        pacing = "adaptive_pacing"
    elif fallback_probability >= 0.64:
        pacing = "recovery_pacing"
    elif stability_score <= 42:
        pacing = "volatile_pacing"
    elif "cinematic" in str(runtime_profile or ""):
        pacing = "cinematic_pacing"
    else:
        pacing = "constrained_pacing"
    return {
        "pacing": pacing,
        "pacing_stability": stability,
        "volatility_index": volatility,
    }
