from __future__ import annotations

from typing import Any, Mapping


def build_adaptation_profile(
    identity_memory_summary: Mapping[str, Any] | None,
    *,
    current_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    summary = dict(identity_memory_summary or {})
    current = dict(current_context or {})
    counts = dict(summary.get("adaptation_profiles") or {})
    adaptation_pressure = int(dict(current.get("coordination_metrics") or {}).get("adaptation_pressure", 0) or 0)
    runtime_resilience = int(dict(current.get("coordination_metrics") or {}).get("runtime_resilience", 0) or 0)
    fallback_probability = float(dict(current.get("execution_timeline") or {}).get("fallback_probability", 0) or 0.0)
    playback_runtime = str(current.get("playback_runtime") or "").strip()
    degradation_risk = int(dict(current.get("execution_metrics") or {}).get("degradation_risk", 0) or 0)
    profile = _dominant(counts, default="stable_adapter")
    if playback_runtime == "external_runtime" and degradation_risk >= 60:
        profile = "degraded_environment_specialist"
    elif adaptation_pressure >= 62:
        profile = "overcorrecting_adapter"
    elif fallback_probability >= 0.68:
        profile = "constrained_survivor"
    elif runtime_resilience >= 76:
        profile = "resilience_optimizer"
    score = max(0, min(100, 44 + runtime_resilience // 2 - adaptation_pressure // 4))
    return {
        "profile": profile,
        "profile_score": score,
        "stability_band": "stable" if score >= 70 else "adaptive" if score >= 46 else "fragile",
    }


def _dominant(counts: Mapping[str, Any], *, default: str) -> str:
    return max(
        ((str(key), int(value or 0)) for key, value in dict(counts or {}).items()),
        key=lambda item: (item[1], item[0]),
        default=(default, 0),
    )[0]
