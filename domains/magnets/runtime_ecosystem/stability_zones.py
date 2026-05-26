from __future__ import annotations

from typing import Any


def build_stability_zone(
    *,
    playback_runtime: str = "",
    stability_score: int = 0,
    degradation_risk: int = 0,
    pressure_score: int = 0,
    runtime_resilience: int = 0,
) -> dict[str, Any]:
    if degradation_risk >= 72 or pressure_score >= 76:
        zone = "degraded_zone"
    elif pressure_score >= 62:
        zone = "volatility_zone"
    elif playback_runtime == "browser_runtime" and stability_score >= 72:
        zone = "cinematic_zone"
    elif runtime_resilience >= 70 and degradation_risk <= 46:
        zone = "stable_zone"
    elif runtime_resilience >= 60:
        zone = "recovery_zone"
    else:
        zone = "constrained_zone"
    return {
        "zone": zone,
        "zone_confidence": _zone_confidence(zone, stability_score, pressure_score),
    }


def _zone_confidence(zone: str, stability_score: int, pressure_score: int) -> int:
    base = 52
    if zone in {"stable_zone", "cinematic_zone"}:
        base += stability_score // 3
    else:
        base += pressure_score // 4
    return max(0, min(100, base))
