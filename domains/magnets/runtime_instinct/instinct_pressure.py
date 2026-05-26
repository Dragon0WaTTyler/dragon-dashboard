from __future__ import annotations

from typing import Any

from .instinct_metrics import clamp


def build_instinct_pressure(
    *,
    pressure_score: int = 0,
    degradation_risk: int = 0,
    fallback_intensity: int = 0,
    continuity_preservation: int = 0,
    cinematic_preservation: int = 0,
) -> dict[str, Any]:
    return {
        "stabilization_pressure": clamp((pressure_score * 0.5) + (degradation_risk * 0.3)),
        "degradation_pressure": clamp((degradation_risk * 0.64) + (fallback_intensity * 0.16)),
        "continuity_pressure": clamp(((100 - continuity_preservation) * 0.72) + (pressure_score * 0.12)),
        "fallback_pressure": clamp((fallback_intensity * 0.7) + (degradation_risk * 0.14)),
        "cinematic_pressure": clamp(((100 - cinematic_preservation) * 0.68) + (fallback_intensity * 0.12)),
    }
