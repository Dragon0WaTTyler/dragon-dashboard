from __future__ import annotations

from typing import Any

from .subconscious_metrics import clamp


def build_subconscious_pressure(
    *,
    pressure_score: int = 0,
    degradation_risk: int = 0,
    continuity_confidence: int = 0,
    cinematic_quality: int = 0,
) -> dict[str, Any]:
    return {
        "dormant_pressure": clamp((pressure_score * 0.42) + ((100 - continuity_confidence) * 0.16)),
        "latent_pressure": clamp((pressure_score * 0.5) + (degradation_risk * 0.22)),
        "continuity_pressure": clamp(((100 - continuity_confidence) * 0.72) + (pressure_score * 0.12)),
        "degradation_pressure": clamp((degradation_risk * 0.68) + (pressure_score * 0.14)),
        "cinematic_pressure": clamp(((100 - cinematic_quality) * 0.66) + (pressure_score * 0.12)),
    }
