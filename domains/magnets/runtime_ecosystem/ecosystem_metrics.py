from __future__ import annotations

from typing import Any


def build_ecosystem_metrics(
    *,
    balance_score: int = 0,
    pressure_score: int = 0,
    resilience_distribution: int = 0,
    propagation_risk: int = 0,
    equilibrium_strength: int = 0,
    climate_stability: int = 0,
) -> dict[str, Any]:
    ecosystem_stability = max(0, min(100, int(round((balance_score + equilibrium_strength + climate_stability) / 3))))
    ecosystem_integrity = max(0, min(100, int(round((ecosystem_stability + resilience_distribution + (100 - propagation_risk)) / 3))))
    orchestration_harmony = max(0, min(100, int(round((ecosystem_stability + resilience_distribution + (100 - pressure_score)) / 3))))
    return {
        "ecosystem_stability": ecosystem_stability,
        "orchestration_pressure_score": max(0, min(100, pressure_score)),
        "resilience_distribution": max(0, min(100, resilience_distribution)),
        "degradation_risk": max(0, min(100, propagation_risk)),
        "equilibrium_strength": max(0, min(100, equilibrium_strength)),
        "climate_stability": max(0, min(100, climate_stability)),
        "ecosystem_integrity": ecosystem_integrity,
        "orchestration_harmony": orchestration_harmony,
    }
