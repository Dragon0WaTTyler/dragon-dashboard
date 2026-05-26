from __future__ import annotations

from typing import Any


def clamp(value: int | float, low: int = 0, high: int = 100) -> int:
    return max(low, min(high, int(round(value))))


def build_subconscious_metrics(
    *,
    latent_stability: int = 0,
    hidden_equilibrium_strength: int = 0,
    dormant_resilience_strength: int = 0,
    orchestration_residue_density: int = 0,
    subconscious_balance: int = 0,
    cinematic_underflow_integrity: int = 0,
    orchestration_echo_strength: int = 0,
) -> dict[str, Any]:
    subconscious_integrity = clamp(
        (latent_stability * 0.2)
        + (hidden_equilibrium_strength * 0.18)
        + (dormant_resilience_strength * 0.16)
        + ((100 - orchestration_residue_density) * 0.12)
        + (subconscious_balance * 0.14)
        + (cinematic_underflow_integrity * 0.12)
        + (orchestration_echo_strength * 0.08)
    )
    return {
        "subconscious_integrity": subconscious_integrity,
        "latent_stability": clamp(latent_stability),
        "hidden_equilibrium_strength": clamp(hidden_equilibrium_strength),
        "dormant_resilience_strength": clamp(dormant_resilience_strength),
        "orchestration_residue_density": clamp(orchestration_residue_density),
        "subconscious_balance": clamp(subconscious_balance),
        "cinematic_underflow_integrity": clamp(cinematic_underflow_integrity),
        "orchestration_echo_strength": clamp(orchestration_echo_strength),
    }
