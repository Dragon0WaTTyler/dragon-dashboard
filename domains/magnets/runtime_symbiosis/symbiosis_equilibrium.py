from __future__ import annotations

from typing import Any


def build_symbiosis_equilibrium(
    *,
    balance_score: int = 0,
    cooperation_score: int = 0,
    mutualism: int = 0,
    dependency_stress: int = 0,
    fragmentation: int = 0,
) -> dict[str, Any]:
    equilibrium_score = _clamp(
        int(
            round(
                (balance_score * 0.28)
                + (cooperation_score * 0.26)
                + (mutualism * 0.22)
                + ((100 - dependency_stress) * 0.12)
                + ((100 - fragmentation) * 0.12)
            )
        )
    )
    if fragmentation >= 68:
        equilibrium_state = "fractured_symbiotic_equilibrium"
    elif equilibrium_score >= 72:
        equilibrium_state = "stable_symbiotic_equilibrium"
    elif equilibrium_score >= 48:
        equilibrium_state = "adaptive_symbiotic_equilibrium"
    else:
        equilibrium_state = "unstable_symbiotic_equilibrium"
    return {
        "equilibrium_score": equilibrium_score,
        "equilibrium_state": equilibrium_state,
    }


def _clamp(value: int) -> int:
    return max(0, min(100, int(value or 0)))
