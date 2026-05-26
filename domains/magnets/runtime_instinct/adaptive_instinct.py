from __future__ import annotations

from typing import Any


def build_adaptive_instinct(
    *,
    stabilization_state: str = "",
    resilience_state: str = "",
    fallback_state: str = "",
    continuity_state: str = "",
    cinematic_state: str = "",
) -> dict[str, Any]:
    if fallback_state in {"fallback_recovery", "fallback_aggressive"} and continuity_state in {"continuity_recovering", "continuity_fragmented"}:
        evolution = "adaptive_recovery"
    elif resilience_state in {"resilience_preserving", "resilience_balanced"} and stabilization_state in {"resilient_stabilization", "strong_stabilization"}:
        evolution = "adaptive_resilience"
    elif stabilization_state in {"strong_stabilization", "adaptive_stabilization"}:
        evolution = "adaptive_stabilization"
    elif cinematic_state in {"cinematic_preserving", "cinematic_resilient"}:
        evolution = "adaptive_cinematic_preservation"
    else:
        evolution = "adaptive_balance"
    return {
        "evolution": evolution,
        "adaptive_state": "deterministic_orchestration_adaptation",
    }
