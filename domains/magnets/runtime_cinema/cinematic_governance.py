from __future__ import annotations

from typing import Any


def build_cinematic_governance(
    *,
    immersion_state: str = "",
    pacing: str = "",
    atmosphere: str = "",
    continuity: str = "",
    tension: str = "",
    balance_state: str = "",
) -> dict[str, Any]:
    actions: list[str] = []
    if immersion_state in {"degraded_immersion", "fragile_immersion"}:
        actions.append("preserve_immersion")
    if pacing in {"volatile_pacing", "recovery_pacing"} or tension == "escalating_tension":
        actions.append("stabilize_pacing")
        actions.append("suppress_volatility")
    if balance_state == "degraded_cinema" or atmosphere == "degraded_atmosphere":
        actions.append("contain_degradation")
    if continuity in {"fragmented_continuity", "unstable_continuity"}:
        actions.append("preserve_cinematic_continuity")
    if atmosphere in {"resilient_atmosphere", "adaptive_atmosphere"}:
        actions.append("maintain_resilience_atmosphere")
    if not actions:
        actions.append("preserve_immersion")
    return {
        "governance_actions": actions,
        "governance_state": "active_cinematic_governance",
    }
