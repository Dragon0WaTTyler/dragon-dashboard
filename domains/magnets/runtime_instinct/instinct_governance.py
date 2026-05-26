from __future__ import annotations

from typing import Any


def build_instinct_governance(
    *,
    stabilization_state: str = "",
    fallback_state: str = "",
    continuity_state: str = "",
    cinematic_state: str = "",
    resilience_state: str = "",
    survival_state: str = "",
    equilibrium_state: str = "",
) -> dict[str, Any]:
    actions: list[str] = []
    if continuity_state in {"continuity_fragmented", "continuity_recovering"}:
        actions.append("preserve_continuity_instinct")
    if stabilization_state in {"degraded_stabilization", "fragmented_stabilization"}:
        actions.append("stabilize_orchestration_reflexes")
    if fallback_state in {"fallback_aggressive", "fallback_recovery"}:
        actions.append("suppress_degradation_reflexes")
    if cinematic_state in {"cinematic_constrained", "cinematic_recovering"}:
        actions.append("preserve_cinematic_instinct")
    if resilience_state in {"resilience_fragile", "resilience_recovering"}:
        actions.append("strengthen_resilience_instinct")
    if survival_state in {"survival_fragile", "survival_recovering"} or equilibrium_state in {"equilibrium_fragmented", "equilibrium_recovering"}:
        actions.append("stabilize_survival_equilibrium")
    if not actions:
        actions.append("preserve_continuity_instinct")
    return {
        "governance_actions": actions,
        "governance_state": "deterministic_instinct_governance",
    }
