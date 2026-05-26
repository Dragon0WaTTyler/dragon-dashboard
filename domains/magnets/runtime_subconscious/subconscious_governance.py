from __future__ import annotations

from typing import Any


def build_subconscious_governance(
    *,
    hidden_equilibrium_state: str = "",
    residue_pattern: str = "",
    dormant_resilience_state: str = "",
    cinematic_underflow_state: str = "",
    latent_pattern: str = "",
    pressure_score: int = 0,
) -> dict[str, Any]:
    actions: list[str] = []
    if hidden_equilibrium_state in {"hidden_fragmentation", "hidden_recovery"}:
        actions.append("preserve_hidden_equilibrium")
    if pressure_score >= 68:
        actions.append("stabilize_subconscious_pressure")
    if residue_pattern in {"fallback_residue", "degradation_residue", "cinematic_residue"}:
        actions.append("contain_orchestration_residue")
    if dormant_resilience_state in {"dormant_fragmented", "dormant_recovering"}:
        actions.append("preserve_dormant_resilience")
    if cinematic_underflow_state in {"cinematic_underflow_fragile", "cinematic_underflow_adaptive"}:
        actions.append("stabilize_cinematic_underflow")
    if latent_pattern == "latent_fragmentation":
        actions.append("suppress_latent_fragmentation")
    if not actions:
        actions.append("preserve_hidden_equilibrium")
    return {
        "governance_actions": actions,
        "governance_state": "deterministic_subconscious_governance",
    }
