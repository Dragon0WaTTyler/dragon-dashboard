from __future__ import annotations

from typing import Any


def build_dream_governance(
    *,
    cinematic_dream: str = "",
    latent_projection: str = "",
    dormant_pathway: str = "",
    adaptive_dreaming: str = "",
    continuity_dream: str = "",
    resilience_dream: str = "",
) -> dict[str, Any]:
    actions: list[str] = []
    if cinematic_dream in {"immersive_dream", "resilient_cinema_dream", "stabilized_cinema_dream"}:
        actions.append("preserve_cinematic_convergence")
    if latent_projection == "latent_fragmentation_projection":
        actions.append("suppress_fragmentation_drift")
    if dormant_pathway in {"dormant_recovery_path", "dormant_resilience_path", "dormant_adaptation_path", "dormant_cinematic_path"}:
        actions.append("stabilize_dormant_pathways")
    if adaptive_dreaming in {"adaptive_equilibrium", "adaptive_resilience"}:
        actions.append("preserve_adaptive_equilibrium")
    if continuity_dream in {"continuity_preservation", "continuity_balance", "continuity_recovery", "continuity_fragmentation"}:
        actions.append("maintain_continuity_projection")
    if resilience_dream in {"dormant_resilience_growth", "dormant_resilience_balance"}:
        actions.append("preserve_resilience_convergence")
    if not actions:
        actions.append("preserve_adaptive_equilibrium")
    return {"governance_actions": actions, "governance_state": "deterministic_dream_governance"}
