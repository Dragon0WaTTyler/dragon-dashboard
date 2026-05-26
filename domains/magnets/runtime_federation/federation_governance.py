from __future__ import annotations

from typing import Any


def build_federation_governance(
    *,
    pressure: int,
    divergence: int,
    restrictive_authority: bool,
    degraded_ecosystem: bool,
    unstable_instinct: bool,
    focused_consciousness: bool,
    optimistic_dreaming: bool,
) -> dict[str, Any]:
    actions: list[str] = []
    if unstable_instinct:
        actions.append("stabilize_instinct_disruption")
    if restrictive_authority:
        actions.append("respect_authority_constraints")
    if degraded_ecosystem:
        actions.append("compensate_for_ecosystem_degradation")
    if divergence >= 52:
        actions.append("suppress_cross_layer_divergence")
    if pressure >= 58:
        actions.append("contain_federation_pressure")
    if focused_consciousness and optimistic_dreaming:
        actions.append("channel_focus_into_recovery")
    if not actions:
        actions.append("maintain_federation_continuity")

    governance_state = "stability_weighted" if pressure >= 58 or divergence >= 52 else "continuity_weighted"
    return {
        "governance_state": governance_state,
        "governance_actions": actions,
        "pressure_policy": "guarded" if pressure >= 58 else "adaptive",
    }
