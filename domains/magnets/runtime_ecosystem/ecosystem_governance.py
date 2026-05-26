from __future__ import annotations

from typing import Any


def build_ecosystem_governance(
    *,
    balance_state: str = "",
    pressure_score: int = 0,
    degradation_current: str = "",
    climate: str = "",
    playback_runtime: str = "",
) -> dict[str, Any]:
    actions: list[str] = []
    if degradation_current in {"cascading_degradation", "fallback_propagation"}:
        actions.append("degradation_suppression")
        actions.append("fallback_containment")
    if pressure_score >= 66:
        actions.append("volatility_mitigation")
    if playback_runtime == "browser_runtime" and balance_state in {"balanced", "resilience_stable"}:
        actions.append("cinematic_preservation")
    if climate in {"degraded_climate", "volatile_climate"}:
        actions.append("stabilization_bias")
    if balance_state == "adaptation_fragmented":
        actions.append("equilibrium_recovery")
    if not actions:
        actions.append("stabilization_bias")
    return {
        "governance_state": actions[0],
        "governance_actions": actions,
    }
