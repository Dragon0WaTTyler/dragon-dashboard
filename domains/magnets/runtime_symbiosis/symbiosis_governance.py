from __future__ import annotations

from typing import Any


def build_symbiosis_governance(
    *,
    symbiotic_phase: str = "",
    dependency_state: str = "",
    recovery_mode: str = "",
    fragmentation: int = 0,
    pressure: int = 0,
) -> dict[str, Any]:
    actions: list[str] = []
    if fragmentation >= 60:
        actions.append("stabilize_runtime_isolation")
    if pressure >= 60:
        actions.append("reduce_coexistence_pressure")
    if "elevated" in str(dependency_state or ""):
        actions.append("rebalance_runtime_dependencies")
    if "shared" in str(recovery_mode or ""):
        actions.append("reinforce_shared_recovery")
    if not actions:
        actions.append("maintain_symbiotic_equilibrium")
    return {
        "governance_state": "symbiosis_governed",
        "governance_actions": actions,
        "governance_priority": "contain_fragmentation" if fragmentation >= 60 else "preserve_mutualism" if symbiotic_phase == "stable_mutualism" else "adaptive_balance",
    }
