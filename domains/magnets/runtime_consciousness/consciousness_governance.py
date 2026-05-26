from __future__ import annotations


def build_consciousness_governance(
    *,
    continuity_awareness: str,
    cognitive_balance: str,
    focus: str,
    runtime_presence: str,
    perception_state: str,
) -> dict[str, list[str] | str]:
    actions: list[str] = []
    if continuity_awareness in {"fragmented_awareness", "unstable_awareness"}:
        actions.append("preserve_continuity_awareness")
    if cognitive_balance in {"pressured_cognition", "fragmented_cognition"}:
        actions.append("stabilize_cognition")
    if cognitive_balance == "fragmented_cognition" or perception_state == "fragmented_perception":
        actions.append("suppress_fragmentation")
    if focus != "equilibrium_focus":
        actions.append("maintain_orchestration_focus")
    if runtime_presence == "degraded_presence":
        actions.append("preserve_runtime_presence")
    if perception_state != "stable_perception":
        actions.append("stabilize_perception")
    if not actions:
        actions.append("preserve_continuity_awareness")
    return {
        "governance_state": "deterministic_consciousness_governance",
        "governance_actions": actions,
    }
