from __future__ import annotations


def build_temporal_governance(
    *,
    runtime_cycle_phase: str,
    continuity_decay_rate: int,
    adaptive_recovery_velocity: str,
    runtime_rhythm_state: str,
    cinematic_temporal_flow: str,
) -> dict[str, object]:
    actions: list[str] = []
    if continuity_decay_rate >= 56:
        actions.append("stabilize_continuity_decay")
    if adaptive_recovery_velocity in {"weak", "guarded"}:
        actions.append("reinforce_recovery_velocity")
    if "compressed" in str(runtime_rhythm_state or ""):
        actions.append("rebalance_temporal_rhythm")
    if "fractured" in str(cinematic_temporal_flow or "") or "unstable" in str(cinematic_temporal_flow or ""):
        actions.append("smooth_cinematic_temporal_flow")
    if "transition" in str(runtime_cycle_phase or ""):
        actions.append("anchor_phase_transition")
    if not actions:
        actions.append("preserve_temporal_continuity")
    return {
        "state": "temporal_governance_active",
        "governance_actions": actions,
    }
