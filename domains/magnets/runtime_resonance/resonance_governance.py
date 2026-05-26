from __future__ import annotations


def build_resonance_governance(
    *,
    resonance_phase: str,
    sync_state: str,
    equilibrium_state: str,
    resonance_fragmentation: int,
    sync_drift: int,
    resonance_pressure: int,
) -> dict[str, object]:
    actions: list[str] = []
    if resonance_fragmentation >= 52:
        actions.append("contain_resonance_fragmentation")
    if sync_drift >= 46:
        actions.append("stabilize_sync_drift")
    if resonance_pressure >= 58:
        actions.append("reduce_equilibrium_pressure")
    if "unstable" in str(equilibrium_state or ""):
        actions.append("rebuild_harmonic_equilibrium")
    if not actions:
        actions.append("preserve_harmonic_alignment")
    return {
        "governance_state": "active_resonance_stabilization" if len(actions) > 1 else "passive_resonance_preservation",
        "governance_actions": actions,
        "resonance_phase": str(resonance_phase or "measured_resonance"),
        "sync_state": str(sync_state or "synchronized"),
    }
