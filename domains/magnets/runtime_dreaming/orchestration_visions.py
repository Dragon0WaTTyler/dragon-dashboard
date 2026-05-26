from __future__ import annotations

from typing import Any


def build_orchestration_visions(
    *,
    stabilization_state: str = "",
    fallback_state: str = "",
    continuity_state: str = "",
    cinematic_state: str = "",
    residue_pattern: str = "",
) -> dict[str, Any]:
    if "fragmented" in continuity_state or "recovery" in continuity_state:
        vision = "continuity_vision"
    elif fallback_state in {"fallback_aggressive", "fallback_recovery"} or residue_pattern == "fallback_residue":
        vision = "fallback_vision"
    elif "recover" in residue_pattern:
        vision = "recovery_vision"
    elif cinematic_state in {"cinematic_underflow_stable", "cinematic_underflow_resilient"}:
        vision = "cinematic_preservation_vision"
    else:
        vision = "stabilization_vision"
    return {"vision": vision, "vision_state": "deterministic_orchestration_vision"}
