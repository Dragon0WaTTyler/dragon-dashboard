from __future__ import annotations

from typing import Any

from .subconscious_metrics import clamp


def build_cinematic_underflow(
    *,
    cinematic_quality: int = 0,
    cinematic_direction: str = "",
    underflow_state: str = "",
    cinematic_instinct: str = "",
    residue_pattern: str = "",
) -> dict[str, Any]:
    integrity = clamp(
        (cinematic_quality * 0.46)
        + (16 if "cinematic" in cinematic_direction else 0)
        + (12 if underflow_state in {"calm_underflow", "resilient_underflow"} else 0)
        + (12 if cinematic_instinct in {"cinematic_preserving", "cinematic_resilient"} else 0)
        + (0 if residue_pattern == "cinematic_residue" else 8)
    )
    if residue_pattern == "cinematic_residue" or "constrained" in cinematic_direction:
        state = "cinematic_underflow_fragile"
    elif underflow_state == "adaptive_underflow":
        state = "cinematic_underflow_adaptive"
    elif integrity >= 82:
        state = "cinematic_underflow_stable"
    elif integrity >= 72:
        state = "cinematic_underflow_resilient"
    else:
        state = "cinematic_underflow_balanced"
    return {
        "state": state,
        "cinematic_underflow_integrity": integrity,
    }
