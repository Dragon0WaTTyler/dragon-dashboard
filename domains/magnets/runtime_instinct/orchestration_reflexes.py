from __future__ import annotations

from typing import Any


def build_orchestration_reflexes(
    *,
    stabilization_state: str = "",
    fallback_state: str = "",
    resilience_state: str = "",
    continuity_state: str = "",
    cinematic_state: str = "",
) -> dict[str, Any]:
    return {
        "stabilization_reflex": "stabilize_orchestration" if "degraded" in stabilization_state or "fragmented" in stabilization_state else "maintain_stability",
        "fallback_reflex": "escalate_fallback" if fallback_state in {"fallback_aggressive", "fallback_recovery"} else "contain_fallback",
        "resilience_reflex": "strengthen_resilience" if resilience_state in {"resilience_fragile", "resilience_recovering"} else "preserve_resilience",
        "continuity_reflex": "restore_continuity" if continuity_state in {"continuity_fragmented", "continuity_recovering"} else "preserve_continuity",
        "cinematic_reflex": "protect_cinematic_continuity" if cinematic_state in {"cinematic_constrained", "cinematic_recovering"} else "preserve_cinematic_quality",
    }
