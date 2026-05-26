from __future__ import annotations

from typing import Any

from .subconscious_metrics import clamp


def build_orchestration_echoes(
    *,
    stabilization_state: str = "",
    resilience_state: str = "",
    fallback_state: str = "",
    continuity_state: str = "",
    cinematic_state: str = "",
) -> dict[str, Any]:
    echo_strength = clamp(
        (22 if stabilization_state in {"strong_stabilization", "resilient_stabilization"} else 10)
        + (18 if resilience_state in {"resilience_preserving", "resilience_balanced"} else 8)
        + (16 if fallback_state in {"fallback_aggressive", "fallback_recovery"} else 6)
        + (16 if "fragmented" in continuity_state or "recovering" in continuity_state else 8)
        + (18 if cinematic_state in {"cinematic_preserving", "cinematic_resilient"} else 8)
    )
    return {
        "stabilization_echoes": "stabilization_echoes",
        "resilience_echoes": "resilience_echoes",
        "fallback_echoes": "fallback_echoes",
        "continuity_echoes": "continuity_echoes",
        "cinematic_echoes": "cinematic_echoes",
        "orchestration_echo_strength": echo_strength,
    }
