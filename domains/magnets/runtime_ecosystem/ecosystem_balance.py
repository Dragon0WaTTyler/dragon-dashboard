from __future__ import annotations

from typing import Any


def build_ecosystem_balance(
    *,
    stability_score: int = 0,
    degradation_risk: int = 0,
    runtime_resilience: int = 0,
    fallback_pressure: int = 0,
    adaptation_pressure: int = 0,
    cluster_alignment: str = "",
) -> dict[str, Any]:
    if degradation_risk >= 72 or fallback_pressure >= 72:
        state = "degradation_heavy"
    elif fallback_pressure >= 60:
        state = "fallback_dominant"
    elif adaptation_pressure >= 62 and cluster_alignment == "fragmented":
        state = "adaptation_fragmented"
    elif runtime_resilience >= 72 and stability_score >= 68:
        state = "resilience_stable"
    elif stability_score >= 60 and degradation_risk <= 42:
        state = "balanced"
    else:
        state = "overloaded"
    score = max(
        0,
        min(
            100,
            int(round((stability_score + runtime_resilience + (100 - degradation_risk) + (100 - fallback_pressure)) / 4)),
        ),
    )
    return {
        "balance_state": state,
        "balance_score": score,
        "balance_bias": _balance_bias(state),
    }


def _balance_bias(state: str) -> str:
    if state in {"balanced", "resilience_stable"}:
        return "stability_bias"
    if state == "fallback_dominant":
        return "fallback_bias"
    if state == "adaptation_fragmented":
        return "fragmentation_bias"
    return "degradation_bias"
