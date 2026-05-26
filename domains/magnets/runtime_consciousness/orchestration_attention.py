from __future__ import annotations


def build_orchestration_attention(
    *,
    degradation_risk: int,
    continuity_confidence: int,
    runtime_resilience: int,
    cinematic_quality: int,
    balance_state: str,
) -> dict[str, int | str | dict[str, int]]:
    weights = {
        "resilience_attention": max(0, min(100, runtime_resilience)),
        "degradation_attention": max(0, min(100, degradation_risk)),
        "continuity_attention": max(0, min(100, 100 - continuity_confidence if continuity_confidence >= 0 else 0)),
        "cinematic_attention": max(0, min(100, cinematic_quality)),
        "equilibrium_attention": 82 if "balanced" in balance_state or "stable" in balance_state else 58,
        "fallback_attention": max(0, min(100, degradation_risk + max(0, 60 - runtime_resilience))),
    }
    dominant = max(weights, key=weights.get) if weights else "equilibrium_attention"
    return {
        "attention_state": dominant,
        "attention_distribution": weights,
        "dominant_attention": dominant,
    }
