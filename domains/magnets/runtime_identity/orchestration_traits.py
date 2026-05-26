from __future__ import annotations

from typing import Any, Mapping


ORCHESTRATION_TRAITS = (
    "prefers_safe_runtime",
    "prefers_high_quality",
    "downgrade_sensitive",
    "recovery_resistant",
    "escalation_prone",
    "volatility_sensitive",
    "fallback_tolerant",
)


def build_orchestration_traits(
    identity_memory_summary: Mapping[str, Any] | None,
    *,
    current_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    summary = dict(identity_memory_summary or {})
    counts = dict(summary.get("orchestration_traits") or {})
    total = max(int(summary.get("total_observations", 0) or 0), 1)
    scores = {name: int(counts.get(name, 0) or 0) for name in ORCHESTRATION_TRAITS}
    ordered = sorted(scores.items(), key=lambda item: (-item[1], item[0]))
    active = [name for name, score in ordered if score > 0][:4]
    coherence = max(0, min(100, int(round(sum(score for _, score in ordered[:3]) * 100 / max(total * 3, 1)))))
    return {
        "traits": active or ["prefers_safe_runtime"],
        "trait_scores": scores,
        "trait_coherence": coherence,
    }
