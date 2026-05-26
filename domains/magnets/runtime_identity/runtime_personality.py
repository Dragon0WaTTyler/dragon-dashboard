from __future__ import annotations

from typing import Any, Mapping


PERSONALITY_TRAITS = (
    "conservative",
    "cinematic",
    "resilience_first",
    "adaptive_balanced",
    "fallback_aggressive",
    "mobile_sensitive",
    "stability_focused",
    "confidence_cautious",
)


def build_runtime_personality(
    identity_memory_summary: Mapping[str, Any] | None,
    *,
    current_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    summary = dict(identity_memory_summary or {})
    context = dict(current_context or {})
    counts = dict(summary.get("trait_counts") or {})
    total = max(int(summary.get("total_observations", 0) or 0), 1)
    weighted: dict[str, int] = {}
    for trait in PERSONALITY_TRAITS:
        weighted[trait] = int(counts.get(trait, 0) or 0)
    runtime_profile = str(context.get("runtime_profile") or "").strip()
    playback_runtime = str(context.get("playback_runtime") or "").strip()
    startup_confidence = str(context.get("startup_confidence") or "").strip()
    source = dict(context.get("selected_source") or {})
    if "cinematic" in runtime_profile:
        weighted["cinematic"] += 2
    if playback_runtime == "external_runtime":
        weighted["resilience_first"] += 2
        weighted["conservative"] += 1
    if startup_confidence == "low":
        weighted["confidence_cautious"] += 2
        weighted["conservative"] += 1
    if not bool(source.get("mobile_friendly", True)):
        weighted["mobile_sensitive"] += 2
    ordered = sorted(weighted.items(), key=lambda item: (-item[1], item[0]))
    active = [name for name, score in ordered if score > 0][:4]
    primary = active[0] if active else "adaptive_balanced"
    strength = max(0, min(100, int(round((weighted.get(primary, 0) / total) * 100))))
    return {
        "primary_trait": primary,
        "traits": active or ["adaptive_balanced"],
        "trait_scores": weighted,
        "personality_strength": strength,
        "evolution_state": "stabilizing" if total >= 3 else "forming",
    }
