from __future__ import annotations

from typing import Any, Mapping


def build_identity_metrics(
    *,
    continuity_state: Mapping[str, Any] | None = None,
    runtime_personality: Mapping[str, Any] | None = None,
    adaptation_profile: Mapping[str, Any] | None = None,
    identity_confidence: int = 0,
    identity_memory_summary: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    continuity = dict(continuity_state or {})
    personality = dict(runtime_personality or {})
    adaptation = dict(adaptation_profile or {})
    memory = dict(identity_memory_summary or {})
    return {
        "orchestration_consistency": int(continuity.get("runtime_consistency", 0) or 0),
        "runtime_personality_strength": int(personality.get("personality_strength", 0) or 0),
        "behavioral_stability": int(continuity.get("behavioral_stability", 0) or 0),
        "adaptation_coherence": int(adaptation.get("profile_score", 0) or 0),
        "continuity_integrity": int(continuity.get("continuity_confidence", 0) or 0),
        "orchestration_maturity": max(0, min(100, int(round(float(memory.get("average_maturity", 0) or 0.0) * 100)))),
        "runtime_identity_confidence": max(0, min(100, int(identity_confidence or 0))),
    }
