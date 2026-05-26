from __future__ import annotations

from typing import Any, Mapping


def build_orchestration_archetype(
    identity_memory_summary: Mapping[str, Any] | None,
    *,
    runtime_personality: Mapping[str, Any] | None = None,
    adaptation_profile: Mapping[str, Any] | None = None,
    behavioral_drift: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    summary = dict(identity_memory_summary or {})
    personality = dict(runtime_personality or {})
    adaptation = dict(adaptation_profile or {})
    drift = dict(behavioral_drift or {})
    counts = dict(summary.get("archetype_counts") or {})
    archetype = max(
        ((str(key), int(value or 0)) for key, value in counts.items()),
        key=lambda item: (item[1], item[0]),
        default=("cautious_stabilizer", 0),
    )[0]
    traits = set(personality.get("traits") or [])
    if "cinematic" in traits:
        archetype = "cinematic_orchestrator"
    elif str(adaptation.get("profile") or "") == "constrained_survivor":
        archetype = "adaptive_survivor"
    elif str(drift.get("drift_state") or "") == "growing_runtime_conservatism":
        archetype = "cautious_stabilizer"
    return {
        "archetype": archetype,
        "archetype_strength": max(0, min(100, 50 + int(personality.get("personality_strength", 0) or 0) // 2)),
    }
