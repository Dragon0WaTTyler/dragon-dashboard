from __future__ import annotations

from typing import Any, Mapping


def build_identity_events(
    *,
    continuity_state: Mapping[str, Any] | None = None,
    behavioral_drift: Mapping[str, Any] | None = None,
    runtime_temperament: Mapping[str, Any] | None = None,
    runtime_personality: Mapping[str, Any] | None = None,
    orchestration_archetype: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    continuity = dict(continuity_state or {})
    drift = dict(behavioral_drift or {})
    temperament = dict(runtime_temperament or {})
    personality = dict(runtime_personality or {})
    archetype = dict(orchestration_archetype or {})
    events: list[dict[str, Any]] = []
    if str(continuity.get("continuity_state") or "") == "fragmented":
        events.append({"event_type": "continuity_fragmented", "severity": "high"})
    if int(drift.get("drift_score", 0) or 0) >= 55:
        events.append({"event_type": "behavioral_drift_elevated", "severity": "medium"})
    if str(temperament.get("temperament") or "") in {"defensive", "cautious"}:
        events.append({"event_type": "runtime_temperament_changed", "severity": "medium"})
    if int(personality.get("personality_strength", 0) or 0) >= 55:
        events.append({"event_type": "orchestration_trait_stabilized", "severity": "low"})
    if str(archetype.get("archetype") or "") in {"adaptive_survivor", "cautious_stabilizer"}:
        events.append({"event_type": "identity_shift_detected", "severity": "medium"})
    return events
