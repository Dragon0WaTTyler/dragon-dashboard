from __future__ import annotations

from typing import Any, Mapping

from .adaptation_profiles import build_adaptation_profile
from .behavioral_drift import build_behavioral_drift
from .continuity_tracker import build_continuity_state
from .environmental_identity import build_environmental_identity
from .identity_events import build_identity_events
from .identity_forecasting import build_identity_forecast
from .identity_memory import build_identity_memory_summary, load_identity_memory, update_identity_memory
from .identity_metrics import build_identity_metrics
from .orchestration_archetypes import build_orchestration_archetype
from .orchestration_traits import build_orchestration_traits
from .preference_evolution import build_preference_evolution
from .runtime_personality import build_runtime_personality
from .runtime_temperament import build_runtime_temperament


def build_runtime_identity(
    orchestration: Mapping[str, Any] | None,
    *,
    persist_memory: bool = True,
    memory_path=None,
    timestamp: str = "",
) -> dict[str, Any]:
    payload = dict(orchestration or {})
    identity_memory_summary = (
        update_identity_memory(payload, path=memory_path, timestamp=timestamp)
        if persist_memory
        else build_identity_memory_summary(load_identity_memory(path=memory_path), current_context=payload)
    )
    runtime_personality = build_runtime_personality(identity_memory_summary, current_context=payload)
    orchestration_traits = build_orchestration_traits(identity_memory_summary, current_context=payload)
    adaptation_profile = build_adaptation_profile(identity_memory_summary, current_context=payload)
    behavioral_drift = build_behavioral_drift(identity_memory_summary, current_context=payload)
    environmental_identity = build_environmental_identity(identity_memory_summary, current_context=payload)
    runtime_temperament = build_runtime_temperament(identity_memory_summary, current_context=payload)
    continuity_state = build_continuity_state(identity_memory_summary, current_context=payload)
    orchestration_archetype = build_orchestration_archetype(
        identity_memory_summary,
        runtime_personality=runtime_personality,
        adaptation_profile=adaptation_profile,
        behavioral_drift=behavioral_drift,
    )
    preference_evolution = build_preference_evolution(identity_memory_summary, current_context=payload)
    identity_confidence = _identity_confidence(
        identity_memory_summary=identity_memory_summary,
        runtime_personality=runtime_personality,
        continuity_state=continuity_state,
        adaptation_profile=adaptation_profile,
    )
    identity_metrics = build_identity_metrics(
        continuity_state=continuity_state,
        runtime_personality=runtime_personality,
        adaptation_profile=adaptation_profile,
        identity_confidence=identity_confidence,
        identity_memory_summary=identity_memory_summary,
    )
    identity_forecast = build_identity_forecast(
        continuity_state=continuity_state,
        behavioral_drift=behavioral_drift,
        preference_evolution=preference_evolution,
        identity_metrics=identity_metrics,
    )
    identity_events = build_identity_events(
        continuity_state=continuity_state,
        behavioral_drift=behavioral_drift,
        runtime_temperament=runtime_temperament,
        runtime_personality=runtime_personality,
        orchestration_archetype=orchestration_archetype,
    )
    warnings = _identity_warnings(
        continuity_state=continuity_state,
        behavioral_drift=behavioral_drift,
        runtime_temperament=runtime_temperament,
    )
    runtime_identity = {
        "identity_state": "persistent_orchestration_character",
        "primary_trait": str(runtime_personality.get("primary_trait") or "adaptive_balanced"),
        "environmental_identity": str(environmental_identity.get("environmental_identity") or "stable_runtime_identity"),
        "preference_signature": preference_evolution,
    }
    return {
        "runtime_identity": runtime_identity,
        "orchestration_archetype": str(orchestration_archetype.get("archetype") or "cautious_stabilizer"),
        "runtime_temperament": str(runtime_temperament.get("temperament") or "calm"),
        "adaptation_profile": str(adaptation_profile.get("profile") or "stable_adapter"),
        "behavioral_drift": behavioral_drift,
        "continuity_state": continuity_state,
        "identity_confidence": identity_confidence,
        "identity_forecast": identity_forecast,
        "persistent_traits": list(runtime_personality.get("traits") or ["adaptive_balanced"]),
        "orchestration_traits": list(orchestration_traits.get("traits") or ["prefers_safe_runtime"]),
        "identity_warnings": warnings,
        "identity_metrics": identity_metrics,
        "identity_events": identity_events,
        "identity_memory_summary": identity_memory_summary,
        "environmental_identity": environmental_identity,
        "preference_evolution": preference_evolution,
        "runtime_personality": runtime_personality,
    }


def _identity_confidence(
    *,
    identity_memory_summary: Mapping[str, Any],
    runtime_personality: Mapping[str, Any],
    continuity_state: Mapping[str, Any],
    adaptation_profile: Mapping[str, Any],
) -> int:
    confidence = int(round(float(identity_memory_summary.get("average_identity_confidence", 0) or 0.0) * 100))
    confidence = max(confidence, int(runtime_personality.get("personality_strength", 0) or 0))
    confidence = int(round((confidence + int(continuity_state.get("continuity_confidence", 0) or 0) + int(adaptation_profile.get("profile_score", 0) or 0)) / 3))
    return max(0, min(100, confidence))


def _identity_warnings(
    *,
    continuity_state: Mapping[str, Any],
    behavioral_drift: Mapping[str, Any],
    runtime_temperament: Mapping[str, Any],
) -> list[str]:
    warnings: list[str] = []
    if str(continuity_state.get("continuity_state") or "") == "fragmented":
        warnings.append("continuity_fragmented")
    if int(behavioral_drift.get("drift_score", 0) or 0) >= 55:
        warnings.append("behavioral_drift_elevated")
    if str(runtime_temperament.get("temperament") or "") == "defensive":
        warnings.append("temperament_defensive")
    return warnings
