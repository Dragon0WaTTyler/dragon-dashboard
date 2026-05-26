from __future__ import annotations

from typing import Any, Mapping

from .awareness_model import build_awareness_state
from .awareness_pressure import build_awareness_pressure
from .cognitive_balance import build_cognitive_balance
from .consciousness_events import build_consciousness_events
from .consciousness_forecasting import build_consciousness_forecast
from .consciousness_governance import build_consciousness_governance
from .consciousness_memory import (
    build_consciousness_memory_summary,
    load_consciousness_memory,
    update_consciousness_memory,
)
from .consciousness_metrics import build_consciousness_metrics
from .continuity_awareness import build_continuity_awareness
from .orchestration_attention import build_orchestration_attention
from .orchestration_focus import build_orchestration_focus
from .orchestration_intuition import build_orchestration_intuition
from .orchestration_perception import build_orchestration_perception
from .runtime_presence import build_runtime_presence
from .runtime_reflection import build_runtime_reflection


def build_runtime_consciousness(
    orchestration: Mapping[str, Any] | None,
    *,
    persist_memory: bool = True,
    memory_path=None,
    timestamp: str = "",
) -> dict[str, Any]:
    payload = dict(orchestration or {})
    prior_memory_summary = build_consciousness_memory_summary(load_consciousness_memory(path=memory_path), current_context=payload)
    execution_metrics = dict(payload.get("execution_metrics") or {})
    coordination_metrics = dict(payload.get("coordination_metrics") or {})
    orchestration_pressure = dict(payload.get("orchestration_pressure") or {})
    continuity_state = dict(payload.get("continuity_state") or {})
    behavioral_drift = dict(payload.get("behavioral_drift") or {})
    adaptation_history = dict(payload.get("adaptation_history") or {})
    cinematic_direction = dict(payload.get("cinematic_direction") or {})
    cinematic_metrics = dict(payload.get("cinematic_metrics") or {})
    ecosystem_balance = dict(payload.get("ecosystem_balance") or {})

    degradation_risk = int(execution_metrics.get("degradation_risk", 0) or 0)
    runtime_resilience = int(coordination_metrics.get("runtime_resilience", 0) or 0)
    adaptation_pressure = int(coordination_metrics.get("adaptation_pressure", 0) or 0)
    continuity_confidence = int(continuity_state.get("continuity_confidence", 0) or 0)
    switch_frequency = int(adaptation_history.get("switch_frequency", 0) or 0)
    drift_score = int(behavioral_drift.get("drift_score", 0) or 0)
    pressure_score = int(orchestration_pressure.get("pressure_score", 0) or 0)
    cinematic_quality = int(cinematic_metrics.get("cinematic_quality", 0) or 0)
    identity_confidence = int(payload.get("identity_confidence", 0) or 0)

    awareness = build_awareness_state(
        runtime_resilience=runtime_resilience,
        degradation_risk=degradation_risk,
        continuity_confidence=continuity_confidence,
        cinematic_quality=cinematic_quality,
        identity_confidence=identity_confidence,
    )
    continuity = build_continuity_awareness(
        continuity_state=str(continuity_state.get("continuity_state") or ""),
        continuity_confidence=continuity_confidence,
        switch_frequency=switch_frequency,
        drift_score=drift_score,
    )
    reflection = build_runtime_reflection(
        awareness_state=str(awareness.get("state") or ""),
        degradation_risk=degradation_risk,
        runtime_resilience=runtime_resilience,
        cinematic_direction=str(cinematic_direction.get("style") or ""),
        adaptation_pressure=adaptation_pressure,
    )
    attention = build_orchestration_attention(
        degradation_risk=degradation_risk,
        continuity_confidence=continuity_confidence,
        runtime_resilience=runtime_resilience,
        cinematic_quality=cinematic_quality,
        balance_state=str(ecosystem_balance.get("balance_state") or ""),
    )
    intuition = build_orchestration_intuition(
        degradation_risk=degradation_risk,
        runtime_resilience=runtime_resilience,
        pressure_direction=str(orchestration_pressure.get("pressure_direction") or ""),
        cinematic_direction=str(cinematic_direction.get("style") or ""),
        continuity_awareness=str(continuity.get("state") or ""),
    )
    balance = build_cognitive_balance(
        awareness_integrity=int(awareness.get("awareness_integrity", 0) or 0),
        pressure_score=pressure_score,
        continuity_score=int(continuity.get("continuity_awareness_score", 0) or 0),
        reflection_strength=int(reflection.get("reflection_strength", 0) or 0),
    )
    pressure = build_awareness_pressure(
        degradation_risk=degradation_risk,
        pressure_score=pressure_score,
        adaptation_pressure=adaptation_pressure,
        cinematic_quality=cinematic_quality,
        continuity_confidence=continuity_confidence,
    )
    focus = build_orchestration_focus(
        awareness_state=str(awareness.get("state") or ""),
        intuition_state=str(intuition.get("state") or ""),
        pressure_score=pressure_score,
        cinematic_quality=cinematic_quality,
        runtime_resilience=runtime_resilience,
    )
    presence = build_runtime_presence(
        awareness_integrity=int(awareness.get("awareness_integrity", 0) or 0),
        continuity_score=int(continuity.get("continuity_awareness_score", 0) or 0),
        runtime_resilience=runtime_resilience,
        cinematic_quality=cinematic_quality,
        degradation_risk=degradation_risk,
    )
    perception = build_orchestration_perception(
        awareness_state=str(awareness.get("state") or ""),
        reflection_state=str(reflection.get("state") or ""),
        continuity_awareness=str(continuity.get("state") or ""),
        runtime_presence=str(presence.get("state") or ""),
        degradation_risk=degradation_risk,
    )
    forecast = build_consciousness_forecast(
        awareness_state=str(awareness.get("state") or ""),
        cognitive_balance=str(balance.get("state") or ""),
        perception_state=str(perception.get("state") or ""),
        continuity_awareness=str(continuity.get("state") or ""),
        focus=str(focus.get("focus") or ""),
        reflection_state=str(reflection.get("state") or ""),
    )
    governance = build_consciousness_governance(
        continuity_awareness=str(continuity.get("state") or ""),
        cognitive_balance=str(balance.get("state") or ""),
        focus=str(focus.get("focus") or ""),
        runtime_presence=str(presence.get("state") or ""),
        perception_state=str(perception.get("state") or ""),
    )
    metrics = build_consciousness_metrics(
        awareness_integrity=int(awareness.get("awareness_integrity", 0) or 0),
        focus_strength=int(focus.get("orchestration_focus_strength", 0) or 0),
        cognitive_stability=int(balance.get("cognitive_stability", 0) or 0),
        runtime_presence_score=int(presence.get("runtime_presence_score", 0) or 0),
        perception_integrity=int(perception.get("perception_integrity", 0) or 0),
        reflection_strength=int(reflection.get("reflection_strength", 0) or 0),
        continuity_awareness_score=int(continuity.get("continuity_awareness_score", 0) or 0),
    )

    current_result = {
        "runtime_consciousness": {
            "consciousness_state": "persistent_orchestration_consciousness",
            "awareness_anchor": str(awareness.get("state") or "stable_awareness"),
            "focus_anchor": str(focus.get("focus") or "equilibrium_focus"),
            "presence_anchor": str(presence.get("state") or "strong_presence"),
        },
        "awareness_state": awareness,
        "orchestration_attention": attention,
        "continuity_awareness": continuity,
        "runtime_reflection": reflection,
        "orchestration_intuition": intuition,
        "cognitive_balance": balance,
        "awareness_pressure": pressure,
        "orchestration_focus": focus,
        "runtime_presence": presence,
        "orchestration_perception": perception,
        "consciousness_forecast": forecast,
        "consciousness_governance": governance,
        "consciousness_metrics": metrics,
    }
    if persist_memory:
        memory_summary = update_consciousness_memory(payload, current_result, path=memory_path, timestamp=timestamp)
    else:
        memory_summary = prior_memory_summary
    current_result["consciousness_memory"] = memory_summary
    previous_entry = dict((prior_memory_summary.get("recent_entries") or [])[-1] or {}) if prior_memory_summary.get("recent_entries") else {}
    current_result["consciousness_events"] = build_consciousness_events(
        awareness_state=str(awareness.get("state") or ""),
        focus=str(focus.get("focus") or ""),
        cognitive_balance=str(balance.get("state") or ""),
        continuity_awareness=str(continuity.get("state") or ""),
        runtime_presence=str(presence.get("state") or ""),
        orchestration_clarity=int(metrics.get("orchestration_clarity", 0) or 0),
        previous_awareness_state=str(previous_entry.get("awareness_state") or ""),
        previous_focus=str(previous_entry.get("focus") or ""),
        previous_clarity=int(previous_entry.get("orchestration_clarity", 0) or 0),
    )
    return current_result
