from __future__ import annotations

from typing import Any, Mapping

from .adaptive_instinct import build_adaptive_instinct
from .cinematic_instinct import build_cinematic_instinct
from .continuity_instinct import build_continuity_instinct
from .equilibrium_instinct import build_equilibrium_instinct
from .fallback_instinct import build_fallback_instinct
from .instinct_events import build_instinct_events
from .instinct_forecasting import build_instinct_forecast
from .instinct_governance import build_instinct_governance
from .instinct_memory import build_instinct_memory_summary, load_instinct_memory, update_instinct_memory
from .instinct_metrics import build_instinct_metrics
from .instinct_pressure import build_instinct_pressure
from .orchestration_reflexes import build_orchestration_reflexes
from .resilience_instinct import build_resilience_instinct
from .runtime_survival import build_runtime_survival
from .stabilization_instinct import build_stabilization_instinct


def build_runtime_instinct(
    orchestration: Mapping[str, Any] | None,
    *,
    persist_memory: bool = True,
    memory_path=None,
    timestamp: str = "",
) -> dict[str, Any]:
    payload = dict(orchestration or {})
    prior_memory_summary = build_instinct_memory_summary(load_instinct_memory(path=memory_path), current_context=payload)
    execution_metrics = dict(payload.get("execution_metrics") or {})
    execution_timeline = dict(payload.get("execution_timeline") or {})
    coordination_metrics = dict(payload.get("coordination_metrics") or {})
    orchestration_pressure = dict(payload.get("orchestration_pressure") or {})
    continuity_state = dict(payload.get("continuity_state") or {})
    continuity_awareness = dict(payload.get("continuity_awareness") or {})
    runtime_identity = dict(payload.get("runtime_identity") or {})
    cinematic_direction = dict(payload.get("cinematic_direction") or {})
    cinematic_metrics = dict(payload.get("cinematic_metrics") or {})
    cinematic_continuity = dict(payload.get("continuity_cinema") or {})
    immersion_state = dict(payload.get("immersion_state") or {})
    awareness_state = dict(payload.get("awareness_state") or {})
    ecosystem_balance = dict(payload.get("ecosystem_balance") or {})
    adaptive_equilibrium = dict(payload.get("adaptive_equilibrium") or {})
    resilience_topology = dict(payload.get("resilience_topology") or {})
    behavioral_drift = dict(payload.get("behavioral_drift") or {})
    adaptation_history = dict(payload.get("adaptation_history") or {})

    degradation_risk = int(execution_metrics.get("degradation_risk", 0) or 0)
    stability_score = int(execution_metrics.get("stability_score", 0) or 0)
    runtime_resilience = int(coordination_metrics.get("runtime_resilience", 0) or 0)
    continuity_confidence = int(continuity_state.get("continuity_confidence", 0) or 0)
    pressure_score = int(orchestration_pressure.get("pressure_score", 0) or 0)
    fallback_probability = float(execution_timeline.get("fallback_probability", 0) or 0.0)
    switch_frequency = int(adaptation_history.get("switch_frequency", 0) or 0)
    drift_score = int(behavioral_drift.get("drift_score", 0) or 0)
    awareness_integrity = int((payload.get("consciousness_metrics") or {}).get("awareness_integrity", 0) or 0)
    cinematic_quality = int(cinematic_metrics.get("cinematic_quality", 0) or 0)

    stabilization = build_stabilization_instinct(
        stability_score=stability_score,
        degradation_risk=degradation_risk,
        continuity_confidence=continuity_confidence,
        awareness_integrity=awareness_integrity,
        pressure_score=pressure_score,
    )
    fallback = build_fallback_instinct(
        fallback_strategy=str(payload.get("fallback_strategy") or (payload.get("runtime_preflight") or {}).get("fallback_strategy") or ""),
        fallback_probability=fallback_probability,
        degradation_risk=degradation_risk,
        startup_confidence=str(payload.get("startup_confidence") or ""),
        authority_state=str(payload.get("authority_state") or ""),
    )
    continuity = build_continuity_instinct(
        continuity_state=str(continuity_state.get("continuity_state") or ""),
        continuity_confidence=continuity_confidence,
        switch_frequency=switch_frequency,
        drift_score=drift_score,
        continuity_awareness=str(continuity_awareness.get("state") or ""),
    )
    survival = build_runtime_survival(
        stabilization_strength=int(stabilization.get("stabilization_strength", 0) or 0),
        resilience_strength=runtime_resilience,
        fallback_intensity=int(fallback.get("fallback_intensity", 0) or 0),
        continuity_preservation=int(continuity.get("continuity_preservation", 0) or 0),
        degradation_risk=degradation_risk,
    )
    resilience = build_resilience_instinct(
        runtime_resilience=runtime_resilience,
        resilience_topology=str(resilience_topology.get("topology") or ""),
        survival_state=str(survival.get("state") or ""),
        degradation_risk=degradation_risk,
        fallback_pressure=int(fallback.get("fallback_intensity", 0) or 0),
    )
    cinematic = build_cinematic_instinct(
        cinematic_quality=cinematic_quality,
        cinematic_direction=str(cinematic_direction.get("style") or ""),
        continuity_style=str(cinematic_continuity.get("continuity") or ""),
        fallback_intensity=int(fallback.get("fallback_intensity", 0) or 0),
        immersion_state=str(immersion_state.get("state") or ""),
    )
    equilibrium = build_equilibrium_instinct(
        equilibrium_state=str(adaptive_equilibrium.get("equilibrium_state") or ""),
        balance_state=str(ecosystem_balance.get("balance_state") or ""),
        pressure_direction=str(orchestration_pressure.get("pressure_direction") or ""),
        runtime_resilience=runtime_resilience,
        degradation_risk=degradation_risk,
    )
    reflexes = build_orchestration_reflexes(
        stabilization_state=str(stabilization.get("state") or ""),
        fallback_state=str(fallback.get("state") or ""),
        resilience_state=str(resilience.get("state") or ""),
        continuity_state=str(continuity.get("state") or ""),
        cinematic_state=str(cinematic.get("state") or ""),
    )
    pressure = build_instinct_pressure(
        pressure_score=pressure_score,
        degradation_risk=degradation_risk,
        fallback_intensity=int(fallback.get("fallback_intensity", 0) or 0),
        continuity_preservation=int(continuity.get("continuity_preservation", 0) or 0),
        cinematic_preservation=int(cinematic.get("cinematic_preservation", 0) or 0),
    )
    adaptive = build_adaptive_instinct(
        stabilization_state=str(stabilization.get("state") or ""),
        resilience_state=str(resilience.get("state") or ""),
        fallback_state=str(fallback.get("state") or ""),
        continuity_state=str(continuity.get("state") or ""),
        cinematic_state=str(cinematic.get("state") or ""),
    )
    forecast = build_instinct_forecast(
        stabilization_state=str(stabilization.get("state") or ""),
        fallback_state=str(fallback.get("state") or ""),
        resilience_state=str(resilience.get("state") or ""),
        continuity_state=str(continuity.get("state") or ""),
        cinematic_state=str(cinematic.get("state") or ""),
        equilibrium_state=str(equilibrium.get("state") or ""),
    )
    governance = build_instinct_governance(
        stabilization_state=str(stabilization.get("state") or ""),
        fallback_state=str(fallback.get("state") or ""),
        continuity_state=str(continuity.get("state") or ""),
        cinematic_state=str(cinematic.get("state") or ""),
        resilience_state=str(resilience.get("state") or ""),
        survival_state=str(survival.get("state") or ""),
        equilibrium_state=str(equilibrium.get("state") or ""),
    )
    metrics = build_instinct_metrics(
        stabilization_strength=int(stabilization.get("stabilization_strength", 0) or 0),
        resilience_strength=int(resilience.get("resilience_strength", runtime_resilience) or 0),
        fallback_intensity=int(fallback.get("fallback_intensity", 0) or 0),
        continuity_preservation=int(continuity.get("continuity_preservation", 0) or 0),
        cinematic_preservation=int(cinematic.get("cinematic_preservation", 0) or 0),
        orchestration_survival_score=int(survival.get("orchestration_survival_score", 0) or 0),
        equilibrium_resilience=int(equilibrium.get("equilibrium_resilience", 0) or 0),
    )

    current_result = {
        "runtime_instinct": {
            "instinct_state": "persistent_orchestration_instinct",
            "identity_anchor": str(runtime_identity.get("primary_trait") or "adaptive_balanced"),
            "awareness_anchor": str(awareness_state.get("state") or "stable_awareness"),
            "stabilization_anchor": str(stabilization.get("state") or "adaptive_stabilization"),
            "survival_anchor": str(survival.get("state") or "survival_adaptive"),
        },
        "stabilization_instinct": stabilization,
        "resilience_instinct": resilience,
        "fallback_instinct": fallback,
        "continuity_instinct": continuity,
        "cinematic_instinct": cinematic,
        "equilibrium_instinct": equilibrium,
        "orchestration_reflexes": reflexes,
        "instinct_pressure": pressure,
        "adaptive_instinct": adaptive,
        "runtime_survival": survival,
        "instinct_forecast": forecast,
        "instinct_governance": governance,
        "instinct_metrics": metrics,
    }
    if persist_memory:
        memory_summary = update_instinct_memory(payload, current_result, path=memory_path, timestamp=timestamp)
    else:
        memory_summary = prior_memory_summary
    current_result["instinct_memory"] = memory_summary
    previous_entry = dict((prior_memory_summary.get("recent_entries") or [])[-1] or {}) if prior_memory_summary.get("recent_entries") else {}
    current_result["instinct_events"] = build_instinct_events(
        stabilization_state=str(stabilization.get("state") or ""),
        fallback_state=str(fallback.get("state") or ""),
        continuity_state=str(continuity.get("state") or ""),
        resilience_state=str(resilience.get("state") or ""),
        survival_state=str(survival.get("state") or ""),
        cinematic_state=str(cinematic.get("state") or ""),
        instinct_integrity=int(metrics.get("instinct_integrity", 0) or 0),
        previous_stabilization_state=str(previous_entry.get("stabilization_state") or ""),
        previous_survival_state=str(previous_entry.get("survival_state") or ""),
    )
    return current_result
