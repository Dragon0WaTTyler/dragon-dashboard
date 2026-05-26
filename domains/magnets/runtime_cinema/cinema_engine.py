from __future__ import annotations

from typing import Any, Mapping

from .atmosphere_model import build_runtime_atmosphere
from .cinematic_balance import build_cinematic_balance
from .cinematic_direction import build_cinematic_direction
from .cinematic_events import build_cinematic_events
from .cinematic_forecasting import build_cinematic_forecast
from .cinematic_governance import build_cinematic_governance
from .cinematic_memory import build_cinematic_memory_summary, load_cinematic_memory, update_cinematic_memory
from .cinematic_metrics import build_cinematic_metrics
from .continuity_cinema import build_continuity_cinema
from .dramatic_tension import build_dramatic_tension
from .immersion_model import build_immersion_state
from .orchestration_mood import build_orchestration_mood
from .pacing_model import build_runtime_pacing
from .runtime_aesthetics import build_runtime_aesthetics
from .scene_energy import build_scene_energy


def build_runtime_cinema(
    orchestration: Mapping[str, Any] | None,
    *,
    persist_memory: bool = True,
    memory_path=None,
    timestamp: str = "",
) -> dict[str, Any]:
    payload = dict(orchestration or {})
    prior_memory_summary = build_cinematic_memory_summary(load_cinematic_memory(path=memory_path), current_context=payload)
    selected_source = dict(payload.get("selected_source") or {})
    execution_metrics = dict(payload.get("execution_metrics") or {})
    execution_timeline = dict(payload.get("execution_timeline") or {})
    coordination_metrics = dict(payload.get("coordination_metrics") or {})
    orchestration_pressure = dict(payload.get("orchestration_pressure") or {})
    resilience_topology = dict(payload.get("resilience_topology") or {})
    adaptive_equilibrium = dict(payload.get("adaptive_equilibrium") or {})
    ecosystem_climate = dict(payload.get("ecosystem_climate") or {})
    continuity_state = dict(payload.get("continuity_state") or {})
    behavioral_drift = dict(payload.get("behavioral_drift") or {})
    orchestration_forecast = dict(payload.get("orchestration_forecast") or {})
    adaptation_history = dict(payload.get("adaptation_history") or {})

    degradation_risk = int(execution_metrics.get("degradation_risk", 0) or 0)
    stability_score = int(execution_metrics.get("stability_score", 0) or 0)
    runtime_resilience = int(coordination_metrics.get("runtime_resilience", 0) or 0)
    adaptation_pressure = int(coordination_metrics.get("adaptation_pressure", 0) or 0)
    continuity_confidence = int(continuity_state.get("continuity_confidence", 0) or 0)
    switch_frequency = int(adaptation_history.get("switch_frequency", 0) or 0)
    drift_score = int(behavioral_drift.get("drift_score", 0) or 0)
    fallback_probability = float(execution_timeline.get("fallback_probability", 0) or 0.0)

    direction = build_cinematic_direction(
        authority_state=str(payload.get("authority_state") or ""),
        pressure_direction=str(orchestration_pressure.get("pressure_direction") or ""),
        equilibrium_state=str(adaptive_equilibrium.get("equilibrium_state") or ""),
        topology=str(resilience_topology.get("topology") or ""),
        archetype=str(payload.get("orchestration_archetype") or ""),
        degradation_risk=degradation_risk,
        runtime_resilience=runtime_resilience,
    )
    pacing = build_runtime_pacing(
        stability_score=stability_score,
        adaptation_pressure=adaptation_pressure,
        fallback_probability=fallback_probability,
        runtime_profile=str(payload.get("runtime_profile") or ""),
    )
    immersion = build_immersion_state(
        playback_runtime=str(payload.get("playback_runtime") or ""),
        startup_confidence=str(payload.get("startup_confidence") or ""),
        degradation_risk=degradation_risk,
        runtime_resilience=runtime_resilience,
        continuity_confidence=continuity_confidence,
    )
    atmosphere = build_runtime_atmosphere(
        pressure_direction=str(orchestration_pressure.get("pressure_direction") or ""),
        climate=str(ecosystem_climate.get("climate") or ""),
        immersion_strength=int(immersion.get("immersion_strength", 0) or 0),
        runtime_resilience=runtime_resilience,
        degradation_risk=degradation_risk,
    )
    tension = build_dramatic_tension(
        pressure_score=int(orchestration_pressure.get("pressure_score", 0) or 0),
        degradation_risk=degradation_risk,
        adaptation_pressure=adaptation_pressure,
        fallback_probability=fallback_probability,
    )
    continuity = build_continuity_cinema(
        continuity_state=str(continuity_state.get("continuity_state") or ""),
        continuity_confidence=continuity_confidence,
        switch_frequency=switch_frequency,
        drift_score=drift_score,
    )
    mood = build_orchestration_mood(
        authority_state=str(payload.get("authority_state") or ""),
        immersion_state=str(immersion.get("state") or ""),
        tension=str(tension.get("tension") or ""),
        forecast_risk=str(orchestration_forecast.get("forecast_risk") or ""),
    )
    energy = build_scene_energy(
        tension_score=int(tension.get("tension_score", 0) or 0),
        adaptation_pressure=adaptation_pressure,
        pacing=str(pacing.get("pacing") or ""),
        runtime_resilience=runtime_resilience,
    )
    balance = build_cinematic_balance(
        immersion_strength=int(immersion.get("immersion_strength", 0) or 0),
        tension_score=int(tension.get("tension_score", 0) or 0),
        continuity_strength=int(continuity.get("continuity_strength", 0) or 0),
        atmosphere_integrity=int(atmosphere.get("atmosphere_integrity", 0) or 0),
        equilibrium_state=str(adaptive_equilibrium.get("equilibrium_state") or ""),
    )
    aesthetics = build_runtime_aesthetics(
        quality_label=str(selected_source.get("quality_label") or selected_source.get("resolution") or ""),
        runtime_profile=str(payload.get("runtime_profile") or ""),
        stability_score=stability_score,
        continuity_strength=int(continuity.get("continuity_strength", 0) or 0),
        atmosphere_integrity=int(atmosphere.get("atmosphere_integrity", 0) or 0),
        degradation_risk=degradation_risk,
    )
    forecast = build_cinematic_forecast(
        pacing=str(pacing.get("pacing") or ""),
        immersion_state=str(immersion.get("state") or ""),
        atmosphere=str(atmosphere.get("atmosphere") or ""),
        tension=str(tension.get("tension") or ""),
        continuity=str(continuity.get("continuity") or ""),
        balance_state=str(balance.get("balance_state") or ""),
    )
    governance = build_cinematic_governance(
        immersion_state=str(immersion.get("state") or ""),
        pacing=str(pacing.get("pacing") or ""),
        atmosphere=str(atmosphere.get("atmosphere") or ""),
        continuity=str(continuity.get("continuity") or ""),
        tension=str(tension.get("tension") or ""),
        balance_state=str(balance.get("balance_state") or ""),
    )
    metrics = build_cinematic_metrics(
        immersion_strength=int(immersion.get("immersion_strength", 0) or 0),
        pacing_stability=int(pacing.get("pacing_stability", 0) or 0),
        atmosphere_integrity=int(atmosphere.get("atmosphere_integrity", 0) or 0),
        continuity_strength=int(continuity.get("continuity_strength", 0) or 0),
        tension_score=int(tension.get("tension_score", 0) or 0),
        runtime_polish=int(aesthetics.get("runtime_polish", 0) or 0),
        cinematic_balance_score=int(balance.get("balance_score", 0) or 0),
    )

    current_result = {
        "runtime_cinema": {
            "cinema_state": "persistent_cinematic_runtime_director",
            "direction_style": str(direction.get("style") or "cinematic_balanced"),
            "mood_anchor": str(mood.get("mood") or "cinematic_mood"),
            "aesthetic_state": str(aesthetics.get("aesthetic_state") or "cinematic_runtime"),
        },
        "cinematic_direction": direction,
        "runtime_pacing": pacing,
        "immersion_state": immersion,
        "runtime_atmosphere": atmosphere,
        "dramatic_tension": tension,
        "continuity_cinema": continuity,
        "orchestration_mood": mood,
        "scene_energy": energy,
        "cinematic_balance": balance,
        "runtime_aesthetics": aesthetics,
        "cinematic_forecast": forecast,
        "cinematic_governance": governance,
        "cinematic_metrics": metrics,
    }
    if persist_memory:
        memory_summary = update_cinematic_memory(payload, current_result, path=memory_path, timestamp=timestamp)
    else:
        memory_summary = prior_memory_summary
    current_result["cinematic_memory"] = memory_summary
    previous_entry = dict((prior_memory_summary.get("recent_entries") or [])[-1] or {}) if prior_memory_summary.get("recent_entries") else {}
    current_result["cinematic_events"] = build_cinematic_events(
        direction_style=str(direction.get("style") or ""),
        pacing=str(pacing.get("pacing") or ""),
        immersion_state=str(immersion.get("state") or ""),
        atmosphere=str(atmosphere.get("atmosphere") or ""),
        balance_state=str(balance.get("balance_state") or ""),
        runtime_polish=int(metrics.get("runtime_polish", 0) or 0),
        previous_direction=str(previous_entry.get("direction_style") or ""),
        previous_balance_state=str(previous_entry.get("balance_state") or ""),
    )
    return current_result
