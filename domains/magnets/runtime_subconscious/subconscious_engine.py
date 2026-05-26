from __future__ import annotations

from typing import Any, Mapping

from .cinematic_underflow import build_cinematic_underflow
from .continuity_underlayers import build_continuity_underlayers
from .dormant_resilience import build_dormant_resilience
from .hidden_equilibrium import build_hidden_equilibrium
from .latent_patterns import build_latent_patterns
from .orchestration_echoes import build_orchestration_echoes
from .orchestration_residue import build_orchestration_residue
from .orchestration_underflow import build_orchestration_underflow
from .silent_adaptation import build_silent_adaptation
from .subconscious_events import build_subconscious_events
from .subconscious_forecasting import build_subconscious_forecast
from .subconscious_governance import build_subconscious_governance
from .subconscious_memory import (
    build_subconscious_memory_summary,
    load_subconscious_memory,
    update_subconscious_memory,
)
from .subconscious_metrics import build_subconscious_metrics
from .subconscious_pressure import build_subconscious_pressure


def build_runtime_subconscious(
    orchestration: Mapping[str, Any] | None,
    *,
    persist_memory: bool = True,
    memory_path=None,
    timestamp: str = "",
) -> dict[str, Any]:
    payload = dict(orchestration or {})
    prior_memory_summary = build_subconscious_memory_summary(load_subconscious_memory(path=memory_path), current_context=payload)
    execution_metrics = dict(payload.get("execution_metrics") or {})
    coordination_metrics = dict(payload.get("coordination_metrics") or {})
    orchestration_pressure = dict(payload.get("orchestration_pressure") or {})
    adaptation_history = dict(payload.get("adaptation_history") or {})
    continuity_state = dict(payload.get("continuity_state") or {})
    continuity_awareness = dict(payload.get("continuity_awareness") or {})
    subconscious_governance_seed = dict(payload.get("instinct_governance") or payload.get("consciousness_governance") or {})
    resilience_topology = dict(payload.get("resilience_topology") or {})
    ecosystem_balance = dict(payload.get("ecosystem_balance") or {})
    adaptive_equilibrium = dict(payload.get("adaptive_equilibrium") or {})
    behavioral_drift = dict(payload.get("behavioral_drift") or {})
    instinct_metrics = dict(payload.get("instinct_metrics") or {})
    consciousness_metrics = dict(payload.get("consciousness_metrics") or {})
    cinematic_metrics = dict(payload.get("cinematic_metrics") or {})
    runtime_memory_summary = dict(payload.get("runtime_memory_summary") or {})
    instinct_memory = dict(payload.get("instinct_memory") or {})
    consciousness_memory = dict(payload.get("consciousness_memory") or {})
    stabilization_instinct = dict(payload.get("stabilization_instinct") or {})
    resilience_instinct = dict(payload.get("resilience_instinct") or {})
    fallback_instinct = dict(payload.get("fallback_instinct") or {})
    continuity_instinct = dict(payload.get("continuity_instinct") or {})
    cinematic_instinct = dict(payload.get("cinematic_instinct") or {})
    runtime_survival = dict(payload.get("runtime_survival") or {})
    cinematic_direction = dict(payload.get("cinematic_direction") or {})

    degradation_risk = int(execution_metrics.get("degradation_risk", 0) or 0)
    runtime_resilience = int(coordination_metrics.get("runtime_resilience", 0) or 0)
    adaptation_pressure = int(coordination_metrics.get("adaptation_pressure", 0) or 0)
    pressure_score = int(orchestration_pressure.get("pressure_score", 0) or 0)
    continuity_confidence = int(continuity_state.get("continuity_confidence", 0) or 0)
    switch_frequency = int(adaptation_history.get("switch_frequency", 0) or 0)
    drift_score = int(behavioral_drift.get("drift_score", 0) or 0)
    instinct_integrity = int(instinct_metrics.get("instinct_integrity", 0) or 0)
    awareness_integrity = int(consciousness_metrics.get("awareness_integrity", 0) or 0)
    cinematic_quality = int(cinematic_metrics.get("cinematic_quality", 0) or 0)
    fallback_intensity = int(instinct_metrics.get("fallback_intensity", 0) or fallback_instinct.get("fallback_intensity", 0) or 0)

    latent = build_latent_patterns(
        instinct_integrity=instinct_integrity,
        awareness_integrity=awareness_integrity,
        cinematic_quality=cinematic_quality,
        degradation_risk=degradation_risk,
        continuity_confidence=continuity_confidence,
    )
    underflow = build_orchestration_underflow(
        pressure_direction=str(orchestration_pressure.get("pressure_direction") or ""),
        pressure_score=pressure_score,
        degradation_risk=degradation_risk,
        fallback_intensity=fallback_intensity,
        runtime_resilience=runtime_resilience,
    )
    hidden = build_hidden_equilibrium(
        equilibrium_state=str(adaptive_equilibrium.get("equilibrium_state") or ""),
        balance_state=str(ecosystem_balance.get("balance_state") or ""),
        underflow_state=str(underflow.get("state") or ""),
        instinct_integrity=instinct_integrity,
        degradation_risk=degradation_risk,
    )
    pressure = build_subconscious_pressure(
        pressure_score=pressure_score,
        degradation_risk=degradation_risk,
        continuity_confidence=continuity_confidence,
        cinematic_quality=cinematic_quality,
    )
    silent = build_silent_adaptation(
        switch_frequency=switch_frequency,
        adaptation_pressure=adaptation_pressure,
        latent_pattern=str(latent.get("pattern") or ""),
        hidden_equilibrium_state=str(hidden.get("state") or ""),
    )
    continuity = build_continuity_underlayers(
        continuity_awareness=str(continuity_awareness.get("state") or ""),
        continuity_instinct=str(continuity_instinct.get("state") or ""),
        switch_frequency=switch_frequency,
        drift_score=drift_score,
        cinematic_quality=cinematic_quality,
    )
    residue = build_orchestration_residue(
        fallback_state=str(fallback_instinct.get("state") or ""),
        resilience_state=str(resilience_instinct.get("state") or ""),
        cinematic_state=str(cinematic_instinct.get("state") or ""),
        degradation_risk=degradation_risk,
        hidden_equilibrium_state=str(hidden.get("state") or ""),
    )
    dormant = build_dormant_resilience(
        runtime_resilience=runtime_resilience,
        resilience_topology=str(resilience_topology.get("topology") or ""),
        latent_pattern=str(latent.get("pattern") or ""),
        survival_state=str(runtime_survival.get("state") or ""),
        degradation_risk=degradation_risk,
    )
    cinematic = build_cinematic_underflow(
        cinematic_quality=cinematic_quality,
        cinematic_direction=str(cinematic_direction.get("style") or ""),
        underflow_state=str(underflow.get("state") or ""),
        cinematic_instinct=str(cinematic_instinct.get("state") or ""),
        residue_pattern=str(residue.get("pattern") or ""),
    )
    echoes = build_orchestration_echoes(
        stabilization_state=str(stabilization_instinct.get("state") or ""),
        resilience_state=str(resilience_instinct.get("state") or ""),
        fallback_state=str(fallback_instinct.get("state") or ""),
        continuity_state=str(continuity_instinct.get("state") or ""),
        cinematic_state=str(cinematic_instinct.get("state") or ""),
    )
    metrics = build_subconscious_metrics(
        latent_stability=int(latent.get("latent_stability", 0) or 0),
        hidden_equilibrium_strength=int(hidden.get("hidden_equilibrium_strength", 0) or 0),
        dormant_resilience_strength=int(dormant.get("dormant_resilience_strength", 0) or 0),
        orchestration_residue_density=int(residue.get("orchestration_residue_density", 0) or 0),
        subconscious_balance=int((hidden.get("hidden_equilibrium_strength", 0) or 0) * 0.55 + (latent.get("latent_stability", 0) or 0) * 0.45),
        cinematic_underflow_integrity=int(cinematic.get("cinematic_underflow_integrity", 0) or 0),
        orchestration_echo_strength=int(echoes.get("orchestration_echo_strength", 0) or 0),
    )
    forecast = build_subconscious_forecast(
        latent_pattern=str(latent.get("pattern") or ""),
        hidden_equilibrium_state=str(hidden.get("state") or ""),
        dormant_resilience_state=str(dormant.get("state") or ""),
        cinematic_underflow_state=str(cinematic.get("state") or ""),
        continuity_underlayers_state=str(continuity.get("state") or ""),
        residue_pattern=str(residue.get("pattern") or ""),
    )
    governance = build_subconscious_governance(
        hidden_equilibrium_state=str(hidden.get("state") or ""),
        residue_pattern=str(residue.get("pattern") or ""),
        dormant_resilience_state=str(dormant.get("state") or ""),
        cinematic_underflow_state=str(cinematic.get("state") or ""),
        latent_pattern=str(latent.get("pattern") or ""),
        pressure_score=max(
            pressure_score,
            int(pressure.get("latent_pressure", 0) or 0),
            int((subconscious_governance_seed.get("governance_actions") and 60) or 0),
            int(runtime_memory_summary.get("total_observations", 0) or 0),
            int(instinct_memory.get("total_observations", 0) or 0),
            int(consciousness_memory.get("total_observations", 0) or 0),
        ),
    )

    current_result = {
        "runtime_subconscious": {
            "subconscious_state": "persistent_orchestration_subconscious",
            "latent_anchor": str(latent.get("pattern") or "latent_resilience"),
            "underflow_anchor": str(underflow.get("state") or "adaptive_underflow"),
            "equilibrium_anchor": str(hidden.get("state") or "hidden_balance"),
            "residue_anchor": str(residue.get("pattern") or "equilibrium_residue"),
        },
        "latent_patterns": latent,
        "orchestration_underflow": underflow,
        "hidden_equilibrium": hidden,
        "subconscious_pressure": pressure,
        "silent_adaptation": silent,
        "continuity_underlayers": continuity,
        "orchestration_residue": residue,
        "dormant_resilience": dormant,
        "cinematic_underflow": cinematic,
        "orchestration_echoes": echoes,
        "subconscious_forecast": forecast,
        "subconscious_governance": governance,
        "subconscious_metrics": metrics,
    }
    if persist_memory:
        memory_summary = update_subconscious_memory(payload, current_result, path=memory_path, timestamp=timestamp)
    else:
        memory_summary = prior_memory_summary
    current_result["subconscious_memory"] = memory_summary
    previous_entry = dict((prior_memory_summary.get("recent_entries") or [])[-1] or {}) if prior_memory_summary.get("recent_entries") else {}
    current_result["subconscious_events"] = build_subconscious_events(
        latent_pattern=str(latent.get("pattern") or ""),
        pressure_score=int(pressure.get("latent_pressure", 0) or 0),
        dormant_resilience_state=str(dormant.get("state") or ""),
        residue_pattern=str(residue.get("pattern") or ""),
        cinematic_underflow_state=str(cinematic.get("state") or ""),
        hidden_equilibrium_state=str(hidden.get("state") or ""),
        previous_latent_pattern=str(previous_entry.get("latent_pattern") or ""),
        previous_hidden_equilibrium=str(previous_entry.get("underflow_state") or ""),
    )
    return current_result
