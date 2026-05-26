from __future__ import annotations

from typing import Any, Mapping

from .adaptive_equilibrium import build_adaptive_equilibrium
from .degradation_currents import build_degradation_currents
from .ecosystem_balance import build_ecosystem_balance
from .ecosystem_events import build_ecosystem_events
from .ecosystem_forecasting import build_ecosystem_forecast
from .ecosystem_governance import build_ecosystem_governance
from .ecosystem_memory import build_ecosystem_memory_summary, load_ecosystem_memory, update_ecosystem_memory
from .ecosystem_metrics import build_ecosystem_metrics
from .orchestration_climate import build_orchestration_climate
from .orchestration_pressure import build_orchestration_pressure
from .resilience_topology import build_resilience_topology
from .runtime_clusters import build_runtime_clusters
from .runtime_ecology import build_runtime_ecology
from .stability_zones import build_stability_zone


def build_runtime_ecosystem(
    orchestration: Mapping[str, Any] | None,
    *,
    persist_memory: bool = True,
    memory_path=None,
    timestamp: str = "",
) -> dict[str, Any]:
    payload = dict(orchestration or {})
    prior_memory_summary = build_ecosystem_memory_summary(load_ecosystem_memory(path=memory_path), current_context=payload)
    selected_source = dict(payload.get("selected_source") or {})
    execution_metrics = dict(payload.get("execution_metrics") or {})
    execution_timeline = dict(payload.get("execution_timeline") or {})
    coordination_metrics = dict(payload.get("coordination_metrics") or {})
    runtime_predictions = dict(payload.get("runtime_predictions") or {})
    identity = dict(payload.get("runtime_identity") or {})
    continuity_state = dict(payload.get("continuity_state") or {})

    degradation_risk = int(execution_metrics.get("degradation_risk", 0) or 0)
    stability_score = int(execution_metrics.get("stability_score", 0) or 0)
    runtime_resilience = int(coordination_metrics.get("runtime_resilience", 0) or 0)
    coordination_confidence = int(coordination_metrics.get("coordination_confidence", 0) or 0)
    adaptation_pressure = int(coordination_metrics.get("adaptation_pressure", 0) or 0)
    prediction_confidence = int(runtime_predictions.get("prediction_confidence", 0) or 0)
    fallback_probability = float(execution_timeline.get("fallback_probability", 0) or 0.0)

    pressure = build_orchestration_pressure(
        selected_source=selected_source,
        degradation_risk=degradation_risk,
        runtime_resilience=runtime_resilience,
        coordination_confidence=coordination_confidence,
        prediction_confidence=prediction_confidence,
        fallback_probability=fallback_probability,
        adaptation_pressure=adaptation_pressure,
    )
    clusters = build_runtime_clusters(
        runtime_profile=str(payload.get("runtime_profile") or ""),
        playback_runtime=str(payload.get("playback_runtime") or ""),
        startup_confidence=str(payload.get("startup_confidence") or ""),
        runtime_resilience=runtime_resilience,
        degradation_risk=degradation_risk,
        fallback_pressure=int((pressure.get("pressure_components") or {}).get("fallback_pressure", 0) or 0),
        adaptation_pressure=adaptation_pressure,
        authority_state=str(payload.get("authority_state") or ""),
    )
    balance = build_ecosystem_balance(
        stability_score=stability_score,
        degradation_risk=degradation_risk,
        runtime_resilience=runtime_resilience,
        fallback_pressure=int((pressure.get("pressure_components") or {}).get("fallback_pressure", 0) or 0),
        adaptation_pressure=adaptation_pressure,
        cluster_alignment=str(clusters.get("cluster_alignment") or ""),
    )
    ecology = build_runtime_ecology(
        runtime_resilience=runtime_resilience,
        degradation_risk=degradation_risk,
        fallback_probability=fallback_probability,
        authority_state=str(payload.get("authority_state") or ""),
        cluster_alignment=str(clusters.get("cluster_alignment") or ""),
    )
    zone = build_stability_zone(
        playback_runtime=str(payload.get("playback_runtime") or ""),
        stability_score=stability_score,
        degradation_risk=degradation_risk,
        pressure_score=int(pressure.get("pressure_score", 0) or 0),
        runtime_resilience=runtime_resilience,
    )
    equilibrium = build_adaptive_equilibrium(
        runtime_resilience=runtime_resilience,
        adaptation_pressure=adaptation_pressure,
        degradation_risk=degradation_risk,
        balance_state=str(balance.get("balance_state") or ""),
    )
    climate = build_orchestration_climate(
        pressure_direction=str(pressure.get("pressure_direction") or ""),
        pressure_score=int(pressure.get("pressure_score", 0) or 0),
        degradation_risk=degradation_risk,
        runtime_resilience=runtime_resilience,
        balance_state=str(balance.get("balance_state") or ""),
    )
    topology = build_resilience_topology(
        runtime_resilience=runtime_resilience,
        fallback_probability=fallback_probability,
        degradation_risk=degradation_risk,
        adaptation_pressure=adaptation_pressure,
    )
    currents = build_degradation_currents(
        degradation_risk=degradation_risk,
        fallback_probability=fallback_probability,
        adaptation_pressure=adaptation_pressure,
        runtime_resilience=runtime_resilience,
    )
    forecast = build_ecosystem_forecast(
        pressure_score=int(pressure.get("pressure_score", 0) or 0),
        pressure_direction=str(pressure.get("pressure_direction") or ""),
        degradation_current=str(currents.get("current") or ""),
        equilibrium_state=str(equilibrium.get("equilibrium_state") or ""),
        topology=str(topology.get("topology") or ""),
        climate=str(climate.get("climate") or ""),
    )
    governance = build_ecosystem_governance(
        balance_state=str(balance.get("balance_state") or ""),
        pressure_score=int(pressure.get("pressure_score", 0) or 0),
        degradation_current=str(currents.get("current") or ""),
        climate=str(climate.get("climate") or ""),
        playback_runtime=str(payload.get("playback_runtime") or ""),
    )
    metrics = build_ecosystem_metrics(
        balance_score=int(balance.get("balance_score", 0) or 0),
        pressure_score=int(pressure.get("pressure_score", 0) or 0),
        resilience_distribution=int(topology.get("resilience_distribution", 0) or 0),
        propagation_risk=int(currents.get("propagation_risk", 0) or 0),
        equilibrium_strength=int(equilibrium.get("equilibrium_strength", 0) or 0),
        climate_stability=int(climate.get("climate_stability", 0) or 0),
    )

    current_result = {
        "runtime_ecosystem": {
            "ecosystem_state": "self_balancing_runtime_ecosystem",
            "identity_anchor": str(identity.get("primary_trait") or "adaptive_balanced"),
            "continuity_anchor": str(continuity_state.get("continuity_state") or "developing"),
            "ecology_state": str(ecology.get("ecology_state") or "coordination_harmonics"),
        },
        "ecosystem_balance": balance,
        "orchestration_pressure": pressure,
        "runtime_clusters": clusters,
        "stability_zone": zone,
        "ecosystem_climate": climate,
        "degradation_currents": currents,
        "resilience_topology": topology,
        "adaptive_equilibrium": equilibrium,
        "ecosystem_forecast": forecast,
        "ecosystem_governance": governance,
        "ecosystem_metrics": metrics,
        "runtime_ecology": ecology,
    }
    if persist_memory:
        memory_summary = update_ecosystem_memory(payload, current_result, path=memory_path, timestamp=timestamp)
    else:
        memory_summary = prior_memory_summary
    current_result["ecosystem_memory"] = memory_summary
    previous_entry = dict((prior_memory_summary.get("recent_entries") or [])[-1] or {}) if prior_memory_summary.get("recent_entries") else {}
    current_result["ecosystem_events"] = build_ecosystem_events(
        balance_state=str(balance.get("balance_state") or ""),
        pressure_direction=str(pressure.get("pressure_direction") or ""),
        equilibrium_state=str(equilibrium.get("equilibrium_state") or ""),
        topology=str(topology.get("topology") or ""),
        degradation_current=str(currents.get("current") or ""),
        previous_balance_state=str(previous_entry.get("balance_state") or ""),
        previous_topology=str(previous_entry.get("topology") or ""),
    )
    return current_result
