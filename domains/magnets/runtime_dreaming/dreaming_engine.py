from __future__ import annotations

from typing import Any, Mapping

from .adaptive_dreaming import build_adaptive_dreaming
from .cinematic_dreams import build_cinematic_dreams
from .continuity_dreams import build_continuity_dreams
from .dormant_pathways import build_dormant_pathways
from .dream_events import build_dream_events
from .dream_forecasting import build_dream_forecast
from .dream_governance import build_dream_governance
from .dream_metrics import build_dream_metrics
from .dreaming_memory import build_dreaming_memory_summary, load_dreaming_memory, update_dreaming_memory
from .latent_projection import build_latent_projection
from .orchestration_visions import build_orchestration_visions
from .resilience_dreams import build_resilience_dreams
from .runtime_mirroring import build_runtime_mirroring
from .stabilization_dreams import build_stabilization_dreams
from .subconscious_projection import build_subconscious_projection


def build_runtime_dreaming(
    orchestration: Mapping[str, Any] | None,
    *,
    persist_memory: bool = True,
    memory_path=None,
    timestamp: str = "",
) -> dict[str, Any]:
    payload = dict(orchestration or {})
    prior_memory_summary = build_dreaming_memory_summary(load_dreaming_memory(path=memory_path), current_context=payload)
    execution_metrics = dict(payload.get("execution_metrics") or {})
    coordination_metrics = dict(payload.get("coordination_metrics") or {})
    continuity_state = dict(payload.get("continuity_state") or {})
    runtime_predictions = dict(payload.get("runtime_predictions") or {})
    resilience_topology = dict(payload.get("resilience_topology") or {})
    runtime_subconscious = dict(payload.get("runtime_subconscious") or {})
    latent_patterns = dict(payload.get("latent_patterns") or {})
    hidden_equilibrium = dict(payload.get("hidden_equilibrium") or {})
    continuity_underlayers = dict(payload.get("continuity_underlayers") or {})
    orchestration_residue = dict(payload.get("orchestration_residue") or {})
    dormant_resilience = dict(payload.get("dormant_resilience") or {})
    cinematic_underflow = dict(payload.get("cinematic_underflow") or {})
    silent_adaptation = dict(payload.get("silent_adaptation") or {})
    stabilization_instinct = dict(payload.get("stabilization_instinct") or {})
    continuity_instinct = dict(payload.get("continuity_instinct") or {})
    cinematic_instinct = dict(payload.get("cinematic_instinct") or {})
    cinematic_direction = dict(payload.get("cinematic_direction") or {})
    subconscious_metrics = dict(payload.get("subconscious_metrics") or {})
    cinematic_metrics = dict(payload.get("cinematic_metrics") or {})
    orchestration_pressure = dict(payload.get("orchestration_pressure") or {})

    degradation_risk = int(execution_metrics.get("degradation_risk", 0) or 0)
    runtime_resilience = int(coordination_metrics.get("runtime_resilience", 0) or 0)
    continuity_confidence = int(continuity_state.get("continuity_confidence", 0) or 0)
    prediction_confidence = int(runtime_predictions.get("prediction_confidence", 0) or 0)
    cinematic_quality = int(cinematic_metrics.get("cinematic_quality", 0) or 0)
    dreaming_pressure = int(orchestration_pressure.get("pressure_score", 0) or 0) + max(0, 60 - prediction_confidence)

    cinematic = build_cinematic_dreams(
        cinematic_quality=cinematic_quality,
        cinematic_underflow=str(cinematic_underflow.get("state") or ""),
        cinematic_instinct=str(cinematic_instinct.get("state") or ""),
        cinematic_direction=str(cinematic_direction.get("style") or ""),
    )
    visions = build_orchestration_visions(
        stabilization_state=str(stabilization_instinct.get("state") or ""),
        fallback_state=str((payload.get("fallback_instinct") or {}).get("state") or ""),
        continuity_state=str((payload.get("continuity_dreams") or {}).get("state") or continuity_underlayers.get("state") or ""),
        cinematic_state=str(cinematic.get("state") or ""),
        residue_pattern=str(orchestration_residue.get("pattern") or ""),
    )
    latent = build_latent_projection(
        latent_pattern=str(latent_patterns.get("pattern") or ""),
        hidden_equilibrium=str(hidden_equilibrium.get("state") or ""),
        dreaming_pressure=dreaming_pressure,
        dormant_resilience=str(dormant_resilience.get("state") or ""),
    )
    stabilization = build_stabilization_dreams(
        stabilization_state=str(stabilization_instinct.get("state") or ""),
        latent_projection=str(latent.get("state") or ""),
        cinematic_dream=str(cinematic.get("state") or ""),
        hidden_equilibrium=str(hidden_equilibrium.get("state") or ""),
    )
    resilience = build_resilience_dreams(
        dormant_resilience=str(dormant_resilience.get("state") or ""),
        runtime_resilience=runtime_resilience,
        degradation_risk=degradation_risk,
    )
    continuity = build_continuity_dreams(
        continuity_underlayers=str(continuity_underlayers.get("state") or ""),
        continuity_instinct=str(continuity_instinct.get("state") or ""),
        continuity_confidence=continuity_confidence,
        switch_frequency=int((payload.get("adaptation_history") or {}).get("switch_frequency", 0) or 0),
    )
    subconscious = build_subconscious_projection(
        hidden_equilibrium=str(hidden_equilibrium.get("state") or ""),
        latent_pattern=str(latent_patterns.get("pattern") or ""),
        cinematic_underflow=str(cinematic_underflow.get("state") or ""),
        residue_pattern=str(orchestration_residue.get("pattern") or ""),
    )
    pathways = build_dormant_pathways(
        dormant_resilience=str(dormant_resilience.get("state") or ""),
        continuity_dream=str(continuity.get("state") or ""),
        cinematic_dream=str(cinematic.get("state") or ""),
        latent_projection=str(latent.get("state") or ""),
    )
    adaptive = build_adaptive_dreaming(
        adaptive_state=str(silent_adaptation.get("state") or ""),
        continuity_dream=str(continuity.get("state") or ""),
        cinematic_dream=str(cinematic.get("state") or ""),
        latent_projection=str(latent.get("state") or ""),
    )
    mirroring = build_runtime_mirroring(
        orchestration_vision=str(visions.get("vision") or ""),
        cinematic_dream=str(cinematic.get("state") or ""),
        continuity_dream=str(continuity.get("state") or ""),
        hidden_equilibrium=str(hidden_equilibrium.get("state") or ""),
    )
    balance = int(
        (int(subconscious_metrics.get("subconscious_balance", 0) or 0) * 0.4)
        + (int(latent.get("latent_projection_stability", 0) or 0) * 0.3)
        + (int(mirroring.get("runtime_mirroring_integrity", 0) or 0) * 0.3)
    )
    metrics = build_dream_metrics(
        cinematic_projection_strength=int(cinematic.get("cinematic_projection_strength", 0) or 0),
        latent_projection_stability=int(latent.get("latent_projection_stability", 0) or 0),
        dormant_pathway_strength=int(pathways.get("dormant_pathway_strength", 0) or 0),
        adaptive_dreaming_strength=int(adaptive.get("adaptive_dreaming_strength", 0) or 0),
        runtime_mirroring_integrity=int(mirroring.get("runtime_mirroring_integrity", 0) or 0),
        continuity_projection_strength=int(continuity.get("continuity_projection_strength", 0) or 0),
        orchestration_dream_balance=balance,
    )
    forecast = build_dream_forecast(
        cinematic_dream=str(cinematic.get("state") or ""),
        stabilization_dream=str(stabilization.get("state") or ""),
        latent_projection=str(latent.get("state") or ""),
        dormant_pathway=str(pathways.get("state") or ""),
        adaptive_dreaming=str(adaptive.get("state") or ""),
        orchestration_vision=str(visions.get("vision") or ""),
    )
    governance = build_dream_governance(
        cinematic_dream=str(cinematic.get("state") or ""),
        latent_projection=str(latent.get("state") or ""),
        dormant_pathway=str(pathways.get("state") or ""),
        adaptive_dreaming=str(adaptive.get("state") or ""),
        continuity_dream=str(continuity.get("state") or ""),
        resilience_dream=str(resilience.get("state") or ""),
    )

    current_result = {
        "runtime_dreaming": {
            "dreaming_state": "persistent_orchestration_dreaming",
            "subconscious_anchor": str(runtime_subconscious.get("subconscious_state") or "persistent_orchestration_subconscious"),
            "cinematic_anchor": str(cinematic.get("state") or "adaptive_cinema_dream"),
            "projection_anchor": str(latent.get("state") or "latent_stability_projection"),
            "mirroring_anchor": str(mirroring.get("state") or "resilient_mirroring"),
        },
        "cinematic_dreams": cinematic,
        "orchestration_visions": visions,
        "latent_projection": latent,
        "stabilization_dreams": stabilization,
        "resilience_dreams": resilience,
        "continuity_dreams": continuity,
        "subconscious_projection": subconscious,
        "dormant_pathways": pathways,
        "adaptive_dreaming": adaptive,
        "runtime_mirroring": mirroring,
        "dream_forecast": forecast,
        "dream_governance": governance,
        "dream_metrics": metrics,
    }
    if persist_memory:
        memory_summary = update_dreaming_memory(payload, current_result, path=memory_path, timestamp=timestamp)
    else:
        memory_summary = prior_memory_summary
    current_result["dreaming_memory"] = memory_summary
    previous_entry = dict((prior_memory_summary.get("recent_entries") or [])[-1] or {}) if prior_memory_summary.get("recent_entries") else {}
    current_result["dream_events"] = build_dream_events(
        cinematic_dream=str(cinematic.get("state") or ""),
        latent_projection=str(latent.get("state") or ""),
        dormant_pathway=str(pathways.get("state") or ""),
        adaptive_dreaming=str(adaptive.get("state") or ""),
        runtime_mirroring=str(mirroring.get("state") or ""),
        dream_balance=int(metrics.get("orchestration_dream_balance", 0) or 0),
        previous_cinematic_dream=str(previous_entry.get("cinematic_dream") or ""),
    )
    return current_result
