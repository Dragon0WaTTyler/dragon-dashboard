from __future__ import annotations

from typing import Any, Mapping

from .symbiosis_balance import build_symbiosis_balance
from .symbiosis_cooperation import build_symbiosis_cooperation
from .symbiosis_dependencies import build_symbiosis_dependencies
from .symbiosis_equilibrium import build_symbiosis_equilibrium
from .symbiosis_events import build_symbiosis_events
from .symbiosis_governance import build_symbiosis_governance
from .symbiosis_memory import (
    build_symbiosis_memory_summary,
    load_symbiosis_memory,
    update_symbiosis_memory,
)
from .symbiosis_metrics import build_symbiosis_metrics
from .symbiosis_projection import build_symbiosis_projection
from .symbiosis_recovery import build_symbiosis_recovery
from .symbiosis_snapshot import build_runtime_symbiosis_snapshot


def build_runtime_symbiosis(
    orchestration: Mapping[str, Any] | None,
    *,
    persist_memory: bool = True,
    memory_path=None,
    timestamp: str = "",
) -> dict[str, Any]:
    payload = dict(orchestration or {})
    prior_memory = build_symbiosis_memory_summary(load_symbiosis_memory(path=memory_path), current_context=payload)
    prior_entries = [dict(item) for item in prior_memory.get("recent_entries") or [] if isinstance(item, Mapping)]
    previous_entry = prior_entries[-1] if prior_entries else {}

    federation_metrics = _as_mapping(payload.get("federation_metrics"))
    temporal_metrics = _as_mapping(payload.get("temporal_metrics"))
    temporal_recovery = _as_mapping(payload.get("temporal_recovery"))
    resonance_metrics = _as_mapping(payload.get("resonance_metrics"))
    resonance_recovery = _as_mapping(payload.get("resonance_recovery"))
    ecosystem_metrics = _as_mapping(payload.get("ecosystem_metrics"))
    degradation_currents = _as_mapping(payload.get("degradation_currents"))
    orchestration_pressure = _as_mapping(payload.get("orchestration_pressure"))
    federation_continuity = _as_mapping(payload.get("federation_continuity"))
    resonance_equilibrium = _as_mapping(payload.get("resonance_equilibrium"))

    federation_coherence = _metric(federation_metrics, "federation_coherence", payload.get("federation_coherence"))
    federation_alignment = _metric(federation_metrics, "federation_alignment", payload.get("federation_alignment"))
    federation_divergence = _metric(federation_metrics, "federation_divergence", payload.get("federation_divergence"))
    federation_resilience = _metric(federation_metrics, "federation_resilience", payload.get("federation_resilience"))
    federation_integrity = _metric(federation_metrics, "federation_integrity", payload.get("federation_integrity"))
    temporal_stability = _metric(temporal_metrics, "temporal_stability", payload.get("temporal_stability"))
    temporal_alignment = _metric(temporal_metrics, "temporal_alignment", payload.get("temporal_alignment"))
    temporal_pressure = _metric(temporal_metrics, "temporal_pressure", payload.get("temporal_pressure"))
    temporal_integrity = _metric(temporal_metrics, "temporal_integrity", payload.get("temporal_integrity"))
    resonance_stability = _metric(resonance_metrics, "resonance_stability", payload.get("resonance_stability"))
    resonance_alignment = _metric(resonance_metrics, "resonance_alignment", payload.get("resonance_alignment"))
    resonance_pressure = _metric(resonance_metrics, "resonance_pressure", payload.get("resonance_pressure"))
    resonance_fragmentation = _metric(resonance_metrics, "resonance_fragmentation", payload.get("resonance_fragmentation"))
    resonance_cohesion = _metric(resonance_metrics, "resonance_cohesion", payload.get("resonance_cohesion"))
    resonance_integrity = _metric(resonance_metrics, "resonance_integrity", payload.get("resonance_integrity"))
    ecosystem_stability = _metric(ecosystem_metrics, "ecosystem_stability")
    ecosystem_integrity = _metric(ecosystem_metrics, "ecosystem_integrity")
    ecosystem_pressure = _metric(ecosystem_metrics, "orchestration_pressure_score", orchestration_pressure.get("pressure_score"))
    ecosystem_degradation = _metric(ecosystem_metrics, "degradation_risk", payload.get("degradation_risk"))

    temporal_recovery_score = _metric(temporal_recovery, "recovery_score")
    resonance_recovery_score = _metric(resonance_recovery, "recovery_score")
    temporal_recovery_velocity = _first_string(temporal_recovery, "adaptive_recovery_velocity")
    ecosystem_current = _first_string(degradation_currents, "current")
    continuity_projection = _first_string(federation_continuity, "continuity_projection", "runtime_continuity_profile")
    harmonic_runtime_state = _first_string(resonance_equilibrium, "equilibrium_state")

    dependencies = build_symbiosis_dependencies(
        federation_divergence=federation_divergence,
        resonance_fragmentation=resonance_fragmentation,
        resonance_pressure=resonance_pressure,
        temporal_pressure=temporal_pressure,
        ecosystem_pressure=ecosystem_pressure,
        ecosystem_degradation=ecosystem_degradation,
        recovery_velocity=temporal_recovery_velocity,
    )
    dependency_stress = _metric(dependencies, "dependency_stress")

    symbiosis_pressure = _clamp(
        int(
            round(
                (dependency_stress * 0.36)
                + (ecosystem_pressure * 0.18)
                + (resonance_pressure * 0.16)
                + (temporal_pressure * 0.14)
                + (federation_divergence * 0.16)
            )
        )
    )
    symbiosis_fragmentation = _clamp(
        int(
            round(
                (resonance_fragmentation * 0.34)
                + (federation_divergence * 0.24)
                + ((100 - ecosystem_integrity) * 0.2)
                + ((100 - federation_integrity) * 0.12)
                + (12 if _contains_any(ecosystem_current, "degrad", "cascade", "collapse") else 0)
            )
        )
    )
    symbiosis_alignment = _clamp(
        int(
            round(
                (federation_alignment * 0.28)
                + (resonance_alignment * 0.28)
                + (temporal_alignment * 0.22)
                + (ecosystem_integrity * 0.12)
                + ((100 - dependency_stress) * 0.1)
            )
        )
    )
    symbiosis_mutualism = _clamp(
        int(
            round(
                (federation_coherence * 0.22)
                + (resonance_cohesion * 0.24)
                + (temporal_stability * 0.18)
                + (ecosystem_stability * 0.14)
                + (federation_resilience * 0.12)
                + ((100 - dependency_stress) * 0.1)
            )
        )
    )

    balance = build_symbiosis_balance(
        federation_coherence=federation_coherence,
        federation_alignment=federation_alignment,
        resonance_alignment=resonance_alignment,
        temporal_alignment=temporal_alignment,
        ecosystem_integrity=ecosystem_integrity,
        pressure=symbiosis_pressure,
        fragmentation=symbiosis_fragmentation,
    )
    recovery = build_symbiosis_recovery(
        temporal_recovery_score=temporal_recovery_score,
        resonance_recovery_score=resonance_recovery_score,
        federation_resilience=federation_resilience,
        ecosystem_stability=ecosystem_stability,
        dependency_stress=dependency_stress,
        temporal_recovery_velocity=temporal_recovery_velocity,
    )
    cooperation = build_symbiosis_cooperation(
        federation_coherence=federation_coherence,
        federation_resilience=federation_resilience,
        resonance_cohesion=resonance_cohesion,
        temporal_stability=temporal_stability,
        recovery_score=_metric(recovery, "recovery_cohesion_score"),
        ecosystem_stability=ecosystem_stability,
        dependency_stress=dependency_stress,
    )
    equilibrium = build_symbiosis_equilibrium(
        balance_score=_metric(balance, "balance_score"),
        cooperation_score=_metric(cooperation, "cooperation_score"),
        mutualism=symbiosis_mutualism,
        dependency_stress=dependency_stress,
        fragmentation=symbiosis_fragmentation,
    )

    symbiosis_integrity = _clamp(
        int(
            round(
                (federation_integrity * 0.22)
                + (temporal_integrity * 0.2)
                + (resonance_integrity * 0.22)
                + (ecosystem_integrity * 0.16)
                + (_metric(equilibrium, "equilibrium_score") * 0.12)
                + (_metric(recovery, "recovery_cohesion_score") * 0.08)
                - (symbiosis_fragmentation * 0.16)
            )
        )
    )
    symbiosis_stability = _clamp(
        int(
            round(
                (symbiosis_integrity * 0.24)
                + (symbiosis_alignment * 0.18)
                + (symbiosis_mutualism * 0.18)
                + (_metric(balance, "balance_score") * 0.14)
                + (_metric(cooperation, "cooperation_score") * 0.12)
                + (_metric(recovery, "recovery_cohesion_score") * 0.1)
                - (symbiosis_pressure * 0.2)
            )
        )
    )
    systemic_runtime_health_index = _clamp(
        int(
            round(
                (symbiosis_stability * 0.24)
                + (symbiosis_integrity * 0.2)
                + (_metric(recovery, "recovery_cohesion_score") * 0.18)
                + (ecosystem_stability * 0.12)
                + (federation_resilience * 0.1)
                + ((100 - dependency_stress) * 0.08)
                + ((100 - symbiosis_fragmentation) * 0.08)
            )
        )
    )

    symbiotic_phase = _derive_phase(
        symbiosis_stability=symbiosis_stability,
        dependency_stress=dependency_stress,
        fragmentation=symbiosis_fragmentation,
        temporal_recovery_velocity=temporal_recovery_velocity,
        ecosystem_current=ecosystem_current,
    )
    runtime_coexistence = _derive_runtime_coexistence(
        symbiotic_phase=symbiotic_phase,
        harmonic_runtime_state=harmonic_runtime_state,
        continuity_projection=continuity_projection,
        cooperation_state=_first_string(cooperation, "cooperation_state"),
    )
    systemic_runtime_health = _derive_systemic_health(
        health_index=systemic_runtime_health_index,
        dependency_stress=dependency_stress,
        temporal_recovery_velocity=temporal_recovery_velocity,
        fragmentation=symbiosis_fragmentation,
    )
    projection = build_symbiosis_projection(
        symbiotic_phase=symbiotic_phase,
        systemic_runtime_health=systemic_runtime_health,
        dependency_stress=dependency_stress,
        recovery_cohesion=_first_string(recovery, "recovery_cohesion"),
        fragmentation=symbiosis_fragmentation,
        prior_phase=str(previous_entry.get("symbiotic_phase") or ""),
    )
    governance = build_symbiosis_governance(
        symbiotic_phase=symbiotic_phase,
        dependency_state=_first_string(dependencies, "dependency_state"),
        recovery_mode=_first_string(recovery, "recovery_mode"),
        fragmentation=symbiosis_fragmentation,
        pressure=symbiosis_pressure,
    )
    metrics = build_symbiosis_metrics(
        symbiosis_stability=symbiosis_stability,
        symbiosis_alignment=symbiosis_alignment,
        symbiosis_integrity=symbiosis_integrity,
        symbiosis_pressure=symbiosis_pressure,
        symbiosis_mutualism=symbiosis_mutualism,
        symbiosis_fragmentation=symbiosis_fragmentation,
        dependency_stress=dependency_stress,
        systemic_runtime_health_index=systemic_runtime_health_index,
        recovery_cohesion_score=_metric(recovery, "recovery_cohesion_score"),
    )
    state = {
        "state": "symbiosis_stable" if symbiosis_stability >= 68 and dependency_stress < 52 else "symbiosis_balancing",
        "symbiotic_phase": symbiotic_phase,
        "runtime_coexistence": runtime_coexistence,
        "systemic_runtime_health": systemic_runtime_health,
        "cooperative_runtime_state": str(cooperation.get("cooperative_runtime_state") or "adaptive_coexistence"),
    }

    current_result = {
        "runtime_symbiosis": build_runtime_symbiosis_snapshot(
            symbiosis_state=state,
            symbiosis_projection=projection,
            symbiosis_metrics=metrics,
        ),
        "symbiosis_state": state,
        "symbiosis_balance": balance,
        "symbiosis_cooperation": cooperation,
        "symbiosis_dependencies": dependencies,
        "symbiosis_recovery": recovery,
        "symbiosis_projection": projection,
        "symbiosis_equilibrium": equilibrium,
        "symbiosis_governance": governance,
        "symbiosis_metrics": metrics,
        "symbiosis_integrity": symbiosis_integrity,
        "symbiosis_alignment": symbiosis_alignment,
        "symbiosis_stability": symbiosis_stability,
        "symbiosis_pressure": symbiosis_pressure,
        "symbiosis_mutualism": symbiosis_mutualism,
        "symbiosis_fragmentation": symbiosis_fragmentation,
        "cooperative_runtime_state": str(cooperation.get("cooperative_runtime_state") or "adaptive_coexistence"),
        "runtime_coexistence": runtime_coexistence,
        "adaptive_mutual_balance": str(balance.get("adaptive_mutual_balance") or "pressured"),
        "systemic_runtime_health": systemic_runtime_health,
        "recovery_cohesion": str(recovery.get("recovery_cohesion") or "guarded"),
        "dependency_stress": dependency_stress,
        "symbiotic_phase": symbiotic_phase,
    }
    if persist_memory:
        memory_summary = update_symbiosis_memory(payload, current_result, path=memory_path, timestamp=timestamp)
    else:
        memory_summary = prior_memory
    current_result["symbiosis_memory_summary"] = memory_summary
    current_result["symbiosis_events"] = build_symbiosis_events(
        symbiotic_phase=symbiotic_phase,
        runtime_coexistence=runtime_coexistence,
        cooperative_runtime_state=str(current_result.get("cooperative_runtime_state") or ""),
        dependency_stress=dependency_stress,
        systemic_runtime_health=systemic_runtime_health,
        previous_phase=str(previous_entry.get("symbiotic_phase") or ""),
    )
    return current_result


def _derive_phase(
    *,
    symbiosis_stability: int,
    dependency_stress: int,
    fragmentation: int,
    temporal_recovery_velocity: str,
    ecosystem_current: str,
) -> str:
    if fragmentation >= 70:
        return "fractured_symbiosis"
    if dependency_stress >= 62:
        return "strained_mutualism"
    if symbiosis_stability >= 72 and dependency_stress < 42:
        return "stable_mutualism"
    if temporal_recovery_velocity in {"strong", "adaptive", "improving"} or _contains_any(ecosystem_current, "localized", "contained"):
        return "adaptive_mutualism"
    return "measured_symbiosis"


def _derive_runtime_coexistence(
    *,
    symbiotic_phase: str,
    harmonic_runtime_state: str,
    continuity_projection: str,
    cooperation_state: str,
) -> str:
    if symbiotic_phase == "fractured_symbiosis":
        return "fragmented_runtime_coexistence"
    if symbiotic_phase == "strained_mutualism":
        return "pressured_runtime_coexistence"
    if "stable" in harmonic_runtime_state and "continuity" in continuity_projection:
        return "stable_runtime_coexistence"
    if cooperation_state == "cooperative":
        return "adaptive_runtime_coexistence"
    return "measured_runtime_coexistence"


def _derive_systemic_health(
    *,
    health_index: int,
    dependency_stress: int,
    temporal_recovery_velocity: str,
    fragmentation: int,
) -> str:
    if fragmentation >= 70:
        return "fractured"
    if temporal_recovery_velocity in {"strong", "adaptive", "improving"} and (dependency_stress >= 48 or health_index >= 58):
        return "recovering"
    if health_index >= 70 and dependency_stress < 48:
        return "stable"
    if dependency_stress >= 64:
        return "pressured"
    return "guarded"


def _metric(mapping: Mapping[str, Any], key: str, fallback: Any = 0) -> int:
    if key in mapping:
        return _clamp(_safe_int(mapping.get(key, 0)))
    return _clamp(_safe_int(fallback))


def _first_string(mapping: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        text = str(mapping.get(key) or "").strip()
        if text:
            return text
    for value in mapping.values():
        text = str(value or "").strip()
        if text and not text.isdigit():
            return text
    return ""


def _contains_any(value: str, *needles: str) -> bool:
    text = str(value or "").lower()
    return any(needle in text for needle in needles)


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _clamp(value: int) -> int:
    return max(0, min(100, int(value or 0)))


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0
