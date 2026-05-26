from __future__ import annotations

from typing import Any, Mapping

from .resonance_equilibrium import build_resonance_equilibrium
from .resonance_events import build_resonance_events
from .resonance_governance import build_resonance_governance
from .resonance_harmony import build_resonance_harmony
from .resonance_interference import build_resonance_interference
from .resonance_memory import build_resonance_memory_summary, load_resonance_memory, update_resonance_memory
from .resonance_metrics import build_resonance_metrics
from .resonance_projection import build_resonance_projection
from .resonance_snapshot import build_runtime_resonance_snapshot
from .resonance_sync import build_resonance_sync


def build_runtime_resonance(
    orchestration: Mapping[str, Any] | None,
    *,
    persist_memory: bool = True,
    memory_path=None,
    timestamp: str = "",
) -> dict[str, Any]:
    payload = dict(orchestration or {})
    prior_memory = build_resonance_memory_summary(load_resonance_memory(path=memory_path), current_context=payload)
    prior_entries = [dict(item) for item in prior_memory.get("recent_entries") or [] if isinstance(item, Mapping)]
    previous_entry = prior_entries[-1] if prior_entries else {}

    federation_metrics = _as_mapping(payload.get("federation_metrics"))
    temporal_metrics = _as_mapping(payload.get("temporal_metrics"))
    temporal_recovery = _as_mapping(payload.get("temporal_recovery"))
    consciousness_metrics = _as_mapping(payload.get("consciousness_metrics"))
    cinematic_metrics = _as_mapping(payload.get("cinematic_metrics"))
    instinct_metrics = _as_mapping(payload.get("instinct_metrics"))
    subconscious_metrics = _as_mapping(payload.get("subconscious_metrics"))

    federation_harmony = _nested_metric(federation_metrics, "federation_harmony", payload.get("federation_harmony"))
    federation_coherence = _nested_metric(federation_metrics, "federation_coherence", payload.get("federation_coherence"))
    federation_alignment = _nested_metric(federation_metrics, "federation_alignment", payload.get("federation_alignment"))
    federation_divergence = _nested_metric(federation_metrics, "federation_divergence", payload.get("federation_divergence"))
    temporal_stability = _nested_metric(temporal_metrics, "temporal_stability", payload.get("temporal_stability"))
    temporal_alignment = _nested_metric(temporal_metrics, "temporal_alignment", payload.get("temporal_alignment"))
    temporal_pressure = _nested_metric(temporal_metrics, "temporal_pressure", payload.get("temporal_pressure"))
    consciousness_clarity = _nested_metric(consciousness_metrics, "orchestration_clarity")
    awareness_integrity = _nested_metric(consciousness_metrics, "awareness_integrity")
    cinematic_quality = _nested_metric(cinematic_metrics, "cinematic_quality")
    cinematic_immersion = _nested_metric(cinematic_metrics, "immersion_depth")
    instinct_pressure = _nested_metric(instinct_metrics, "survival_pressure")
    instinct_integrity = _nested_metric(instinct_metrics, "instinct_integrity")
    subconscious_integrity = _nested_metric(subconscious_metrics, "subconscious_integrity")

    resonance_pressure = _clamp(
        int(
            round(
                (temporal_pressure * 0.34)
                + (federation_divergence * 0.24)
                + (instinct_pressure * 0.22)
                + ((100 - temporal_stability) * 0.08)
                + ((100 - subconscious_integrity) * 0.06)
                + ((100 - awareness_integrity) * 0.06)
            )
        )
    )
    resonance_alignment = _clamp(
        int(
            round(
                (temporal_alignment * 0.26)
                + (federation_alignment * 0.24)
                + (federation_coherence * 0.18)
                + (consciousness_clarity * 0.16)
                + (cinematic_quality * 0.16)
            )
        )
    )
    resonance_integrity = _clamp(
        int(
            round(
                (temporal_stability * 0.22)
                + (federation_coherence * 0.18)
                + (awareness_integrity * 0.16)
                + (instinct_integrity * 0.14)
                + (cinematic_quality * 0.14)
                + (subconscious_integrity * 0.16)
                - (resonance_pressure * 0.18)
            )
        )
    )

    interference = build_resonance_interference(
        temporal_pressure=temporal_pressure,
        federation_divergence=federation_divergence,
        instinct_pressure=instinct_pressure,
        subconscious_integrity=subconscious_integrity,
        consciousness_clarity=consciousness_clarity,
        temporal_stability=temporal_stability,
    )
    resonance_fragmentation = _clamp(int(interference.get("fragmentation_index", 0) or 0))
    resonance_cohesion = _clamp(
        int(
            round(
                (federation_harmony * 0.24)
                + (cinematic_immersion * 0.18)
                + (consciousness_clarity * 0.16)
                + (temporal_stability * 0.16)
                + (subconscious_integrity * 0.12)
                + (instinct_integrity * 0.14)
                - (resonance_fragmentation * 0.24)
            )
        )
    )
    resonance_stability = _clamp(
        int(
            round(
                (resonance_integrity * 0.28)
                + (resonance_alignment * 0.22)
                + (resonance_cohesion * 0.22)
                + (temporal_stability * 0.16)
                + (federation_coherence * 0.12)
                - (resonance_pressure * 0.24)
            )
        )
    )

    harmony = build_resonance_harmony(
        federation_harmony=federation_harmony,
        cinematic_quality=cinematic_quality,
        cinematic_immersion=cinematic_immersion,
        consciousness_clarity=consciousness_clarity,
        instinct_pressure=instinct_pressure,
        temporal_stability=temporal_stability,
        resonance_fragmentation=resonance_fragmentation,
    )
    sync = build_resonance_sync(
        temporal_stability=temporal_stability,
        temporal_alignment=temporal_alignment,
        federation_alignment=federation_alignment,
        federation_coherence=federation_coherence,
        consciousness_clarity=consciousness_clarity,
        cinematic_quality=cinematic_quality,
        resonance_pressure=resonance_pressure,
        resonance_fragmentation=resonance_fragmentation,
    )
    sync_drift = _clamp(int(sync.get("sync_drift", 0) or 0))
    adaptive_sync_balance = _clamp(
        int(
            round(
                (resonance_stability * 0.22)
                + (resonance_alignment * 0.18)
                + (resonance_cohesion * 0.16)
                + ((100 - sync_drift) * 0.16)
                + ((100 - resonance_pressure) * 0.14)
                + ((100 - resonance_fragmentation) * 0.14)
            )
        )
    )
    runtime_harmony_index = _clamp(
        int(
            round(
                (resonance_cohesion * 0.26)
                + (resonance_alignment * 0.22)
                + (resonance_stability * 0.2)
                + ((100 - sync_drift) * 0.16)
                + (federation_harmony * 0.16)
            )
        )
    )
    equilibrium = build_resonance_equilibrium(
        resonance_stability=resonance_stability,
        resonance_pressure=resonance_pressure,
        resonance_fragmentation=resonance_fragmentation,
        sync_drift=sync_drift,
        adaptive_sync_balance=adaptive_sync_balance,
    )
    resonance_phase = _derive_phase(
        resonance_stability=resonance_stability,
        resonance_fragmentation=resonance_fragmentation,
        sync_drift=sync_drift,
        resonance_pressure=resonance_pressure,
        orchestration_resonance=str(sync.get("orchestration_resonance") or ""),
        cinematic_resonance=str(harmony.get("cinematic_resonance") or ""),
    )
    resonance_recovery = _build_recovery(
        resonance_stability=resonance_stability,
        resonance_pressure=resonance_pressure,
        resonance_fragmentation=resonance_fragmentation,
        sync_drift=sync_drift,
        temporal_recovery=temporal_recovery,
        adaptive_sync_balance=adaptive_sync_balance,
    )
    projection = build_resonance_projection(
        resonance_phase=resonance_phase,
        sync_drift=sync_drift,
        resonance_fragmentation=resonance_fragmentation,
        recovery_velocity=str(resonance_recovery.get("adaptive_sync_recovery") or ""),
        harmonic_runtime_state=str(equilibrium.get("equilibrium_state") or ""),
        cinematic_resonance=str(harmony.get("cinematic_resonance") or ""),
        prior_phase=str(previous_entry.get("resonance_phase") or ""),
    )
    governance = build_resonance_governance(
        resonance_phase=resonance_phase,
        sync_state=str(sync.get("sync_state") or ""),
        equilibrium_state=str(equilibrium.get("equilibrium_state") or ""),
        resonance_fragmentation=resonance_fragmentation,
        sync_drift=sync_drift,
        resonance_pressure=resonance_pressure,
    )
    metrics = build_resonance_metrics(
        resonance_stability=resonance_stability,
        resonance_alignment=resonance_alignment,
        resonance_integrity=resonance_integrity,
        resonance_pressure=resonance_pressure,
        resonance_fragmentation=resonance_fragmentation,
        resonance_cohesion=resonance_cohesion,
        sync_drift=sync_drift,
        runtime_harmony_index=runtime_harmony_index,
        adaptive_sync_balance=adaptive_sync_balance,
    )
    state = {
        "state": "resonance_stable" if resonance_stability >= 68 and sync_drift < 40 else "resonance_balancing",
        "resonance_phase": resonance_phase,
        "harmonic_runtime_state": str(equilibrium.get("equilibrium_state") or "measured_harmonic_balance"),
        "interference_state": str(interference.get("interference_state") or "resonance_interference_managed"),
        "sync_state": str(sync.get("sync_state") or "synchronized"),
    }

    current_result = {
        "runtime_resonance": build_runtime_resonance_snapshot(
            resonance_state=state,
            resonance_harmony=harmony,
            resonance_sync=sync,
            resonance_projection=projection,
            resonance_metrics=metrics,
        ),
        "resonance_state": state,
        "resonance_harmony": harmony,
        "resonance_sync": sync,
        "resonance_projection": projection,
        "resonance_equilibrium": equilibrium,
        "resonance_governance": governance,
        "resonance_metrics": metrics,
        "resonance_integrity": resonance_integrity,
        "resonance_alignment": resonance_alignment,
        "resonance_stability": resonance_stability,
        "resonance_pressure": resonance_pressure,
        "resonance_fragmentation": resonance_fragmentation,
        "resonance_cohesion": resonance_cohesion,
        "resonance_recovery": resonance_recovery,
        "harmonic_runtime_state": str(equilibrium.get("equilibrium_state") or "measured_harmonic_balance"),
        "cinematic_resonance": str(harmony.get("cinematic_resonance") or "measured_cinematic_resonance"),
        "orchestration_resonance": str(sync.get("orchestration_resonance") or "moderate"),
        "adaptive_sync_balance": adaptive_sync_balance,
        "resonance_phase": resonance_phase,
        "sync_drift": sync_drift,
        "runtime_harmony_index": runtime_harmony_index,
    }
    if persist_memory:
        memory_summary = update_resonance_memory(payload, current_result, path=memory_path, timestamp=timestamp)
    else:
        memory_summary = prior_memory
    current_result["resonance_memory_summary"] = memory_summary
    current_result["resonance_events"] = build_resonance_events(
        resonance_phase=resonance_phase,
        harmonic_runtime_state=str(current_result.get("harmonic_runtime_state") or ""),
        cinematic_resonance=str(current_result.get("cinematic_resonance") or ""),
        sync_drift=sync_drift,
        resonance_fragmentation=resonance_fragmentation,
        previous_phase=str(previous_entry.get("resonance_phase") or ""),
    )
    return current_result


def _build_recovery(
    *,
    resonance_stability: int,
    resonance_pressure: int,
    resonance_fragmentation: int,
    sync_drift: int,
    temporal_recovery: Mapping[str, Any],
    adaptive_sync_balance: int,
) -> dict[str, Any]:
    recovery_score = _clamp(
        int(
            round(
                (resonance_stability * 0.24)
                + (adaptive_sync_balance * 0.26)
                + ((100 - resonance_pressure) * 0.2)
                + ((100 - resonance_fragmentation) * 0.16)
                + ((100 - sync_drift) * 0.14)
            )
        )
    )
    temporal_velocity = str(temporal_recovery.get("adaptive_recovery_velocity") or "")
    if recovery_score >= 72 or temporal_velocity == "strong":
        velocity = "strong"
    elif recovery_score >= 52 or temporal_velocity == "adaptive":
        velocity = "adaptive"
    else:
        velocity = "guarded"
    return {
        "recovery_score": recovery_score,
        "adaptive_sync_recovery": velocity,
        "recovery_bias": "harmonic_realignment" if resonance_fragmentation >= 48 else "continuity_preservation",
    }


def _derive_phase(
    *,
    resonance_stability: int,
    resonance_fragmentation: int,
    sync_drift: int,
    resonance_pressure: int,
    orchestration_resonance: str,
    cinematic_resonance: str,
) -> str:
    if resonance_fragmentation >= 68 or sync_drift >= 70:
        return "fractured_resonance"
    if resonance_pressure >= 60 or "strained" in str(orchestration_resonance or "") or "fragmenting" in str(cinematic_resonance or ""):
        return "strained_harmony"
    if resonance_stability >= 72 and sync_drift < 32:
        return "harmonic_convergence"
    return "adaptive_equilibrium"


def _nested_metric(mapping: Mapping[str, Any], key: str, fallback: Any = 0) -> int:
    return _clamp(_safe_int(mapping.get(key, fallback)))


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _clamp(value: int) -> int:
    return max(0, min(100, int(value or 0)))


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0
