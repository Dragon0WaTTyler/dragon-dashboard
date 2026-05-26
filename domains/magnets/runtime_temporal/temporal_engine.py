from __future__ import annotations

from typing import Any, Mapping

from .temporal_continuity import build_temporal_continuity
from .temporal_cycles import build_runtime_cycle_phase
from .temporal_decay import build_temporal_decay
from .temporal_events import build_temporal_events
from .temporal_forecast import build_temporal_forecast
from .temporal_governance import build_temporal_governance
from .temporal_memory import build_temporal_memory_summary, load_temporal_memory, update_temporal_memory
from .temporal_metrics import build_temporal_metrics
from .temporal_recovery import build_temporal_recovery
from .temporal_rhythm import build_temporal_rhythm
from .temporal_snapshot import build_runtime_temporal_snapshot


def build_runtime_temporal(
    orchestration: Mapping[str, Any] | None,
    *,
    persist_memory: bool = True,
    memory_path=None,
    timestamp: str = "",
) -> dict[str, Any]:
    payload = dict(orchestration or {})
    prior_memory = build_temporal_memory_summary(load_temporal_memory(path=memory_path), current_context=payload)
    federation = _as_mapping(payload.get("runtime_federation"))
    federation_metrics = _as_mapping(payload.get("federation_metrics"))
    federation_continuity = _as_mapping(payload.get("federation_continuity"))
    federation_projection = _as_mapping(payload.get("federation_projection"))
    subconscious = _as_mapping(payload.get("runtime_subconscious"))
    subconscious_metrics = _as_mapping(payload.get("subconscious_metrics"))
    consciousness_focus = _as_mapping(payload.get("orchestration_focus"))
    consciousness_metrics = _as_mapping(payload.get("consciousness_metrics"))
    continuity_awareness = _as_mapping(payload.get("continuity_awareness"))
    dreaming = _as_mapping(payload.get("runtime_dreaming"))
    dream_forecast = _as_mapping(payload.get("dream_forecast"))
    dream_metrics = _as_mapping(payload.get("dream_metrics"))

    prior_entries = [dict(item) for item in prior_memory.get("recent_entries") or [] if isinstance(item, Mapping)]
    previous_entry = prior_entries[-1] if prior_entries else {}

    federation_pressure = _metric(payload, "federation_pressure")
    federation_alignment = _metric(payload, "federation_alignment")
    federation_integrity = _metric(payload, "federation_integrity")
    federation_resilience = _metric(payload, "federation_resilience")
    federation_divergence = _metric(payload, "federation_divergence")
    consciousness_clarity = _nested_metric(consciousness_metrics, "orchestration_clarity")
    awareness_integrity = _nested_metric(consciousness_metrics, "awareness_integrity")
    subconscious_integrity = _nested_metric(subconscious_metrics, "subconscious_integrity")
    subconscious_balance = _nested_metric(subconscious_metrics, "subconscious_balance")
    dreaming_integrity = _nested_metric(dream_metrics, "dreaming_integrity")
    dream_balance = _nested_metric(dream_metrics, "orchestration_dream_balance")
    dream_projection = _nested_metric(dream_metrics, "continuity_projection_strength")

    focused_consciousness = _contains_any(_first_string(consciousness_focus, "focus"), "focus", "clarity", "equilibrium")
    unstable_subconscious = _contains_any(_first_string(subconscious, "subconscious_state"), "unstable", "fragment", "volatile")
    optimistic_dreaming = _contains_any(_first_string(dream_forecast, "forecast"), "optim", "recover", "stabil", "continuity")

    temporal_pressure = _clamp(
        int(
            round(
                (federation_pressure * 0.42)
                + (federation_divergence * 0.2)
                + ((100 - subconscious_integrity) * 0.12)
                + ((100 - dreaming_integrity) * 0.08)
                + (10 if unstable_subconscious else 0)
            )
        )
    )
    temporal_alignment = _clamp(
        int(
            round(
                (federation_alignment * 0.4)
                + (awareness_integrity * 0.18)
                + (subconscious_balance * 0.16)
                + (dream_balance * 0.16)
                + (dream_projection * 0.1)
            )
        )
    )
    temporal_stability = _clamp(
        int(
            round(
                (federation_integrity * 0.28)
                + (federation_resilience * 0.22)
                + (consciousness_clarity * 0.18)
                + (subconscious_integrity * 0.16)
                + (dreaming_integrity * 0.16)
                - (temporal_pressure * 0.2)
            )
        )
    )
    temporal_momentum = _clamp(
        int(
            round(
                (federation_resilience * 0.28)
                + (consciousness_clarity * 0.2)
                + (dream_projection * 0.22)
                + (dream_balance * 0.12)
                + (8 if optimistic_dreaming else 0)
                - (federation_divergence * 0.1)
            )
        )
    )

    continuity = build_temporal_continuity(
        federation_continuity=str(federation_continuity.get("continuity_projection") or federation_projection.get("continuity_projection") or ""),
        subconscious_state=_first_string(subconscious, "subconscious_state"),
        continuity_awareness=_first_string(continuity_awareness, "state"),
        temporal_stability=temporal_stability,
        temporal_alignment=temporal_alignment,
        prior_continuity=str(previous_entry.get("continuity_state") or ""),
    )
    decay = build_temporal_decay(
        temporal_pressure=temporal_pressure,
        federation_divergence=federation_divergence,
        subconscious_integrity=subconscious_integrity,
        continuity_persistence=int(continuity.get("continuity_persistence", 0) or 0),
        prior_decay_rate=int(previous_entry.get("continuity_decay_rate", 0) or 0),
    )
    recovery = build_temporal_recovery(
        temporal_pressure=temporal_pressure,
        continuity_decay_rate=int(decay.get("continuity_decay_rate", 0) or 0),
        federation_resilience=federation_resilience,
        consciousness_clarity=consciousness_clarity,
        dreaming_integrity=dreaming_integrity,
        continuity_persistence=int(continuity.get("continuity_persistence", 0) or 0),
    )
    phase = build_runtime_cycle_phase(
        temporal_pressure=temporal_pressure,
        temporal_stability=temporal_stability,
        temporal_momentum=temporal_momentum,
        continuity_decay_rate=int(decay.get("continuity_decay_rate", 0) or 0),
        recovery_score=int(recovery.get("recovery_score", 0) or 0),
        focused_consciousness=focused_consciousness,
        unstable_subconscious=unstable_subconscious,
        optimistic_dreaming=optimistic_dreaming,
    )
    rhythm = build_temporal_rhythm(
        runtime_cycle_phase=str(phase.get("phase") or ""),
        temporal_momentum=temporal_momentum,
        temporal_pressure=temporal_pressure,
        continuity_decay_rate=int(decay.get("continuity_decay_rate", 0) or 0),
        cinematic_runtime_state=str(federation.get("cinematic_runtime_state") or payload.get("cinematic_runtime_state") or ""),
        consciousness_focus=_first_string(consciousness_focus, "focus"),
        dream_forecast=_first_string(dream_forecast, "forecast"),
    )
    adaptive_temporal_balance = _clamp(
        int(
            round(
                (temporal_stability * 0.28)
                + (temporal_alignment * 0.22)
                + (int(recovery.get("recovery_score", 0) or 0) * 0.2)
                + (int(rhythm.get("rhythm_strength", 0) or 0) * 0.14)
                - (int(decay.get("continuity_decay_rate", 0) or 0) * 0.14)
                - (temporal_pressure * 0.1)
            )
        )
    )
    temporal_integrity = _clamp(
        int(
            round(
                (federation_integrity * 0.34)
                + (int(continuity.get("continuity_persistence", 0) or 0) * 0.2)
                + (temporal_alignment * 0.18)
                + (int(recovery.get("recovery_score", 0) or 0) * 0.14)
                + ((100 - int(decay.get("continuity_decay_rate", 0) or 0)) * 0.14)
            )
        )
    )
    forecast = build_temporal_forecast(
        runtime_cycle_phase=str(phase.get("phase") or ""),
        runtime_rhythm_state=str(rhythm.get("rhythm_state") or ""),
        cinematic_temporal_flow=str(rhythm.get("cinematic_temporal_flow") or ""),
        continuity_decay_rate=int(decay.get("continuity_decay_rate", 0) or 0),
        adaptive_recovery_velocity=str(recovery.get("adaptive_recovery_velocity") or ""),
        temporal_alignment=temporal_alignment,
    )
    governance = build_temporal_governance(
        runtime_cycle_phase=str(phase.get("phase") or ""),
        continuity_decay_rate=int(decay.get("continuity_decay_rate", 0) or 0),
        adaptive_recovery_velocity=str(recovery.get("adaptive_recovery_velocity") or ""),
        runtime_rhythm_state=str(rhythm.get("rhythm_state") or ""),
        cinematic_temporal_flow=str(rhythm.get("cinematic_temporal_flow") or ""),
    )
    metrics = build_temporal_metrics(
        temporal_stability=temporal_stability,
        temporal_momentum=temporal_momentum,
        temporal_pressure=temporal_pressure,
        temporal_alignment=temporal_alignment,
        temporal_integrity=temporal_integrity,
        continuity_decay_rate=int(decay.get("continuity_decay_rate", 0) or 0),
        continuity_persistence=int(continuity.get("continuity_persistence", 0) or 0),
        recovery_score=int(recovery.get("recovery_score", 0) or 0),
        rhythm_strength=int(rhythm.get("rhythm_strength", 0) or 0),
        adaptive_temporal_balance=adaptive_temporal_balance,
    )

    temporal_state = {
        "state": "temporal_stable" if temporal_stability >= 66 and int(decay.get("continuity_decay_rate", 0) or 0) < 48 else "temporal_balancing",
        "federation_anchor": str(federation.get("state") or "federation_balancing"),
        "dream_anchor": str(dreaming.get("projection_anchor") or "latent_stability_projection"),
        "continuity_state": str(continuity.get("state") or "adaptive_temporal_continuity"),
        "stability_persistence": "persistent" if int(continuity.get("continuity_persistence", 0) or 0) >= 68 else "adaptive",
    }

    current_result = {
        "runtime_temporal": build_runtime_temporal_snapshot(
            temporal_state=temporal_state,
            temporal_phase=phase,
            temporal_rhythm=rhythm,
            temporal_metrics=metrics,
            temporal_forecast=forecast,
        ),
        "temporal_state": temporal_state,
        "temporal_phase": phase,
        "temporal_rhythm": rhythm,
        "temporal_forecast": forecast,
        "temporal_continuity": continuity,
        "temporal_decay": decay,
        "temporal_recovery": recovery,
        "temporal_governance": governance,
        "temporal_metrics": metrics,
        "temporal_integrity": temporal_integrity,
        "temporal_alignment": temporal_alignment,
        "temporal_stability": temporal_stability,
        "temporal_momentum": temporal_momentum,
        "temporal_pressure": temporal_pressure,
        "continuity_decay_rate": int(decay.get("continuity_decay_rate", 0) or 0),
        "runtime_rhythm_state": str(rhythm.get("rhythm_state") or "measured_pacing"),
        "cinematic_temporal_flow": str(rhythm.get("cinematic_temporal_flow") or "steady_cinematic_flow"),
        "orchestration_phase_velocity": str(phase.get("orchestration_phase_velocity") or "modulating"),
        "adaptive_temporal_balance": adaptive_temporal_balance,
        "runtime_cycle_phase": str(phase.get("phase") or "measured_continuity"),
        "temporal_projection": str(forecast.get("forecast") or "measured_future_shaping"),
    }
    if persist_memory:
        memory_summary = update_temporal_memory(payload, current_result, path=memory_path, timestamp=timestamp)
    else:
        memory_summary = prior_memory
    current_result["temporal_memory_summary"] = memory_summary
    current_result["temporal_events"] = build_temporal_events(
        runtime_cycle_phase=str(current_result.get("runtime_cycle_phase") or ""),
        runtime_rhythm_state=str(current_result.get("runtime_rhythm_state") or ""),
        cinematic_temporal_flow=str(current_result.get("cinematic_temporal_flow") or ""),
        continuity_decay_rate=int(current_result.get("continuity_decay_rate", 0) or 0),
        adaptive_recovery_velocity=str(recovery.get("adaptive_recovery_velocity") or ""),
        previous_phase=str(previous_entry.get("runtime_cycle_phase") or ""),
    )
    return current_result


def _metric(payload: Mapping[str, Any], key: str) -> int:
    return _clamp(_safe_int(payload.get(key, 0)))


def _nested_metric(mapping: Mapping[str, Any], key: str) -> int:
    return _clamp(_safe_int(mapping.get(key, 0)))


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
