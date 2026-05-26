from __future__ import annotations

from typing import Any, Mapping

from .federation_continuity import build_federation_continuity
from .federation_events import build_federation_events
from .federation_forecast import build_federation_forecast
from .federation_governance import build_federation_governance
from .federation_memory import build_federation_memory_summary, load_federation_memory, update_federation_memory
from .federation_metrics import build_federation_metrics
from .federation_projection import build_federation_projection
from .federation_snapshot import build_runtime_federation_snapshot
from .federation_state import build_federation_state


LAYER_ORDER = [
    "coordination",
    "authority",
    "identity",
    "ecosystem",
    "cinema",
    "consciousness",
    "instinct",
    "subconscious",
    "dreaming",
]


def build_runtime_federation(
    orchestration: Mapping[str, Any] | None,
    *,
    persist_memory: bool = True,
    memory_path=None,
    timestamp: str = "",
) -> dict[str, Any]:
    payload = dict(orchestration or {})
    prior_memory = build_federation_memory_summary(load_federation_memory(path=memory_path), current_context=payload)
    layer_signals = _build_layer_signals(payload)
    convergence_count = sum(1 for item in layer_signals if item["polarity"] > 0)
    divergence_count = sum(1 for item in layer_signals if item["polarity"] < 0)
    restrictive_authority = _has_tag(layer_signals, "authority", "restrictive")
    degraded_ecosystem = _has_tag(layer_signals, "ecosystem", "degraded")
    focused_consciousness = _has_tag(layer_signals, "consciousness", "focused")
    unstable_instinct = _has_tag(layer_signals, "instinct", "unstable")
    optimistic_dreaming = _has_tag(layer_signals, "dreaming", "optimistic")
    instability_layers = [item["layer"] for item in layer_signals if item["polarity"] < 0]

    alignment = _clamp(50 + ((convergence_count - divergence_count) * 7))
    divergence = _clamp(10 + (divergence_count * 12) + (12 if restrictive_authority and optimistic_dreaming else 0))
    pressure = _clamp(
        int(round(
            (_metric(payload, "orchestration_pressure", "pressure_score") * 0.35)
            + (_metric(payload, "instinct_pressure", "pressure_score") * 0.25)
            + (_metric(payload, "subconscious_pressure", "latent_pressure") * 0.2)
            + (divergence * 0.2)
            + (8 if restrictive_authority else 0)
            + (8 if degraded_ecosystem else 0)
        ))
    )
    coherence = _clamp(
        int(round(
            (_metric(payload, "coordination_metrics", "coordination_confidence") * 0.18)
            + (_metric(payload, "identity_metrics", "orchestration_maturity") * 0.14)
            + (_metric(payload, "ecosystem_metrics", "ecosystem_integrity") * 0.14)
            + (_metric(payload, "cinematic_metrics", "cinematic_quality") * 0.14)
            + (_metric(payload, "consciousness_metrics", "orchestration_clarity") * 0.14)
            + (_metric(payload, "instinct_metrics", "instinct_integrity") * 0.13)
            + (_metric(payload, "subconscious_metrics", "subconscious_integrity") * 0.07)
            + (_metric(payload, "dream_metrics", "dreaming_integrity") * 0.06)
            + ((convergence_count - divergence_count) * 4)
            - (6 if degraded_ecosystem else 0)
        ))
    )
    harmony = _clamp(
        int(round(
            (alignment * 0.4)
            + (_metric(payload, "cinematic_metrics", "runtime_polish") * 0.15)
            + (_metric(payload, "consciousness_metrics", "awareness_integrity") * 0.15)
            + (_metric(payload, "subconscious_metrics", "subconscious_balance") * 0.15)
            + (_metric(payload, "dream_metrics", "orchestration_dream_balance") * 0.15)
            - (divergence * 0.2)
        ))
    )
    resilience = _clamp(
        int(round(
            (_metric(payload, "coordination_metrics", "runtime_resilience") * 0.3)
            + (_metric(payload, "ecosystem_metrics", "ecosystem_stability") * 0.15)
            + (_metric(payload, "instinct_metrics", "orchestration_survival_score") * 0.2)
            + (_metric(payload, "subconscious_metrics", "dormant_resilience_strength") * 0.15)
            + (_metric(payload, "dream_metrics", "continuity_projection_strength") * 0.1)
            + (_metric(payload, "identity_confidence") * 0.1)
            - (pressure * 0.12)
        ))
    )
    integrity = _clamp(
        int(round(
            (coherence * 0.4)
            + (resilience * 0.25)
            + (_metric(payload, "continuity_state", "continuity_confidence") * 0.15)
            + (_metric(payload, "consciousness_metrics", "perception_integrity") * 0.1)
            + (_metric(payload, "dream_metrics", "runtime_mirroring_integrity") * 0.1)
            - (divergence * 0.18)
        ))
    )
    adaptive_balance = _clamp(int(round((coherence * 0.35) + (resilience * 0.35) + (harmony * 0.3) - (pressure * 0.2))))

    federation_state = build_federation_state(
        coherence=coherence,
        harmony=harmony,
        pressure=pressure,
        integrity=integrity,
        resilience=resilience,
        alignment=alignment,
        divergence=divergence,
        instability_layers=instability_layers,
        restrictive_authority=restrictive_authority,
        degraded_ecosystem=degraded_ecosystem,
        optimistic_dreaming=optimistic_dreaming,
        focused_consciousness=focused_consciousness,
    )
    federation_projection = build_federation_projection(
        phase_transition=str(federation_state.get("phase_transition") or ""),
        orchestration_unity=str(federation_state.get("orchestration_unity") or ""),
        coherence=coherence,
        resilience=resilience,
        pressure=pressure,
        optimistic_dreaming=optimistic_dreaming,
        degraded_ecosystem=degraded_ecosystem,
    )
    federation_governance = build_federation_governance(
        pressure=pressure,
        divergence=divergence,
        restrictive_authority=restrictive_authority,
        degraded_ecosystem=degraded_ecosystem,
        unstable_instinct=unstable_instinct,
        focused_consciousness=focused_consciousness,
        optimistic_dreaming=optimistic_dreaming,
    )
    federation_continuity = build_federation_continuity(
        continuity_projection=str(federation_projection.get("continuity_projection") or ""),
        coherence=coherence,
        integrity=integrity,
        resilience=resilience,
        divergence=divergence,
    )
    federation_forecast = build_federation_forecast(
        phase_transition=str(federation_state.get("phase_transition") or ""),
        continuity_projection=str(federation_projection.get("continuity_projection") or ""),
        cinematic_runtime_state=str(federation_projection.get("cinematic_runtime_state") or ""),
        pressure=pressure,
        resilience=resilience,
        divergence=divergence,
    )
    federation_metrics = build_federation_metrics(
        coherence=coherence,
        harmony=harmony,
        pressure=pressure,
        integrity=integrity,
        resilience=resilience,
        alignment=alignment,
        divergence=divergence,
        convergence_count=convergence_count,
        divergence_count=divergence_count,
        adaptive_balance=adaptive_balance,
    )

    current_result = {
        "runtime_federation": build_runtime_federation_snapshot(
            federation_state=federation_state,
            federation_projection=federation_projection,
            federation_metrics=federation_metrics,
        ),
        "federation_state": federation_state,
        "federation_projection": federation_projection,
        "federation_forecast": federation_forecast,
        "federation_governance": federation_governance,
        "federation_continuity": federation_continuity,
        "federation_metrics": federation_metrics,
        "federation_coherence": coherence,
        "federation_harmony": harmony,
        "federation_pressure": pressure,
        "federation_integrity": integrity,
        "federation_resilience": resilience,
        "federation_alignment": alignment,
        "federation_divergence": divergence,
        "runtime_continuity_profile": str(federation_continuity.get("runtime_continuity_profile") or "adaptive_cinematic_continuity"),
        "cinematic_runtime_state": str(federation_projection.get("cinematic_runtime_state") or "adaptive_cinematic_balance"),
        "orchestration_unity": str(federation_state.get("orchestration_unity") or "moderate"),
        "adaptive_federation_balance": adaptive_balance,
        "runtime_phase_transition": str(federation_state.get("phase_transition") or "steady_continuity"),
        "continuity_projection": str(federation_projection.get("continuity_projection") or "measured_continuity"),
    }
    if persist_memory:
        memory_summary = update_federation_memory(payload, current_result, path=memory_path, timestamp=timestamp)
    else:
        memory_summary = prior_memory
    current_result["federation_memory_summary"] = memory_summary
    previous_entry = dict((prior_memory.get("recent_entries") or [])[-1] or {}) if prior_memory.get("recent_entries") else {}
    current_result["federation_events"] = build_federation_events(
        phase_transition=str(current_result.get("runtime_phase_transition") or ""),
        orchestration_unity=str(current_result.get("orchestration_unity") or ""),
        continuity_projection=str(current_result.get("continuity_projection") or ""),
        cinematic_runtime_state=str(current_result.get("cinematic_runtime_state") or ""),
        instability_layers=instability_layers,
        previous_phase_transition=str(previous_entry.get("phase_transition") or ""),
    )
    return current_result


def _build_layer_signals(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        _signal("coordination", payload, _first_string(_as_mapping(payload.get("runtime_negotiation")), "selected_runtime"), _metric(payload, "coordination_metrics", "coordination_confidence")),
        _signal("authority", payload, str(payload.get("authority_state") or ""), _metric(payload, "authority_confidence")),
        _signal("identity", payload, str(payload.get("runtime_temperament") or ""), _metric(payload, "identity_confidence")),
        _signal("ecosystem", payload, _first_string(_as_mapping(payload.get("ecosystem_climate")), "climate"), _metric(payload, "ecosystem_metrics", "ecosystem_integrity")),
        _signal("cinema", payload, _first_string(_as_mapping(payload.get("runtime_atmosphere")), "atmosphere"), _metric(payload, "cinematic_metrics", "cinematic_quality")),
        _signal("consciousness", payload, _first_string(_as_mapping(payload.get("orchestration_focus")), "focus"), _metric(payload, "consciousness_metrics", "awareness_integrity")),
        _signal("instinct", payload, _first_string(_as_mapping(payload.get("runtime_instinct")), "instinct_state", "state"), _metric(payload, "instinct_metrics", "instinct_integrity")),
        _signal("subconscious", payload, _first_string(_as_mapping(payload.get("runtime_subconscious")), "subconscious_state"), _metric(payload, "subconscious_metrics", "subconscious_integrity")),
        _signal("dreaming", payload, _first_string(_as_mapping(payload.get("dream_forecast")), "forecast"), _metric(payload, "dream_metrics", "dreaming_integrity")),
    ]


def _signal(layer: str, payload: Mapping[str, Any], state: str, score: int) -> dict[str, Any]:
    text = str(state or "").strip().lower()
    tags: list[str] = []
    polarity = 0
    if layer == "authority" and (_list_size(payload.get("governance_actions")) > 0 or _list_size(payload.get("forced_constraints")) > 0 or _list_size(payload.get("blocked_paths")) > 0 or "guard" in text or "restrict" in text):
        tags.append("restrictive")
        polarity -= 1
    if layer == "ecosystem" and ("degrad" in text or "fragile" in text or score < 52):
        tags.append("degraded")
        polarity -= 1
    if layer == "consciousness" and ("focus" in text or "clarity" in text or score >= 72):
        tags.append("focused")
        polarity += 1
    if layer == "instinct" and ("unstable" in text or "fragile" in text or "aggressive" in text or score < 52):
        tags.append("unstable")
        polarity -= 1
    if layer == "dreaming" and ("stabil" in text or "continuity" in text or "optim" in text or "recovery" in text or score >= 68):
        tags.append("optimistic")
        polarity += 1
    if not tags and score >= 66:
        polarity += 1
    if not tags and score <= 40:
        polarity -= 1
    return {
        "layer": layer,
        "state": text or "unknown",
        "score": score,
        "tags": tags,
        "polarity": polarity,
    }


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


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _metric(payload: Mapping[str, Any], key: str, nested_key: str | None = None) -> int:
    if nested_key is None:
        return _clamp(_safe_int(payload.get(key, 0)))
    container = _as_mapping(payload.get(key))
    return _clamp(_safe_int(container.get(nested_key, 0)))


def _clamp(value: int) -> int:
    return max(0, min(100, int(value or 0)))


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _has_tag(signals: list[dict[str, Any]], layer: str, tag: str) -> bool:
    for signal in signals:
        if signal.get("layer") == layer and tag in list(signal.get("tags") or []):
            return True
    return False


def _list_size(value: Any) -> int:
    if isinstance(value, list):
        return len(value)
    return 0
