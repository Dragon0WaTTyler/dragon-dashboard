from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any, Mapping

from dragon.cache import save_json_file
from dragon.paths import CACHE_DIR


DEFAULT_IDENTITY_MEMORY = {
    "version": 1,
    "corrupted_recoveries": 0,
    "entries": [],
    "aggregates": {
        "total_runs": 0,
        "continuity_score_total": 0,
        "maturity_score_total": 0,
        "identity_confidence_total": 0,
        "temperament_counts": {},
        "archetype_counts": {},
        "adaptation_profiles": {},
        "environmental_identities": {},
        "trait_counts": {},
        "orchestration_traits": {},
        "drift_signals": {},
        "runtime_preferences": {},
        "fallback_preferences": {},
        "bandwidth_preferences": {},
        "subtitle_preferences": {},
    },
}

_IDENTITY_MEMORY_LOCK = threading.Lock()


def identity_memory_path(path: Path | None = None) -> Path:
    return Path(path or (CACHE_DIR / "magnets" / "runtime_identity_memory.json"))


def load_identity_memory(*, path: Path | None = None) -> dict[str, Any]:
    target = identity_memory_path(path)
    payload, corrupted = _read_memory_file(target)
    normalized = _normalize_memory(payload)
    if corrupted:
        normalized["corrupted_recoveries"] = int(normalized.get("corrupted_recoveries", 0) or 0) + 1
        save_json_file(target, normalized)
    return normalized


def extract_identity_memory_record(
    orchestration: Mapping[str, Any] | None,
    *,
    timestamp: str = "",
) -> dict[str, Any]:
    payload = dict(orchestration or {})
    source = dict(payload.get("selected_source") or {})
    execution_metrics = dict(payload.get("execution_metrics") or {})
    execution_timeline = dict(payload.get("execution_timeline") or {})
    coordination_metrics = dict(payload.get("coordination_metrics") or {})
    confidence_evolution = dict(payload.get("confidence_evolution") or {})
    forecast = dict(payload.get("orchestration_forecast") or {})
    preflight = dict(payload.get("runtime_preflight") or {})
    warnings = [str(item or "").strip() for item in payload.get("runtime_warnings") or [] if str(item or "").strip()]

    runtime_profile = str(payload.get("runtime_profile") or "").strip() or "external_player_only"
    playback_runtime = str(payload.get("playback_runtime") or "").strip() or "external_runtime"
    startup_confidence = str(payload.get("startup_confidence") or "").strip() or "low"
    degradation_risk = int(execution_metrics.get("degradation_risk", 0) or 0)
    stability_score = int(execution_metrics.get("stability_score", 0) or 0)
    fallback_probability = float(execution_timeline.get("fallback_probability", 0) or 0.0)
    runtime_resilience = int(coordination_metrics.get("runtime_resilience", 0) or 0)
    adaptation_pressure = int(coordination_metrics.get("adaptation_pressure", 0) or 0)
    continuity_score = _continuity_score(
        runtime_profile=runtime_profile,
        playback_runtime=playback_runtime,
        startup_confidence=startup_confidence,
        degradation_risk=degradation_risk,
        stability_score=stability_score,
        fallback_probability=fallback_probability,
    )
    maturity_score = _maturity_score(
        continuity_score=continuity_score,
        runtime_resilience=runtime_resilience,
        adaptation_pressure=adaptation_pressure,
        forecast_risk=str(forecast.get("forecast_risk") or "").strip(),
    )
    temperament = _temperament(
        degradation_risk=degradation_risk,
        fallback_probability=fallback_probability,
        runtime_resilience=runtime_resilience,
        startup_confidence=startup_confidence,
    )
    archetype = _archetype(
        playback_runtime=playback_runtime,
        degradation_risk=degradation_risk,
        fallback_probability=fallback_probability,
        runtime_profile=runtime_profile,
        runtime_resilience=runtime_resilience,
    )
    adaptation_profile = _adaptation_profile(
        playback_runtime=playback_runtime,
        fallback_probability=fallback_probability,
        degradation_risk=degradation_risk,
        runtime_resilience=runtime_resilience,
        adaptation_pressure=adaptation_pressure,
    )
    environmental_identity = _environmental_identity(
        source=source,
        playback_runtime=playback_runtime,
        fallback_probability=fallback_probability,
        degradation_risk=degradation_risk,
        warnings=warnings,
    )
    traits = _traits(
        source=source,
        runtime_profile=runtime_profile,
        playback_runtime=playback_runtime,
        degradation_risk=degradation_risk,
        fallback_probability=fallback_probability,
        startup_confidence=startup_confidence,
        runtime_resilience=runtime_resilience,
    )
    orchestration_traits = _orchestration_traits(
        source=source,
        degradation_risk=degradation_risk,
        fallback_probability=fallback_probability,
        warnings=warnings,
        runtime_resilience=runtime_resilience,
    )
    drift_signals = _drift_signals(
        playback_runtime=playback_runtime,
        degradation_risk=degradation_risk,
        fallback_probability=fallback_probability,
        startup_confidence=startup_confidence,
        runtime_resilience=runtime_resilience,
        forecast_risk=str(forecast.get("forecast_risk") or "").strip(),
    )
    preferences = _preferences(
        playback_runtime=playback_runtime,
        preflight=preflight,
        source=source,
    )
    return {
        "timestamp": str(timestamp or payload.get("updated_at") or payload.get("timestamp") or "").strip(),
        "runtime_profile": runtime_profile,
        "playback_runtime": playback_runtime,
        "startup_confidence": startup_confidence,
        "temperament": temperament,
        "archetype": archetype,
        "adaptation_profile": adaptation_profile,
        "environmental_identity": environmental_identity,
        "traits": traits,
        "orchestration_traits": orchestration_traits,
        "drift_signals": drift_signals,
        "runtime_preference": preferences["runtime_preference"],
        "fallback_preference": preferences["fallback_preference"],
        "bandwidth_preference": preferences["bandwidth_preference"],
        "subtitle_preference": preferences["subtitle_preference"],
        "continuity_score": continuity_score,
        "maturity_score": maturity_score,
        "identity_confidence": _identity_confidence(
            continuity_score=continuity_score,
            maturity_score=maturity_score,
            startup_confidence=startup_confidence,
            confidence_evolution=confidence_evolution,
        ),
    }


def update_identity_memory(
    orchestration: Mapping[str, Any] | None,
    *,
    path: Path | None = None,
    timestamp: str = "",
) -> dict[str, Any]:
    with _IDENTITY_MEMORY_LOCK:
        memory = load_identity_memory(path=path)
        record = extract_identity_memory_record(orchestration, timestamp=timestamp)
        memory["entries"] = [*list(memory.get("entries") or []), record]
        _update_aggregates(memory, record)
        save_json_file(identity_memory_path(path), memory)
    return build_identity_memory_summary(memory, current_context=orchestration)


def build_identity_memory_summary(
    memory: Mapping[str, Any] | None,
    *,
    current_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    payload = _normalize_memory(memory)
    aggregates = dict(payload.get("aggregates") or {})
    entries = [dict(item) for item in payload.get("entries") or [] if isinstance(item, Mapping)]
    total_runs = int(aggregates.get("total_runs", 0) or 0)
    current = dict(current_context or {})
    return {
        "memory_status": "recovered" if int(payload.get("corrupted_recoveries", 0) or 0) else "healthy",
        "corrupted_recoveries": int(payload.get("corrupted_recoveries", 0) or 0),
        "total_observations": total_runs,
        "average_continuity": _average(aggregates.get("continuity_score_total", 0), total_runs),
        "average_maturity": _average(aggregates.get("maturity_score_total", 0), total_runs),
        "average_identity_confidence": _average(aggregates.get("identity_confidence_total", 0), total_runs),
        "temperament_counts": dict(aggregates.get("temperament_counts") or {}),
        "archetype_counts": dict(aggregates.get("archetype_counts") or {}),
        "adaptation_profiles": dict(aggregates.get("adaptation_profiles") or {}),
        "environmental_identities": dict(aggregates.get("environmental_identities") or {}),
        "trait_counts": dict(aggregates.get("trait_counts") or {}),
        "orchestration_traits": dict(aggregates.get("orchestration_traits") or {}),
        "drift_signals": dict(aggregates.get("drift_signals") or {}),
        "runtime_preferences": dict(aggregates.get("runtime_preferences") or {}),
        "fallback_preferences": dict(aggregates.get("fallback_preferences") or {}),
        "bandwidth_preferences": dict(aggregates.get("bandwidth_preferences") or {}),
        "subtitle_preferences": dict(aggregates.get("subtitle_preferences") or {}),
        "recent_runtime_profile": str(current.get("runtime_profile") or "").strip() or "unknown",
        "recent_entries": entries[-6:],
    }


def _read_memory_file(path: Path) -> tuple[Any, bool]:
    if not path.exists():
        return DEFAULT_IDENTITY_MEMORY, False
    try:
        return json.loads(path.read_text(encoding="utf-8")), False
    except Exception:
        return DEFAULT_IDENTITY_MEMORY, True


def _normalize_memory(payload: Mapping[str, Any] | Any) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        payload = {}
    return {
        "version": 1,
        "corrupted_recoveries": int(payload.get("corrupted_recoveries", 0) or 0),
        "entries": [dict(item) for item in payload.get("entries") or [] if isinstance(item, Mapping)],
        "aggregates": {
            **DEFAULT_IDENTITY_MEMORY["aggregates"],
            **dict(payload.get("aggregates") or {}),
        },
    }


def _update_aggregates(memory: dict[str, Any], record: Mapping[str, Any]) -> None:
    aggregates = dict(memory.get("aggregates") or {})
    aggregates["total_runs"] = int(aggregates.get("total_runs", 0) or 0) + 1
    aggregates["continuity_score_total"] = int(aggregates.get("continuity_score_total", 0) or 0) + int(record.get("continuity_score", 0) or 0)
    aggregates["maturity_score_total"] = int(aggregates.get("maturity_score_total", 0) or 0) + int(record.get("maturity_score", 0) or 0)
    aggregates["identity_confidence_total"] = int(aggregates.get("identity_confidence_total", 0) or 0) + int(record.get("identity_confidence", 0) or 0)
    _increment_bucket(aggregates, "temperament_counts", str(record.get("temperament") or "calm"))
    _increment_bucket(aggregates, "archetype_counts", str(record.get("archetype") or "cautious_stabilizer"))
    _increment_bucket(aggregates, "adaptation_profiles", str(record.get("adaptation_profile") or "stable_adapter"))
    _increment_bucket(aggregates, "environmental_identities", str(record.get("environmental_identity") or "constrained_runtime_identity"))
    _increment_bucket(aggregates, "runtime_preferences", str(record.get("runtime_preference") or "external_runtime"))
    _increment_bucket(aggregates, "fallback_preferences", str(record.get("fallback_preference") or "measured"))
    _increment_bucket(aggregates, "bandwidth_preferences", str(record.get("bandwidth_preference") or "balanced"))
    _increment_bucket(aggregates, "subtitle_preferences", str(record.get("subtitle_preference") or "standard"))
    for trait in record.get("traits") or []:
        _increment_bucket(aggregates, "trait_counts", str(trait or "adaptive_balanced"))
    for trait in record.get("orchestration_traits") or []:
        _increment_bucket(aggregates, "orchestration_traits", str(trait or "prefers_safe_runtime"))
    for signal in record.get("drift_signals") or []:
        _increment_bucket(aggregates, "drift_signals", str(signal or "stability_bias"))
    memory["aggregates"] = aggregates


def _increment_bucket(aggregates: dict[str, Any], bucket: str, key: str) -> None:
    groups = dict(aggregates.get(bucket) or {})
    groups[key] = int(groups.get(key, 0) or 0) + 1
    aggregates[bucket] = groups


def _continuity_score(
    *,
    runtime_profile: str,
    playback_runtime: str,
    startup_confidence: str,
    degradation_risk: int,
    stability_score: int,
    fallback_probability: float,
) -> int:
    score = 46
    score += 8 if playback_runtime == "browser_runtime" else 4
    score += 8 if "balanced" in runtime_profile or "cinematic" in runtime_profile else 4
    score += 10 if startup_confidence == "high" else 6 if startup_confidence == "medium" else 2
    score += max(0, min(16, stability_score // 6))
    score -= min(24, degradation_risk // 3)
    score -= min(18, int(round(fallback_probability * 24)))
    return max(0, min(100, score))


def _maturity_score(
    *,
    continuity_score: int,
    runtime_resilience: int,
    adaptation_pressure: int,
    forecast_risk: str,
) -> int:
    score = 24 + continuity_score // 2
    score += min(18, runtime_resilience // 5)
    score -= min(12, adaptation_pressure // 6)
    if forecast_risk == "high":
        score -= 10
    elif forecast_risk == "low":
        score += 6
    return max(0, min(100, score))


def _identity_confidence(
    *,
    continuity_score: int,
    maturity_score: int,
    startup_confidence: str,
    confidence_evolution: Mapping[str, Any],
) -> int:
    score = int(round((continuity_score + maturity_score) / 2))
    if startup_confidence == "high":
        score += 8
    elif startup_confidence == "low":
        score -= 8
    stability = str(confidence_evolution.get("confidence_stability") or "").strip()
    if stability == "stable":
        score += 6
    elif stability == "volatile":
        score -= 10
    score += max(-8, min(8, int(confidence_evolution.get("confidence_delta", 0) or 0) // 2))
    return max(0, min(100, score))


def _temperament(
    *,
    degradation_risk: int,
    fallback_probability: float,
    runtime_resilience: int,
    startup_confidence: str,
) -> str:
    if degradation_risk >= 75 or fallback_probability >= 0.72:
        return "defensive"
    if degradation_risk >= 58 or startup_confidence == "low":
        return "cautious"
    if runtime_resilience >= 78 and fallback_probability <= 0.3:
        return "optimistic"
    if runtime_resilience >= 64:
        return "calm"
    return "constrained"


def _archetype(
    *,
    playback_runtime: str,
    degradation_risk: int,
    fallback_probability: float,
    runtime_profile: str,
    runtime_resilience: int,
) -> str:
    if "cinematic" in runtime_profile and degradation_risk <= 48:
        return "cinematic_orchestrator"
    if fallback_probability >= 0.7 and runtime_resilience >= 60:
        return "adaptive_survivor"
    if degradation_risk >= 72:
        return "cautious_stabilizer"
    if playback_runtime == "external_runtime" and runtime_resilience >= 70:
        return "resilient_guardian"
    if runtime_resilience >= 76:
        return "volatility_controller"
    return "constrained_optimizer"


def _adaptation_profile(
    *,
    playback_runtime: str,
    fallback_probability: float,
    degradation_risk: int,
    runtime_resilience: int,
    adaptation_pressure: int,
) -> str:
    if playback_runtime == "external_runtime" and degradation_risk >= 60:
        return "degraded_environment_specialist"
    if fallback_probability >= 0.68 and adaptation_pressure >= 48:
        return "constrained_survivor"
    if runtime_resilience >= 78 and degradation_risk <= 42:
        return "stable_adapter"
    if adaptation_pressure >= 62:
        return "overcorrecting_adapter"
    if runtime_resilience >= 64:
        return "resilience_optimizer"
    return "cinematic_optimizer"


def _environmental_identity(
    *,
    source: Mapping[str, Any],
    playback_runtime: str,
    fallback_probability: float,
    degradation_risk: int,
    warnings: list[str],
) -> str:
    if not bool(source.get("mobile_friendly")):
        return "unstable_mobile_identity"
    if playback_runtime == "browser_runtime" and ("2160" in str(source.get("quality_label") or source.get("resolution") or "")):
        return "cinematic_desktop_identity"
    if fallback_probability >= 0.62 or any("browser" in item for item in warnings):
        return "fallback_sensitive_environment"
    if degradation_risk >= 58 or bool(source.get("high_bandwidth_required")):
        return "constrained_runtime_identity"
    return "stable_runtime_identity"


def _traits(
    *,
    source: Mapping[str, Any],
    runtime_profile: str,
    playback_runtime: str,
    degradation_risk: int,
    fallback_probability: float,
    startup_confidence: str,
    runtime_resilience: int,
) -> list[str]:
    traits: list[str] = []
    if degradation_risk >= 58 or startup_confidence == "low":
        traits.append("conservative")
    if "cinematic" in runtime_profile or "2160" in str(source.get("quality_label") or source.get("resolution") or ""):
        traits.append("cinematic")
    if playback_runtime == "external_runtime" or runtime_resilience >= 70:
        traits.append("resilience_first")
    if 0.28 <= fallback_probability <= 0.62:
        traits.append("adaptive_balanced")
    if fallback_probability >= 0.63:
        traits.append("fallback_aggressive")
    if not bool(source.get("mobile_friendly")):
        traits.append("mobile_sensitive")
    if runtime_resilience >= 72 or degradation_risk <= 36:
        traits.append("stability_focused")
    if startup_confidence != "high":
        traits.append("confidence_cautious")
    return traits or ["adaptive_balanced"]


def _orchestration_traits(
    *,
    source: Mapping[str, Any],
    degradation_risk: int,
    fallback_probability: float,
    warnings: list[str],
    runtime_resilience: int,
) -> list[str]:
    traits = ["prefers_safe_runtime"]
    if "2160" in str(source.get("quality_label") or source.get("resolution") or ""):
        traits.append("prefers_high_quality")
    if bool(source.get("high_bandwidth_required")) or degradation_risk >= 60:
        traits.append("downgrade_sensitive")
    if runtime_resilience >= 76 and fallback_probability <= 0.34:
        traits.append("recovery_resistant")
    if fallback_probability >= 0.58:
        traits.append("escalation_prone")
    if degradation_risk >= 52:
        traits.append("volatility_sensitive")
    if any("browser" in item for item in warnings) or fallback_probability >= 0.46:
        traits.append("fallback_tolerant")
    return traits


def _drift_signals(
    *,
    playback_runtime: str,
    degradation_risk: int,
    fallback_probability: float,
    startup_confidence: str,
    runtime_resilience: int,
    forecast_risk: str,
) -> list[str]:
    signals: list[str] = []
    if startup_confidence == "low" or degradation_risk >= 56:
        signals.append("increased_caution")
    if fallback_probability >= 0.56:
        signals.append("stronger_fallback_dependency")
        signals.append("increased_degradation_tolerance")
    if playback_runtime == "external_runtime":
        signals.append("growing_runtime_conservatism")
    if runtime_resilience >= 78 and fallback_probability <= 0.3:
        signals.append("elevated_cinematic_preference")
    if forecast_risk == "high":
        signals.append("adaptation_fatigue")
    return signals or ["stability_bias"]


def _preferences(
    *,
    playback_runtime: str,
    preflight: Mapping[str, Any],
    source: Mapping[str, Any],
) -> dict[str, str]:
    return {
        "runtime_preference": playback_runtime or "external_runtime",
        "fallback_preference": "assertive" if str(preflight.get("fallback_strategy") or "") not in {"", "none"} else "measured",
        "bandwidth_preference": "constrained" if bool(source.get("high_bandwidth_required")) else "balanced",
        "subtitle_preference": "minimal" if "2160" in str(source.get("quality_label") or source.get("resolution") or "") else "standard",
    }


def _average(value: Any, total: int) -> float:
    denominator = max(int(total or 0), 1)
    return round(int(value or 0) / denominator, 4)
