from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any, Mapping

from dragon.cache import save_json_file
from dragon.paths import CACHE_DIR


DEFAULT_RUNTIME_MEMORY = {
    "version": 1,
    "corrupted_recoveries": 0,
    "entries": [],
    "aggregates": {
        "total_runs": 0,
        "degradation_count": 0,
        "fallback_count": 0,
        "instability_count": 0,
        "recovery_success_count": 0,
        "browser_rejection_count": 0,
        "runtime_profiles": {},
        "playback_runtimes": {},
        "transport_classes": {},
        "fallback_paths": {},
        "source_characteristics": {},
        "adaptation_chains": {},
    },
}

_MEMORY_LOCK = threading.Lock()


def runtime_memory_path(path: Path | None = None) -> Path:
    return Path(path or (CACHE_DIR / "magnets" / "runtime_memory.json"))


def load_runtime_memory(*, path: Path | None = None) -> dict[str, Any]:
    target = runtime_memory_path(path)
    payload, corrupted = _read_memory_file(target)
    normalized = _normalize_memory(payload)
    if corrupted:
        normalized["corrupted_recoveries"] = int(normalized.get("corrupted_recoveries", 0) or 0) + 1
        save_json_file(target, normalized)
    return normalized


def extract_runtime_memory_record(orchestration: Mapping[str, Any] | None, *, timestamp: str = "") -> dict[str, Any]:
    payload = dict(orchestration or {})
    source = dict(payload.get("selected_source") or {})
    execution_metrics = dict(payload.get("execution_metrics") or {})
    execution_timeline = dict(payload.get("execution_timeline") or {})
    recovery_path = dict(payload.get("recovery_path") or {})
    coordination_metrics = dict(payload.get("coordination_metrics") or {})
    runtime_negotiation = dict(payload.get("runtime_negotiation") or {})
    adaptive_strategy = dict(payload.get("adaptive_strategy") or {})
    transport = dict(payload.get("transport_descriptor") or payload.get("runtime_transport") or {})
    guardrails = dict(payload.get("guardrails") or {})
    fallback_negotiation = dict(payload.get("fallback_negotiation") or {})

    degradation = _bool(
        payload.get("simulated_runtime_health") in {"degraded", "guarded"}
        or execution_metrics.get("degradation_risk", 0) >= 50
        or payload.get("execution_outcome") == "fallback"
    )
    fallback = _bool(
        payload.get("execution_outcome") == "fallback"
        or str(adaptive_strategy.get("target_runtime") or "") == "external_runtime"
        or str(runtime_negotiation.get("selected_runtime") or "") == "external_runtime"
    )
    instability = _bool(
        payload.get("simulated_runtime_health") in {"degraded", "guarded"}
        or execution_metrics.get("stability_score", 0) < 65
        or execution_timeline.get("fallback_probability", 0) >= 0.45
    )
    recovery_success = _bool(
        payload.get("execution_outcome") == "recovered"
        or str(payload.get("coordination_state") or "") == "recovery_negotiated"
        or str(adaptive_strategy.get("adaptation_rule") or "").startswith("recover")
    )
    browser_rejection = _bool(
        guardrails.get("rejected")
        or any("rejection" in str(item or "") for item in guardrails.get("blocking_reasons") or [])
    )
    adaptation_chain = _adaptation_chain(payload)
    source_characteristic = _source_characteristic(source)
    return {
        "timestamp": str(timestamp or payload.get("timestamp") or payload.get("updated_at") or "").strip(),
        "runtime_profile": str(payload.get("runtime_profile") or "").strip() or "unknown",
        "playback_runtime": str(payload.get("playback_runtime") or "").strip() or "unknown",
        "runtime_mode": str(payload.get("runtime_mode") or "").strip() or "unknown",
        "selected_runtime": str(runtime_negotiation.get("selected_runtime") or payload.get("playback_runtime") or "").strip() or "unknown",
        "transport_class": str(transport.get("transport_class") or "").strip() or "unknown",
        "source_characteristic": source_characteristic,
        "degraded": degradation,
        "fallback": fallback,
        "instability": instability,
        "recovery_success": recovery_success,
        "browser_rejection": browser_rejection,
        "recovery_path": str(recovery_path.get("path") or "").strip() or "unknown",
        "fallback_urgency": str(fallback_negotiation.get("fallback_urgency") or "").strip() or "low",
        "confidence_before": str(payload.get("startup_confidence") or "").strip() or "low",
        "execution_health": str(payload.get("simulated_runtime_health") or "").strip() or "unknown",
        "adaptation_chain": adaptation_chain,
        "degradation_risk": int(execution_metrics.get("degradation_risk", 0) or 0),
        "runtime_resilience": int(coordination_metrics.get("runtime_resilience", 0) or 0),
        "coordination_confidence": int(coordination_metrics.get("coordination_confidence", 0) or 0),
        "fallback_probability": float(execution_timeline.get("fallback_probability", 0) or 0.0),
    }


def update_runtime_memory(
    orchestration: Mapping[str, Any] | None,
    *,
    path: Path | None = None,
    timestamp: str = "",
) -> dict[str, Any]:
    with _MEMORY_LOCK:
        memory = load_runtime_memory(path=path)
        record = extract_runtime_memory_record(orchestration, timestamp=timestamp)
        memory["entries"] = [*list(memory.get("entries") or []), record]
        _update_aggregates(memory, record)
        save_json_file(runtime_memory_path(path), memory)
    return build_runtime_memory_summary(memory, current_context=orchestration)


def build_runtime_memory_summary(
    memory: Mapping[str, Any] | None,
    *,
    current_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    payload = _normalize_memory(memory)
    entries = [dict(item) for item in payload.get("entries") or [] if isinstance(item, Mapping)]
    aggregates = dict(payload.get("aggregates") or {})
    current = dict(current_context or {})
    total_runs = int(aggregates.get("total_runs", 0) or 0)
    degradation_count = int(aggregates.get("degradation_count", 0) or 0)
    fallback_count = int(aggregates.get("fallback_count", 0) or 0)
    instability_count = int(aggregates.get("instability_count", 0) or 0)
    recovery_success_count = int(aggregates.get("recovery_success_count", 0) or 0)
    browser_rejection_count = int(aggregates.get("browser_rejection_count", 0) or 0)
    runtime_profile = str(current.get("runtime_profile") or "").strip()
    profile_stats = dict((aggregates.get("runtime_profiles") or {}).get(runtime_profile) or {})
    return {
        "memory_status": "recovered" if int(payload.get("corrupted_recoveries", 0) or 0) else "healthy",
        "corrupted_recoveries": int(payload.get("corrupted_recoveries", 0) or 0),
        "total_observations": total_runs,
        "degradation_frequency": _ratio(degradation_count, total_runs),
        "fallback_frequency": _ratio(fallback_count, total_runs),
        "runtime_instability": _ratio(instability_count, total_runs),
        "recovery_success_rate": _ratio(recovery_success_count, max(fallback_count, 1)),
        "browser_rejection_trend": _ratio(browser_rejection_count, total_runs),
        "recent_runtime_profile": runtime_profile or "unknown",
        "current_profile_degradation_frequency": _ratio(
            int(profile_stats.get("degraded", 0) or 0),
            int(profile_stats.get("selected", 0) or 0),
        ),
        "runtime_profiles": dict(aggregates.get("runtime_profiles") or {}),
        "playback_runtimes": dict(aggregates.get("playback_runtimes") or {}),
        "transport_classes": dict(aggregates.get("transport_classes") or {}),
        "fallback_paths": dict(aggregates.get("fallback_paths") or {}),
        "source_characteristics": dict(aggregates.get("source_characteristics") or {}),
        "adaptation_chains": dict(aggregates.get("adaptation_chains") or {}),
        "recent_entries": entries[-5:],
    }


def _read_memory_file(path: Path) -> tuple[Any, bool]:
    if not path.exists():
        return DEFAULT_RUNTIME_MEMORY, False
    try:
        return json.loads(path.read_text(encoding="utf-8")), False
    except Exception:
        return DEFAULT_RUNTIME_MEMORY, True


def _normalize_memory(payload: Mapping[str, Any] | Any) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        payload = {}
    normalized = {
        "version": 1,
        "corrupted_recoveries": int(payload.get("corrupted_recoveries", 0) or 0),
        "entries": [dict(item) for item in payload.get("entries") or [] if isinstance(item, Mapping)],
        "aggregates": dict(payload.get("aggregates") or {}),
    }
    normalized["aggregates"] = {
        **DEFAULT_RUNTIME_MEMORY["aggregates"],
        **normalized["aggregates"],
    }
    return normalized


def _update_aggregates(memory: dict[str, Any], record: Mapping[str, Any]) -> None:
    aggregates = dict(memory.get("aggregates") or {})
    aggregates["total_runs"] = int(aggregates.get("total_runs", 0) or 0) + 1
    if record.get("degraded"):
        aggregates["degradation_count"] = int(aggregates.get("degradation_count", 0) or 0) + 1
    if record.get("fallback"):
        aggregates["fallback_count"] = int(aggregates.get("fallback_count", 0) or 0) + 1
    if record.get("instability"):
        aggregates["instability_count"] = int(aggregates.get("instability_count", 0) or 0) + 1
    if record.get("recovery_success"):
        aggregates["recovery_success_count"] = int(aggregates.get("recovery_success_count", 0) or 0) + 1
    if record.get("browser_rejection"):
        aggregates["browser_rejection_count"] = int(aggregates.get("browser_rejection_count", 0) or 0) + 1
    _update_group_stats(aggregates, "runtime_profiles", str(record.get("runtime_profile") or "unknown"), record)
    _update_group_stats(aggregates, "playback_runtimes", str(record.get("playback_runtime") or "unknown"), record)
    _update_group_stats(aggregates, "transport_classes", str(record.get("transport_class") or "unknown"), record)
    _update_group_stats(aggregates, "fallback_paths", str(record.get("recovery_path") or "unknown"), record)
    _update_group_stats(aggregates, "source_characteristics", str(record.get("source_characteristic") or "unknown"), record)
    chain = str(record.get("adaptation_chain") or "retain_runtime")
    aggregates["adaptation_chains"] = dict(aggregates.get("adaptation_chains") or {})
    aggregates["adaptation_chains"][chain] = int((aggregates["adaptation_chains"]).get(chain, 0) or 0) + 1
    memory["aggregates"] = aggregates


def _update_group_stats(aggregates: dict[str, Any], bucket: str, key: str, record: Mapping[str, Any]) -> None:
    groups = dict(aggregates.get(bucket) or {})
    payload = dict(groups.get(key) or {})
    payload["selected"] = int(payload.get("selected", 0) or 0) + 1
    payload["total"] = int(payload.get("total", 0) or 0) + 1
    if record.get("degraded"):
        payload["degraded"] = int(payload.get("degraded", 0) or 0) + 1
    if record.get("fallback"):
        payload["fallback"] = int(payload.get("fallback", 0) or 0) + 1
    if record.get("instability"):
        payload["instability"] = int(payload.get("instability", 0) or 0) + 1
    if record.get("recovery_success"):
        payload["recovered"] = int(payload.get("recovered", 0) or 0) + 1
    if record.get("browser_rejection"):
        payload["browser_rejection"] = int(payload.get("browser_rejection", 0) or 0) + 1
    groups[key] = payload
    aggregates[bucket] = groups


def _adaptation_chain(payload: Mapping[str, Any]) -> str:
    switch_history = [dict(item) for item in payload.get("runtime_switch_history") or [] if isinstance(item, Mapping)]
    target = str((switch_history[-1] if switch_history else {}).get("target_runtime") or payload.get("playback_runtime") or "").strip()
    if not switch_history:
        return "retain_runtime"
    current = str((switch_history[0] if switch_history else {}).get("current_runtime") or payload.get("playback_runtime") or "").strip()
    return f"{current or 'unknown'}->{target or 'unknown'}"


def _source_characteristic(source: Mapping[str, Any]) -> str:
    release_type = str(source.get("source_type") or source.get("release_type") or "unknown").strip().lower()
    codec = str(source.get("codec") or source.get("video_codec") or "unknown").strip().lower()
    resolution = str(source.get("quality_label") or source.get("resolution") or "unknown").strip().lower()
    return f"{release_type}|{codec}|{resolution}"


def _ratio(value: int, total: int) -> float:
    denominator = max(int(total or 0), 1)
    return round(int(value or 0) / denominator, 4)


def _bool(value: Any) -> bool:
    return bool(value)
