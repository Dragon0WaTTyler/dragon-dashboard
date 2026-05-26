from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any, Mapping

from dragon.cache import save_json_file
from dragon.paths import CACHE_DIR


DEFAULT_INSTINCT_MEMORY = {
    "version": 1,
    "corrupted_recoveries": 0,
    "entries": [],
    "aggregates": {
        "total_runs": 0,
        "instinct_integrity_total": 0,
        "survival_score_total": 0,
        "fallback_intensity_total": 0,
        "instinct_drift_total": 0,
        "stabilization_counts": {},
        "fallback_counts": {},
        "resilience_counts": {},
        "continuity_counts": {},
        "survival_counts": {},
        "forecast_counts": {},
    },
}

_INSTINCT_MEMORY_LOCK = threading.Lock()


def instinct_memory_path(path: Path | None = None) -> Path:
    return Path(path or (CACHE_DIR / "magnets" / "runtime_instinct_memory.json"))


def load_instinct_memory(*, path: Path | None = None) -> dict[str, Any]:
    target = instinct_memory_path(path)
    payload, corrupted = _read_memory_file(target)
    normalized = _normalize_memory(payload)
    if corrupted:
        normalized["corrupted_recoveries"] = int(normalized.get("corrupted_recoveries", 0) or 0) + 1
        save_json_file(target, normalized)
    return normalized


def extract_instinct_memory_record(
    orchestration: Mapping[str, Any] | None,
    instinct_result: Mapping[str, Any] | None = None,
    *,
    timestamp: str = "",
) -> dict[str, Any]:
    payload = dict(orchestration or {})
    result = dict(instinct_result or {})
    stabilization = dict(result.get("stabilization_instinct") or payload.get("stabilization_instinct") or {})
    fallback = dict(result.get("fallback_instinct") or payload.get("fallback_instinct") or {})
    resilience = dict(result.get("resilience_instinct") or payload.get("resilience_instinct") or {})
    continuity = dict(result.get("continuity_instinct") or payload.get("continuity_instinct") or {})
    survival = dict(result.get("runtime_survival") or payload.get("runtime_survival") or {})
    forecast = dict(result.get("instinct_forecast") or payload.get("instinct_forecast") or {})
    metrics = dict(result.get("instinct_metrics") or payload.get("instinct_metrics") or {})
    prior = dict(payload.get("instinct_memory") or {})
    prior_entries = [dict(item) for item in prior.get("recent_entries") or [] if isinstance(item, Mapping)]
    previous_state = str((prior_entries[-1] or {}).get("stabilization_state") or "") if prior_entries else ""
    stabilization_state = str(stabilization.get("state") or "adaptive_stabilization")
    drift = 0 if not previous_state or previous_state == stabilization_state else 24
    return {
        "timestamp": str(timestamp or payload.get("updated_at") or payload.get("timestamp") or "").strip(),
        "stabilization_state": stabilization_state,
        "fallback_state": str(fallback.get("state") or "fallback_balanced"),
        "resilience_state": str(resilience.get("state") or "resilience_balanced"),
        "continuity_state": str(continuity.get("state") or "continuity_adaptive"),
        "survival_state": str(survival.get("state") or "survival_adaptive"),
        "forecast": str(forecast.get("forecast") or "resilience_convergence"),
        "instinct_integrity": int(metrics.get("instinct_integrity", 0) or 0),
        "survival_score": int(metrics.get("orchestration_survival_score", survival.get("orchestration_survival_score", 0)) or 0),
        "fallback_intensity": int(metrics.get("fallback_intensity", fallback.get("fallback_intensity", 0)) or 0),
        "instinct_drift": drift,
    }


def update_instinct_memory(
    orchestration: Mapping[str, Any] | None,
    instinct_result: Mapping[str, Any] | None,
    *,
    path: Path | None = None,
    timestamp: str = "",
) -> dict[str, Any]:
    with _INSTINCT_MEMORY_LOCK:
        memory = load_instinct_memory(path=path)
        record = extract_instinct_memory_record(orchestration, instinct_result, timestamp=timestamp)
        memory["entries"] = [*list(memory.get("entries") or []), record]
        _update_aggregates(memory, record)
        save_json_file(instinct_memory_path(path), memory)
    return build_instinct_memory_summary(memory, current_context=instinct_result or orchestration)


def build_instinct_memory_summary(
    memory: Mapping[str, Any] | None,
    *,
    current_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    payload = _normalize_memory(memory)
    aggregates = dict(payload.get("aggregates") or {})
    total_runs = int(aggregates.get("total_runs", 0) or 0)
    entries = [dict(item) for item in payload.get("entries") or [] if isinstance(item, Mapping)]
    current = dict(current_context or {})
    stabilization = dict(current.get("stabilization_instinct") or {})
    return {
        "memory_status": "recovered" if int(payload.get("corrupted_recoveries", 0) or 0) else "healthy",
        "corrupted_recoveries": int(payload.get("corrupted_recoveries", 0) or 0),
        "total_observations": total_runs,
        "average_instinct_integrity": _average(aggregates.get("instinct_integrity_total", 0), total_runs),
        "average_survival_score": _average(aggregates.get("survival_score_total", 0), total_runs),
        "average_fallback_intensity": _average(aggregates.get("fallback_intensity_total", 0), total_runs),
        "average_instinct_drift": _average(aggregates.get("instinct_drift_total", 0), total_runs),
        "stabilization_counts": dict(aggregates.get("stabilization_counts") or {}),
        "fallback_counts": dict(aggregates.get("fallback_counts") or {}),
        "resilience_counts": dict(aggregates.get("resilience_counts") or {}),
        "continuity_counts": dict(aggregates.get("continuity_counts") or {}),
        "survival_counts": dict(aggregates.get("survival_counts") or {}),
        "forecast_counts": dict(aggregates.get("forecast_counts") or {}),
        "recent_stabilization_state": str(stabilization.get("state") or "unknown"),
        "recent_entries": entries[-6:],
    }


def _read_memory_file(path: Path) -> tuple[Any, bool]:
    if not path.exists():
        return DEFAULT_INSTINCT_MEMORY, False
    try:
        return json.loads(path.read_text(encoding="utf-8")), False
    except Exception:
        return DEFAULT_INSTINCT_MEMORY, True


def _normalize_memory(payload: Mapping[str, Any] | Any) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        payload = {}
    return {
        "version": 1,
        "corrupted_recoveries": int(payload.get("corrupted_recoveries", 0) or 0),
        "entries": [dict(item) for item in payload.get("entries") or [] if isinstance(item, Mapping)],
        "aggregates": {
            **DEFAULT_INSTINCT_MEMORY["aggregates"],
            **dict(payload.get("aggregates") or {}),
        },
    }


def _update_aggregates(memory: dict[str, Any], record: Mapping[str, Any]) -> None:
    aggregates = dict(memory.get("aggregates") or {})
    aggregates["total_runs"] = int(aggregates.get("total_runs", 0) or 0) + 1
    aggregates["instinct_integrity_total"] = int(aggregates.get("instinct_integrity_total", 0) or 0) + int(record.get("instinct_integrity", 0) or 0)
    aggregates["survival_score_total"] = int(aggregates.get("survival_score_total", 0) or 0) + int(record.get("survival_score", 0) or 0)
    aggregates["fallback_intensity_total"] = int(aggregates.get("fallback_intensity_total", 0) or 0) + int(record.get("fallback_intensity", 0) or 0)
    aggregates["instinct_drift_total"] = int(aggregates.get("instinct_drift_total", 0) or 0) + int(record.get("instinct_drift", 0) or 0)
    _increment_bucket(aggregates, "stabilization_counts", str(record.get("stabilization_state") or "adaptive_stabilization"))
    _increment_bucket(aggregates, "fallback_counts", str(record.get("fallback_state") or "fallback_balanced"))
    _increment_bucket(aggregates, "resilience_counts", str(record.get("resilience_state") or "resilience_balanced"))
    _increment_bucket(aggregates, "continuity_counts", str(record.get("continuity_state") or "continuity_adaptive"))
    _increment_bucket(aggregates, "survival_counts", str(record.get("survival_state") or "survival_adaptive"))
    _increment_bucket(aggregates, "forecast_counts", str(record.get("forecast") or "resilience_convergence"))
    memory["aggregates"] = aggregates


def _increment_bucket(aggregates: dict[str, Any], bucket: str, key: str) -> None:
    groups = dict(aggregates.get(bucket) or {})
    groups[key] = int(groups.get(key, 0) or 0) + 1
    aggregates[bucket] = groups


def _average(value: Any, total: int) -> float:
    denominator = max(int(total or 0), 1)
    return round(int(value or 0) / denominator, 4)
