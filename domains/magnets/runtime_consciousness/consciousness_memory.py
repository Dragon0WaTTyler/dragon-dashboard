from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any, Mapping

from dragon.cache import save_json_file
from dragon.paths import CACHE_DIR


DEFAULT_CONSCIOUSNESS_MEMORY = {
    "version": 1,
    "corrupted_recoveries": 0,
    "entries": [],
    "aggregates": {
        "total_runs": 0,
        "awareness_integrity_total": 0,
        "cognitive_stability_total": 0,
        "perception_integrity_total": 0,
        "reflection_strength_total": 0,
        "awareness_drift_total": 0,
        "awareness_counts": {},
        "focus_counts": {},
        "continuity_counts": {},
        "forecast_counts": {},
        "governance_counts": {},
    },
}

_CONSCIOUSNESS_MEMORY_LOCK = threading.Lock()


def consciousness_memory_path(path: Path | None = None) -> Path:
    return Path(path or (CACHE_DIR / "magnets" / "runtime_consciousness_memory.json"))


def load_consciousness_memory(*, path: Path | None = None) -> dict[str, Any]:
    target = consciousness_memory_path(path)
    payload, corrupted = _read_memory_file(target)
    normalized = _normalize_memory(payload)
    if corrupted:
        normalized["corrupted_recoveries"] = int(normalized.get("corrupted_recoveries", 0) or 0) + 1
        save_json_file(target, normalized)
    return normalized


def extract_consciousness_memory_record(
    orchestration: Mapping[str, Any] | None,
    consciousness_result: Mapping[str, Any] | None = None,
    *,
    timestamp: str = "",
) -> dict[str, Any]:
    payload = dict(orchestration or {})
    result = dict(consciousness_result or {})
    awareness = dict(result.get("awareness_state") or payload.get("awareness_state") or {})
    focus = dict(result.get("orchestration_focus") or payload.get("orchestration_focus") or {})
    continuity = dict(result.get("continuity_awareness") or payload.get("continuity_awareness") or {})
    reflection = dict(result.get("runtime_reflection") or payload.get("runtime_reflection") or {})
    perception = dict(result.get("orchestration_perception") or payload.get("orchestration_perception") or {})
    metrics = dict(result.get("consciousness_metrics") or payload.get("consciousness_metrics") or {})
    forecast = dict(result.get("consciousness_forecast") or payload.get("consciousness_forecast") or {})
    governance = dict(result.get("consciousness_governance") or payload.get("consciousness_governance") or {})
    prior = dict(payload.get("consciousness_memory") or {})
    prior_entries = [dict(item) for item in prior.get("recent_entries") or [] if isinstance(item, Mapping)]
    previous_awareness = str((prior_entries[-1] or {}).get("awareness_state") or "") if prior_entries else ""
    awareness_state = str(awareness.get("state") or "stable_awareness")
    drift = 0 if not previous_awareness or previous_awareness == awareness_state else 24
    return {
        "timestamp": str(timestamp or payload.get("updated_at") or payload.get("timestamp") or "").strip(),
        "awareness_state": awareness_state,
        "focus": str(focus.get("focus") or "equilibrium_focus"),
        "continuity_awareness": str(continuity.get("state") or "adaptive_awareness"),
        "reflection_state": str(reflection.get("state") or "stable_reflection"),
        "perception_state": str(perception.get("state") or "stable_perception"),
        "forecast": str(forecast.get("forecast") or "awareness_stabilization"),
        "governance_focus": str((list(governance.get("governance_actions") or []) or ["preserve_continuity_awareness"])[0]),
        "awareness_integrity": int(metrics.get("awareness_integrity", awareness.get("awareness_integrity", 0)) or 0),
        "cognitive_stability": int(metrics.get("cognitive_stability", 0) or 0),
        "perception_integrity": int(metrics.get("perception_integrity", perception.get("perception_integrity", 0)) or 0),
        "reflection_strength": int(metrics.get("reflection_strength", reflection.get("reflection_strength", 0)) or 0),
        "orchestration_clarity": int(metrics.get("orchestration_clarity", 0) or 0),
        "awareness_drift": drift,
    }


def update_consciousness_memory(
    orchestration: Mapping[str, Any] | None,
    consciousness_result: Mapping[str, Any] | None,
    *,
    path: Path | None = None,
    timestamp: str = "",
) -> dict[str, Any]:
    with _CONSCIOUSNESS_MEMORY_LOCK:
        memory = load_consciousness_memory(path=path)
        record = extract_consciousness_memory_record(orchestration, consciousness_result, timestamp=timestamp)
        memory["entries"] = [*list(memory.get("entries") or []), record]
        _update_aggregates(memory, record)
        save_json_file(consciousness_memory_path(path), memory)
    return build_consciousness_memory_summary(memory, current_context=consciousness_result or orchestration)


def build_consciousness_memory_summary(
    memory: Mapping[str, Any] | None,
    *,
    current_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    payload = _normalize_memory(memory)
    aggregates = dict(payload.get("aggregates") or {})
    total_runs = int(aggregates.get("total_runs", 0) or 0)
    entries = [dict(item) for item in payload.get("entries") or [] if isinstance(item, Mapping)]
    current = dict(current_context or {})
    awareness = dict(current.get("awareness_state") or {})
    return {
        "memory_status": "recovered" if int(payload.get("corrupted_recoveries", 0) or 0) else "healthy",
        "corrupted_recoveries": int(payload.get("corrupted_recoveries", 0) or 0),
        "total_observations": total_runs,
        "average_awareness_integrity": _average(aggregates.get("awareness_integrity_total", 0), total_runs),
        "average_cognitive_stability": _average(aggregates.get("cognitive_stability_total", 0), total_runs),
        "average_perception_integrity": _average(aggregates.get("perception_integrity_total", 0), total_runs),
        "average_reflection_strength": _average(aggregates.get("reflection_strength_total", 0), total_runs),
        "average_awareness_drift": _average(aggregates.get("awareness_drift_total", 0), total_runs),
        "awareness_counts": dict(aggregates.get("awareness_counts") or {}),
        "focus_counts": dict(aggregates.get("focus_counts") or {}),
        "continuity_counts": dict(aggregates.get("continuity_counts") or {}),
        "forecast_counts": dict(aggregates.get("forecast_counts") or {}),
        "governance_counts": dict(aggregates.get("governance_counts") or {}),
        "recent_awareness_state": str(awareness.get("state") or "unknown"),
        "recent_entries": entries[-6:],
    }


def _read_memory_file(path: Path) -> tuple[Any, bool]:
    if not path.exists():
        return DEFAULT_CONSCIOUSNESS_MEMORY, False
    try:
        return json.loads(path.read_text(encoding="utf-8")), False
    except Exception:
        return DEFAULT_CONSCIOUSNESS_MEMORY, True


def _normalize_memory(payload: Mapping[str, Any] | Any) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        payload = {}
    return {
        "version": 1,
        "corrupted_recoveries": int(payload.get("corrupted_recoveries", 0) or 0),
        "entries": [dict(item) for item in payload.get("entries") or [] if isinstance(item, Mapping)],
        "aggregates": {
            **DEFAULT_CONSCIOUSNESS_MEMORY["aggregates"],
            **dict(payload.get("aggregates") or {}),
        },
    }


def _update_aggregates(memory: dict[str, Any], record: Mapping[str, Any]) -> None:
    aggregates = dict(memory.get("aggregates") or {})
    aggregates["total_runs"] = int(aggregates.get("total_runs", 0) or 0) + 1
    aggregates["awareness_integrity_total"] = int(aggregates.get("awareness_integrity_total", 0) or 0) + int(record.get("awareness_integrity", 0) or 0)
    aggregates["cognitive_stability_total"] = int(aggregates.get("cognitive_stability_total", 0) or 0) + int(record.get("cognitive_stability", 0) or 0)
    aggregates["perception_integrity_total"] = int(aggregates.get("perception_integrity_total", 0) or 0) + int(record.get("perception_integrity", 0) or 0)
    aggregates["reflection_strength_total"] = int(aggregates.get("reflection_strength_total", 0) or 0) + int(record.get("reflection_strength", 0) or 0)
    aggregates["awareness_drift_total"] = int(aggregates.get("awareness_drift_total", 0) or 0) + int(record.get("awareness_drift", 0) or 0)
    _increment_bucket(aggregates, "awareness_counts", str(record.get("awareness_state") or "stable_awareness"))
    _increment_bucket(aggregates, "focus_counts", str(record.get("focus") or "equilibrium_focus"))
    _increment_bucket(aggregates, "continuity_counts", str(record.get("continuity_awareness") or "adaptive_awareness"))
    _increment_bucket(aggregates, "forecast_counts", str(record.get("forecast") or "awareness_stabilization"))
    _increment_bucket(aggregates, "governance_counts", str(record.get("governance_focus") or "preserve_continuity_awareness"))
    memory["aggregates"] = aggregates


def _increment_bucket(aggregates: dict[str, Any], bucket: str, key: str) -> None:
    groups = dict(aggregates.get(bucket) or {})
    groups[key] = int(groups.get(key, 0) or 0) + 1
    aggregates[bucket] = groups


def _average(value: Any, total: int) -> float:
    denominator = max(int(total or 0), 1)
    return round(int(value or 0) / denominator, 4)
