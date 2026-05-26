from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any, Mapping

from dragon.cache import save_json_file
from dragon.paths import CACHE_DIR


MAX_TEMPORAL_MEMORY_ENTRIES = 48

DEFAULT_TEMPORAL_MEMORY = {
    "version": 1,
    "corrupted_recoveries": 0,
    "entries": [],
    "aggregates": {
        "total_runs": 0,
        "stability_total": 0,
        "momentum_total": 0,
        "decay_total": 0,
        "phase_counts": {},
        "rhythm_counts": {},
        "flow_counts": {},
        "projection_counts": {},
    },
}

_TEMPORAL_MEMORY_LOCK = threading.Lock()


def temporal_memory_path(path: Path | None = None) -> Path:
    return Path(path or (CACHE_DIR / "magnets" / "runtime_temporal_memory.json"))


def load_temporal_memory(*, path: Path | None = None) -> dict[str, Any]:
    target = temporal_memory_path(path)
    payload, corrupted = _read_memory_file(target)
    normalized = _normalize_memory(payload)
    if corrupted:
        normalized["corrupted_recoveries"] = int(normalized.get("corrupted_recoveries", 0) or 0) + 1
        save_json_file(target, normalized)
    return normalized


def extract_temporal_memory_record(
    orchestration: Mapping[str, Any] | None,
    temporal_result: Mapping[str, Any] | None = None,
    *,
    timestamp: str = "",
) -> dict[str, Any]:
    payload = dict(orchestration or {})
    result = dict(temporal_result or {})
    metrics = dict(result.get("temporal_metrics") or payload.get("temporal_metrics") or {})
    return {
        "timestamp": str(timestamp or payload.get("updated_at") or payload.get("timestamp") or "").strip(),
        "runtime_cycle_phase": str(result.get("runtime_cycle_phase") or payload.get("runtime_cycle_phase") or "measured_continuity"),
        "runtime_rhythm_state": str(result.get("runtime_rhythm_state") or payload.get("runtime_rhythm_state") or "measured_pacing"),
        "cinematic_temporal_flow": str(result.get("cinematic_temporal_flow") or payload.get("cinematic_temporal_flow") or "steady_cinematic_flow"),
        "temporal_projection": str(result.get("temporal_projection") or payload.get("temporal_projection") or "measured_future_shaping"),
        "continuity_state": str((result.get("temporal_continuity") or {}).get("state") or (payload.get("temporal_continuity") or {}).get("state") or "adaptive_temporal_continuity"),
        "temporal_stability": int(metrics.get("temporal_stability", result.get("temporal_stability", payload.get("temporal_stability", 0))) or 0),
        "temporal_momentum": int(metrics.get("temporal_momentum", result.get("temporal_momentum", payload.get("temporal_momentum", 0))) or 0),
        "continuity_decay_rate": int(metrics.get("continuity_decay_rate", result.get("continuity_decay_rate", payload.get("continuity_decay_rate", 0))) or 0),
    }


def update_temporal_memory(
    orchestration: Mapping[str, Any] | None,
    temporal_result: Mapping[str, Any] | None,
    *,
    path: Path | None = None,
    timestamp: str = "",
) -> dict[str, Any]:
    with _TEMPORAL_MEMORY_LOCK:
        memory = load_temporal_memory(path=path)
        record = extract_temporal_memory_record(orchestration, temporal_result, timestamp=timestamp)
        memory["entries"] = [*list(memory.get("entries") or []), record][-MAX_TEMPORAL_MEMORY_ENTRIES:]
        _update_aggregates(memory, record)
        save_json_file(temporal_memory_path(path), memory)
    return build_temporal_memory_summary(memory, current_context=temporal_result or orchestration)


def build_temporal_memory_summary(
    memory: Mapping[str, Any] | None,
    *,
    current_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    payload = _normalize_memory(memory)
    aggregates = dict(payload.get("aggregates") or {})
    total = int(aggregates.get("total_runs", 0) or 0)
    current = dict(current_context or {})
    return {
        "memory_status": "recovered" if int(payload.get("corrupted_recoveries", 0) or 0) else "healthy",
        "corrupted_recoveries": int(payload.get("corrupted_recoveries", 0) or 0),
        "total_observations": total,
        "average_temporal_stability": _average(aggregates.get("stability_total", 0), total),
        "average_temporal_momentum": _average(aggregates.get("momentum_total", 0), total),
        "average_continuity_decay_rate": _average(aggregates.get("decay_total", 0), total),
        "phase_counts": dict(aggregates.get("phase_counts") or {}),
        "rhythm_counts": dict(aggregates.get("rhythm_counts") or {}),
        "flow_counts": dict(aggregates.get("flow_counts") or {}),
        "projection_counts": dict(aggregates.get("projection_counts") or {}),
        "recent_entries": [dict(item) for item in payload.get("entries") or [] if isinstance(item, Mapping)][-6:],
        "current_cycle_phase": str(current.get("runtime_cycle_phase") or "unknown"),
    }


def _read_memory_file(path: Path) -> tuple[Any, bool]:
    if not path.exists():
        return DEFAULT_TEMPORAL_MEMORY, False
    try:
        return json.loads(path.read_text(encoding="utf-8")), False
    except Exception:
        return DEFAULT_TEMPORAL_MEMORY, True


def _normalize_memory(payload: Mapping[str, Any] | Any) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        payload = {}
    return {
        "version": 1,
        "corrupted_recoveries": int(payload.get("corrupted_recoveries", 0) or 0),
        "entries": [dict(item) for item in payload.get("entries") or [] if isinstance(item, Mapping)][-MAX_TEMPORAL_MEMORY_ENTRIES:],
        "aggregates": {
            **DEFAULT_TEMPORAL_MEMORY["aggregates"],
            **dict(payload.get("aggregates") or {}),
        },
    }


def _update_aggregates(memory: dict[str, Any], record: Mapping[str, Any]) -> None:
    aggregates = dict(memory.get("aggregates") or {})
    aggregates["total_runs"] = int(aggregates.get("total_runs", 0) or 0) + 1
    aggregates["stability_total"] = int(aggregates.get("stability_total", 0) or 0) + int(record.get("temporal_stability", 0) or 0)
    aggregates["momentum_total"] = int(aggregates.get("momentum_total", 0) or 0) + int(record.get("temporal_momentum", 0) or 0)
    aggregates["decay_total"] = int(aggregates.get("decay_total", 0) or 0) + int(record.get("continuity_decay_rate", 0) or 0)
    _increment_bucket(aggregates, "phase_counts", str(record.get("runtime_cycle_phase") or "measured_continuity"))
    _increment_bucket(aggregates, "rhythm_counts", str(record.get("runtime_rhythm_state") or "measured_pacing"))
    _increment_bucket(aggregates, "flow_counts", str(record.get("cinematic_temporal_flow") or "steady_cinematic_flow"))
    _increment_bucket(aggregates, "projection_counts", str(record.get("temporal_projection") or "measured_future_shaping"))
    memory["aggregates"] = aggregates


def _increment_bucket(aggregates: dict[str, Any], bucket: str, key: str) -> None:
    groups = dict(aggregates.get(bucket) or {})
    groups[key] = int(groups.get(key, 0) or 0) + 1
    aggregates[bucket] = groups


def _average(value: Any, total: int) -> float:
    denominator = max(int(total or 0), 1)
    return round(int(value or 0) / denominator, 4)
