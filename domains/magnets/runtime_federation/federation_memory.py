from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any, Mapping

from dragon.cache import save_json_file
from dragon.paths import CACHE_DIR


MAX_FEDERATION_MEMORY_ENTRIES = 48

DEFAULT_FEDERATION_MEMORY = {
    "version": 1,
    "corrupted_recoveries": 0,
    "entries": [],
    "aggregates": {
        "total_runs": 0,
        "coherence_total": 0,
        "pressure_total": 0,
        "resilience_total": 0,
        "unity_counts": {},
        "phase_counts": {},
        "continuity_counts": {},
        "cinematic_state_counts": {},
    },
}

_FEDERATION_MEMORY_LOCK = threading.Lock()


def federation_memory_path(path: Path | None = None) -> Path:
    return Path(path or (CACHE_DIR / "magnets" / "runtime_federation_memory.json"))


def load_federation_memory(*, path: Path | None = None) -> dict[str, Any]:
    target = federation_memory_path(path)
    payload, corrupted = _read_memory_file(target)
    normalized = _normalize_memory(payload)
    if corrupted:
        normalized["corrupted_recoveries"] = int(normalized.get("corrupted_recoveries", 0) or 0) + 1
        save_json_file(target, normalized)
    return normalized


def extract_federation_memory_record(
    orchestration: Mapping[str, Any] | None,
    federation_result: Mapping[str, Any] | None = None,
    *,
    timestamp: str = "",
) -> dict[str, Any]:
    payload = dict(orchestration or {})
    result = dict(federation_result or {})
    metrics = dict(result.get("federation_metrics") or payload.get("federation_metrics") or {})
    continuity = dict(result.get("federation_continuity") or payload.get("federation_continuity") or {})
    state = dict(result.get("federation_state") or payload.get("federation_state") or {})
    projection = dict(result.get("federation_projection") or payload.get("federation_projection") or {})
    return {
        "timestamp": str(timestamp or payload.get("updated_at") or payload.get("timestamp") or "").strip(),
        "phase_transition": str(state.get("phase_transition") or payload.get("runtime_phase_transition") or "steady_continuity"),
        "orchestration_unity": str(state.get("orchestration_unity") or payload.get("orchestration_unity") or "moderate"),
        "continuity_projection": str(continuity.get("continuity_projection") or projection.get("continuity_projection") or payload.get("continuity_projection") or "measured_continuity"),
        "cinematic_runtime_state": str(projection.get("cinematic_runtime_state") or payload.get("cinematic_runtime_state") or "adaptive_cinematic_balance"),
        "federation_coherence": int(metrics.get("federation_coherence", payload.get("federation_coherence", 0)) or 0),
        "federation_pressure": int(metrics.get("federation_pressure", payload.get("federation_pressure", 0)) or 0),
        "federation_resilience": int(metrics.get("federation_resilience", payload.get("federation_resilience", 0)) or 0),
    }


def update_federation_memory(
    orchestration: Mapping[str, Any] | None,
    federation_result: Mapping[str, Any] | None,
    *,
    path: Path | None = None,
    timestamp: str = "",
) -> dict[str, Any]:
    with _FEDERATION_MEMORY_LOCK:
        memory = load_federation_memory(path=path)
        record = extract_federation_memory_record(orchestration, federation_result, timestamp=timestamp)
        memory["entries"] = [*list(memory.get("entries") or []), record][-MAX_FEDERATION_MEMORY_ENTRIES:]
        _update_aggregates(memory, record)
        save_json_file(federation_memory_path(path), memory)
    return build_federation_memory_summary(memory, current_context=federation_result or orchestration)


def build_federation_memory_summary(
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
        "average_federation_coherence": _average(aggregates.get("coherence_total", 0), total),
        "average_federation_pressure": _average(aggregates.get("pressure_total", 0), total),
        "average_federation_resilience": _average(aggregates.get("resilience_total", 0), total),
        "unity_counts": dict(aggregates.get("unity_counts") or {}),
        "phase_counts": dict(aggregates.get("phase_counts") or {}),
        "continuity_counts": dict(aggregates.get("continuity_counts") or {}),
        "cinematic_state_counts": dict(aggregates.get("cinematic_state_counts") or {}),
        "recent_entries": [dict(item) for item in payload.get("entries") or [] if isinstance(item, Mapping)][-6:],
        "current_phase_transition": str(current.get("runtime_phase_transition") or "unknown"),
    }


def _read_memory_file(path: Path) -> tuple[Any, bool]:
    if not path.exists():
        return DEFAULT_FEDERATION_MEMORY, False
    try:
        return json.loads(path.read_text(encoding="utf-8")), False
    except Exception:
        return DEFAULT_FEDERATION_MEMORY, True


def _normalize_memory(payload: Mapping[str, Any] | Any) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        payload = {}
    return {
        "version": 1,
        "corrupted_recoveries": int(payload.get("corrupted_recoveries", 0) or 0),
        "entries": [dict(item) for item in payload.get("entries") or [] if isinstance(item, Mapping)][-MAX_FEDERATION_MEMORY_ENTRIES:],
        "aggregates": {
            **DEFAULT_FEDERATION_MEMORY["aggregates"],
            **dict(payload.get("aggregates") or {}),
        },
    }


def _update_aggregates(memory: dict[str, Any], record: Mapping[str, Any]) -> None:
    aggregates = dict(memory.get("aggregates") or {})
    aggregates["total_runs"] = int(aggregates.get("total_runs", 0) or 0) + 1
    aggregates["coherence_total"] = int(aggregates.get("coherence_total", 0) or 0) + int(record.get("federation_coherence", 0) or 0)
    aggregates["pressure_total"] = int(aggregates.get("pressure_total", 0) or 0) + int(record.get("federation_pressure", 0) or 0)
    aggregates["resilience_total"] = int(aggregates.get("resilience_total", 0) or 0) + int(record.get("federation_resilience", 0) or 0)
    _increment_bucket(aggregates, "unity_counts", str(record.get("orchestration_unity") or "moderate"))
    _increment_bucket(aggregates, "phase_counts", str(record.get("phase_transition") or "steady_continuity"))
    _increment_bucket(aggregates, "continuity_counts", str(record.get("continuity_projection") or "measured_continuity"))
    _increment_bucket(aggregates, "cinematic_state_counts", str(record.get("cinematic_runtime_state") or "adaptive_cinematic_balance"))
    memory["aggregates"] = aggregates


def _increment_bucket(aggregates: dict[str, Any], bucket: str, key: str) -> None:
    groups = dict(aggregates.get(bucket) or {})
    groups[key] = int(groups.get(key, 0) or 0) + 1
    aggregates[bucket] = groups


def _average(value: Any, total: int) -> float:
    denominator = max(int(total or 0), 1)
    return round(int(value or 0) / denominator, 4)
