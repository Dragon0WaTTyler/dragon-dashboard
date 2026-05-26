from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any, Mapping

from dragon.cache import save_json_file
from dragon.paths import CACHE_DIR


MAX_SYMBIOSIS_MEMORY_ENTRIES = 48

DEFAULT_SYMBIOSIS_MEMORY = {
    "version": 1,
    "corrupted_recoveries": 0,
    "entries": [],
    "aggregates": {
        "total_runs": 0,
        "stability_total": 0,
        "stress_total": 0,
        "mutualism_total": 0,
        "phase_counts": {},
        "health_counts": {},
        "coexistence_counts": {},
    },
}

_SYMBIOSIS_MEMORY_LOCK = threading.Lock()


def symbiosis_memory_path(path: Path | None = None) -> Path:
    return Path(path or (CACHE_DIR / "magnets" / "runtime_symbiosis_memory.json"))


def load_symbiosis_memory(*, path: Path | None = None) -> dict[str, Any]:
    target = symbiosis_memory_path(path)
    payload, corrupted = _read_memory_file(target)
    normalized = _normalize_memory(payload)
    if corrupted:
        normalized["corrupted_recoveries"] = int(normalized.get("corrupted_recoveries", 0) or 0) + 1
        save_json_file(target, normalized)
    return normalized


def extract_symbiosis_memory_record(
    orchestration: Mapping[str, Any] | None,
    symbiosis_result: Mapping[str, Any] | None = None,
    *,
    timestamp: str = "",
) -> dict[str, Any]:
    payload = dict(orchestration or {})
    result = dict(symbiosis_result or {})
    metrics = dict(result.get("symbiosis_metrics") or payload.get("symbiosis_metrics") or {})
    return {
        "timestamp": str(timestamp or payload.get("updated_at") or payload.get("timestamp") or "").strip(),
        "symbiotic_phase": str(result.get("symbiotic_phase") or payload.get("symbiotic_phase") or "measured_symbiosis"),
        "runtime_coexistence": str(result.get("runtime_coexistence") or payload.get("runtime_coexistence") or "measured_coexistence"),
        "systemic_runtime_health": str(result.get("systemic_runtime_health") or payload.get("systemic_runtime_health") or "measured_health"),
        "symbiosis_stability": int(metrics.get("symbiosis_stability", result.get("symbiosis_stability", payload.get("symbiosis_stability", 0))) or 0),
        "dependency_stress": int(metrics.get("dependency_stress", result.get("dependency_stress", payload.get("dependency_stress", 0))) or 0),
        "symbiosis_mutualism": int(metrics.get("symbiosis_mutualism", result.get("symbiosis_mutualism", payload.get("symbiosis_mutualism", 0))) or 0),
    }


def update_symbiosis_memory(
    orchestration: Mapping[str, Any] | None,
    symbiosis_result: Mapping[str, Any] | None,
    *,
    path: Path | None = None,
    timestamp: str = "",
) -> dict[str, Any]:
    with _SYMBIOSIS_MEMORY_LOCK:
        memory = load_symbiosis_memory(path=path)
        record = extract_symbiosis_memory_record(orchestration, symbiosis_result, timestamp=timestamp)
        memory["entries"] = [*list(memory.get("entries") or []), record][-MAX_SYMBIOSIS_MEMORY_ENTRIES:]
        _update_aggregates(memory, record)
        save_json_file(symbiosis_memory_path(path), memory)
    return build_symbiosis_memory_summary(memory, current_context=symbiosis_result or orchestration)


def build_symbiosis_memory_summary(
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
        "average_symbiosis_stability": _average(aggregates.get("stability_total", 0), total),
        "average_dependency_stress": _average(aggregates.get("stress_total", 0), total),
        "average_symbiosis_mutualism": _average(aggregates.get("mutualism_total", 0), total),
        "phase_counts": dict(aggregates.get("phase_counts") or {}),
        "health_counts": dict(aggregates.get("health_counts") or {}),
        "coexistence_counts": dict(aggregates.get("coexistence_counts") or {}),
        "recent_entries": [dict(item) for item in payload.get("entries") or [] if isinstance(item, Mapping)][-6:],
        "current_symbiotic_phase": str(current.get("symbiotic_phase") or "unknown"),
    }


def _read_memory_file(path: Path) -> tuple[Any, bool]:
    if not path.exists():
        return DEFAULT_SYMBIOSIS_MEMORY, False
    try:
        return json.loads(path.read_text(encoding="utf-8")), False
    except Exception:
        return DEFAULT_SYMBIOSIS_MEMORY, True


def _normalize_memory(payload: Mapping[str, Any] | Any) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        payload = {}
    return {
        "version": 1,
        "corrupted_recoveries": int(payload.get("corrupted_recoveries", 0) or 0),
        "entries": [dict(item) for item in payload.get("entries") or [] if isinstance(item, Mapping)][-MAX_SYMBIOSIS_MEMORY_ENTRIES:],
        "aggregates": {
            **DEFAULT_SYMBIOSIS_MEMORY["aggregates"],
            **dict(payload.get("aggregates") or {}),
        },
    }


def _update_aggregates(memory: dict[str, Any], record: Mapping[str, Any]) -> None:
    aggregates = dict(memory.get("aggregates") or {})
    aggregates["total_runs"] = int(aggregates.get("total_runs", 0) or 0) + 1
    aggregates["stability_total"] = int(aggregates.get("stability_total", 0) or 0) + int(record.get("symbiosis_stability", 0) or 0)
    aggregates["stress_total"] = int(aggregates.get("stress_total", 0) or 0) + int(record.get("dependency_stress", 0) or 0)
    aggregates["mutualism_total"] = int(aggregates.get("mutualism_total", 0) or 0) + int(record.get("symbiosis_mutualism", 0) or 0)
    _increment_bucket(aggregates, "phase_counts", str(record.get("symbiotic_phase") or "measured_symbiosis"))
    _increment_bucket(aggregates, "health_counts", str(record.get("systemic_runtime_health") or "measured_health"))
    _increment_bucket(aggregates, "coexistence_counts", str(record.get("runtime_coexistence") or "measured_coexistence"))
    memory["aggregates"] = aggregates


def _increment_bucket(aggregates: dict[str, Any], bucket: str, key: str) -> None:
    values = dict(aggregates.get(bucket) or {})
    values[key] = int(values.get(key, 0) or 0) + 1
    aggregates[bucket] = values


def _average(value: Any, total: int) -> float:
    denominator = max(int(total or 0), 1)
    return round(int(value or 0) / denominator, 4)
