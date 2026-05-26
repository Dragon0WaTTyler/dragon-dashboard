from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any, Mapping

from dragon.cache import save_json_file
from dragon.paths import CACHE_DIR


MAX_RESONANCE_MEMORY_ENTRIES = 48

DEFAULT_RESONANCE_MEMORY = {
    "version": 1,
    "corrupted_recoveries": 0,
    "entries": [],
    "aggregates": {
        "total_runs": 0,
        "stability_total": 0,
        "pressure_total": 0,
        "drift_total": 0,
        "phase_counts": {},
        "resonance_counts": {},
        "cinematic_counts": {},
        "equilibrium_counts": {},
    },
}

_RESONANCE_MEMORY_LOCK = threading.Lock()


def resonance_memory_path(path: Path | None = None) -> Path:
    return Path(path or (CACHE_DIR / "magnets" / "runtime_resonance_memory.json"))


def load_resonance_memory(*, path: Path | None = None) -> dict[str, Any]:
    target = resonance_memory_path(path)
    payload, corrupted = _read_memory_file(target)
    normalized = _normalize_memory(payload)
    if corrupted:
        normalized["corrupted_recoveries"] = int(normalized.get("corrupted_recoveries", 0) or 0) + 1
        save_json_file(target, normalized)
    return normalized


def extract_resonance_memory_record(
    orchestration: Mapping[str, Any] | None,
    resonance_result: Mapping[str, Any] | None = None,
    *,
    timestamp: str = "",
) -> dict[str, Any]:
    payload = dict(orchestration or {})
    result = dict(resonance_result or {})
    metrics = dict(result.get("resonance_metrics") or payload.get("resonance_metrics") or {})
    equilibrium = dict(result.get("resonance_equilibrium") or payload.get("resonance_equilibrium") or {})
    harmony = dict(result.get("resonance_harmony") or payload.get("resonance_harmony") or {})
    sync = dict(result.get("resonance_sync") or payload.get("resonance_sync") or {})
    return {
        "timestamp": str(timestamp or payload.get("updated_at") or payload.get("timestamp") or "").strip(),
        "resonance_phase": str(result.get("resonance_phase") or payload.get("resonance_phase") or "measured_resonance"),
        "orchestration_resonance": str(sync.get("orchestration_resonance") or payload.get("orchestration_resonance") or "moderate"),
        "cinematic_resonance": str(harmony.get("cinematic_resonance") or payload.get("cinematic_resonance") or "measured_cinematic_resonance"),
        "harmonic_runtime_state": str(equilibrium.get("equilibrium_state") or payload.get("harmonic_runtime_state") or "measured_harmonic_balance"),
        "resonance_stability": int(metrics.get("resonance_stability", result.get("resonance_stability", payload.get("resonance_stability", 0))) or 0),
        "resonance_pressure": int(metrics.get("resonance_pressure", result.get("resonance_pressure", payload.get("resonance_pressure", 0))) or 0),
        "sync_drift": int(metrics.get("sync_drift", result.get("sync_drift", payload.get("sync_drift", 0))) or 0),
    }


def update_resonance_memory(
    orchestration: Mapping[str, Any] | None,
    resonance_result: Mapping[str, Any] | None,
    *,
    path: Path | None = None,
    timestamp: str = "",
) -> dict[str, Any]:
    with _RESONANCE_MEMORY_LOCK:
        memory = load_resonance_memory(path=path)
        record = extract_resonance_memory_record(orchestration, resonance_result, timestamp=timestamp)
        memory["entries"] = [*list(memory.get("entries") or []), record][-MAX_RESONANCE_MEMORY_ENTRIES:]
        _update_aggregates(memory, record)
        save_json_file(resonance_memory_path(path), memory)
    return build_resonance_memory_summary(memory, current_context=resonance_result or orchestration)


def build_resonance_memory_summary(
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
        "average_resonance_stability": _average(aggregates.get("stability_total", 0), total),
        "average_resonance_pressure": _average(aggregates.get("pressure_total", 0), total),
        "average_sync_drift": _average(aggregates.get("drift_total", 0), total),
        "phase_counts": dict(aggregates.get("phase_counts") or {}),
        "resonance_counts": dict(aggregates.get("resonance_counts") or {}),
        "cinematic_counts": dict(aggregates.get("cinematic_counts") or {}),
        "equilibrium_counts": dict(aggregates.get("equilibrium_counts") or {}),
        "recent_entries": [dict(item) for item in payload.get("entries") or [] if isinstance(item, Mapping)][-6:],
        "current_resonance_phase": str(current.get("resonance_phase") or "unknown"),
    }


def _read_memory_file(path: Path) -> tuple[Any, bool]:
    if not path.exists():
        return DEFAULT_RESONANCE_MEMORY, False
    try:
        return json.loads(path.read_text(encoding="utf-8")), False
    except Exception:
        return DEFAULT_RESONANCE_MEMORY, True


def _normalize_memory(payload: Mapping[str, Any] | Any) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        payload = {}
    return {
        "version": 1,
        "corrupted_recoveries": int(payload.get("corrupted_recoveries", 0) or 0),
        "entries": [dict(item) for item in payload.get("entries") or [] if isinstance(item, Mapping)][-MAX_RESONANCE_MEMORY_ENTRIES:],
        "aggregates": {
            **DEFAULT_RESONANCE_MEMORY["aggregates"],
            **dict(payload.get("aggregates") or {}),
        },
    }


def _update_aggregates(memory: dict[str, Any], record: Mapping[str, Any]) -> None:
    aggregates = dict(memory.get("aggregates") or {})
    aggregates["total_runs"] = int(aggregates.get("total_runs", 0) or 0) + 1
    aggregates["stability_total"] = int(aggregates.get("stability_total", 0) or 0) + int(record.get("resonance_stability", 0) or 0)
    aggregates["pressure_total"] = int(aggregates.get("pressure_total", 0) or 0) + int(record.get("resonance_pressure", 0) or 0)
    aggregates["drift_total"] = int(aggregates.get("drift_total", 0) or 0) + int(record.get("sync_drift", 0) or 0)
    _increment_bucket(aggregates, "phase_counts", str(record.get("resonance_phase") or "measured_resonance"))
    _increment_bucket(aggregates, "resonance_counts", str(record.get("orchestration_resonance") or "moderate"))
    _increment_bucket(aggregates, "cinematic_counts", str(record.get("cinematic_resonance") or "measured_cinematic_resonance"))
    _increment_bucket(aggregates, "equilibrium_counts", str(record.get("harmonic_runtime_state") or "measured_harmonic_balance"))
    memory["aggregates"] = aggregates


def _increment_bucket(aggregates: dict[str, Any], bucket: str, key: str) -> None:
    groups = dict(aggregates.get(bucket) or {})
    groups[key] = int(groups.get(key, 0) or 0) + 1
    aggregates[bucket] = groups


def _average(value: Any, total: int) -> float:
    denominator = max(int(total or 0), 1)
    return round(int(value or 0) / denominator, 4)
