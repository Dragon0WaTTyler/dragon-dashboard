from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any, Mapping

from dragon.cache import save_json_file
from dragon.paths import CACHE_DIR


DEFAULT_CINEMATIC_MEMORY = {
    "version": 1,
    "corrupted_recoveries": 0,
    "entries": [],
    "aggregates": {
        "total_runs": 0,
        "quality_total": 0,
        "immersion_total": 0,
        "polish_total": 0,
        "drift_total": 0,
        "direction_counts": {},
        "pacing_counts": {},
        "atmosphere_counts": {},
        "governance_counts": {},
        "aesthetic_counts": {},
    },
}

_CINEMATIC_MEMORY_LOCK = threading.Lock()


def cinematic_memory_path(path: Path | None = None) -> Path:
    return Path(path or (CACHE_DIR / "magnets" / "runtime_cinema_memory.json"))


def load_cinematic_memory(*, path: Path | None = None) -> dict[str, Any]:
    target = cinematic_memory_path(path)
    payload, corrupted = _read_memory_file(target)
    normalized = _normalize_memory(payload)
    if corrupted:
        normalized["corrupted_recoveries"] = int(normalized.get("corrupted_recoveries", 0) or 0) + 1
        save_json_file(target, normalized)
    return normalized


def extract_cinematic_memory_record(
    orchestration: Mapping[str, Any] | None,
    cinema_result: Mapping[str, Any] | None = None,
    *,
    timestamp: str = "",
) -> dict[str, Any]:
    payload = dict(orchestration or {})
    result = dict(cinema_result or {})
    direction = dict(result.get("cinematic_direction") or payload.get("cinematic_direction") or {})
    pacing = dict(result.get("runtime_pacing") or payload.get("runtime_pacing") or {})
    immersion = dict(result.get("immersion_state") or payload.get("immersion_state") or {})
    atmosphere = dict(result.get("runtime_atmosphere") or payload.get("runtime_atmosphere") or {})
    governance = dict(result.get("cinematic_governance") or payload.get("cinematic_governance") or {})
    aesthetics = dict(result.get("runtime_aesthetics") or payload.get("runtime_aesthetics") or {})
    balance = dict(result.get("cinematic_balance") or payload.get("cinematic_balance") or {})
    metrics = dict(result.get("cinematic_metrics") or payload.get("cinematic_metrics") or {})
    prior = dict(payload.get("cinematic_memory") or {})
    prior_entries = [dict(item) for item in prior.get("recent_entries") or [] if isinstance(item, Mapping)]
    previous_direction = str((prior_entries[-1] or {}).get("direction_style") or "") if prior_entries else ""
    drift = 0 if not previous_direction or previous_direction == str(direction.get("style") or "") else 28
    return {
        "timestamp": str(timestamp or payload.get("updated_at") or payload.get("timestamp") or "").strip(),
        "direction_style": str(direction.get("style") or "cinematic_balanced"),
        "pacing": str(pacing.get("pacing") or "smooth_pacing"),
        "immersion_state": str(immersion.get("state") or "partially_immersive"),
        "atmosphere": str(atmosphere.get("atmosphere") or "calm_atmosphere"),
        "balance_state": str(balance.get("balance_state") or "balanced_cinema"),
        "governance_focus": str((list(governance.get("governance_actions") or []) or ["preserve_immersion"])[0]),
        "aesthetic_state": str(aesthetics.get("aesthetic_state") or "cinematic_runtime"),
        "cinematic_quality": int(metrics.get("cinematic_quality", 0) or 0),
        "immersion_strength": int(metrics.get("immersion_strength", 0) or 0),
        "runtime_polish": int(metrics.get("runtime_polish", 0) or 0),
        "cinematic_drift": drift,
    }


def update_cinematic_memory(
    orchestration: Mapping[str, Any] | None,
    cinema_result: Mapping[str, Any] | None,
    *,
    path: Path | None = None,
    timestamp: str = "",
) -> dict[str, Any]:
    with _CINEMATIC_MEMORY_LOCK:
        memory = load_cinematic_memory(path=path)
        record = extract_cinematic_memory_record(orchestration, cinema_result, timestamp=timestamp)
        memory["entries"] = [*list(memory.get("entries") or []), record]
        _update_aggregates(memory, record)
        save_json_file(cinematic_memory_path(path), memory)
    return build_cinematic_memory_summary(memory, current_context=cinema_result or orchestration)


def build_cinematic_memory_summary(
    memory: Mapping[str, Any] | None,
    *,
    current_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    payload = _normalize_memory(memory)
    aggregates = dict(payload.get("aggregates") or {})
    total_runs = int(aggregates.get("total_runs", 0) or 0)
    entries = [dict(item) for item in payload.get("entries") or [] if isinstance(item, Mapping)]
    current = dict(current_context or {})
    direction = dict(current.get("cinematic_direction") or {})
    return {
        "memory_status": "recovered" if int(payload.get("corrupted_recoveries", 0) or 0) else "healthy",
        "corrupted_recoveries": int(payload.get("corrupted_recoveries", 0) or 0),
        "total_observations": total_runs,
        "average_cinematic_quality": _average(aggregates.get("quality_total", 0), total_runs),
        "average_immersion_strength": _average(aggregates.get("immersion_total", 0), total_runs),
        "average_runtime_polish": _average(aggregates.get("polish_total", 0), total_runs),
        "average_cinematic_drift": _average(aggregates.get("drift_total", 0), total_runs),
        "direction_counts": dict(aggregates.get("direction_counts") or {}),
        "pacing_counts": dict(aggregates.get("pacing_counts") or {}),
        "atmosphere_counts": dict(aggregates.get("atmosphere_counts") or {}),
        "governance_counts": dict(aggregates.get("governance_counts") or {}),
        "aesthetic_counts": dict(aggregates.get("aesthetic_counts") or {}),
        "recent_direction_style": str(direction.get("style") or "unknown"),
        "recent_entries": entries[-6:],
    }


def _read_memory_file(path: Path) -> tuple[Any, bool]:
    if not path.exists():
        return DEFAULT_CINEMATIC_MEMORY, False
    try:
        return json.loads(path.read_text(encoding="utf-8")), False
    except Exception:
        return DEFAULT_CINEMATIC_MEMORY, True


def _normalize_memory(payload: Mapping[str, Any] | Any) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        payload = {}
    return {
        "version": 1,
        "corrupted_recoveries": int(payload.get("corrupted_recoveries", 0) or 0),
        "entries": [dict(item) for item in payload.get("entries") or [] if isinstance(item, Mapping)],
        "aggregates": {
            **DEFAULT_CINEMATIC_MEMORY["aggregates"],
            **dict(payload.get("aggregates") or {}),
        },
    }


def _update_aggregates(memory: dict[str, Any], record: Mapping[str, Any]) -> None:
    aggregates = dict(memory.get("aggregates") or {})
    aggregates["total_runs"] = int(aggregates.get("total_runs", 0) or 0) + 1
    aggregates["quality_total"] = int(aggregates.get("quality_total", 0) or 0) + int(record.get("cinematic_quality", 0) or 0)
    aggregates["immersion_total"] = int(aggregates.get("immersion_total", 0) or 0) + int(record.get("immersion_strength", 0) or 0)
    aggregates["polish_total"] = int(aggregates.get("polish_total", 0) or 0) + int(record.get("runtime_polish", 0) or 0)
    aggregates["drift_total"] = int(aggregates.get("drift_total", 0) or 0) + int(record.get("cinematic_drift", 0) or 0)
    _increment_bucket(aggregates, "direction_counts", str(record.get("direction_style") or "cinematic_balanced"))
    _increment_bucket(aggregates, "pacing_counts", str(record.get("pacing") or "smooth_pacing"))
    _increment_bucket(aggregates, "atmosphere_counts", str(record.get("atmosphere") or "calm_atmosphere"))
    _increment_bucket(aggregates, "governance_counts", str(record.get("governance_focus") or "preserve_immersion"))
    _increment_bucket(aggregates, "aesthetic_counts", str(record.get("aesthetic_state") or "cinematic_runtime"))
    memory["aggregates"] = aggregates


def _increment_bucket(aggregates: dict[str, Any], bucket: str, key: str) -> None:
    groups = dict(aggregates.get(bucket) or {})
    groups[key] = int(groups.get(key, 0) or 0) + 1
    aggregates[bucket] = groups


def _average(value: Any, total: int) -> float:
    denominator = max(int(total or 0), 1)
    return round(int(value or 0) / denominator, 4)
