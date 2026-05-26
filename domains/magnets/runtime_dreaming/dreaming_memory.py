from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any, Mapping

from dragon.cache import save_json_file
from dragon.paths import CACHE_DIR


DEFAULT_DREAMING_MEMORY = {
    "version": 1,
    "corrupted_recoveries": 0,
    "entries": [],
    "aggregates": {
        "total_runs": 0,
        "dreaming_integrity_total": 0,
        "projection_strength_total": 0,
        "mirroring_integrity_total": 0,
        "dream_drift_total": 0,
        "cinematic_counts": {},
        "projection_counts": {},
        "pathway_counts": {},
        "forecast_counts": {},
        "vision_counts": {},
    },
}

_DREAMING_MEMORY_LOCK = threading.Lock()


def dreaming_memory_path(path: Path | None = None) -> Path:
    return Path(path or (CACHE_DIR / "magnets" / "runtime_dreaming_memory.json"))


def load_dreaming_memory(*, path: Path | None = None) -> dict[str, Any]:
    target = dreaming_memory_path(path)
    payload, corrupted = _read_memory_file(target)
    normalized = _normalize_memory(payload)
    if corrupted:
        normalized["corrupted_recoveries"] = int(normalized.get("corrupted_recoveries", 0) or 0) + 1
        save_json_file(target, normalized)
    return normalized


def extract_dreaming_memory_record(
    orchestration: Mapping[str, Any] | None,
    dreaming_result: Mapping[str, Any] | None = None,
    *,
    timestamp: str = "",
) -> dict[str, Any]:
    payload = dict(orchestration or {})
    result = dict(dreaming_result or {})
    cinematic = dict(result.get("cinematic_dreams") or payload.get("cinematic_dreams") or {})
    projection = dict(result.get("latent_projection") or payload.get("latent_projection") or {})
    pathway = dict(result.get("dormant_pathways") or payload.get("dormant_pathways") or {})
    forecast = dict(result.get("dream_forecast") or payload.get("dream_forecast") or {})
    vision = dict(result.get("orchestration_visions") or payload.get("orchestration_visions") or {})
    metrics = dict(result.get("dream_metrics") or payload.get("dream_metrics") or {})
    prior = dict(payload.get("dreaming_memory") or {})
    prior_entries = [dict(item) for item in prior.get("recent_entries") or [] if isinstance(item, Mapping)]
    previous_dream = str((prior_entries[-1] or {}).get("cinematic_dream") or "") if prior_entries else ""
    cinematic_dream = str(cinematic.get("state") or "adaptive_cinema_dream")
    drift = 0 if not previous_dream or previous_dream == cinematic_dream else 24
    return {
        "timestamp": str(timestamp or payload.get("updated_at") or payload.get("timestamp") or "").strip(),
        "cinematic_dream": cinematic_dream,
        "latent_projection": str(projection.get("state") or "latent_stability_projection"),
        "dormant_pathway": str(pathway.get("state") or "dormant_adaptation_path"),
        "forecast": str(forecast.get("forecast") or "orchestration_recovery_projection"),
        "vision": str(vision.get("vision") or "stabilization_vision"),
        "dreaming_integrity": int(metrics.get("dreaming_integrity", 0) or 0),
        "projection_strength": int(metrics.get("cinematic_projection_strength", 0) or 0),
        "mirroring_integrity": int(metrics.get("runtime_mirroring_integrity", 0) or 0),
        "dream_drift": drift,
    }


def update_dreaming_memory(
    orchestration: Mapping[str, Any] | None,
    dreaming_result: Mapping[str, Any] | None,
    *,
    path: Path | None = None,
    timestamp: str = "",
) -> dict[str, Any]:
    with _DREAMING_MEMORY_LOCK:
        memory = load_dreaming_memory(path=path)
        record = extract_dreaming_memory_record(orchestration, dreaming_result, timestamp=timestamp)
        memory["entries"] = [*list(memory.get("entries") or []), record]
        _update_aggregates(memory, record)
        save_json_file(dreaming_memory_path(path), memory)
    return build_dreaming_memory_summary(memory, current_context=dreaming_result or orchestration)


def build_dreaming_memory_summary(
    memory: Mapping[str, Any] | None,
    *,
    current_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    payload = _normalize_memory(memory)
    aggregates = dict(payload.get("aggregates") or {})
    total_runs = int(aggregates.get("total_runs", 0) or 0)
    entries = [dict(item) for item in payload.get("entries") or [] if isinstance(item, Mapping)]
    current = dict(current_context or {})
    cinematic = dict(current.get("cinematic_dreams") or {})
    return {
        "memory_status": "recovered" if int(payload.get("corrupted_recoveries", 0) or 0) else "healthy",
        "corrupted_recoveries": int(payload.get("corrupted_recoveries", 0) or 0),
        "total_observations": total_runs,
        "average_dreaming_integrity": _average(aggregates.get("dreaming_integrity_total", 0), total_runs),
        "average_projection_strength": _average(aggregates.get("projection_strength_total", 0), total_runs),
        "average_mirroring_integrity": _average(aggregates.get("mirroring_integrity_total", 0), total_runs),
        "average_dream_drift": _average(aggregates.get("dream_drift_total", 0), total_runs),
        "cinematic_counts": dict(aggregates.get("cinematic_counts") or {}),
        "projection_counts": dict(aggregates.get("projection_counts") or {}),
        "pathway_counts": dict(aggregates.get("pathway_counts") or {}),
        "forecast_counts": dict(aggregates.get("forecast_counts") or {}),
        "vision_counts": dict(aggregates.get("vision_counts") or {}),
        "recent_cinematic_dream": str(cinematic.get("state") or "unknown"),
        "recent_entries": entries[-6:],
    }


def _read_memory_file(path: Path) -> tuple[Any, bool]:
    if not path.exists():
        return DEFAULT_DREAMING_MEMORY, False
    try:
        return json.loads(path.read_text(encoding="utf-8")), False
    except Exception:
        return DEFAULT_DREAMING_MEMORY, True


def _normalize_memory(payload: Mapping[str, Any] | Any) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        payload = {}
    return {
        "version": 1,
        "corrupted_recoveries": int(payload.get("corrupted_recoveries", 0) or 0),
        "entries": [dict(item) for item in payload.get("entries") or [] if isinstance(item, Mapping)],
        "aggregates": {
            **DEFAULT_DREAMING_MEMORY["aggregates"],
            **dict(payload.get("aggregates") or {}),
        },
    }


def _update_aggregates(memory: dict[str, Any], record: Mapping[str, Any]) -> None:
    aggregates = dict(memory.get("aggregates") or {})
    aggregates["total_runs"] = int(aggregates.get("total_runs", 0) or 0) + 1
    aggregates["dreaming_integrity_total"] = int(aggregates.get("dreaming_integrity_total", 0) or 0) + int(record.get("dreaming_integrity", 0) or 0)
    aggregates["projection_strength_total"] = int(aggregates.get("projection_strength_total", 0) or 0) + int(record.get("projection_strength", 0) or 0)
    aggregates["mirroring_integrity_total"] = int(aggregates.get("mirroring_integrity_total", 0) or 0) + int(record.get("mirroring_integrity", 0) or 0)
    aggregates["dream_drift_total"] = int(aggregates.get("dream_drift_total", 0) or 0) + int(record.get("dream_drift", 0) or 0)
    _increment_bucket(aggregates, "cinematic_counts", str(record.get("cinematic_dream") or "adaptive_cinema_dream"))
    _increment_bucket(aggregates, "projection_counts", str(record.get("latent_projection") or "latent_stability_projection"))
    _increment_bucket(aggregates, "pathway_counts", str(record.get("dormant_pathway") or "dormant_adaptation_path"))
    _increment_bucket(aggregates, "forecast_counts", str(record.get("forecast") or "orchestration_recovery_projection"))
    _increment_bucket(aggregates, "vision_counts", str(record.get("vision") or "stabilization_vision"))
    memory["aggregates"] = aggregates


def _increment_bucket(aggregates: dict[str, Any], bucket: str, key: str) -> None:
    groups = dict(aggregates.get(bucket) or {})
    groups[key] = int(groups.get(key, 0) or 0) + 1
    aggregates[bucket] = groups


def _average(value: Any, total: int) -> float:
    denominator = max(int(total or 0), 1)
    return round(int(value or 0) / denominator, 4)
