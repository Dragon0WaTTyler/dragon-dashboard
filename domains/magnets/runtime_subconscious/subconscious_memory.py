from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any, Mapping

from dragon.cache import save_json_file
from dragon.paths import CACHE_DIR


DEFAULT_SUBCONSCIOUS_MEMORY = {
    "version": 1,
    "corrupted_recoveries": 0,
    "entries": [],
    "aggregates": {
        "total_runs": 0,
        "subconscious_integrity_total": 0,
        "residue_density_total": 0,
        "echo_strength_total": 0,
        "subconscious_drift_total": 0,
        "latent_counts": {},
        "underflow_counts": {},
        "resilience_counts": {},
        "residue_counts": {},
        "forecast_counts": {},
    },
}

_SUBCONSCIOUS_MEMORY_LOCK = threading.Lock()


def subconscious_memory_path(path: Path | None = None) -> Path:
    return Path(path or (CACHE_DIR / "magnets" / "runtime_subconscious_memory.json"))


def load_subconscious_memory(*, path: Path | None = None) -> dict[str, Any]:
    target = subconscious_memory_path(path)
    payload, corrupted = _read_memory_file(target)
    normalized = _normalize_memory(payload)
    if corrupted:
        normalized["corrupted_recoveries"] = int(normalized.get("corrupted_recoveries", 0) or 0) + 1
        save_json_file(target, normalized)
    return normalized


def extract_subconscious_memory_record(
    orchestration: Mapping[str, Any] | None,
    subconscious_result: Mapping[str, Any] | None = None,
    *,
    timestamp: str = "",
) -> dict[str, Any]:
    payload = dict(orchestration or {})
    result = dict(subconscious_result or {})
    latent = dict(result.get("latent_patterns") or payload.get("latent_patterns") or {})
    underflow = dict(result.get("orchestration_underflow") or payload.get("orchestration_underflow") or {})
    dormant = dict(result.get("dormant_resilience") or payload.get("dormant_resilience") or {})
    residue = dict(result.get("orchestration_residue") or payload.get("orchestration_residue") or {})
    forecast = dict(result.get("subconscious_forecast") or payload.get("subconscious_forecast") or {})
    metrics = dict(result.get("subconscious_metrics") or payload.get("subconscious_metrics") or {})
    prior = dict(payload.get("subconscious_memory") or {})
    prior_entries = [dict(item) for item in prior.get("recent_entries") or [] if isinstance(item, Mapping)]
    previous_pattern = str((prior_entries[-1] or {}).get("latent_pattern") or "") if prior_entries else ""
    latent_pattern = str(latent.get("pattern") or "latent_resilience")
    drift = 0 if not previous_pattern or previous_pattern == latent_pattern else 24
    return {
        "timestamp": str(timestamp or payload.get("updated_at") or payload.get("timestamp") or "").strip(),
        "latent_pattern": latent_pattern,
        "underflow_state": str(underflow.get("state") or "adaptive_underflow"),
        "dormant_resilience_state": str(dormant.get("state") or "dormant_adaptive"),
        "residue_pattern": str(residue.get("pattern") or "equilibrium_residue"),
        "forecast": str(forecast.get("forecast") or "equilibrium_convergence"),
        "subconscious_integrity": int(metrics.get("subconscious_integrity", 0) or 0),
        "residue_density": int(metrics.get("orchestration_residue_density", residue.get("orchestration_residue_density", 0)) or 0),
        "echo_strength": int(metrics.get("orchestration_echo_strength", 0) or 0),
        "subconscious_drift": drift,
    }


def update_subconscious_memory(
    orchestration: Mapping[str, Any] | None,
    subconscious_result: Mapping[str, Any] | None,
    *,
    path: Path | None = None,
    timestamp: str = "",
) -> dict[str, Any]:
    with _SUBCONSCIOUS_MEMORY_LOCK:
        memory = load_subconscious_memory(path=path)
        record = extract_subconscious_memory_record(orchestration, subconscious_result, timestamp=timestamp)
        memory["entries"] = [*list(memory.get("entries") or []), record]
        _update_aggregates(memory, record)
        save_json_file(subconscious_memory_path(path), memory)
    return build_subconscious_memory_summary(memory, current_context=subconscious_result or orchestration)


def build_subconscious_memory_summary(
    memory: Mapping[str, Any] | None,
    *,
    current_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    payload = _normalize_memory(memory)
    aggregates = dict(payload.get("aggregates") or {})
    total_runs = int(aggregates.get("total_runs", 0) or 0)
    entries = [dict(item) for item in payload.get("entries") or [] if isinstance(item, Mapping)]
    current = dict(current_context or {})
    latent = dict(current.get("latent_patterns") or {})
    return {
        "memory_status": "recovered" if int(payload.get("corrupted_recoveries", 0) or 0) else "healthy",
        "corrupted_recoveries": int(payload.get("corrupted_recoveries", 0) or 0),
        "total_observations": total_runs,
        "average_subconscious_integrity": _average(aggregates.get("subconscious_integrity_total", 0), total_runs),
        "average_residue_density": _average(aggregates.get("residue_density_total", 0), total_runs),
        "average_echo_strength": _average(aggregates.get("echo_strength_total", 0), total_runs),
        "average_subconscious_drift": _average(aggregates.get("subconscious_drift_total", 0), total_runs),
        "latent_counts": dict(aggregates.get("latent_counts") or {}),
        "underflow_counts": dict(aggregates.get("underflow_counts") or {}),
        "resilience_counts": dict(aggregates.get("resilience_counts") or {}),
        "residue_counts": dict(aggregates.get("residue_counts") or {}),
        "forecast_counts": dict(aggregates.get("forecast_counts") or {}),
        "recent_latent_pattern": str(latent.get("pattern") or "unknown"),
        "recent_entries": entries[-6:],
    }


def _read_memory_file(path: Path) -> tuple[Any, bool]:
    if not path.exists():
        return DEFAULT_SUBCONSCIOUS_MEMORY, False
    try:
        return json.loads(path.read_text(encoding="utf-8")), False
    except Exception:
        return DEFAULT_SUBCONSCIOUS_MEMORY, True


def _normalize_memory(payload: Mapping[str, Any] | Any) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        payload = {}
    return {
        "version": 1,
        "corrupted_recoveries": int(payload.get("corrupted_recoveries", 0) or 0),
        "entries": [dict(item) for item in payload.get("entries") or [] if isinstance(item, Mapping)],
        "aggregates": {
            **DEFAULT_SUBCONSCIOUS_MEMORY["aggregates"],
            **dict(payload.get("aggregates") or {}),
        },
    }


def _update_aggregates(memory: dict[str, Any], record: Mapping[str, Any]) -> None:
    aggregates = dict(memory.get("aggregates") or {})
    aggregates["total_runs"] = int(aggregates.get("total_runs", 0) or 0) + 1
    aggregates["subconscious_integrity_total"] = int(aggregates.get("subconscious_integrity_total", 0) or 0) + int(record.get("subconscious_integrity", 0) or 0)
    aggregates["residue_density_total"] = int(aggregates.get("residue_density_total", 0) or 0) + int(record.get("residue_density", 0) or 0)
    aggregates["echo_strength_total"] = int(aggregates.get("echo_strength_total", 0) or 0) + int(record.get("echo_strength", 0) or 0)
    aggregates["subconscious_drift_total"] = int(aggregates.get("subconscious_drift_total", 0) or 0) + int(record.get("subconscious_drift", 0) or 0)
    _increment_bucket(aggregates, "latent_counts", str(record.get("latent_pattern") or "latent_resilience"))
    _increment_bucket(aggregates, "underflow_counts", str(record.get("underflow_state") or "adaptive_underflow"))
    _increment_bucket(aggregates, "resilience_counts", str(record.get("dormant_resilience_state") or "dormant_adaptive"))
    _increment_bucket(aggregates, "residue_counts", str(record.get("residue_pattern") or "equilibrium_residue"))
    _increment_bucket(aggregates, "forecast_counts", str(record.get("forecast") or "equilibrium_convergence"))
    memory["aggregates"] = aggregates


def _increment_bucket(aggregates: dict[str, Any], bucket: str, key: str) -> None:
    groups = dict(aggregates.get(bucket) or {})
    groups[key] = int(groups.get(key, 0) or 0) + 1
    aggregates[bucket] = groups


def _average(value: Any, total: int) -> float:
    denominator = max(int(total or 0), 1)
    return round(int(value or 0) / denominator, 4)
