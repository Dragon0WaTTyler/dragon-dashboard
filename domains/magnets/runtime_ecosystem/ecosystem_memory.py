from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any, Mapping

from dragon.cache import save_json_file
from dragon.paths import CACHE_DIR


DEFAULT_ECOSYSTEM_MEMORY = {
    "version": 1,
    "corrupted_recoveries": 0,
    "entries": [],
    "aggregates": {
        "total_runs": 0,
        "pressure_score_total": 0,
        "integrity_total": 0,
        "equilibrium_strength_total": 0,
        "balance_counts": {},
        "zone_counts": {},
        "climate_counts": {},
        "degradation_currents": {},
        "topology_counts": {},
        "forecast_counts": {},
    },
}

_ECOSYSTEM_MEMORY_LOCK = threading.Lock()


def ecosystem_memory_path(path: Path | None = None) -> Path:
    return Path(path or (CACHE_DIR / "magnets" / "runtime_ecosystem_memory.json"))


def load_ecosystem_memory(*, path: Path | None = None) -> dict[str, Any]:
    target = ecosystem_memory_path(path)
    payload, corrupted = _read_memory_file(target)
    normalized = _normalize_memory(payload)
    if corrupted:
        normalized["corrupted_recoveries"] = int(normalized.get("corrupted_recoveries", 0) or 0) + 1
        save_json_file(target, normalized)
    return normalized


def extract_ecosystem_memory_record(
    orchestration: Mapping[str, Any] | None,
    ecosystem_result: Mapping[str, Any] | None = None,
    *,
    timestamp: str = "",
) -> dict[str, Any]:
    payload = dict(orchestration or {})
    result = dict(ecosystem_result or {})
    metrics = dict(result.get("ecosystem_metrics") or payload.get("ecosystem_metrics") or {})
    pressure = dict(result.get("orchestration_pressure") or payload.get("orchestration_pressure") or {})
    balance = dict(result.get("ecosystem_balance") or payload.get("ecosystem_balance") or {})
    zone = dict(result.get("stability_zone") or payload.get("stability_zone") or {})
    climate = dict(result.get("ecosystem_climate") or payload.get("ecosystem_climate") or {})
    equilibrium = dict(result.get("adaptive_equilibrium") or payload.get("adaptive_equilibrium") or {})
    currents = dict(result.get("degradation_currents") or payload.get("degradation_currents") or {})
    topology = dict(result.get("resilience_topology") or payload.get("resilience_topology") or {})
    forecast = dict(result.get("ecosystem_forecast") or payload.get("ecosystem_forecast") or {})
    return {
        "timestamp": str(timestamp or payload.get("updated_at") or payload.get("timestamp") or "").strip(),
        "balance_state": str(balance.get("balance_state") or "balanced"),
        "stability_zone": str(zone.get("zone") or "stable_zone"),
        "climate": str(climate.get("climate") or "calm_climate"),
        "degradation_current": str(currents.get("current") or "localized_degradation"),
        "topology": str(topology.get("topology") or "concentrated_resilience"),
        "forecast": str(forecast.get("forecast") or "future_stability"),
        "pressure_score": int(metrics.get("orchestration_pressure_score", pressure.get("pressure_score", 0)) or 0),
        "ecosystem_integrity": int(metrics.get("ecosystem_integrity", 0) or 0),
        "equilibrium_strength": int(metrics.get("equilibrium_strength", equilibrium.get("equilibrium_strength", 0)) or 0),
    }


def update_ecosystem_memory(
    orchestration: Mapping[str, Any] | None,
    ecosystem_result: Mapping[str, Any] | None,
    *,
    path: Path | None = None,
    timestamp: str = "",
) -> dict[str, Any]:
    with _ECOSYSTEM_MEMORY_LOCK:
        memory = load_ecosystem_memory(path=path)
        record = extract_ecosystem_memory_record(orchestration, ecosystem_result, timestamp=timestamp)
        memory["entries"] = [*list(memory.get("entries") or []), record]
        _update_aggregates(memory, record)
        save_json_file(ecosystem_memory_path(path), memory)
    return build_ecosystem_memory_summary(memory, current_context=ecosystem_result or orchestration)


def build_ecosystem_memory_summary(
    memory: Mapping[str, Any] | None,
    *,
    current_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    payload = _normalize_memory(memory)
    aggregates = dict(payload.get("aggregates") or {})
    total_runs = int(aggregates.get("total_runs", 0) or 0)
    entries = [dict(item) for item in payload.get("entries") or [] if isinstance(item, Mapping)]
    current = dict(current_context or {})
    return {
        "memory_status": "recovered" if int(payload.get("corrupted_recoveries", 0) or 0) else "healthy",
        "corrupted_recoveries": int(payload.get("corrupted_recoveries", 0) or 0),
        "total_observations": total_runs,
        "average_pressure_score": _average(aggregates.get("pressure_score_total", 0), total_runs),
        "average_ecosystem_integrity": _average(aggregates.get("integrity_total", 0), total_runs),
        "average_equilibrium_strength": _average(aggregates.get("equilibrium_strength_total", 0), total_runs),
        "balance_counts": dict(aggregates.get("balance_counts") or {}),
        "zone_counts": dict(aggregates.get("zone_counts") or {}),
        "climate_counts": dict(aggregates.get("climate_counts") or {}),
        "degradation_currents": dict(aggregates.get("degradation_currents") or {}),
        "topology_counts": dict(aggregates.get("topology_counts") or {}),
        "forecast_counts": dict(aggregates.get("forecast_counts") or {}),
        "recent_balance_state": str((current.get("ecosystem_balance") or {}).get("balance_state") or current.get("balance_state") or "unknown"),
        "recent_entries": entries[-6:],
    }


def _read_memory_file(path: Path) -> tuple[Any, bool]:
    if not path.exists():
        return DEFAULT_ECOSYSTEM_MEMORY, False
    try:
        return json.loads(path.read_text(encoding="utf-8")), False
    except Exception:
        return DEFAULT_ECOSYSTEM_MEMORY, True


def _normalize_memory(payload: Mapping[str, Any] | Any) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        payload = {}
    return {
        "version": 1,
        "corrupted_recoveries": int(payload.get("corrupted_recoveries", 0) or 0),
        "entries": [dict(item) for item in payload.get("entries") or [] if isinstance(item, Mapping)],
        "aggregates": {
            **DEFAULT_ECOSYSTEM_MEMORY["aggregates"],
            **dict(payload.get("aggregates") or {}),
        },
    }


def _update_aggregates(memory: dict[str, Any], record: Mapping[str, Any]) -> None:
    aggregates = dict(memory.get("aggregates") or {})
    aggregates["total_runs"] = int(aggregates.get("total_runs", 0) or 0) + 1
    aggregates["pressure_score_total"] = int(aggregates.get("pressure_score_total", 0) or 0) + int(record.get("pressure_score", 0) or 0)
    aggregates["integrity_total"] = int(aggregates.get("integrity_total", 0) or 0) + int(record.get("ecosystem_integrity", 0) or 0)
    aggregates["equilibrium_strength_total"] = int(aggregates.get("equilibrium_strength_total", 0) or 0) + int(record.get("equilibrium_strength", 0) or 0)
    _increment_bucket(aggregates, "balance_counts", str(record.get("balance_state") or "balanced"))
    _increment_bucket(aggregates, "zone_counts", str(record.get("stability_zone") or "stable_zone"))
    _increment_bucket(aggregates, "climate_counts", str(record.get("climate") or "calm_climate"))
    _increment_bucket(aggregates, "degradation_currents", str(record.get("degradation_current") or "localized_degradation"))
    _increment_bucket(aggregates, "topology_counts", str(record.get("topology") or "concentrated_resilience"))
    _increment_bucket(aggregates, "forecast_counts", str(record.get("forecast") or "future_stability"))
    memory["aggregates"] = aggregates


def _increment_bucket(aggregates: dict[str, Any], bucket: str, key: str) -> None:
    groups = dict(aggregates.get(bucket) or {})
    groups[key] = int(groups.get(key, 0) or 0) + 1
    aggregates[bucket] = groups


def _average(value: Any, total: int) -> float:
    denominator = max(int(total or 0), 1)
    return round(int(value or 0) / denominator, 4)
