from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any, Mapping

from dragon.cache import save_json_file
from dragon.paths import CACHE_DIR


DEFAULT_CAPABILITY_MEMORY = {
    "version": 1,
    "corrupted_recoveries": 0,
    "entries": [],
    "aggregates": {
        "total_runs": 0,
        "degraded_count": 0,
        "unstable_count": 0,
        "infeasible_count": 0,
        "resource_limit_count": 0,
        "thermal_risk_count": 0,
        "runtime_profiles": {},
        "device_profiles": {},
        "network_profiles": {},
        "infeasible_paths": {},
    },
}

_CAPABILITY_MEMORY_LOCK = threading.Lock()


def capability_memory_path(path: Path | None = None) -> Path:
    return Path(path or (CACHE_DIR / "magnets" / "runtime_capability_memory.json"))


def load_capability_memory(*, path: Path | None = None) -> dict[str, Any]:
    target = capability_memory_path(path)
    payload, corrupted = _read_memory_file(target)
    normalized = _normalize_memory(payload)
    if corrupted:
        normalized["corrupted_recoveries"] = int(normalized.get("corrupted_recoveries", 0) or 0) + 1
        save_json_file(target, normalized)
    return normalized


def build_capability_memory_summary(
    memory: Mapping[str, Any] | None,
    *,
    current_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    payload = _normalize_memory(memory)
    aggregates = dict(payload.get("aggregates") or {})
    current = dict(current_context or {})
    total = max(int(aggregates.get("total_runs", 0) or 0), 1)
    profile = str(current.get("runtime_profile") or "").strip() or "unknown"
    return {
        "memory_status": "recovered" if int(payload.get("corrupted_recoveries", 0) or 0) else "healthy",
        "corrupted_recoveries": int(payload.get("corrupted_recoveries", 0) or 0),
        "total_observations": int(aggregates.get("total_runs", 0) or 0),
        "degradation_frequency": round(int(aggregates.get("degraded_count", 0) or 0) / total, 4),
        "unstable_frequency": round(int(aggregates.get("unstable_count", 0) or 0) / total, 4),
        "infeasible_frequency": round(int(aggregates.get("infeasible_count", 0) or 0) / total, 4),
        "resource_limit_frequency": round(int(aggregates.get("resource_limit_count", 0) or 0) / total, 4),
        "thermal_risk_frequency": round(int(aggregates.get("thermal_risk_count", 0) or 0) / total, 4),
        "runtime_profiles": dict(aggregates.get("runtime_profiles") or {}),
        "device_profiles": dict(aggregates.get("device_profiles") or {}),
        "network_profiles": dict(aggregates.get("network_profiles") or {}),
        "infeasible_paths": dict(aggregates.get("infeasible_paths") or {}),
        "recent_entries": [dict(item) for item in payload.get("entries") or [] if isinstance(item, Mapping)][-5:],
        "current_runtime_profile": profile,
    }


def update_capability_memory(
    orchestration: Mapping[str, Any] | None,
    capability_result: Mapping[str, Any] | None,
    *,
    path: Path | None = None,
    timestamp: str = "",
) -> dict[str, Any]:
    with _CAPABILITY_MEMORY_LOCK:
        memory = load_capability_memory(path=path)
        record = extract_capability_memory_record(orchestration, capability_result, timestamp=timestamp)
        memory["entries"] = [*list(memory.get("entries") or []), record]
        _update_aggregates(memory, record)
        save_json_file(capability_memory_path(path), memory)
    return build_capability_memory_summary(memory, current_context=orchestration)


def extract_capability_memory_record(
    orchestration: Mapping[str, Any] | None,
    capability_result: Mapping[str, Any] | None,
    *,
    timestamp: str = "",
) -> dict[str, Any]:
    context = dict(orchestration or {})
    result = dict(capability_result or {})
    resource = dict(result.get("resource_state") or {})
    thermal = dict(result.get("thermal_profile") or {})
    device = dict(result.get("device_profile") or {})
    network = dict(result.get("network_profile") or {})
    feasibility = str(result.get("runtime_feasibility") or "").strip() or "feasible"
    approved_runtime = str(context.get("playback_runtime") or context.get("runtime_mode") or "").strip() or "unknown"
    path_key = f"{approved_runtime}->{feasibility}"
    return {
        "timestamp": str(timestamp or context.get("updated_at") or "").strip(),
        "runtime_profile": str(context.get("runtime_profile") or "").strip() or "unknown",
        "device_profile": str(device.get("profile") or "").strip() or "unknown",
        "network_profile": str(network.get("profile") or "").strip() or "unknown",
        "runtime_feasibility": feasibility,
        "degraded": feasibility in {"degraded", "constrained"},
        "unstable": feasibility == "unstable",
        "infeasible": feasibility in {"unsafe", "impossible"},
        "resource_limited": int(resource.get("resource_pressure_score", 0) or 0) >= 65,
        "thermal_risk": str(thermal.get("thermal_state") or "") in {"elevated_thermal_risk", "sustained_runtime_pressure", "mobile_heat_sensitive"},
        "infeasible_path": path_key,
    }


def _read_memory_file(path: Path) -> tuple[Any, bool]:
    if not path.exists():
        return DEFAULT_CAPABILITY_MEMORY, False
    try:
        return json.loads(path.read_text(encoding="utf-8")), False
    except Exception:
        return DEFAULT_CAPABILITY_MEMORY, True


def _normalize_memory(payload: Mapping[str, Any] | Any) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        payload = {}
    return {
        "version": 1,
        "corrupted_recoveries": int(payload.get("corrupted_recoveries", 0) or 0),
        "entries": [dict(item) for item in payload.get("entries") or [] if isinstance(item, Mapping)],
        "aggregates": {
            **DEFAULT_CAPABILITY_MEMORY["aggregates"],
            **dict(payload.get("aggregates") or {}),
        },
    }


def _update_aggregates(memory: dict[str, Any], record: Mapping[str, Any]) -> None:
    aggregates = dict(memory.get("aggregates") or {})
    aggregates["total_runs"] = int(aggregates.get("total_runs", 0) or 0) + 1
    if record.get("degraded"):
        aggregates["degraded_count"] = int(aggregates.get("degraded_count", 0) or 0) + 1
    if record.get("unstable"):
        aggregates["unstable_count"] = int(aggregates.get("unstable_count", 0) or 0) + 1
    if record.get("infeasible"):
        aggregates["infeasible_count"] = int(aggregates.get("infeasible_count", 0) or 0) + 1
    if record.get("resource_limited"):
        aggregates["resource_limit_count"] = int(aggregates.get("resource_limit_count", 0) or 0) + 1
    if record.get("thermal_risk"):
        aggregates["thermal_risk_count"] = int(aggregates.get("thermal_risk_count", 0) or 0) + 1
    _increment_bucket(aggregates, "runtime_profiles", str(record.get("runtime_profile") or "unknown"))
    _increment_bucket(aggregates, "device_profiles", str(record.get("device_profile") or "unknown"))
    _increment_bucket(aggregates, "network_profiles", str(record.get("network_profile") or "unknown"))
    _increment_bucket(aggregates, "infeasible_paths", str(record.get("infeasible_path") or "unknown"))
    memory["aggregates"] = aggregates


def _increment_bucket(aggregates: dict[str, Any], key: str, item: str) -> None:
    bucket = dict(aggregates.get(key) or {})
    bucket[item] = int(bucket.get(item, 0) or 0) + 1
    aggregates[key] = bucket
