from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any, Mapping

from dragon.cache import save_json_file
from dragon.paths import CACHE_DIR


DEFAULT_AUTHORITY_MEMORY = {
    "version": 1,
    "corrupted_recoveries": 0,
    "entries": [],
    "aggregates": {
        "total_runs": 0,
        "arbitration_count": 0,
        "blocked_runtime_count": 0,
        "forced_fallback_count": 0,
        "stability_intervention_count": 0,
        "suppressed_confidence_count": 0,
        "oscillation_prevention_count": 0,
        "prevented_failures": 0,
    },
}

_AUTHORITY_MEMORY_LOCK = threading.Lock()


def authority_memory_path(path: Path | None = None) -> Path:
    return Path(path or (CACHE_DIR / "magnets" / "runtime_authority_memory.json"))


def load_authority_memory(*, path: Path | None = None) -> dict[str, Any]:
    target = authority_memory_path(path)
    payload, corrupted = _read_memory_file(target)
    normalized = _normalize_memory(payload)
    if corrupted:
        normalized["corrupted_recoveries"] = int(normalized.get("corrupted_recoveries", 0) or 0) + 1
        save_json_file(target, normalized)
    return normalized


def build_authority_memory_summary(
    memory: Mapping[str, Any] | None,
    *,
    current_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    payload = _normalize_memory(memory)
    aggregates = dict(payload.get("aggregates") or {})
    total = max(int(aggregates.get("total_runs", 0) or 0), 1)
    current = dict(current_context or {})
    return {
        "memory_status": "recovered" if int(payload.get("corrupted_recoveries", 0) or 0) else "healthy",
        "total_observations": int(aggregates.get("total_runs", 0) or 0),
        "arbitration_frequency": round(int(aggregates.get("arbitration_count", 0) or 0) / total, 4),
        "fallback_loop_frequency": round(int(aggregates.get("forced_fallback_count", 0) or 0) / total, 4),
        "blocked_runtime_frequency": round(int(aggregates.get("blocked_runtime_count", 0) or 0) / total, 4),
        "stability_intervention_frequency": round(int(aggregates.get("stability_intervention_count", 0) or 0) / total, 4),
        "suppressed_confidence_frequency": round(int(aggregates.get("suppressed_confidence_count", 0) or 0) / total, 4),
        "oscillation_prevention_frequency": round(int(aggregates.get("oscillation_prevention_count", 0) or 0) / total, 4),
        "prevented_failures": int(aggregates.get("prevented_failures", 0) or 0),
        "recent_entries": [dict(item) for item in payload.get("entries") or [] if isinstance(item, Mapping)][-5:],
        "current_runtime_profile": str(current.get("runtime_profile") or "").strip() or "unknown",
    }


def update_authority_memory(
    orchestration: Mapping[str, Any] | None,
    authority_result: Mapping[str, Any] | None,
    *,
    path: Path | None = None,
    timestamp: str = "",
) -> dict[str, Any]:
    with _AUTHORITY_MEMORY_LOCK:
        memory = load_authority_memory(path=path)
        record = extract_authority_memory_record(orchestration, authority_result, timestamp=timestamp)
        memory["entries"] = [*list(memory.get("entries") or []), record]
        _update_aggregates(memory, record)
        save_json_file(authority_memory_path(path), memory)
    return build_authority_memory_summary(memory, current_context=orchestration)


def extract_authority_memory_record(
    orchestration: Mapping[str, Any] | None,
    authority_result: Mapping[str, Any] | None,
    *,
    timestamp: str = "",
) -> dict[str, Any]:
    context = dict(orchestration or {})
    result = dict(authority_result or {})
    confidence = dict(result.get("confidence_governance") or {})
    stability = dict(result.get("stability_state") or {})
    return {
        "timestamp": str(timestamp or context.get("updated_at") or "").strip(),
        "authority_state": str(result.get("authority_state") or "").strip() or "approved",
        "approved_runtime": str(result.get("approved_runtime") or "").strip() or "external_runtime",
        "arbitration_triggered": str(dict(result.get("arbitration_result") or {}).get("arbitration_result") or "") == "runtime_overridden",
        "blocked_runtime": bool(result.get("blocked_paths")),
        "forced_fallback": bool(result.get("forced_fallback")),
        "stability_intervention": str(stability.get("guard_intervention") or "") != "none",
        "confidence_suppressed": bool(confidence.get("suppressed")),
        "oscillation_prevented": "oscillation_prevented" in list(result.get("governance_actions") or []),
        "prevented_failure": bool(result.get("forced_fallback")) or str(result.get("authority_state") or "") == "guarded",
    }


def _update_aggregates(memory: dict[str, Any], record: Mapping[str, Any]) -> None:
    aggregates = dict(memory.get("aggregates") or {})
    aggregates["total_runs"] = int(aggregates.get("total_runs", 0) or 0) + 1
    if record.get("arbitration_triggered"):
        aggregates["arbitration_count"] = int(aggregates.get("arbitration_count", 0) or 0) + 1
    if record.get("blocked_runtime"):
        aggregates["blocked_runtime_count"] = int(aggregates.get("blocked_runtime_count", 0) or 0) + 1
    if record.get("forced_fallback"):
        aggregates["forced_fallback_count"] = int(aggregates.get("forced_fallback_count", 0) or 0) + 1
    if record.get("stability_intervention"):
        aggregates["stability_intervention_count"] = int(aggregates.get("stability_intervention_count", 0) or 0) + 1
    if record.get("confidence_suppressed"):
        aggregates["suppressed_confidence_count"] = int(aggregates.get("suppressed_confidence_count", 0) or 0) + 1
    if record.get("oscillation_prevented"):
        aggregates["oscillation_prevention_count"] = int(aggregates.get("oscillation_prevention_count", 0) or 0) + 1
    if record.get("prevented_failure"):
        aggregates["prevented_failures"] = int(aggregates.get("prevented_failures", 0) or 0) + 1
    memory["aggregates"] = aggregates


def _read_memory_file(path: Path) -> tuple[Any, bool]:
    if not path.exists():
        return DEFAULT_AUTHORITY_MEMORY, False
    try:
        return json.loads(path.read_text(encoding="utf-8")), False
    except Exception:
        return DEFAULT_AUTHORITY_MEMORY, True


def _normalize_memory(payload: Mapping[str, Any] | Any) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        payload = {}
    return {
        "version": 1,
        "corrupted_recoveries": int(payload.get("corrupted_recoveries", 0) or 0),
        "entries": [dict(item) for item in payload.get("entries") or [] if isinstance(item, Mapping)],
        "aggregates": {
            **DEFAULT_AUTHORITY_MEMORY["aggregates"],
            **dict(payload.get("aggregates") or {}),
        },
    }
