from __future__ import annotations

from typing import Any, Mapping


def build_runtime_reputation(
    runtime_memory_summary: Mapping[str, Any] | None,
    *,
    current_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    summary = dict(runtime_memory_summary or {})
    return {
        "runtime_profiles": _bucket_reputation(summary.get("runtime_profiles")),
        "transport_classes": _bucket_reputation(summary.get("transport_classes")),
        "source_characteristics": _bucket_reputation(summary.get("source_characteristics")),
        "fallback_paths": _bucket_reputation(summary.get("fallback_paths")),
        "reputation_state": "learned" if int(summary.get("total_observations", 0) or 0) else "baseline",
    }


def _bucket_reputation(bucket: Mapping[str, Any] | None) -> dict[str, Any]:
    reputations: dict[str, Any] = {}
    for key, raw_payload in dict(bucket or {}).items():
        payload = dict(raw_payload or {})
        total = int(payload.get("selected", 0) or payload.get("total", 0) or 0)
        stability = _score(1 - _ratio(payload.get("instability", 0), total))
        recovery = _score(_ratio(payload.get("recovered", 0), max(payload.get("fallback", 0), 1)))
        degradation = _score(1 - _ratio(payload.get("degraded", 0), total))
        trust = max(0, min(100, int(round((stability + recovery + degradation) / 3))))
        reputations[str(key or "unknown")] = {
            "stability_reputation": stability,
            "recovery_reputation": recovery,
            "degradation_reputation": degradation,
            "orchestration_trust": trust,
            "evidence_count": total,
        }
    return reputations


def _ratio(value: Any, total: Any) -> float:
    return int(value or 0) / max(int(total or 0), 1)


def _score(value: float) -> int:
    return max(20, min(96, int(round(value * 100))))
