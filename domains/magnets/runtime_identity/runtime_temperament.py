from __future__ import annotations

from typing import Any, Mapping


def build_runtime_temperament(
    identity_memory_summary: Mapping[str, Any] | None,
    *,
    current_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    summary = dict(identity_memory_summary or {})
    current = dict(current_context or {})
    counts = dict(summary.get("temperament_counts") or {})
    degradation_risk = int(dict(current.get("execution_metrics") or {}).get("degradation_risk", 0) or 0)
    fallback_probability = float(dict(current.get("execution_timeline") or {}).get("fallback_probability", 0) or 0.0)
    base = max(
        ((str(key), int(value or 0)) for key, value in counts.items()),
        key=lambda item: (item[1], item[0]),
        default=("calm", 0),
    )[0]
    if degradation_risk >= 76 or fallback_probability >= 0.72:
        base = "defensive"
    elif degradation_risk >= 58:
        base = "cautious"
    drivers = []
    if degradation_risk >= 58:
        drivers.append("degradation_pressure")
    if fallback_probability >= 0.5:
        drivers.append("fallback_risk")
    if not drivers:
        drivers.append("continuity_stability")
    return {
        "temperament": base,
        "drivers": drivers,
        "temperament_confidence": max(0, min(100, 52 + len(drivers) * 12)),
    }
