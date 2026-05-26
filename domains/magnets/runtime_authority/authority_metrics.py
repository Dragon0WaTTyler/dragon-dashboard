from __future__ import annotations

from typing import Any, Mapping


def build_authority_metrics(
    *,
    authority_memory_summary: Mapping[str, Any] | None = None,
    authority_result: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    memory = dict(authority_memory_summary or {})
    result = dict(authority_result or {})
    prevented_failures = int(memory.get("prevented_failures", 0) or 0) + (1 if bool(result.get("forced_fallback")) else 0)
    arbitration_frequency = float(memory.get("arbitration_frequency", 0) or 0)
    confidence_score = int(dict(result.get("confidence_governance") or {}).get("regulated_confidence_score", 0) or 0)
    safety_score = int(dict(result.get("runtime_safety") or {}).get("runtime_safety_score", 0) or 0)

    return {
        "stabilization_effectiveness": max(0, min(100, int(round((safety_score * 0.55) + ((1 - arbitration_frequency) * 45))))),
        "prevented_failures": prevented_failures,
        "arbitration_frequency": arbitration_frequency,
        "confidence_stability_score": confidence_score,
        "runtime_safety_score": safety_score,
        "orchestration_reliability": max(0, min(100, int(round((confidence_score + safety_score) / 2)))),
    }
