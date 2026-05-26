from __future__ import annotations

from typing import Any, Mapping


def assess_runtime_safety(
    *,
    approved_runtime: str = "",
    runtime_risk: Mapping[str, Any] | None = None,
    orchestration_constraints: Mapping[str, Any] | None = None,
    stability_guard: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    risk = dict(runtime_risk or {})
    constraints = dict(orchestration_constraints or {})
    stability = dict(stability_guard or {})

    score = 82
    if approved_runtime == "browser_runtime":
        score -= 8
    score -= int(risk.get("risk_score", 0) or 0) // 4
    if str(constraints.get("constraint_state") or "") == "constrained":
        score -= 6
    if str(stability.get("stability_state") or "") == "intervening":
        score -= 8
    safety_score = max(0, min(100, score))

    return {
        "runtime_safety_score": safety_score,
        "runtime_safety_state": "safe" if safety_score >= 68 else ("guarded" if safety_score >= 48 else "unsafe"),
    }
