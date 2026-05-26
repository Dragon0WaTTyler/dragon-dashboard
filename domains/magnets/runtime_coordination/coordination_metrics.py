from __future__ import annotations

from typing import Any, Mapping


def build_coordination_metrics(
    *,
    runtime_negotiation: Mapping[str, Any] | None = None,
    adaptive_strategy: Mapping[str, Any] | None = None,
    degradation_report: Mapping[str, Any] | None = None,
    switch_plan: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    negotiation = dict(runtime_negotiation or {})
    strategy = dict(adaptive_strategy or {})
    degradation = dict(degradation_report or {})
    switching = dict(switch_plan or {})
    recovery = dict(switching.get("estimated_recovery") or {})

    adaptation_pressure = int(strategy.get("adaptation_pressure") or 0)
    severity = int(degradation.get("degradation_severity") or 0)
    switching_cost = min(100, max(0, int(round((int(recovery.get("eta_ms") or 0) / 60)))))
    fallback_readiness = 86 if str(negotiation.get("fallback_runtime") or "") == "external_runtime" else 62
    runtime_resilience = max(0, min(100, 92 - severity + (8 if fallback_readiness >= 80 else 0)))
    orchestration_stability = max(0, min(100, int(round((runtime_resilience + max(0, 100 - adaptation_pressure) + max(0, 100 - switching_cost)) / 3))))
    coordination_confidence = max(0, min(100, int(round((orchestration_stability + fallback_readiness + runtime_resilience) / 3))))

    if coordination_confidence >= 85:
        grade = "A"
    elif coordination_confidence >= 70:
        grade = "B"
    elif coordination_confidence >= 55:
        grade = "C"
    elif coordination_confidence >= 40:
        grade = "D"
    else:
        grade = "F"

    risk_summary = "stable"
    if severity >= 75:
        risk_summary = "fallback escalation likely"
    elif adaptation_pressure >= 50:
        risk_summary = "runtime rebalance required"
    elif switching_cost >= 60:
        risk_summary = "switching cost elevated"

    return {
        "orchestration_stability": orchestration_stability,
        "adaptation_pressure": adaptation_pressure,
        "fallback_readiness": fallback_readiness,
        "runtime_resilience": runtime_resilience,
        "switching_cost": switching_cost,
        "coordination_confidence": coordination_confidence,
        "runtime_orchestration_grade": grade,
        "coordination_risk_summary": risk_summary,
    }
