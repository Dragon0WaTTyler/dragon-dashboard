from __future__ import annotations

from typing import Any, Mapping

from .arbitration_rules import arbitrate_runtime
from .authority_events import build_authority_events
from .authority_memory import build_authority_memory_summary, load_authority_memory, update_authority_memory
from .authority_metrics import build_authority_metrics
from .confidence_governance import govern_confidence
from .execution_policies import resolve_execution_policy
from .fallback_authority import govern_fallback
from .orchestration_constraints import evaluate_orchestration_constraints
from .runtime_governor import govern_runtime
from .runtime_risk import build_runtime_risk
from .runtime_safety import assess_runtime_safety
from .stability_guard import build_stability_guard


def build_runtime_authority(
    orchestration: Mapping[str, Any] | None,
    *,
    persist_memory: bool = True,
    memory_path=None,
    timestamp: str = "",
) -> dict[str, Any]:
    payload = dict(orchestration or {})
    authority_memory = load_authority_memory(path=memory_path)
    authority_memory_summary = build_authority_memory_summary(authority_memory, current_context=payload)

    execution_policy = resolve_execution_policy(
        runtime_profile=str(payload.get("runtime_profile") or ""),
        playback_runtime=str(payload.get("playback_runtime") or ""),
        selected_source=payload.get("selected_source"),
        capability_snapshot=payload.get("capability_snapshot") or payload.get("readiness_snapshot"),
    )
    constraints = evaluate_orchestration_constraints(
        capability_snapshot=payload.get("capability_snapshot") or payload.get("readiness_snapshot"),
        selected_source=payload.get("selected_source"),
        playback_runtime=str(payload.get("playback_runtime") or ""),
        runtime_profile=str(payload.get("runtime_profile") or ""),
        execution_policy=execution_policy,
        authority_memory_summary=authority_memory_summary,
    )
    stability = build_stability_guard(
        runtime_switch_history=payload.get("runtime_switch_history"),
        execution_timeline=payload.get("execution_timeline"),
        confidence_evolution=payload.get("confidence_evolution"),
        runtime_memory_summary=payload.get("runtime_memory_summary"),
        authority_memory_summary=authority_memory_summary,
    )
    runtime_risk = build_runtime_risk(
        execution_metrics=payload.get("execution_metrics"),
        execution_timeline=payload.get("execution_timeline"),
        coordination_metrics=payload.get("coordination_metrics"),
        orchestration_forecast=payload.get("orchestration_forecast"),
        stability_guard=stability,
    )
    confidence = govern_confidence(
        confidence_evolution=payload.get("confidence_evolution"),
        runtime_predictions=payload.get("runtime_predictions"),
        runtime_learning=payload.get("runtime_learning"),
        runtime_risk=runtime_risk,
        stability_guard=stability,
    )
    fallback = govern_fallback(
        fallback_negotiation=payload.get("fallback_negotiation"),
        playback_readiness=str(payload.get("playback_readiness") or ""),
        runtime_risk=runtime_risk,
        stability_guard=stability,
        authority_memory_summary=authority_memory_summary,
    )
    arbitration = arbitrate_runtime(
        playback_runtime=str(payload.get("playback_runtime") or ""),
        playback_readiness=str(payload.get("playback_readiness") or ""),
        runtime_negotiation=payload.get("runtime_negotiation"),
        runtime_predictions=payload.get("runtime_predictions"),
        execution_metrics=payload.get("execution_metrics"),
        coordination_metrics=payload.get("coordination_metrics"),
        orchestration_constraints=constraints,
        runtime_risk=runtime_risk,
        fallback_authority=fallback,
        confidence_governance=confidence,
    )
    governor = govern_runtime(
        arbitration=arbitration,
        orchestration_constraints=constraints,
        stability_guard=stability,
        fallback_authority=fallback,
        execution_policy=execution_policy,
        runtime_risk=runtime_risk,
    )
    safety = assess_runtime_safety(
        approved_runtime=str(governor.get("approved_runtime") or arbitration.get("approved_runtime") or ""),
        runtime_risk=runtime_risk,
        orchestration_constraints=constraints,
        stability_guard=stability,
    )

    approved_runtime = str(governor.get("approved_runtime") or arbitration.get("approved_runtime") or payload.get("playback_runtime") or "external_runtime")
    authority_state = _authority_state(governor=governor, safety=safety, constraints=constraints)
    reasoning = _build_reasoning(
        execution_policy=execution_policy,
        constraints=constraints,
        arbitration=arbitration,
        governor=governor,
        runtime_risk=runtime_risk,
        confidence=confidence,
        stability=stability,
    )
    result = {
        "approved_runtime": approved_runtime,
        "authority_state": authority_state,
        "authority_confidence": max(0, min(100, int(round((int(arbitration.get("arbitration_confidence", 0) or 0) + int(confidence.get("regulated_confidence_score", 0) or 0) + int(safety.get("runtime_safety_score", 0) or 0)) / 3)))),
        "authority_reasoning": reasoning,
        "blocked_paths": list(arbitration.get("blocked_paths") or constraints.get("blocked_paths") or []),
        "forced_fallback": bool(governor.get("forced_fallback")),
        "risk_state": str(runtime_risk.get("risk_state") or "low_risk"),
        "runtime_risk": runtime_risk,
        "execution_policy": execution_policy,
        "forced_constraints": list(constraints.get("forced_constraints") or []),
        "stability_state": stability,
        "fallback_authority": fallback,
        "confidence_governance": confidence,
        "arbitration_result": arbitration,
        "arbitration_trace": list(arbitration.get("arbitration_trace") or []),
        "governance_actions": list(governor.get("governance_actions") or []),
        "runtime_safety": safety,
    }
    result["authority_events"] = build_authority_events(
        authority_state=authority_state,
        arbitration=arbitration,
        confidence_governance=confidence,
        stability_guard=stability,
        orchestration_constraints=constraints,
        governor=governor,
    )
    if persist_memory:
        result["authority_memory_summary"] = update_authority_memory(payload, result, path=memory_path, timestamp=timestamp)
    else:
        result["authority_memory_summary"] = authority_memory_summary
    result["authority_metrics"] = build_authority_metrics(
        authority_memory_summary=result["authority_memory_summary"],
        authority_result=result,
    )
    return result


def _authority_state(
    *,
    governor: Mapping[str, Any],
    safety: Mapping[str, Any],
    constraints: Mapping[str, Any],
) -> str:
    if str(safety.get("runtime_safety_state") or "") == "unsafe":
        return "blocked"
    if bool(governor.get("forced_fallback")) or str(constraints.get("constraint_state") or "") == "constrained":
        return "guarded"
    return "approved"


def _build_reasoning(
    *,
    execution_policy: Mapping[str, Any],
    constraints: Mapping[str, Any],
    arbitration: Mapping[str, Any],
    governor: Mapping[str, Any],
    runtime_risk: Mapping[str, Any],
    confidence: Mapping[str, Any],
    stability: Mapping[str, Any],
) -> list[str]:
    reasoning = []
    reasoning.extend(list(execution_policy.get("policy_reasons") or []))
    reasoning.extend(item.get("reason") for item in constraints.get("forced_constraints") or [] if isinstance(item, Mapping))
    reasoning.extend(item.get("reason") for item in arbitration.get("arbitration_trace") or [] if isinstance(item, Mapping))
    reasoning.extend(f"Runtime risk classified as {str(runtime_risk.get('risk_state') or 'low_risk').replace('_', ' ')}.")
    reasoning.append(f"Governed confidence settled at {confidence.get('regulated_confidence_label', 'low')}.")
    if str(stability.get("guard_intervention") or "") != "none":
        reasoning.append(f"Stability guard intervention: {str(stability.get('guard_intervention') or '').replace('_', ' ')}.")
    if governor.get("governance_actions"):
        reasoning.append("Governor actions: " + ", ".join(str(item).replace("_", " ") for item in governor.get("governance_actions") or []))
    return [str(item) for item in reasoning if str(item or "").strip()]
