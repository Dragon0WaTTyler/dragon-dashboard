from __future__ import annotations

from typing import Any, Mapping


def arbitrate_runtime(
    *,
    playback_runtime: str = "",
    playback_readiness: str = "",
    runtime_negotiation: Mapping[str, Any] | None = None,
    runtime_predictions: Mapping[str, Any] | None = None,
    execution_metrics: Mapping[str, Any] | None = None,
    coordination_metrics: Mapping[str, Any] | None = None,
    orchestration_constraints: Mapping[str, Any] | None = None,
    runtime_risk: Mapping[str, Any] | None = None,
    fallback_authority: Mapping[str, Any] | None = None,
    confidence_governance: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    negotiation = dict(runtime_negotiation or {})
    prediction = dict(runtime_predictions or {})
    execution = dict(execution_metrics or {})
    coordination = dict(coordination_metrics or {})
    constraints = dict(orchestration_constraints or {})
    risk = dict(runtime_risk or {})
    fallback = dict(fallback_authority or {})
    governed_confidence = dict(confidence_governance or {})

    proposed = str(negotiation.get("selected_runtime") or playback_runtime or "external_runtime").strip()
    approved = proposed
    traces: list[dict[str, Any]] = []
    reasons: list[str] = []
    blocked_paths = list(constraints.get("blocked_paths") or [])

    if proposed in set(constraints.get("blocked_runtimes") or []):
        approved = "external_runtime"
        reasons.append("constraint_forced_runtime_override")
        traces.append(_trace("coordination_vs_constraints", proposed, approved, "Hard constraints block the negotiated runtime."))

    if (
        approved == "browser_runtime"
        and str(playback_readiness or "") != "browser_deferred"
        and str(prediction.get("predicted_outcome") or "") == "likely_external_fallback"
        and str(risk.get("risk_state") or "") in {"fallback_sensitive", "unstable_runtime", "orchestration_hazard"}
    ):
        approved = "external_runtime"
        reasons.append("prediction_execution_disagreement")
        traces.append(_trace("prediction_vs_execution", proposed, approved, "Prediction and risk signals reject browser execution."))

    if (
        approved == "browser_runtime"
        and str(playback_readiness or "") != "browser_deferred"
        and int(governed_confidence.get("regulated_confidence_score", 0) or 0) < 55
        and int(execution.get("runtime_confidence", 0) or 0) < 60
    ):
        approved = "external_runtime"
        reasons.append("confidence_governance_override")
        traces.append(_trace("confidence_vs_runtime", proposed, approved, "Governed confidence is too low for browser runtime approval."))

    if bool(fallback.get("fallback_mandatory")):
        approved = "external_runtime"
        reasons.append("fallback_authority_override")
        traces.append(_trace("resilience_vs_coordination", proposed, approved, "Fallback authority requires the safe fallback path."))

    if not traces:
        traces.append(_trace("authority_confirmation", proposed, approved, "Negotiated runtime passed authority arbitration."))

    return {
        "approved_runtime": approved,
        "arbitration_result": "runtime_overridden" if approved != proposed else "runtime_confirmed",
        "arbitration_trace": traces,
        "arbitration_reasons": reasons or ["runtime_confirmed"],
        "blocked_paths": blocked_paths,
        "arbitration_confidence": max(52, min(94, int(round((int(coordination.get("coordination_confidence", 0) or 0) + int(governed_confidence.get("regulated_confidence_score", 0) or 0)) / 2)))),
    }


def _trace(rule: str, proposed: str, approved: str, reason: str) -> dict[str, str]:
    return {
        "rule": rule,
        "proposed_runtime": proposed,
        "approved_runtime": approved,
        "reason": reason,
    }
