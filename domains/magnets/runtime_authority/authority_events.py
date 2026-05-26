from __future__ import annotations

from typing import Any, Mapping


def build_authority_events(
    *,
    authority_state: str = "",
    arbitration: Mapping[str, Any] | None = None,
    confidence_governance: Mapping[str, Any] | None = None,
    stability_guard: Mapping[str, Any] | None = None,
    orchestration_constraints: Mapping[str, Any] | None = None,
    governor: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    arbitration_payload = dict(arbitration or {})
    confidence = dict(confidence_governance or {})
    stability = dict(stability_guard or {})
    constraints = dict(orchestration_constraints or {})
    governor_payload = dict(governor or {})

    if str(arbitration_payload.get("arbitration_result") or "") == "runtime_overridden":
        events.append({"event_type": "arbitration_triggered", "details": {"approved_runtime": arbitration_payload.get("approved_runtime")}})
    if bool(governor_payload.get("forced_fallback")):
        events.append({"event_type": "fallback_forced", "details": {"approved_runtime": governor_payload.get("approved_runtime")}})
    if bool(confidence.get("suppressed")):
        events.append({"event_type": "confidence_suppressed", "details": {"regulated_confidence_score": confidence.get("regulated_confidence_score")}})
    if str(stability.get("guard_intervention") or "") != "none":
        events.append({"event_type": "stability_intervention", "details": {"intervention": stability.get("guard_intervention")}})
    if str(constraints.get("constraint_state") or "") == "constrained":
        events.append({"event_type": "orchestration_constrained", "details": {"count": len(constraints.get("forced_constraints") or [])}})
    if str(authority_state or "") == "blocked":
        events.append({"event_type": "runtime_blocked", "details": {}})
    return events
