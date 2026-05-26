from __future__ import annotations

from typing import Any, Mapping


def govern_runtime(
    *,
    arbitration: Mapping[str, Any] | None = None,
    orchestration_constraints: Mapping[str, Any] | None = None,
    stability_guard: Mapping[str, Any] | None = None,
    fallback_authority: Mapping[str, Any] | None = None,
    execution_policy: Mapping[str, Any] | None = None,
    runtime_risk: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    arbitration_payload = dict(arbitration or {})
    constraints = dict(orchestration_constraints or {})
    stability = dict(stability_guard or {})
    fallback = dict(fallback_authority or {})
    policy = dict(execution_policy or {})
    risk = dict(runtime_risk or {})

    approved_runtime = str(arbitration_payload.get("approved_runtime") or "external_runtime").strip()
    approved_profile = "balanced"
    actions: list[str] = []
    forced_fallback = False

    if bool(fallback.get("fallback_mandatory")):
        approved_runtime = str(fallback.get("fallback_runtime") or "external_runtime")
        forced_fallback = True
        actions.append("fallback_forced")
    if str(stability.get("guard_intervention") or "") == "freeze_to_safe_fallback" and (
        bool(stability.get("oscillation_detected")) or bool(fallback.get("fallback_mandatory"))
    ):
        approved_runtime = "external_runtime"
        forced_fallback = True
        actions.append("oscillation_prevented")
    if str(policy.get("id") or "") in {"mobile-safe", "resilience-first", "conservative", "low-bandwidth"}:
        approved_profile = "balanced"
        actions.append("aggressive_runtime_downgraded")
    if str(risk.get("risk_state") or "") in {"unstable_runtime", "orchestration_hazard"}:
        approved_profile = "balanced"
        actions.append("runtime_stabilized_under_risk")
    if str(constraints.get("constraint_state") or "") == "constrained":
        actions.append("hard_constraints_enforced")

    return {
        "approved_runtime": approved_runtime,
        "approved_profile": approved_profile,
        "governance_actions": _unique(actions),
        "forced_fallback": forced_fallback,
        "governor_state": "guarded" if actions else "pass_through",
    }


def _unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in seen:
            seen.add(text)
            ordered.append(text)
    return ordered
