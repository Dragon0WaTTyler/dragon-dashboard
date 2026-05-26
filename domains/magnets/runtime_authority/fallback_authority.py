from __future__ import annotations

from typing import Any, Mapping


def govern_fallback(
    *,
    fallback_negotiation: Mapping[str, Any] | None = None,
    playback_readiness: str = "",
    runtime_risk: Mapping[str, Any] | None = None,
    stability_guard: Mapping[str, Any] | None = None,
    authority_memory_summary: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    fallback = dict(fallback_negotiation or {})
    risk = dict(runtime_risk or {})
    stability = dict(stability_guard or {})
    memory = dict(authority_memory_summary or {})

    allowed = True
    mandatory = False
    terminate_loops = False
    state = "fallback_optional"
    reasons: list[str] = []

    if str(playback_readiness or "").strip() == "browser_deferred" and not bool(stability.get("oscillation_detected")):
        return {
            "fallback_state": "fallback_observed",
            "fallback_allowed": True,
            "fallback_mandatory": False,
            "terminate_loops": False,
            "fallback_reasons": ["deferred_browser_runtime_stays_observable"],
            "fallback_runtime": "external_runtime",
        }

    if (
        (
            float(memory.get("fallback_loop_frequency", 0) or 0) >= 0.34
            and str(risk.get("risk_state") or "") in {"fallback_sensitive", "unstable_runtime", "orchestration_hazard"}
        )
        or str(stability.get("guard_intervention") or "") == "freeze_to_safe_fallback"
    ):
        mandatory = True
        terminate_loops = True
        state = "fallback_loop_terminated"
        reasons.append("fallback_loops_must_terminate")
    elif str(risk.get("risk_state") or "") in {"fallback_sensitive", "unstable_runtime", "orchestration_hazard"}:
        mandatory = True
        state = "fallback_mandatory"
        reasons.append("runtime_risk_requires_fallback")
    elif str(fallback.get("fallback_urgency") or "") == "high":
        state = "fallback_recommended"
        reasons.append("coordination_signaled_high_fallback_urgency")
    else:
        reasons.append("fallback_kept_available_as_safety_net")

    if terminate_loops:
        allowed = False

    return {
        "fallback_state": state,
        "fallback_allowed": allowed,
        "fallback_mandatory": mandatory,
        "terminate_loops": terminate_loops,
        "fallback_reasons": reasons,
        "fallback_runtime": "external_runtime",
    }
