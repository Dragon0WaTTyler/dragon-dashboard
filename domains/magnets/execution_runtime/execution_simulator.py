from __future__ import annotations

from typing import Any, Mapping

from .execution_events import build_execution_event
from .execution_failures import build_execution_failure
from .execution_guardrails import evaluate_execution_guardrails
from .execution_metrics import build_runtime_grade, summarize_execution_metrics
from .execution_recovery import select_recovery_path
from .execution_state import evolve_execution_state
from .execution_timeline import build_execution_timeline
from .execution_transport import classify_execution_transport


def simulate_execution_runtime(
    *,
    capability_snapshot: Mapping[str, Any] | None = None,
    playback_readiness: str = "",
    source_metadata: Mapping[str, Any] | None = None,
    runtime_manifest: Mapping[str, Any] | None = None,
    bootstrap_plan: Mapping[str, Any] | None = None,
    readiness_snapshot: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    capability = dict(capability_snapshot or {})
    source = dict(source_metadata or {})
    manifest = dict(runtime_manifest or {})
    bootstrap = dict(bootstrap_plan or {})
    readiness = dict(readiness_snapshot or {})

    transport = classify_execution_transport(
        runtime_manifest=manifest,
        capability_snapshot=capability,
        bootstrap_plan=bootstrap,
        source_metadata=source,
    )
    guardrails = evaluate_execution_guardrails(
        source_metadata=source,
        capability_snapshot=capability,
        runtime_manifest=manifest,
        transport_descriptor=transport,
    )
    metrics = summarize_execution_metrics(
        capability_snapshot=capability,
        playback_readiness=playback_readiness,
        transport_descriptor=transport,
        guardrails=guardrails,
    )
    runtime_grade = build_runtime_grade(metrics)
    timeline = build_execution_timeline(
        capability_snapshot=capability,
        readiness_snapshot=readiness,
        runtime_manifest=manifest,
        transport_descriptor=transport,
        execution_metrics=metrics,
    )

    events: list[dict[str, Any]] = []
    state = "idle"
    state = evolve_execution_state(state, "bootstrapping")
    events.append(build_execution_event("runtime_bootstrap_started", event_order=1, execution_state=state, details={"bootstrap_mode": str(bootstrap.get("bootstrap_mode") or "blocked")}))
    state = evolve_execution_state(state, "preparing_transport")
    events.append(build_execution_event("transport_prepared", event_order=2, execution_state=state, details={"transport_class": transport["transport_class"]}))
    state = evolve_execution_state(state, "validating_runtime")

    failures: list[dict[str, Any]] = []
    degradation_reasons: list[str] = []
    outcome = "runtime_active"

    if guardrails.get("rejected"):
        state = evolve_execution_state(state, "fallback_transition")
        failure = build_execution_failure(
            guardrails.get("failure_category") or "transport_rejection",
            details={"blocking_reasons": list(guardrails.get("blocking_reasons") or [])},
            state=state,
            transport=str(transport.get("transport_class") or ""),
        )
        failures.append(failure)
        degradation_reasons.extend(list(guardrails.get("blocking_reasons") or []))
        events.append(build_execution_event("runtime_fallback_selected", event_order=3, execution_state=state, details={"reason": failure["category"]}))
        outcome = "fallback"
    elif timeline["fallback_probability"] >= 0.5 or metrics["degradation_risk"] >= 55:
        state = evolve_execution_state(state, "startup_pending")
        state = evolve_execution_state(state, "startup_degraded")
        degradation_reasons.append("startup_pressure_detected")
        events.append(build_execution_event("startup_degraded", event_order=3, execution_state=state, details={"risk_window": timeline["risk_window"]}))
        failure = build_execution_failure(
            "startup_timeout" if metrics["startup_score"] < 55 else "runtime_instability",
            details={"startup_score": metrics["startup_score"], "stability_score": metrics["stability_score"]},
            state=state,
            transport=str(transport.get("transport_class") or ""),
        )
        failures.append(failure)
        if metrics["runtime_confidence"] >= 58:
            state = evolve_execution_state(state, "runtime_active")
            events.append(build_execution_event("runtime_recovered", event_order=4, execution_state=state, details={"recovery_mode": "guarded_resume"}))
            outcome = "recovered"
        else:
            state = evolve_execution_state(state, "runtime_unstable")
            state = evolve_execution_state(state, "runtime_recovering")
            state = evolve_execution_state(state, "fallback_transition")
            events.append(build_execution_event("runtime_fallback_selected", event_order=4, execution_state=state, details={"reason": failure["category"]}))
            outcome = "fallback"
    else:
        state = evolve_execution_state(state, "startup_pending")
        state = evolve_execution_state(state, "runtime_active")
        outcome = "runtime_active"

    recovery_path = select_recovery_path(
        failure=failures[0] if failures else None,
        guardrails=guardrails,
        transport_descriptor=transport,
        capability_snapshot=capability,
    )

    if outcome == "fallback":
        simulated_runtime_health = "degraded"
        state = evolve_execution_state(state, "runtime_completed")
        events.append(build_execution_event("runtime_completed", event_order=len(events) + 1, execution_state=state, details={"completion_mode": "fallback_exit"}))
    elif outcome == "recovered":
        simulated_runtime_health = "guarded"
        state = evolve_execution_state(state, "runtime_completed")
        events.append(build_execution_event("runtime_completed", event_order=len(events) + 1, execution_state=state, details={"completion_mode": "recovered"}))
    elif outcome == "runtime_active":
        simulated_runtime_health = "stable"
        state = evolve_execution_state(state, "runtime_completed")
        events.append(build_execution_event("runtime_completed", event_order=len(events) + 1, execution_state=state, details={"completion_mode": "stable"}))
    else:
        state = evolve_execution_state(state, "runtime_failed")
        simulated_runtime_health = "failed"
        events.append(build_execution_event("runtime_failed", event_order=len(events) + 1, execution_state=state, details={"reason": "unresolved_execution_state"}))

    return {
        "execution_state": state,
        "execution_outcome": outcome,
        "simulated_runtime_health": simulated_runtime_health,
        "degradation_reasons": degradation_reasons,
        "fallback_recommendations": [recovery_path["path"]] if outcome == "fallback" or guardrails.get("rejected") else [],
        "recovery_hints": list(recovery_path.get("hints") or []),
        "recovery_path": recovery_path,
        "transport_descriptor": transport,
        "guardrails": guardrails,
        "execution_metrics": metrics,
        "runtime_grade": runtime_grade,
        "execution_timeline": timeline,
        "execution_failures": failures,
        "execution_events": events,
        "intelligence_signals": {
            "execution_risk": "high" if metrics["degradation_risk"] >= 55 or guardrails.get("rejected") else ("moderate" if metrics["degradation_risk"] >= 35 else "low"),
            "recovery_likelihood": "high" if outcome == "recovered" or metrics["runtime_confidence"] >= 62 else ("moderate" if recovery_path.get("path") != "external_handoff" else "low"),
            "browser_rejection": bool(guardrails.get("rejected")),
            "fallback_bias": "external" if outcome == "fallback" else "runtime_retained",
        },
    }
