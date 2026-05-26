from __future__ import annotations

from typing import Any, Mapping

from .adaptive_strategy import build_adaptive_runtime_strategy
from .coordination_events import append_coordination_event, build_coordination_event
from .coordination_metrics import build_coordination_metrics
from .coordination_state import evolve_coordination_state
from .orchestration_graph import build_orchestration_graph
from .runtime_degradation import assess_runtime_degradation
from .runtime_negotiation import negotiate_runtime
from .runtime_persistence import build_coordination_persistence_payload
from .runtime_switching import plan_runtime_switch


def coordinate_runtime(
    *,
    capability_snapshot: Mapping[str, Any] | None = None,
    execution_metrics: Mapping[str, Any] | None = None,
    readiness_snapshot: Mapping[str, Any] | None = None,
    runtime_pressure: str = "",
) -> dict[str, Any]:
    capability = dict(capability_snapshot or {})
    metrics = dict(execution_metrics or {})
    readiness = dict(readiness_snapshot or {})
    pressure = str(runtime_pressure or "").strip().lower() or "medium"

    coordination_state = "coordination_pending"
    events: list[dict[str, Any]] = []
    degradation = assess_runtime_degradation(
        capability_snapshot=capability,
        execution_metrics=metrics,
        readiness_snapshot=readiness,
        runtime_pressure=pressure,
    )
    negotiation = negotiate_runtime(
        capability_snapshot=capability,
        execution_metrics=metrics,
        readiness_snapshot=readiness,
        runtime_pressure=pressure,
        degradation_risk=int(degradation.get("degradation_severity") or 0),
    )
    coordination_state = evolve_coordination_state(coordination_state, "runtime_negotiated")
    events = append_coordination_event(
        events,
        build_coordination_event(
            "runtime_negotiated",
            coordination_state=coordination_state,
            event_order=1,
            details={
                "selected_runtime": str(negotiation.get("selected_runtime") or ""),
                "fallback_runtime": str(negotiation.get("fallback_runtime") or ""),
            },
        ),
    )

    adaptive_strategy = build_adaptive_runtime_strategy(
        selected_runtime=str(negotiation.get("selected_runtime") or ""),
        fallback_runtime=str(negotiation.get("fallback_runtime") or ""),
        degradation_report=degradation,
        readiness_snapshot=readiness,
        execution_metrics=metrics,
        runtime_pressure=pressure,
    )
    if adaptive_strategy.get("adaptation_required"):
        coordination_state = evolve_coordination_state(coordination_state, "adaptation_required")
        events = append_coordination_event(
            events,
            build_coordination_event(
                "adaptation_triggered",
                coordination_state=coordination_state,
                event_order=2,
                details={"adaptation_rule": str(adaptive_strategy.get("adaptation_rule") or "")},
            ),
        )

    target_runtime = str(adaptive_strategy.get("target_runtime") or negotiation.get("selected_runtime") or "")
    if target_runtime == "external_runtime":
        coordination_state = evolve_coordination_state(coordination_state, "fallback_negotiated")
        event_type = "fallback_escalated"
    elif target_runtime != str(negotiation.get("selected_runtime") or ""):
        coordination_state = evolve_coordination_state(coordination_state, "recovery_negotiated")
        event_type = "recovery_negotiated"
    else:
        event_type = "runtime_rebalanced"

    switch_plan = plan_runtime_switch(
        current_runtime=str(negotiation.get("selected_runtime") or ""),
        target_runtime=target_runtime,
        switch_reason=str(adaptive_strategy.get("adaptation_rule") or degradation.get("downgrade_recommendation") or ""),
        degradation_report=degradation,
        execution_metrics=metrics,
    )
    coordination_state = evolve_coordination_state(coordination_state, "runtime_rebalanced" if event_type != "fallback_escalated" else "fallback_negotiated")
    events = append_coordination_event(
        events,
        build_coordination_event(
            event_type,
            coordination_state=coordination_state,
            event_order=3,
            details={
                "target_runtime": target_runtime,
                "switch_strategy": str(switch_plan.get("switch_strategy") or ""),
            },
        ),
    )

    graph = build_orchestration_graph(
        selected_runtime=str(negotiation.get("selected_runtime") or ""),
        fallback_runtime=str(negotiation.get("fallback_runtime") or ""),
        adaptive_strategy=adaptive_strategy,
        degradation_report=degradation,
    )
    fallback_negotiation = {
        "fallback_runtime": str(negotiation.get("fallback_runtime") or ""),
        "fallback_urgency": str(degradation.get("fallback_urgency") or ""),
        "switch_strategy": str(switch_plan.get("switch_strategy") or ""),
    }
    metrics_payload = build_coordination_metrics(
        runtime_negotiation=negotiation,
        adaptive_strategy=adaptive_strategy,
        degradation_report=degradation,
        switch_plan=switch_plan,
    )
    result = {
        "coordination_state": coordination_state,
        "runtime_negotiation": negotiation,
        "adaptive_strategy": adaptive_strategy,
        "orchestration_graph": graph,
        "coordination_metrics": metrics_payload,
        "runtime_switch_history": [switch_plan],
        "fallback_negotiation": fallback_negotiation,
        "coordination_events": events,
        "intelligence_signals": {
            "coordination_stability_band": "high" if int(metrics_payload.get("coordination_confidence", 0) or 0) >= 75 else ("moderate" if int(metrics_payload.get("coordination_confidence", 0) or 0) >= 55 else "low"),
            "fallback_escalation_hint": str(fallback_negotiation.get("fallback_urgency") or "low"),
            "degradation_frequency_hint": "elevated" if int(degradation.get("degradation_severity", 0) or 0) >= 55 else "contained",
        },
    }
    result["persistence"] = build_coordination_persistence_payload(result)
    return result
