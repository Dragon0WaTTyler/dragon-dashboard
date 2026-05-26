from __future__ import annotations

from typing import Any, Mapping


def build_stability_guard(
    *,
    runtime_switch_history: list[Mapping[str, Any]] | None = None,
    execution_timeline: Mapping[str, Any] | None = None,
    confidence_evolution: Mapping[str, Any] | None = None,
    runtime_memory_summary: Mapping[str, Any] | None = None,
    authority_memory_summary: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    switch_history = [dict(item) for item in runtime_switch_history or [] if isinstance(item, Mapping)]
    timeline = dict(execution_timeline or {})
    confidence = dict(confidence_evolution or {})
    runtime_memory = dict(runtime_memory_summary or {})
    authority_memory = dict(authority_memory_summary or {})

    warnings: list[str] = []
    intervention = "none"

    if len(switch_history) >= 2:
        warnings.append("oscillation_loop_detected")
    if float(timeline.get("fallback_probability", 0) or 0) >= 0.6:
        warnings.append("fallback_thrashing_risk")
    if str(confidence.get("confidence_stability") or "") == "volatile" or int(confidence.get("confidence_delta", 0) or 0) <= -15:
        warnings.append("confidence_collapse")
    if float(runtime_memory.get("runtime_instability", 0) or 0) >= 0.45:
        warnings.append("repeated_downgrade_cycles")
    if (
        float(authority_memory.get("fallback_loop_frequency", 0) or 0) >= 0.34
        and float(timeline.get("fallback_probability", 0) or 0) >= 0.45
    ):
        warnings.append("unstable_recovery_chain")

    if "oscillation_loop_detected" in warnings or "unstable_recovery_chain" in warnings:
        intervention = "freeze_to_safe_fallback"
        state = "intervening"
    elif warnings:
        intervention = "stabilize_runtime"
        state = "guarded"
    else:
        state = "stable"

    return {
        "stability_state": state,
        "stability_warnings": warnings,
        "guard_intervention": intervention,
        "oscillation_detected": "oscillation_loop_detected" in warnings,
        "stability_confidence": 88 if warnings else 74,
    }
