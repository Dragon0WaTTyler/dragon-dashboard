from __future__ import annotations

from typing import Any, Mapping


def plan_runtime_switch(
    *,
    current_runtime: str,
    target_runtime: str,
    switch_reason: str,
    degradation_report: Mapping[str, Any] | None = None,
    execution_metrics: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    degradation = dict(degradation_report or {})
    metrics = dict(execution_metrics or {})
    current = str(current_runtime or "").strip() or "browser_runtime"
    target = str(target_runtime or "").strip() or current
    reason = str(switch_reason or degradation.get("downgrade_recommendation") or "runtime_rebalance").strip()

    strategy = "hold_runtime"
    if current == "browser_runtime" and target == "external_runtime":
        strategy = "browser_to_external_handoff"
    elif current == "cinematic_runtime" and target == "browser_runtime":
        strategy = "cinematic_to_balanced"
    elif current in {"browser_runtime", "cinematic_runtime"} and target == "mobile_safe_runtime":
        strategy = "balanced_to_mobile_safe"
    elif target == "degraded_runtime":
        strategy = "retry_bootstrap"

    recovery_ms = 4200
    if strategy == "browser_to_external_handoff":
        recovery_ms = 1600
    elif strategy == "retry_bootstrap":
        recovery_ms = 5200
    elif strategy == "cinematic_to_balanced":
        recovery_ms = 2800
    if int(metrics.get("fallback_pressure") or 0) >= 70:
        recovery_ms += 800

    return {
        "from_runtime": current,
        "target_runtime": target,
        "switch_strategy": strategy,
        "switch_reason": reason,
        "estimated_recovery": {
            "eta_ms": recovery_ms,
            "outcome": "guarded" if int(degradation.get("degradation_severity") or 0) >= 50 else "stable",
        },
    }
