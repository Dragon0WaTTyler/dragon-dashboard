from __future__ import annotations

from typing import Any, Mapping


def build_adaptive_runtime_strategy(
    *,
    selected_runtime: str,
    fallback_runtime: str,
    degradation_report: Mapping[str, Any] | None = None,
    readiness_snapshot: Mapping[str, Any] | None = None,
    execution_metrics: Mapping[str, Any] | None = None,
    runtime_pressure: str = "",
) -> dict[str, Any]:
    degradation = dict(degradation_report or {})
    readiness = dict(readiness_snapshot or {})
    metrics = dict(execution_metrics or {})
    selected = str(selected_runtime or "").strip() or "degraded_runtime"
    fallback = str(fallback_runtime or "").strip() or "external_runtime"
    pressure = str(runtime_pressure or "").strip().lower() or "medium"
    profile = str(readiness.get("runtime_profile") or "").strip()

    rule = "retain_runtime"
    target_runtime = selected
    reason = "runtime stable under current pressure."
    severity = int(degradation.get("degradation_severity") or 0)

    if profile == "browser_cinematic" and severity >= 32:
        rule = "downgrade_cinematic_to_balanced"
        target_runtime = "browser_runtime"
        reason = "Cinematic startup cost exceeds the current stability envelope."
    elif selected == "browser_runtime" and (pressure == "high" or int(metrics.get("fallback_pressure") or 0) >= 70):
        rule = "switch_browser_to_external"
        target_runtime = fallback
        reason = "Browser startup pressure is too high for stable orchestration."
    elif severity >= 52:
        rule = "downgrade_balanced_to_mobile_safe"
        target_runtime = "mobile_safe_runtime"
        reason = "Deterministic downgrade applied to avoid unstable transport."
    elif severity >= 75:
        rule = "escalate_to_degraded_runtime"
        target_runtime = "degraded_runtime"
        reason = "Fallback urgency is immediate under severe degradation."

    adaptation_required = target_runtime != selected or rule != "retain_runtime"
    return {
        "selected_runtime": selected,
        "target_runtime": target_runtime,
        "adaptation_rule": rule,
        "adaptation_required": adaptation_required,
        "adaptation_reason": reason,
        "adaptation_pressure": min(100, max(0, severity + (12 if pressure == "high" else 0))),
    }
