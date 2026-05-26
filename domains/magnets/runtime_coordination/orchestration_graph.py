from __future__ import annotations

from typing import Any, Mapping


def build_orchestration_graph(
    *,
    selected_runtime: str,
    fallback_runtime: str,
    adaptive_strategy: Mapping[str, Any] | None = None,
    degradation_report: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    strategy = dict(adaptive_strategy or {})
    degradation = dict(degradation_report or {})
    selected = str(selected_runtime or "").strip() or "browser_runtime"
    fallback = str(fallback_runtime or "").strip() or "external_runtime"
    target = str(strategy.get("target_runtime") or selected).strip() or selected

    nodes = [
        {"id": "browser_cinematic", "runtime": "cinematic_runtime", "label": "Browser Cinematic"},
        {"id": "startup_degraded", "runtime": "degraded_runtime", "label": "Startup Degraded"},
        {"id": "balanced_runtime", "runtime": "browser_runtime", "label": "Balanced Runtime"},
        {"id": "fallback_external", "runtime": "external_runtime", "label": "Fallback External"},
        {"id": "mobile_safe", "runtime": "mobile_safe_runtime", "label": "Mobile Safe"},
    ]
    transitions = [
        {"from": "browser_cinematic", "to": "startup_degraded", "reason": "startup_pressure"},
        {"from": "startup_degraded", "to": "balanced_runtime", "reason": "adaptive_rebalance"},
        {"from": "balanced_runtime", "to": "fallback_external", "reason": "fallback_escalation"},
        {"from": "balanced_runtime", "to": "mobile_safe", "reason": "mobile_protection"},
    ]
    downgrade_paths = [
        {"path": ["browser_cinematic", "balanced_runtime"], "reason": "heavy_source_downgrade"},
        {"path": ["balanced_runtime", "mobile_safe"], "reason": "avoid_unstable_transport"},
        {"path": ["balanced_runtime", "fallback_external"], "reason": "browser_instability"},
    ]
    recovery_edges = [
        {"from": "fallback_external", "to": "balanced_runtime", "reason": "recovery_negotiated"},
        {"from": "mobile_safe", "to": "balanced_runtime", "reason": "runtime_rebalanced"},
    ]
    active_path = [selected, target if target != selected else fallback]
    return {
        "nodes": nodes,
        "transitions": transitions,
        "downgrade_paths": downgrade_paths,
        "recovery_edges": recovery_edges,
        "entry_runtime": selected,
        "target_runtime": target,
        "fallback_runtime": fallback,
        "active_path": active_path,
        "degradation_severity": int(degradation.get("degradation_severity") or 0),
    }
