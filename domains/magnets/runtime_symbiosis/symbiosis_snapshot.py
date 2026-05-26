from __future__ import annotations

from typing import Any, Mapping


def build_runtime_symbiosis_snapshot(
    *,
    symbiosis_state: Mapping[str, Any],
    symbiosis_projection: Mapping[str, Any],
    symbiosis_metrics: Mapping[str, Any],
) -> dict[str, Any]:
    state = dict(symbiosis_state or {})
    projection = dict(symbiosis_projection or {})
    metrics = dict(symbiosis_metrics or {})
    return {
        "state": str(state.get("state") or "symbiosis_balancing"),
        "symbiotic_phase": str(state.get("symbiotic_phase") or "measured_symbiosis"),
        "runtime_coexistence": str(state.get("runtime_coexistence") or "measured_coexistence"),
        "systemic_runtime_health": str(state.get("systemic_runtime_health") or "measured_health"),
        "cooperative_runtime_state": str(state.get("cooperative_runtime_state") or "adaptive_coexistence"),
        "symbiosis_projection": str(projection.get("forecast") or "measured_symbiosis_projection"),
        "symbiosis_stability": int(metrics.get("symbiosis_stability", 0) or 0),
        "dependency_stress": int(metrics.get("dependency_stress", 0) or 0),
        "symbiosis_mutualism": int(metrics.get("symbiosis_mutualism", 0) or 0),
    }
