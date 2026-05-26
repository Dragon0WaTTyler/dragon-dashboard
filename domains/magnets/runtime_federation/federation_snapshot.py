from __future__ import annotations

from typing import Any, Mapping


def build_runtime_federation_snapshot(
    *,
    federation_state: Mapping[str, Any],
    federation_projection: Mapping[str, Any],
    federation_metrics: Mapping[str, Any],
) -> dict[str, Any]:
    state = dict(federation_state or {})
    projection = dict(federation_projection or {})
    metrics = dict(federation_metrics or {})
    return {
        "state": str(state.get("state") or "federation_balancing"),
        "phase_transition": str(state.get("phase_transition") or "steady_continuity"),
        "orchestration_unity": str(state.get("orchestration_unity") or "moderate"),
        "continuity_projection": str(projection.get("continuity_projection") or "measured_continuity"),
        "cinematic_runtime_state": str(projection.get("cinematic_runtime_state") or "adaptive_cinematic_balance"),
        "federation_coherence": int(metrics.get("federation_coherence", 0) or 0),
        "federation_harmony": int(metrics.get("federation_harmony", 0) or 0),
        "federation_pressure": int(metrics.get("federation_pressure", 0) or 0),
    }
