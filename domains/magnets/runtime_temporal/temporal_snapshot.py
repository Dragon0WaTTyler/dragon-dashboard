from __future__ import annotations

from typing import Any, Mapping


def build_runtime_temporal_snapshot(
    *,
    temporal_state: Mapping[str, Any],
    temporal_phase: Mapping[str, Any],
    temporal_rhythm: Mapping[str, Any],
    temporal_metrics: Mapping[str, Any],
    temporal_forecast: Mapping[str, Any],
) -> dict[str, Any]:
    state = dict(temporal_state or {})
    phase = dict(temporal_phase or {})
    rhythm = dict(temporal_rhythm or {})
    metrics = dict(temporal_metrics or {})
    forecast = dict(temporal_forecast or {})
    return {
        "state": str(state.get("state") or "temporal_balancing"),
        "temporal_phase": str(phase.get("phase") or "measured_continuity"),
        "temporal_rhythm": str(rhythm.get("rhythm_state") or "measured_pacing"),
        "temporal_forecast": str(forecast.get("forecast") or "measured_future_shaping"),
        "temporal_stability": int(metrics.get("temporal_stability", 0) or 0),
        "temporal_momentum": int(metrics.get("temporal_momentum", 0) or 0),
        "temporal_pressure": int(metrics.get("temporal_pressure", 0) or 0),
    }
