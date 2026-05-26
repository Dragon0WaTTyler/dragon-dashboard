from __future__ import annotations

from typing import Any, Mapping


def build_runtime_resonance_snapshot(
    *,
    resonance_state: Mapping[str, Any],
    resonance_harmony: Mapping[str, Any],
    resonance_sync: Mapping[str, Any],
    resonance_projection: Mapping[str, Any],
    resonance_metrics: Mapping[str, Any],
) -> dict[str, Any]:
    state = dict(resonance_state or {})
    harmony = dict(resonance_harmony or {})
    sync = dict(resonance_sync or {})
    projection = dict(resonance_projection or {})
    metrics = dict(resonance_metrics or {})
    return {
        "state": str(state.get("state") or "resonance_balancing"),
        "resonance_phase": str(state.get("resonance_phase") or "measured_resonance"),
        "harmonic_runtime_state": str(state.get("harmonic_runtime_state") or "measured_harmonic_balance"),
        "orchestration_resonance": str(sync.get("orchestration_resonance") or "moderate"),
        "cinematic_resonance": str(harmony.get("cinematic_resonance") or "measured_cinematic_resonance"),
        "resonance_projection": str(projection.get("forecast") or "measured_resonance_projection"),
        "resonance_stability": int(metrics.get("resonance_stability", 0) or 0),
        "resonance_pressure": int(metrics.get("resonance_pressure", 0) or 0),
        "sync_drift": int(metrics.get("sync_drift", 0) or 0),
    }
