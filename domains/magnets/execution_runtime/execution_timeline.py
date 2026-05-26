from __future__ import annotations

from typing import Any, Mapping


def build_execution_timeline(
    *,
    capability_snapshot: Mapping[str, Any] | None = None,
    readiness_snapshot: Mapping[str, Any] | None = None,
    runtime_manifest: Mapping[str, Any] | None = None,
    transport_descriptor: Mapping[str, Any] | None = None,
    execution_metrics: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    capability = dict(capability_snapshot or {})
    readiness = dict(readiness_snapshot or {})
    transport = dict(transport_descriptor or {})
    metrics = dict(execution_metrics or {})
    manifest = dict(runtime_manifest or {})

    base_startup = int(capability.get("startup_timeout_estimate_seconds") or 20) * 1000
    if str(readiness.get("startup_confidence") or manifest.get("startup_confidence") or "") == "low":
        base_startup += 6000
    if str(transport.get("startup_behavior") or "") == "buffer_sensitive":
        base_startup += 4000
    if str(transport.get("transport_class") or "") == "external_handoff":
        base_startup = max(1800, base_startup - 8000)

    degradation_risk = int(metrics.get("degradation_risk") or 0)
    fallback_pressure = int(metrics.get("fallback_pressure") or 0)
    stability_score = int(metrics.get("stability_score") or 0)

    return {
        "estimated_startup_ms": max(1000, base_startup),
        "estimated_stability": _stability_label(stability_score),
        "risk_window": _risk_window_label(degradation_risk),
        "fallback_probability": round(min(0.95, max(0.05, fallback_pressure / 100)), 2),
        "degradation_window_ms": max(4000, 6000 + degradation_risk * 120),
        "recovery_window_ms": max(3000, 4000 + (100 - stability_score) * 140),
    }


def _stability_label(score: int) -> str:
    if score >= 80:
        return "stable"
    if score >= 60:
        return "guarded"
    if score >= 40:
        return "fragile"
    return "volatile"


def _risk_window_label(risk: int) -> str:
    if risk >= 70:
        return "early_runtime"
    if risk >= 45:
        return "startup_to_midstream"
    return "background_only"
