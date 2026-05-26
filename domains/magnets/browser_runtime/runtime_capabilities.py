from __future__ import annotations

from typing import Any, Mapping

from .runtime_limits import build_runtime_limits


_CODEC_ASSUMPTIONS = {
    "x264": "supported",
    "av1": "partial",
    "x265": "unsupported",
    "hevc": "unsupported",
    "": "unknown",
}


def build_capability_snapshot(
    source: Mapping[str, Any] | None,
    *,
    runtime_manifest: Mapping[str, Any] | None = None,
    readiness_snapshot: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    descriptor = dict(source or {})
    manifest = dict(runtime_manifest or {})
    readiness = dict(readiness_snapshot or {})
    limits = build_runtime_limits(source=descriptor)
    codec = str(descriptor.get("codec") or "").strip().lower()
    bandwidth_class = str(readiness.get("bandwidth_class") or "").strip().lower() or _bandwidth_class(descriptor, limits)
    browser_risk = estimate_browser_risk(
        source=descriptor,
        limits=limits,
        runtime_manifest=manifest,
        readiness_snapshot=readiness,
    )
    mobile_risk = estimate_mobile_runtime_risk(source=descriptor, limits=limits)
    return {
        "browser_codec_support_assumption": _CODEC_ASSUMPTIONS.get(codec, "unknown"),
        "mobile_limitations": [
            "mobile_size_limit" if not limits.get("mobile_safe") else "",
            "hdr_runtime_penalty" if descriptor.get("runtime_safe_metadata", {}).get("hdr") else "",
            "dolby_vision_penalty" if descriptor.get("runtime_safe_metadata", {}).get("dolby_vision") else "",
        ],
        "bandwidth_class": bandwidth_class,
        "memory_risk": str(limits.get("memory_class") or "unknown"),
        "startup_viability": "viable" if browser_risk in {"low", "medium"} else "fragile",
        "browser_safety_class": _browser_safety_class(codec_support=_CODEC_ASSUMPTIONS.get(codec, "unknown"), browser_risk=browser_risk),
        "browser_risk": browser_risk,
        "mobile_runtime_risk": mobile_risk,
        "degradation_warnings": list(limits.get("degradation_rules") or []),
        "startup_timeout_estimate_seconds": int(limits.get("startup_timeout_estimate_seconds") or 20),
    }


def estimate_browser_risk(
    *,
    source: Mapping[str, Any] | None,
    limits: Mapping[str, Any] | None = None,
    runtime_manifest: Mapping[str, Any] | None = None,
    readiness_snapshot: Mapping[str, Any] | None = None,
) -> str:
    descriptor = dict(source or {})
    resolved_limits = dict(limits or build_runtime_limits(source=descriptor))
    manifest = dict(runtime_manifest or {})
    readiness = dict(readiness_snapshot or {})
    if not descriptor.get("source_valid", True):
        return "high"
    if str(manifest.get("runtime_mode") or "").strip() == "blocked":
        return "high"
    degradation = list(resolved_limits.get("degradation_rules") or [])
    if "browser_size_limit_exceeded" in degradation or "memory_pressure_risk" in degradation:
        return "high"
    if str(readiness.get("startup_confidence") or manifest.get("startup_confidence") or "").strip().lower() == "low":
        return "medium"
    return "low"


def estimate_mobile_runtime_risk(*, source: Mapping[str, Any] | None, limits: Mapping[str, Any] | None = None) -> str:
    descriptor = dict(source or {})
    resolved_limits = dict(limits or build_runtime_limits(source=descriptor))
    if not descriptor.get("source_valid", True):
        return "high"
    if not resolved_limits.get("mobile_safe"):
        return "high"
    if descriptor.get("runtime_safe_metadata", {}).get("hdr") or descriptor.get("runtime_safe_metadata", {}).get("dolby_vision"):
        return "medium"
    return "low"


def _browser_safety_class(*, codec_support: str, browser_risk: str) -> str:
    if codec_support == "unsupported" or browser_risk == "high":
        return "unsafe"
    if codec_support == "partial" or browser_risk == "medium":
        return "limited"
    if codec_support == "supported":
        return "safe"
    return "unknown"


def _bandwidth_class(source: Mapping[str, Any], limits: Mapping[str, Any]) -> str:
    if "startup_timeout_risk" in list(limits.get("degradation_rules") or []):
        return "high"
    size_gb = _float_value(source.get("size_gb"))
    if size_gb >= 8:
        return "medium"
    if size_gb > 0:
        return "low"
    return "unknown"


def _float_value(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
