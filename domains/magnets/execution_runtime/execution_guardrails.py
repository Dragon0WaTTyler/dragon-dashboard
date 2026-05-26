from __future__ import annotations

from typing import Any, Mapping


def evaluate_execution_guardrails(
    *,
    source_metadata: Mapping[str, Any] | None = None,
    capability_snapshot: Mapping[str, Any] | None = None,
    runtime_manifest: Mapping[str, Any] | None = None,
    transport_descriptor: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    source = dict(source_metadata or {})
    capability = dict(capability_snapshot or {})
    manifest = dict(runtime_manifest or {})
    transport = dict(transport_descriptor or {})
    reasons: list[str] = []
    warnings: list[str] = []
    failure_category = ""

    memory_risk = str(capability.get("memory_risk") or "unknown")
    browser_safety = str(capability.get("browser_safety_class") or "unknown")
    codec_support = str(capability.get("browser_codec_support_assumption") or "unknown")
    mobile_risk = str(capability.get("mobile_runtime_risk") or "unknown")
    transport_class = str(transport.get("transport_class") or "")
    startup_viability = str(capability.get("startup_viability") or "")
    runtime_mode = str(manifest.get("runtime_mode") or "")

    if browser_safety == "unsafe" and runtime_mode == "browser_runtime":
        reasons.append("unsafe_runtime_rejection")
        failure_category = "transport_rejection"
    if memory_risk == "high":
        reasons.append("high_memory_rejection")
        failure_category = failure_category or "browser_memory_pressure"
    if transport_class == "browser_heavy":
        reasons.append("browser_overload_rejection")
        failure_category = failure_category or "browser_memory_pressure"
    if mobile_risk == "high" and bool(source.get("mobile_friendly") is False or capability.get("mobile_runtime_risk") == "high"):
        reasons.append("mobile_instability_rejection")
        failure_category = failure_category or "mobile_runtime_failure"
    if codec_support == "unsupported":
        reasons.append("unsupported_codec_rejection")
        failure_category = failure_category or "unsupported_codec"
    if startup_viability == "fragile":
        warnings.append("startup_fragility_detected")
    if not reasons and transport_class == "mobile_limited":
        warnings.append("mobile_runtime_limited")

    return {
        "rejected": bool(reasons),
        "blocking_reasons": reasons,
        "warnings": warnings,
        "failure_category": failure_category or ("runtime_instability" if warnings else ""),
    }
