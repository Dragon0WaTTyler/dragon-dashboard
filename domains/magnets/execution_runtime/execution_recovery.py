from __future__ import annotations

from typing import Any, Mapping

from .execution_failures import build_execution_failure, normalize_execution_failure_category


RECOVERY_PATHS = {
    "degrade_quality",
    "switch_runtime",
    "external_handoff",
    "retry_bootstrap",
    "mobile_safe_fallback",
}

_RECOVERY_HINTS = {
    "degrade_quality": ["reduce_runtime_pressure", "prefer_browser_progressive_transport"],
    "switch_runtime": ["select_external_runtime", "preserve_manifest_context"],
    "external_handoff": ["handoff_to_external_player", "retain_playback_session"],
    "retry_bootstrap": ["revalidate_runtime_surface", "retry_with_guarded_bootstrap"],
    "mobile_safe_fallback": ["disable_mobile_runtime", "prefer_external_handoff"],
}


def normalize_recovery_path(value: Any) -> str:
    path = str(value or "").strip().lower()
    if path in RECOVERY_PATHS:
        return path
    return "switch_runtime"


def select_recovery_path(
    *,
    failure: Mapping[str, Any] | None = None,
    guardrails: Mapping[str, Any] | None = None,
    transport_descriptor: Mapping[str, Any] | None = None,
    capability_snapshot: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    failure_payload = dict(failure or {})
    guardrail_payload = dict(guardrails or {})
    transport = dict(transport_descriptor or {})
    capability = dict(capability_snapshot or {})

    category = normalize_execution_failure_category(failure_payload.get("category"))
    if guardrail_payload.get("rejected"):
        category = normalize_execution_failure_category(guardrail_payload.get("failure_category") or category)

    recovery = "switch_runtime"
    if category == "browser_memory_pressure":
        recovery = "degrade_quality"
    elif category == "unsupported_codec":
        recovery = "external_handoff"
    elif category == "startup_timeout":
        recovery = "retry_bootstrap"
    elif category == "mobile_runtime_failure":
        recovery = "mobile_safe_fallback"
    elif category == "transport_rejection":
        recovery = "external_handoff"

    if str(transport.get("transport_class") or "") == "unsupported_transport":
        recovery = "external_handoff"
    if str(capability.get("browser_safety_class") or "") == "unsafe":
        recovery = "external_handoff"

    normalized_recovery = normalize_recovery_path(recovery)
    return {
        "path": normalized_recovery,
        "hints": list(_RECOVERY_HINTS.get(normalized_recovery, [])),
        "failure": failure_payload or build_execution_failure(category),
    }
