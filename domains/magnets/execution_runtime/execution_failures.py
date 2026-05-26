from __future__ import annotations

from typing import Any, Mapping


EXECUTION_FAILURE_CATEGORIES = {
    "browser_memory_pressure",
    "unsupported_codec",
    "startup_timeout",
    "runtime_instability",
    "transport_rejection",
    "mobile_runtime_failure",
}

_DEFAULT_RECOVERY = {
    "browser_memory_pressure": "degrade_quality",
    "unsupported_codec": "external_handoff",
    "startup_timeout": "retry_bootstrap",
    "runtime_instability": "switch_runtime",
    "transport_rejection": "external_handoff",
    "mobile_runtime_failure": "mobile_safe_fallback",
}

_SEVERITY = {
    "browser_memory_pressure": "high",
    "unsupported_codec": "high",
    "startup_timeout": "medium",
    "runtime_instability": "medium",
    "transport_rejection": "high",
    "mobile_runtime_failure": "medium",
}


def normalize_execution_failure_category(value: Any) -> str:
    category = str(value or "").strip().lower()
    if category in EXECUTION_FAILURE_CATEGORIES:
        return category
    return "runtime_instability"


def build_execution_failure(
    category: Any,
    *,
    details: Mapping[str, Any] | None = None,
    state: str = "",
    transport: str = "",
) -> dict[str, Any]:
    normalized = normalize_execution_failure_category(category)
    return {
        "category": normalized,
        "severity": _SEVERITY.get(normalized, "medium"),
        "recoverable": normalized != "unsupported_codec",
        "recommended_recovery": _DEFAULT_RECOVERY.get(normalized, "switch_runtime"),
        "state": str(state or "").strip(),
        "transport": str(transport or "").strip(),
        "details": dict(details or {}),
    }
