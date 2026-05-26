from __future__ import annotations

from typing import Any, Mapping


BROWSER_RUNTIME_MAX_SIZE_GB = 10.0
BROWSER_RUNTIME_MOBILE_MAX_SIZE_GB = 6.5
BROWSER_RUNTIME_MAX_MEMORY_CLASS = "medium"
BROWSER_RUNTIME_TIMEOUT_SECONDS = {
    "low": 12,
    "medium": 18,
    "high": 28,
    "unknown": 20,
}


def build_runtime_limits(*, source: Mapping[str, Any] | None = None) -> dict[str, Any]:
    data = dict(source or {})
    size_gb = _float_value(data.get("size_gb"))
    mobile_safe = size_gb <= BROWSER_RUNTIME_MOBILE_MAX_SIZE_GB and size_gb > 0
    memory_class = _memory_class(size_gb)
    bandwidth_class = str(data.get("bandwidth_class") or "").strip().lower() or _bandwidth_class(size_gb)
    return {
        "max_safe_browser_size_gb": BROWSER_RUNTIME_MAX_SIZE_GB,
        "max_mobile_safe_size_gb": BROWSER_RUNTIME_MOBILE_MAX_SIZE_GB,
        "max_memory_class": BROWSER_RUNTIME_MAX_MEMORY_CLASS,
        "startup_timeout_estimate_seconds": int(BROWSER_RUNTIME_TIMEOUT_SECONDS.get(bandwidth_class, 20)),
        "memory_class": memory_class,
        "mobile_safe": mobile_safe,
        "degradation_rules": compute_runtime_degradation(
            size_gb=size_gb,
            memory_class=memory_class,
            bandwidth_class=bandwidth_class,
            mobile_safe=mobile_safe,
        ),
    }


def compute_runtime_degradation(
    *,
    size_gb: float,
    memory_class: str,
    bandwidth_class: str,
    mobile_safe: bool,
) -> list[str]:
    warnings: list[str] = []
    if size_gb > BROWSER_RUNTIME_MAX_SIZE_GB:
        warnings.append("browser_size_limit_exceeded")
    if memory_class == "high":
        warnings.append("memory_pressure_risk")
    if bandwidth_class == "high":
        warnings.append("startup_timeout_risk")
    if not mobile_safe:
        warnings.append("mobile_runtime_limited")
    return warnings


def _memory_class(size_gb: float) -> str:
    if size_gb >= 12:
        return "high"
    if size_gb >= 6:
        return "medium"
    if size_gb > 0:
        return "low"
    return "unknown"


def _bandwidth_class(size_gb: float) -> str:
    if size_gb >= 18:
        return "high"
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
