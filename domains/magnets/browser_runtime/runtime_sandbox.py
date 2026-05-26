from __future__ import annotations

from typing import Any, Mapping


def evaluate_runtime_sandbox(
    *,
    runtime_manifest: Mapping[str, Any] | None,
    source_descriptor: Mapping[str, Any] | None,
    capability_snapshot: Mapping[str, Any] | None,
) -> dict[str, Any]:
    manifest = dict(runtime_manifest or {})
    source = dict(source_descriptor or {})
    capability = dict(capability_snapshot or {})
    mode = str(manifest.get("runtime_mode") or "").strip()
    state = str(manifest.get("runtime_state") or "").strip()
    blocking_reasons: list[str] = []
    warnings: list[str] = []

    if mode not in {"browser_runtime", "external_runtime"}:
        blocking_reasons.append("unsupported_runtime_mode")
    if state in {"runtime_blocked", "failed", "expired"}:
        blocking_reasons.append("blocked_runtime_manifest")
    if not source.get("source_valid", False):
        blocking_reasons.extend(list(source.get("errors") or ["malformed_runtime_source"]))
    if str(capability.get("browser_risk") or "").strip() == "high":
        blocking_reasons.append("high_risk_startup")
    if str(capability.get("browser_safety_class") or "").strip() == "unsafe":
        blocking_reasons.append("unsafe_browser_runtime")
    warnings.extend(list(capability.get("degradation_warnings") or []))
    warnings.extend(list(source.get("warnings") or []))
    status = "sandbox_ready" if not blocking_reasons and mode == "browser_runtime" else ("sandbox_limited" if not blocking_reasons else "sandbox_blocked")
    return {
        "sandbox_allowed": not blocking_reasons and mode == "browser_runtime",
        "sandbox_status": status,
        "blocking_reasons": _unique_strings(blocking_reasons),
        "warnings": _unique_strings(warnings),
    }


def _unique_strings(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return ordered
