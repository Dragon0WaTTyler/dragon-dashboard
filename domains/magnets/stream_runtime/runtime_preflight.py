from __future__ import annotations

from typing import Any, Mapping

from .runtime_guardrails import evaluate_runtime_guardrails


def build_runtime_preflight(
    *,
    source: Mapping[str, Any] | None = None,
    capability_snapshot: Mapping[str, Any] | None = None,
    runtime_mode: str = "",
    runtime_profile: str = "",
    startup_confidence: str = "",
    player_sources: list[Mapping[str, Any]] | None = None,
    fallback_urls: list[str] | None = None,
    fallbacks: list[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    source_data = dict(source or {})
    capability = dict(capability_snapshot or {})
    mode = str(runtime_mode or "").strip() or "external_runtime"
    browser_surface = bool(player_sources or fallback_urls)
    external_fallback_available = any(
        bool(item.get("available"))
        for item in (fallbacks or [])
        if str(item.get("runtime") or "").strip() == "external_runtime"
    ) or bool(source_data.get("magnet"))
    guardrails = evaluate_runtime_guardrails(
        source=source_data,
        capability_snapshot=capability,
        runtime_mode=mode,
        startup_confidence=startup_confidence,
        runtime_profile=runtime_profile,
    )
    runtime_allowed = bool(guardrails.get("allowed"))
    blocking_reasons = list(guardrails.get("blocking_reasons") or [])
    warnings = list(guardrails.get("warnings") or [])

    if mode == "browser_runtime" and not browser_surface:
        runtime_allowed = False
        warnings.append("browser_surface_missing")
        if "browser_policy_block" not in blocking_reasons:
            blocking_reasons.append("browser_policy_block")

    if mode == "browser_runtime" and runtime_allowed:
        resolved_mode = "browser_runtime"
        fallback_strategy = "external_player_fallback" if external_fallback_available else "none"
    elif external_fallback_available and bool(capability.get("external_player_ready")):
        resolved_mode = "external_runtime"
        runtime_allowed = True
        fallback_strategy = "external_player"
        if mode == "browser_runtime":
            warnings.append("browser_degraded_to_external")
    else:
        resolved_mode = "blocked"
        runtime_allowed = False
        fallback_strategy = "none"

    return {
        "runtime_allowed": runtime_allowed,
        "runtime_mode": resolved_mode,
        "warnings": _unique_strings(warnings),
        "blocking_reasons": _unique_strings(blocking_reasons),
        "fallback_strategy": fallback_strategy,
        "checks": {
            "browser_capability": bool(capability.get("browser_friendly")),
            "bandwidth_viability": not bool(capability.get("high_bandwidth_required")),
            "magnet_validity": bool(capability.get("magnet_valid")),
            "source_sanity": bool(dict(capability.get("size_sanity") or {}).get("is_sane", True))
            and bool(capability.get("likely_streamable", True)),
            "runtime_policy_compatibility": not bool(guardrails.get("blocking_reasons")),
            "external_fallback_availability": external_fallback_available,
        },
        "failures": list(guardrails.get("failures") or []),
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
