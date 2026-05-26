from __future__ import annotations

from typing import Any, Mapping

from ..runtime.observability import emit_event
from .capability_matrix import evaluate_browser_capability


def prepare_browser_runtime(
    source: Mapping[str, Any],
    *,
    profile: Mapping[str, Any],
    compatibility: Mapping[str, Any] | None = None,
    diagnostics: Mapping[str, Any] | None = None,
    fallback_urls: list[str] | None = None,
    player_sources: list[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    comp = dict(compatibility or evaluate_browser_capability(source))
    diag = dict(diagnostics or {})
    urls = [str(item or "").strip() for item in (fallback_urls or []) if str(item or "").strip()]
    structured_player_sources = [
        dict(item)
        for item in (player_sources or [])
        if isinstance(item, Mapping) and str(item.get("url") or "").strip()
    ]
    browser_viable = bool(comp.get("browser_friendly"))
    browser_surface_available = bool(urls or structured_player_sources)
    startup_feasible = bool(browser_viable and diag.get("magnet_valid", True) and browser_surface_available)
    readiness = "browser_ready" if startup_feasible else ("browser_deferred" if browser_viable else "external_recommended")
    confidence = _confidence_level(
        reliability=str(profile.get("startup_reliability") or ""),
        browser_viable=browser_viable,
        startup_feasible=startup_feasible,
    )
    warnings = list(diag.get("warnings") or [])
    if not urls and not structured_player_sources:
        warnings.append("no_browser_surface")
    if not browser_viable:
        warnings.append("browser_blocked")
    if comp.get("browser_hard_fail_codec"):
        warnings.append("browser_hard_fail_codec")
    if comp.get("hdr") or comp.get("dolby_vision"):
        warnings.append("browser_dynamic_range_risk")

    emit_event(
        "[playback-preflight]",
        runtime="browser",
        browser_viable=1 if browser_viable else 0,
        startup_feasible=1 if startup_feasible else 0,
        confidence=confidence,
    )
    return {
        "runtime": "browser_runtime",
        "readiness": readiness,
        "browser_viable": browser_viable,
        "startup_feasible": startup_feasible,
        "startup_confidence": confidence,
        "warnings": _unique_strings(warnings),
        "player_sources": structured_player_sources,
        "fallback_urls": urls,
        "launch_strategy": "browser_embed" if (structured_player_sources or urls) else "browser_deferred",
    }


def _confidence_level(*, reliability: str, browser_viable: bool, startup_feasible: bool) -> str:
    if browser_viable and startup_feasible and reliability == "high":
        return "high"
    if browser_viable and startup_feasible:
        return "medium"
    return "low"


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
