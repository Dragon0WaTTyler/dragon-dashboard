from __future__ import annotations

from typing import Any, Mapping

from ..runtime.observability import emit_event
from .capability_matrix import evaluate_capability_matrix


EXTERNAL_PLAYERS = ("VLC", "Kodi", "IINA", "Infuse")


def build_external_runtime(
    source: Mapping[str, Any],
    *,
    profile: Mapping[str, Any],
    compatibility: Mapping[str, Any] | None = None,
    diagnostics: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    comp = dict(compatibility or evaluate_capability_matrix(source))
    diag = dict(diagnostics or {})
    mobile_friendly = bool(comp.get("mobile_friendly"))
    external_ready = bool(comp.get("external_player_ready") or diag.get("magnet_valid"))
    fallback_reason = "browser_blocked" if not comp.get("browser_friendly") else "external_preferred"
    handoff_priority = "magnet_uri" if diag.get("magnet_valid") else "clipboard_copy"
    warnings = list(diag.get("warnings") or [])
    if not mobile_friendly:
        warnings.append("mobile_handoff_risk")

    emit_event(
        "[playback-runtime]",
        runtime="external",
        external_ready=1 if external_ready else 0,
        handoff=handoff_priority,
        profile=str(profile.get("id") or "external_player_only"),
    )
    return {
        "runtime": "external_runtime",
        "readiness": "external_ready" if external_ready else "limited",
        "startup_confidence": "high" if external_ready else "low",
        "players": list(EXTERNAL_PLAYERS),
        "magnet_handoff_priority": handoff_priority,
        "mobile_safe_fallback": "copy_magnet" if mobile_friendly else "open_external",
        "browser_blocked_fallback": "copy_magnet",
        "warnings": _unique_strings(warnings + [fallback_reason]),
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
