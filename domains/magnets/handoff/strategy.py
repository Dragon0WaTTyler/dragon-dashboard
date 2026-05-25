from __future__ import annotations

from typing import Any, Mapping

from .diagnostics import evaluate_streamability


def build_handoff_profile(candidate: Mapping[str, Any], *, movie: Mapping[str, Any] | None = None) -> dict[str, Any]:
    data = dict(candidate or {})
    diagnostics = evaluate_streamability(data, movie=movie)
    compatibility = diagnostics["compatibility"]
    magnet_value = str(data.get("magnet") or "").strip()
    primary_strategy = "browser_fallback"
    handoff_type = "unavailable"
    likely_compatibility = "limited"
    if diagnostics["summary"]["magnet_valid"]:
        primary_strategy = "magnet_uri"
        handoff_type = "protocol_open"
        if compatibility["external_player_ready"]:
            likely_compatibility = "external_player_ready"
        elif compatibility["browser_friendly"]:
            likely_compatibility = "browser_friendly"
        else:
            likely_compatibility = "magnet_only"

    actions = [
        _action(
            action_id="open_magnet",
            label="Open Magnet",
            strategy="magnet_uri",
            handoff_type="protocol_open",
            magnet=magnet_value,
            enabled=diagnostics["summary"]["magnet_valid"],
        ),
        _action(
            action_id="copy_magnet",
            label="Copy Magnet",
            strategy="clipboard_copy",
            handoff_type="clipboard",
            magnet=magnet_value,
            enabled=bool(magnet_value),
        ),
        _action(
            action_id="open_external",
            label="Open Externally",
            strategy="external_app",
            handoff_type="external_player",
            magnet=magnet_value,
            enabled=diagnostics["summary"]["magnet_valid"],
        ),
    ]

    return {
        "open_strategy": primary_strategy,
        "handoff_type": handoff_type,
        "likely_compatibility": likely_compatibility,
        "external_player_hints": _external_player_hints(compatibility=compatibility, candidate=data),
        "compatibility": compatibility,
        "diagnostics": diagnostics["summary"],
        "actions": actions,
        "fallback_strategy": _fallback_strategy(diagnostics=diagnostics),
    }


def resolve_handoff_action(
    action: str,
    candidate: Mapping[str, Any],
    *,
    movie: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    profile = build_handoff_profile(candidate, movie=movie)
    action_id = str(action or "").strip().lower()
    selected_action = next((item for item in profile["actions"] if item["id"] == action_id), None)
    if not selected_action:
        return {"ok": False, "error": "Unsupported handoff action.", "handoff": profile}
    if not selected_action["enabled"]:
        return {
            "ok": False,
            "error": selected_action["disabled_reason"] or "This handoff is unavailable for the selected source.",
            "handoff": profile,
        }
    profile["selected_action"] = selected_action
    return {"ok": True, "handoff": profile}


def _fallback_strategy(*, diagnostics: Mapping[str, Any]) -> str:
    summary = dict(diagnostics.get("summary") or {})
    if summary.get("magnet_valid"):
        return "clipboard_copy"
    return "disabled"


def _external_player_hints(*, compatibility: Mapping[str, Any], candidate: Mapping[str, Any]) -> list[str]:
    hints: list[str] = []
    codec = str(candidate.get("codec") or "").strip() or "unknown codec"
    resolution = str(candidate.get("resolution") or "").strip() or "unknown resolution"
    if compatibility.get("external_player_ready"):
        hints.append(f"Best opened in VLC, Stremio, or another magnet-capable player for {codec} {resolution}.")
    elif compatibility.get("browser_friendly"):
        hints.append("Should degrade cleanly in a browser handoff, but external players remain more reliable for magnets.")
    else:
        hints.append("Treat this as a handoff-only source; use an external magnet-capable player if the browser refuses the protocol.")
    if compatibility.get("high_bandwidth_required"):
        hints.append("This release likely needs a stable high-bandwidth connection.")
    if not compatibility.get("mobile_friendly"):
        hints.append("Mobile handoff is likely weaker than desktop for this source profile.")
    return hints


def _action(
    *,
    action_id: str,
    label: str,
    strategy: str,
    handoff_type: str,
    magnet: str,
    enabled: bool,
) -> dict[str, Any]:
    return {
        "id": action_id,
        "label": label,
        "strategy": strategy,
        "handoff_type": handoff_type,
        "enabled": enabled,
        "target": magnet if enabled else "",
        "disabled_reason": "" if enabled else "A valid magnet payload is required for this handoff.",
    }
