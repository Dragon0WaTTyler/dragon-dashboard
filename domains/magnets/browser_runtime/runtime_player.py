from __future__ import annotations

from typing import Mapping


def build_runtime_player_descriptor(
    *,
    runtime_manifest: Mapping[str, object] | None,
    capability_snapshot: Mapping[str, object] | None,
    bootstrap_mode: str,
    fallback_available: bool,
) -> dict[str, object]:
    manifest = dict(runtime_manifest or {})
    capability = dict(capability_snapshot or {})
    runtime_profile = str(manifest.get("runtime_profile") or "").strip() or "browser_balanced"
    player_mode = "browser_sandbox" if str(manifest.get("runtime_mode") or "").strip() == "browser_runtime" else "external_handoff"
    ui_strategy = "inline_runtime_surface" if player_mode == "browser_sandbox" else "handoff_notice"
    return {
        "player_mode": player_mode,
        "preferred_runtime": runtime_profile,
        "ui_strategy": ui_strategy,
        "transport_expectation": "magnet_handoff" if player_mode != "browser_sandbox" else "sandbox_bootstrap",
        "fallback_available": bool(fallback_available),
        "browser_safety_class": str(capability.get("browser_safety_class") or "unknown"),
        "bootstrap_mode": str(bootstrap_mode or "").strip(),
    }
