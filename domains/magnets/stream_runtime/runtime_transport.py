from __future__ import annotations

from typing import Any, Mapping


def determine_runtime_transport(
    *,
    runtime_mode: str,
    browser_runtime: Mapping[str, Any] | None = None,
    external_runtime: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    mode = str(runtime_mode or "").strip() or "external_runtime"
    browser = dict(browser_runtime or {})
    external = dict(external_runtime or {})
    if mode == "browser_runtime":
        return {
            "mode": mode,
            "strategy": str(browser.get("launch_strategy") or "browser_deferred"),
            "target": "browser",
        }
    return {
        "mode": "external_runtime",
        "strategy": str(external.get("magnet_handoff_priority") or "magnet_uri"),
        "target": "external_player",
    }
