from __future__ import annotations

from typing import Any, Mapping


def build_runtime_fallbacks(
    source: Mapping[str, Any],
    *,
    browser_runtime: Mapping[str, Any],
    external_runtime: Mapping[str, Any],
) -> list[dict[str, Any]]:
    fallbacks: list[dict[str, Any]] = []
    if browser_runtime.get("launch_strategy") == "browser_embed":
        fallbacks.append(
            {
                "id": "browser_embed",
                "label": "Browser Ready",
                "runtime": "browser_runtime",
                "available": True,
            }
        )
    fallbacks.append(
        {
            "id": str(external_runtime.get("magnet_handoff_priority") or "magnet_uri"),
            "label": "External Recommended",
            "runtime": "external_runtime",
            "available": bool(source.get("magnet")),
        }
    )
    fallbacks.append(
        {
            "id": "copy_magnet",
            "label": "Copy Magnet",
            "runtime": "external_runtime",
            "available": bool(source.get("magnet")),
        }
    )
    return fallbacks
