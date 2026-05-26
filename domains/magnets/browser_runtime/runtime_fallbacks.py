from __future__ import annotations

from typing import Any, Mapping


def build_browser_runtime_fallbacks(
    *,
    runtime_manifest: Mapping[str, Any] | None = None,
    playback_plan: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    manifest = dict(runtime_manifest or {})
    plan = dict(playback_plan or {})
    preflight = dict(manifest.get("preflight") or plan.get("runtime_preflight") or {})
    fallback_strategy = str(preflight.get("fallback_strategy") or "").strip() or "none"
    fallbacks = [dict(item) for item in plan.get("fallbacks") or manifest.get("fallbacks") or [] if isinstance(item, Mapping)]
    fallback_runtime = "external_runtime" if fallback_strategy not in {"", "none"} else "none"
    return {
        "fallback_runtime": fallback_runtime,
        "fallback_strategy": fallback_strategy,
        "fallback_available": fallback_runtime == "external_runtime",
        "fallback_paths": fallbacks,
    }
