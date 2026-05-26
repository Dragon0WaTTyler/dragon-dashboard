from __future__ import annotations

from typing import Any, Mapping


def build_preference_evolution(
    identity_memory_summary: Mapping[str, Any] | None,
    *,
    current_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    summary = dict(identity_memory_summary or {})
    preferences = {
        "browser_preference": _dominant(summary.get("runtime_preferences"), default="external_runtime"),
        "fallback_path_preference": _dominant(summary.get("fallback_preferences"), default="measured"),
        "bandwidth_adaptation_preference": _dominant(summary.get("bandwidth_preferences"), default="balanced"),
        "subtitle_complexity_tolerance": _dominant(summary.get("subtitle_preferences"), default="standard"),
    }
    preferences["preference_stability"] = "stable" if int(summary.get("total_observations", 0) or 0) >= 3 else "forming"
    return preferences


def _dominant(values: Mapping[str, Any] | None, *, default: str) -> str:
    return max(
        ((str(key), int(value or 0)) for key, value in dict(values or {}).items()),
        key=lambda item: (item[1], item[0]),
        default=(default, 0),
    )[0]
