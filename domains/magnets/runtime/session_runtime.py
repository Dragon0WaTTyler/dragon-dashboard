from __future__ import annotations

from typing import Any, Mapping


RUNTIME_INTENTS = {
    "browser_stream",
    "external_player",
    "mobile_handoff",
    "future_webtorrent",
    "future_direct_stream",
}


def normalize_runtime_intent(value: Any) -> str:
    intent = str(value or "").strip().lower()
    if intent in RUNTIME_INTENTS:
        return intent
    return ""


def resolve_runtime_intent(
    *,
    preferred_runtime: Any,
    handoff_mode: Any,
    admission_policy: Mapping[str, Any] | None = None,
) -> str:
    normalized_preference = normalize_runtime_intent(preferred_runtime)
    if normalized_preference:
        return normalized_preference

    normalized_handoff = str(handoff_mode or "").strip().lower()
    policy = dict(admission_policy or {})
    if normalized_handoff == "mobile_handoff" and policy.get("mobile_safe"):
        return "mobile_handoff"
    if policy.get("allowed_for_browser"):
        return "browser_stream"
    if policy.get("external_only") or normalized_handoff in {"external_player", "external_handoff"}:
        return "external_player"
    return "external_player"

