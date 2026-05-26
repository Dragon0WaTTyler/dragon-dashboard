from __future__ import annotations

from typing import Any, Mapping


_POLICIES = {
    "conservative": {
        "id": "conservative",
        "label": "Conservative",
        "preferred_runtime": "external_runtime",
        "max_risk_score": 42,
        "allows_cinematic": False,
        "fallback_bias": "high",
    },
    "balanced": {
        "id": "balanced",
        "label": "Balanced",
        "preferred_runtime": "browser_runtime",
        "max_risk_score": 58,
        "allows_cinematic": False,
        "fallback_bias": "medium",
    },
    "cinematic": {
        "id": "cinematic",
        "label": "Cinematic",
        "preferred_runtime": "browser_runtime",
        "max_risk_score": 34,
        "allows_cinematic": True,
        "fallback_bias": "low",
    },
    "resilience-first": {
        "id": "resilience-first",
        "label": "Resilience First",
        "preferred_runtime": "external_runtime",
        "max_risk_score": 68,
        "allows_cinematic": False,
        "fallback_bias": "high",
    },
    "mobile-safe": {
        "id": "mobile-safe",
        "label": "Mobile Safe",
        "preferred_runtime": "external_runtime",
        "max_risk_score": 36,
        "allows_cinematic": False,
        "fallback_bias": "high",
    },
    "low-bandwidth": {
        "id": "low-bandwidth",
        "label": "Low Bandwidth",
        "preferred_runtime": "external_runtime",
        "max_risk_score": 40,
        "allows_cinematic": False,
        "fallback_bias": "high",
    },
}


def resolve_execution_policy(
    *,
    runtime_profile: str = "",
    requested_policy: str = "",
    playback_runtime: str = "",
    selected_source: Mapping[str, Any] | None = None,
    capability_snapshot: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    source = dict(selected_source or {})
    capability = dict(capability_snapshot or {})
    requested = str(requested_policy or "").strip().lower()
    if requested in _POLICIES:
        policy_id = requested
    else:
        profile = str(runtime_profile or "").strip().lower()
        if str(capability.get("mobile_runtime_risk") or "") == "high" or not bool(source.get("mobile_friendly", True)):
            policy_id = "mobile-safe"
        elif str(capability.get("memory_risk") or "") == "high":
            policy_id = "resilience-first"
        elif bool(source.get("high_bandwidth_required")):
            policy_id = "low-bandwidth"
        elif "cinematic" in profile:
            policy_id = "cinematic"
        elif str(playback_runtime or "").strip() == "browser_runtime":
            policy_id = "balanced"
        else:
            policy_id = "conservative"

    policy = dict(_POLICIES.get(policy_id) or _POLICIES["conservative"])
    policy["policy_state"] = "strict" if policy["id"] in {"mobile-safe", "resilience-first", "conservative", "low-bandwidth"} else "adaptive"
    policy["policy_reasons"] = _build_policy_reasons(policy, source=source, capability=capability, runtime_profile=runtime_profile)
    return policy


def _build_policy_reasons(
    policy: Mapping[str, Any],
    *,
    source: Mapping[str, Any],
    capability: Mapping[str, Any],
    runtime_profile: str,
) -> list[str]:
    reasons: list[str] = [f"Execution policy resolved to {policy.get('label', 'Conservative')}."]
    if str(capability.get("mobile_runtime_risk") or "") == "high":
        reasons.append("Mobile runtime risk is high.")
    if str(capability.get("memory_risk") or "") == "high":
        reasons.append("Memory pressure requires a more resilient runtime.")
    if bool(source.get("high_bandwidth_required")):
        reasons.append("Source startup is bandwidth-sensitive.")
    if "cinematic" in str(runtime_profile or "").lower():
        reasons.append("Requested profile carries cinematic startup pressure.")
    return reasons
