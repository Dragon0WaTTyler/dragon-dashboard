from __future__ import annotations

from typing import Any, Mapping


def evaluate_orchestration_constraints(
    *,
    capability_snapshot: Mapping[str, Any] | None = None,
    selected_source: Mapping[str, Any] | None = None,
    playback_runtime: str = "",
    runtime_profile: str = "",
    execution_policy: Mapping[str, Any] | None = None,
    authority_memory_summary: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    capability = dict(capability_snapshot or {})
    source = dict(selected_source or {})
    policy = dict(execution_policy or {})
    memory = dict(authority_memory_summary or {})
    runtime = str(playback_runtime or "").strip() or "external_runtime"
    profile = str(runtime_profile or "").strip().lower()

    forced_constraints: list[dict[str, Any]] = []
    blocked_runtimes: list[str] = []
    blocked_paths: list[str] = []
    blocked_profiles: list[str] = []

    if str(capability.get("browser_safety_class") or "") == "unsafe" or str(capability.get("browser_risk") or "") == "high":
        blocked_runtimes.append("browser_runtime")
        blocked_paths.append("browser_stream_path")
        forced_constraints.append(_constraint("browser_capability_cap", "browser_runtime", "block", "Browser capability is below the authority safety bar."))
    if str(capability.get("mobile_runtime_risk") or "") == "high" or not bool(source.get("mobile_friendly", True)):
        blocked_profiles.append("cinematic")
        blocked_paths.append("cinematic_mobile_path")
        forced_constraints.append(_constraint("mobile_max_bitrate_policy", "cinematic", "downgrade", "Mobile conditions cannot sustain the cinematic path."))
    if str(capability.get("memory_risk") or "") == "high":
        blocked_profiles.append("browser_cinematic")
        forced_constraints.append(_constraint("low_memory_runtime_restrictions", "browser_cinematic", "downgrade", "Memory pressure requires a non-cinematic runtime."))
    if bool(source.get("high_bandwidth_required")) and str(policy.get("id") or "") in {"mobile-safe", "low-bandwidth", "conservative"}:
        blocked_paths.append("aggressive_bandwidth_path")
        forced_constraints.append(_constraint("bandwidth_guardrail", runtime or "browser_runtime", "cap", "The current execution policy suppresses high-bandwidth startup paths."))
    return {
        "constraint_state": "constrained" if forced_constraints else "clear",
        "forced_constraints": forced_constraints,
        "blocked_runtimes": _unique(blocked_runtimes),
        "blocked_profiles": _unique(blocked_profiles),
        "blocked_paths": _unique(blocked_paths),
        "timeout_boundary_ms": 12000 if runtime == "browser_runtime" else 8000,
        "constraint_confidence": 86 if forced_constraints else 72,
        "policy_runtime_cap": "external_runtime" if "browser_runtime" in blocked_runtimes else runtime,
        "policy_profile_cap": "balanced" if blocked_profiles else profile,
    }


def _constraint(name: str, target: str, action: str, reason: str) -> dict[str, str]:
    return {
        "constraint": name,
        "target": target,
        "action": action,
        "reason": reason,
    }


def _unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in seen:
            seen.add(text)
            ordered.append(text)
    return ordered
