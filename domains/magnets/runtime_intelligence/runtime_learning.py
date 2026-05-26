from __future__ import annotations

from typing import Any, Mapping


def build_runtime_learning(
    runtime_memory_summary: Mapping[str, Any] | None,
    *,
    current_context: Mapping[str, Any] | None = None,
    historical_patterns: list[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    summary = dict(runtime_memory_summary or {})
    context = dict(current_context or {})
    patterns = [dict(item) for item in historical_patterns or [] if isinstance(item, Mapping)]
    rules: list[dict[str, Any]] = []
    runtime_confidence_adjustment = 0
    fallback_trust_adjustment = 0
    browser_cinematic_penalty = 0
    mobile_viability_adjustment = 0

    if float(summary.get("runtime_instability", 0) or 0) >= 0.45:
        rules.append(_rule("repeated_instability_lowers_runtime_confidence", -14, int(summary.get("total_observations", 0) or 0)))
        runtime_confidence_adjustment -= 14
    if float(summary.get("recovery_success_rate", 0) or 0) >= 0.45:
        rules.append(_rule("repeated_recovery_improves_fallback_trust", 12, int(summary.get("total_observations", 0) or 0)))
        fallback_trust_adjustment += 12
    if _mobile_safe_success(summary, context):
        rules.append(_rule("successful_mobile_safe_runtime_boosts_viability", 9, 2))
        mobile_viability_adjustment += 9
    if float(summary.get("browser_rejection_trend", 0) or 0) >= 0.2:
        rules.append(_rule("browser_rejection_reduces_cinematic_preference", -11, int(summary.get("total_observations", 0) or 0)))
        browser_cinematic_penalty -= 11
    for pattern in patterns:
        if pattern.get("pattern_type") == "cinematic_profile_often_downgrades":
            browser_cinematic_penalty -= 8
        if pattern.get("pattern_type") == "external_runtime_succeeds_after_startup_degradation":
            fallback_trust_adjustment += 6

    return {
        "learning_rules": rules,
        "runtime_confidence_adjustment": runtime_confidence_adjustment,
        "fallback_trust_adjustment": fallback_trust_adjustment,
        "browser_cinematic_penalty": browser_cinematic_penalty,
        "mobile_viability_adjustment": mobile_viability_adjustment,
        "learning_state": "adaptive" if rules else "baseline",
    }


def _mobile_safe_success(summary: Mapping[str, Any], context: Mapping[str, Any]) -> bool:
    source = dict(context.get("selected_source") or {})
    if not bool(source.get("mobile_friendly")):
        return False
    source_characteristics = dict(summary.get("source_characteristics") or {})
    for key, payload in source_characteristics.items():
        descriptor = str(key or "").lower()
        if "x264" in descriptor and "1080" in descriptor and int(dict(payload).get("recovered", 0) or 0) >= 1:
            return True
    return False


def _rule(name: str, confidence_delta: int, evidence_count: int) -> dict[str, Any]:
    return {
        "rule": name,
        "confidence_delta": confidence_delta,
        "evidence_count": int(evidence_count or 0),
        "reasoning": name.replace("_", " "),
    }
