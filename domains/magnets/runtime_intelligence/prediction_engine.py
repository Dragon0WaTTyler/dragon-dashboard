from __future__ import annotations

from typing import Any, Mapping


def predict_runtime_outcome(
    *,
    execution_metrics: Mapping[str, Any] | None = None,
    coordination_metrics: Mapping[str, Any] | None = None,
    runtime_history: Mapping[str, Any] | None = None,
    capability_snapshot: Mapping[str, Any] | None = None,
    readiness_snapshot: Mapping[str, Any] | None = None,
    runtime_learning: Mapping[str, Any] | None = None,
    runtime_reputation: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    execution = dict(execution_metrics or {})
    coordination = dict(coordination_metrics or {})
    history = dict(runtime_history or {})
    capability = dict(capability_snapshot or {})
    readiness = dict(readiness_snapshot or {})
    learning = dict(runtime_learning or {})
    reputation = dict(runtime_reputation or {})
    profile = str(readiness.get("runtime_profile") or "unknown").strip()
    runtime = str(readiness.get("playback_runtime") or readiness.get("runtime_mode") or "unknown").strip()
    profile_reputation = dict((reputation.get("runtime_profiles") or {}).get(profile) or {})

    reasons: list[str] = []
    score = 50
    if bool(capability.get("rejected")) or str(capability.get("browser_safety_class") or "") == "unsafe":
        score -= 24
        reasons.append("browser guardrails indicate rejection risk")
    if int(execution.get("startup_score", 0) or 0) < 55:
        score -= 16
        reasons.append("startup score is below safe runtime threshold")
    if int(execution.get("stability_score", 0) or 0) < 60:
        score -= 12
        reasons.append("stability score indicates elevated runtime instability")
    if float(history.get("fallback_frequency", 0) or 0) >= 0.45:
        score -= 10
        reasons.append("runtime history shows frequent fallback escalation")
    if float(history.get("browser_rejection_trend", 0) or 0) >= 0.2:
        score -= 8
        reasons.append("browser rejection trend is rising")
    score += int(learning.get("runtime_confidence_adjustment", 0) or 0)
    score += int(learning.get("mobile_viability_adjustment", 0) or 0)
    score += int(profile_reputation.get("orchestration_trust", 0) or 0) // 10
    score = max(0, min(100, score))

    predicted_outcome = "likely_stable_browser_runtime"
    if str(capability.get("mobile_runtime_risk") or "") == "high" and not bool(dict(readiness.get("selected_source") or {}).get("mobile_friendly")):
        predicted_outcome = "likely_mobile_instability"
        reasons.append("mobile runtime risk is high for the current source")
    elif runtime == "external_runtime":
        predicted_outcome = "likely_external_fallback"
    elif score < 35:
        predicted_outcome = "likely_runtime_failure"
    elif score < 48:
        predicted_outcome = "likely_external_fallback"
    elif int(execution.get("runtime_confidence", 0) or 0) < 62 and int(coordination.get("runtime_resilience", 0) or 0) >= 70:
        predicted_outcome = "likely_runtime_recovery"
        reasons.append("recovery resilience is stronger than startup confidence")

    if not reasons:
        reasons.append("runtime history and execution metrics remain within stable thresholds")
    return {
        "predicted_outcome": predicted_outcome,
        "prediction_confidence": score,
        "prediction_reasoning": reasons,
        "prediction_basis": {
            "runtime_profile": profile,
            "playback_runtime": runtime,
            "history_observations": int(history.get("total_observations", 0) or 0),
        },
    }
