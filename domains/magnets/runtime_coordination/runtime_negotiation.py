from __future__ import annotations

from typing import Any, Mapping

from .runtime_priority import compute_runtime_priority, explain_runtime_priority


_RUNTIMES = (
    "browser_runtime",
    "external_runtime",
    "mobile_safe_runtime",
    "cinematic_runtime",
    "degraded_runtime",
)


def negotiate_runtime(
    *,
    capability_snapshot: Mapping[str, Any] | None = None,
    execution_metrics: Mapping[str, Any] | None = None,
    readiness_snapshot: Mapping[str, Any] | None = None,
    runtime_pressure: str = "",
    degradation_risk: int = 0,
) -> dict[str, Any]:
    capability = dict(capability_snapshot or {})
    metrics = dict(execution_metrics or {})
    readiness = dict(readiness_snapshot or {})
    pressure = str(runtime_pressure or "").strip().lower() or "medium"

    priorities = [
        compute_runtime_priority(
            runtime,
            capability_snapshot=capability,
            execution_metrics=metrics,
            readiness_snapshot=readiness,
            runtime_pressure=pressure,
            degradation_risk=degradation_risk,
        )
        for runtime in _RUNTIMES
    ]
    priorities.sort(key=lambda item: int(item.get("score") or 0), reverse=True)

    rejected: list[dict[str, Any]] = []
    eligible: list[dict[str, Any]] = []
    for priority in priorities:
        runtime = str(priority.get("runtime") or "")
        score = int(priority.get("score") or 0)
        reasons = explain_runtime_priority(priority)
        hard_reject = (
            (runtime == "browser_runtime" and str(capability.get("browser_safety_class") or "") == "unsafe")
            or (runtime == "mobile_safe_runtime" and str(capability.get("mobile_runtime_risk") or "") == "high")
            or (runtime == "cinematic_runtime" and pressure == "high")
        )
        if hard_reject or score < 35:
            rejected.append({"runtime": runtime, "reason": reasons[0] if reasons else "runtime rejected"})
            continue
        eligible.append({"runtime": runtime, "score": score, "reasoning": reasons})

    selected = eligible[0] if eligible else {"runtime": "degraded_runtime", "score": 18, "reasoning": ["No stable runtime met the negotiation threshold."]}
    fallback = "external_runtime"
    if selected["runtime"] == "external_runtime":
        fallback = next((item["runtime"] for item in eligible if item["runtime"] != "external_runtime"), "degraded_runtime")
    elif not any(item["runtime"] == "external_runtime" for item in eligible):
        fallback = next((item["runtime"] for item in eligible if item["runtime"] != selected["runtime"]), "degraded_runtime")
    negotiation_reason = selected["reasoning"][0] if selected.get("reasoning") else "Runtime negotiated from deterministic priority scoring."
    return {
        "selected_runtime": selected["runtime"],
        "negotiation_reason": negotiation_reason,
        "rejected_runtimes": rejected,
        "fallback_runtime": fallback,
        "runtime_candidates": eligible,
    }
