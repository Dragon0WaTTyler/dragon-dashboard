from __future__ import annotations

from typing import Any, Mapping


def classify_execution_transport(
    *,
    runtime_manifest: Mapping[str, Any] | None = None,
    capability_snapshot: Mapping[str, Any] | None = None,
    bootstrap_plan: Mapping[str, Any] | None = None,
    source_metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    manifest = dict(runtime_manifest or {})
    capability = dict(capability_snapshot or {})
    bootstrap = dict(bootstrap_plan or {})
    source = dict(source_metadata or {})

    runtime_mode = str(manifest.get("runtime_mode") or "")
    bootstrap_mode = str(bootstrap.get("bootstrap_mode") or "")
    startup_viability = str(capability.get("startup_viability") or "")
    memory_risk = str(capability.get("memory_risk") or "")
    mobile_risk = str(capability.get("mobile_runtime_risk") or "")

    transport_class = "unsupported_transport"
    startup_behavior = "blocked"
    runtime_pressure = "critical"
    degradation_likelihood = "high"

    if runtime_mode == "external_runtime" or bootstrap_mode == "external_handoff":
        transport_class = "external_handoff"
        startup_behavior = "delegated"
        runtime_pressure = "low"
        degradation_likelihood = "low"
    elif runtime_mode == "browser_runtime":
        if mobile_risk == "high" and not bool(source.get("mobile_friendly", True)):
            transport_class = "mobile_limited"
            startup_behavior = "constrained"
            runtime_pressure = "medium"
            degradation_likelihood = "high"
        elif startup_viability == "viable" and memory_risk in {"low", "unknown"}:
            transport_class = "browser_progressive"
            startup_behavior = "progressive"
            runtime_pressure = "low"
            degradation_likelihood = "low"
        else:
            transport_class = "browser_heavy"
            startup_behavior = "buffer_sensitive"
            runtime_pressure = "high"
            degradation_likelihood = "medium"

    return {
        "transport_class": transport_class,
        "runtime_mode": runtime_mode or "external_runtime",
        "bootstrap_mode": bootstrap_mode or "blocked",
        "startup_behavior": startup_behavior,
        "runtime_pressure": runtime_pressure,
        "degradation_likelihood": degradation_likelihood,
    }
