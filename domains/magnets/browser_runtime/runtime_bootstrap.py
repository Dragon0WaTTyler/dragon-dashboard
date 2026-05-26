from __future__ import annotations

from typing import Any, Mapping


def build_runtime_bootstrap(
    *,
    runtime_manifest: Mapping[str, Any] | None,
    playback_plan: Mapping[str, Any] | None,
    readiness_snapshot: Mapping[str, Any] | None,
    source_metadata: Mapping[str, Any] | None,
    capability_snapshot: Mapping[str, Any] | None = None,
    sandbox: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    manifest = dict(runtime_manifest or {})
    plan = dict(playback_plan or {})
    readiness = dict(readiness_snapshot or {})
    source = dict(source_metadata or {})
    capability = dict(capability_snapshot or {})
    sandbox_result = dict(sandbox or {})
    runtime_mode = str(manifest.get("runtime_mode") or plan.get("runtime_mode") or "").strip()
    fallback_runtime = "external_runtime" if str(readiness.get("fallback_strategy") or "").strip() not in {"", "none"} else "none"
    warnings: list[str] = list(sandbox_result.get("warnings") or [])
    steps = [
        "validate_runtime_manifest",
        "resolve_runtime_source",
        "snapshot_browser_capabilities",
        "evaluate_runtime_sandbox",
        "prepare_runtime_surface" if runtime_mode == "browser_runtime" else "prepare_external_handoff",
    ]
    bootstrap_allowed = bool(sandbox_result.get("sandbox_allowed")) and runtime_mode == "browser_runtime"
    if not source.get("source_valid", False):
        warnings.append("runtime_source_invalid")
    if str(capability.get("startup_viability") or "").strip() == "fragile":
        warnings.append("startup_viability_fragile")
    bootstrap_mode = "browser_sandbox" if bootstrap_allowed else ("external_handoff" if fallback_runtime == "external_runtime" else "blocked")
    return {
        "bootstrap_allowed": bootstrap_allowed,
        "bootstrap_mode": bootstrap_mode,
        "bootstrap_steps": steps,
        "runtime_target": "experimental_browser_runtime" if runtime_mode == "browser_runtime" else "external_runtime",
        "fallback_runtime": fallback_runtime,
        "warnings": _unique_strings(warnings),
    }


def _unique_strings(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return ordered
