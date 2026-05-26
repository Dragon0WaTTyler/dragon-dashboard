from __future__ import annotations

from typing import Any, Mapping

from .runtime_bootstrap import build_runtime_bootstrap
from .runtime_capabilities import build_capability_snapshot
from .runtime_fallbacks import build_browser_runtime_fallbacks
from .runtime_player import build_runtime_player_descriptor
from .runtime_sandbox import evaluate_runtime_sandbox
from .runtime_sources import normalize_runtime_source


def build_browser_runtime_bridge(
    *,
    runtime_manifest: Mapping[str, Any] | None,
    playback_plan: Mapping[str, Any] | None,
    readiness_snapshot: Mapping[str, Any] | None,
    source_metadata: Mapping[str, Any] | None,
) -> dict[str, Any]:
    manifest = dict(runtime_manifest or {})
    plan = dict(playback_plan or {})
    readiness = dict(readiness_snapshot or {})
    source_descriptor = normalize_runtime_source(source_metadata)
    capability_snapshot = build_capability_snapshot(
        source_descriptor,
        runtime_manifest=manifest,
        readiness_snapshot=readiness,
    )
    sandbox = evaluate_runtime_sandbox(
        runtime_manifest=manifest,
        source_descriptor=source_descriptor,
        capability_snapshot=capability_snapshot,
    )
    fallbacks = build_browser_runtime_fallbacks(runtime_manifest=manifest, playback_plan=plan)
    bootstrap = build_runtime_bootstrap(
        runtime_manifest=manifest,
        playback_plan=plan,
        readiness_snapshot=readiness,
        source_metadata=source_descriptor,
        capability_snapshot=capability_snapshot,
        sandbox=sandbox,
    )
    player_descriptor = build_runtime_player_descriptor(
        runtime_manifest=manifest,
        capability_snapshot=capability_snapshot,
        bootstrap_mode=str(bootstrap.get("bootstrap_mode") or ""),
        fallback_available=bool(fallbacks.get("fallback_available")),
    )
    return {
        "experimental": True,
        "bridge_allowed": bool(bootstrap.get("bootstrap_allowed")),
        "bridge_mode": str(bootstrap.get("bootstrap_mode") or "").strip(),
        "runtime_handoff": {
            "runtime_id": str(manifest.get("runtime_id") or "").strip(),
            "runtime_mode": str(manifest.get("runtime_mode") or "").strip(),
            "runtime_state": str(manifest.get("runtime_state") or "").strip(),
        },
        "source_descriptor": source_descriptor,
        "capability_snapshot": capability_snapshot,
        "sandbox": sandbox,
        "bootstrap": bootstrap,
        "player_descriptor": player_descriptor,
        "fallbacks": fallbacks,
        "execution_runtime": {},
        "coordination": {},
        "runtime_payload": {
            "runtime_target": str(bootstrap.get("runtime_target") or "").strip(),
            "transport": dict(manifest.get("transport") or {}),
            "selected_source": dict(manifest.get("selected_source") or {}),
        },
    }
