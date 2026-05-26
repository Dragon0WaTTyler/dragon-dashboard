from __future__ import annotations

from typing import Any, Mapping

from ..browser_runtime import build_browser_runtime_bridge
from ..execution_runtime import simulate_execution_runtime
from ..runtime_authority import build_runtime_authority
from ..runtime_cinema import build_runtime_cinema
from ..runtime_consciousness import build_runtime_consciousness
from ..runtime_ecosystem import build_runtime_ecosystem
from ..runtime_identity import build_runtime_identity
from ..runtime_instinct import build_runtime_instinct
from ..runtime_intelligence import build_runtime_intelligence
from ..runtime_subconscious import build_runtime_subconscious
from ..runtime_dreaming import build_runtime_dreaming
from ..runtime_federation import build_runtime_federation
from ..runtime_resonance import build_runtime_resonance
from ..runtime_symbiosis import build_runtime_symbiosis
from ..runtime_temporal import build_runtime_temporal
from ..runtime_coordination import coordinate_runtime
from ..runtime.observability import emit_event
from ..stream_runtime import (
    build_runtime_event,
    build_runtime_manifest,
    build_runtime_preflight,
    determine_runtime_transport,
    evolve_runtime_state,
)
from .capability_matrix import evaluate_capability_matrix
from .browser_runtime import prepare_browser_runtime
from .external_runtime import build_external_runtime
from .playback_session import build_playback_session_payload
from .readiness_snapshot import build_playback_readiness_snapshot
from .runtime_diagnostics import build_runtime_diagnostics
from .runtime_fallbacks import build_runtime_fallbacks
from .runtime_profile import evaluate_runtime_profile, recommend_runtime_profile
from .source_selector import select_playback_candidates


def prepare_playback_runtime(
    *,
    movie: Mapping[str, Any] | None = None,
    sources: list[Mapping[str, Any]] | None = None,
    selected_source: Mapping[str, Any] | None = None,
    requested_profile: str = "",
    player_sources: list[Mapping[str, Any]] | None = None,
    fallback_urls: list[str] | None = None,
) -> dict[str, Any]:
    movie_data = dict(movie or {})
    source_list = [dict(item) for item in (sources or []) if isinstance(item, Mapping)]
    if selected_source and isinstance(selected_source, Mapping):
        source = dict(selected_source)
        if "browser_playable_candidate" not in source or "runtime_profile" not in source:
            decorated = select_playback_candidates([source], movie=movie_data)
            source = dict(decorated.get("selected_source") or source)
    else:
        selection = select_playback_candidates(source_list, movie=movie_data)
        source = dict(selection.get("selected_source") or {})
        source_list = list(selection.get("sources") or source_list)

    compatibility = evaluate_capability_matrix(source)
    profile_set = recommend_runtime_profile(source, compatibility=compatibility)
    resolved_profile = _resolve_profile(
        requested_profile=requested_profile,
        default_profile=dict(profile_set.get("recommended") or {}),
        source=source,
        compatibility=compatibility,
    )
    browser_runtime = prepare_browser_runtime(
        source,
        profile=resolved_profile,
        compatibility=compatibility,
        diagnostics={
            "magnet_valid": bool(compatibility.get("magnet_valid")),
            "warnings": list(compatibility.get("notes") or []),
        },
        fallback_urls=fallback_urls,
        player_sources=player_sources,
    )
    external_runtime = build_external_runtime(
        source,
        profile=resolved_profile,
        compatibility=compatibility,
        diagnostics={
            "magnet_valid": bool(compatibility.get("magnet_valid")),
            "warnings": list(compatibility.get("notes") or []),
        },
    )
    playback_runtime = "browser_runtime" if browser_runtime.get("browser_viable") else "external_runtime"
    readiness = str(
        browser_runtime.get("readiness") if playback_runtime == "browser_runtime" else external_runtime.get("readiness")
    )
    confidence = str(
        browser_runtime.get("startup_confidence") if playback_runtime == "browser_runtime" else external_runtime.get("startup_confidence")
    )
    warnings = list(browser_runtime.get("warnings") or []) + list(external_runtime.get("warnings") or [])
    fallbacks = build_runtime_fallbacks(
        source,
        browser_runtime=browser_runtime,
        external_runtime=external_runtime,
    )
    runtime_diagnostics = build_runtime_diagnostics(
        source=source,
        capability=compatibility,
        playback_runtime=playback_runtime,
        runtime_profile=resolved_profile,
        penalties=list(source.get("playback_warnings") or []),
        browser_runtime=browser_runtime,
        external_runtime=external_runtime,
    )
    runtime_preflight = build_runtime_preflight(
        source=source,
        capability_snapshot=compatibility,
        runtime_mode=playback_runtime,
        runtime_profile=str(resolved_profile.get("id") or "external_player_only"),
        startup_confidence=confidence,
        player_sources=player_sources,
        fallback_urls=fallback_urls,
        fallbacks=fallbacks,
    )
    runtime_state = _resolve_runtime_state(runtime_preflight)
    runtime_transport = determine_runtime_transport(
        runtime_mode=str(runtime_preflight.get("runtime_mode") or playback_runtime),
        browser_runtime=browser_runtime,
        external_runtime=external_runtime,
    )
    runtime_manifest = build_runtime_manifest(
        selected_source=source,
        runtime_mode=str(runtime_preflight.get("runtime_mode") or playback_runtime),
        runtime_state=runtime_state,
        startup_confidence=confidence,
        capability_snapshot=compatibility,
        diagnostics=runtime_diagnostics,
        fallbacks=fallbacks,
        preflight=runtime_preflight,
        transport=runtime_transport,
    )
    runtime_events = [
        build_runtime_event(
            "runtime-preflight-passed" if runtime_preflight.get("runtime_allowed") else "runtime-preflight-blocked",
            runtime_state=runtime_state,
            runtime_mode=str(runtime_preflight.get("runtime_mode") or playback_runtime),
            details={
                "blocking_reasons": list(runtime_preflight.get("blocking_reasons") or []),
                "fallback_strategy": str(runtime_preflight.get("fallback_strategy") or "").strip(),
            },
        )
    ]
    if str(runtime_preflight.get("fallback_strategy") or "").strip() not in {"", "none"}:
        runtime_events.append(
            build_runtime_event(
                "runtime-fallback-selected",
                runtime_state=runtime_state,
                runtime_mode=str(runtime_preflight.get("runtime_mode") or playback_runtime),
                details={"fallback_strategy": str(runtime_preflight.get("fallback_strategy") or "").strip()},
            )
        )

    emit_event(
        "[playback-runtime]",
        movie=_movie_name(movie_data or source),
        runtime=playback_runtime,
        profile=str(resolved_profile.get("id") or "external_player_only"),
        readiness=readiness,
        confidence=confidence,
    )
    plan = {
        "selected_source": source,
        "playback_runtime": playback_runtime,
        "runtime_profile": str(resolved_profile.get("id") or "external_player_only"),
        "runtime_profile_label": str(resolved_profile.get("label") or "External Player Only"),
        "playback_readiness": readiness,
        "startup_confidence": confidence,
        "runtime_warnings": _unique_strings(warnings),
        "browser_runtime": browser_runtime,
        "external_runtime": external_runtime,
        "fallbacks": fallbacks,
        "profiles": list(profile_set.get("profiles") or []),
        "runtime_diagnostics": runtime_diagnostics,
        "runtime_preflight": runtime_preflight,
        "runtime_state": runtime_state,
        "runtime_manifest": runtime_manifest,
        "runtime_events": runtime_events,
        "runtime_mode": str(runtime_preflight.get("runtime_mode") or playback_runtime),
        "runtime_transport": runtime_transport,
    }
    plan["readiness_meter"] = _readiness_meter(plan)
    plan["readiness_snapshot"] = build_playback_readiness_snapshot(plan)
    plan["browser_runtime_bridge"] = build_browser_runtime_bridge(
        runtime_manifest=runtime_manifest,
        playback_plan=plan,
        readiness_snapshot=plan["readiness_snapshot"],
        source_metadata=source,
    )
    execution_runtime = simulate_execution_runtime(
        capability_snapshot=dict(plan["browser_runtime_bridge"].get("capability_snapshot") or compatibility),
        playback_readiness=str(plan.get("playback_readiness") or ""),
        source_metadata=source,
        runtime_manifest=runtime_manifest,
        bootstrap_plan=dict(plan["browser_runtime_bridge"].get("bootstrap") or {}),
        readiness_snapshot=plan["readiness_snapshot"],
    )
    plan["browser_runtime_bridge"]["execution_runtime"] = dict(execution_runtime)
    plan["execution_state"] = str(execution_runtime.get("execution_state") or "")
    plan["execution_metrics"] = dict(execution_runtime.get("execution_metrics") or {})
    plan["execution_timeline"] = dict(execution_runtime.get("execution_timeline") or {})
    plan["simulated_runtime_health"] = str(execution_runtime.get("simulated_runtime_health") or "")
    plan["recovery_path"] = dict(execution_runtime.get("recovery_path") or {})
    plan["execution_events"] = list(execution_runtime.get("execution_events") or [])
    plan["runtime_grade"] = dict(execution_runtime.get("runtime_grade") or {})
    plan["execution_failures"] = list(execution_runtime.get("execution_failures") or [])
    plan["execution_intelligence_signals"] = dict(execution_runtime.get("intelligence_signals") or {})
    plan["readiness_snapshot"] = build_playback_readiness_snapshot(plan)
    coordination = coordinate_runtime(
        capability_snapshot=dict(plan["browser_runtime_bridge"].get("capability_snapshot") or compatibility),
        execution_metrics=dict(plan.get("execution_metrics") or {}),
        readiness_snapshot=dict(plan.get("readiness_snapshot") or {}),
        runtime_pressure=str((execution_runtime.get("transport_descriptor") or {}).get("runtime_pressure") or ""),
    )
    plan.update(dict(coordination.get("persistence") or {}))
    plan["browser_runtime_bridge"]["coordination"] = {
        "coordination_state": str(plan.get("coordination_state") or ""),
        "runtime_negotiation": dict(plan.get("runtime_negotiation") or {}),
        "coordination_metrics": dict(plan.get("coordination_metrics") or {}),
    }
    plan["coordination_intelligence_signals"] = dict(coordination.get("intelligence_signals") or {})
    plan["readiness_snapshot"] = build_playback_readiness_snapshot(plan)
    intelligence = build_runtime_intelligence(
        {
            **plan,
            "capability_snapshot": dict(plan["browser_runtime_bridge"].get("capability_snapshot") or compatibility),
            "transport_descriptor": dict(execution_runtime.get("transport_descriptor") or {}),
            "guardrails": dict(execution_runtime.get("guardrails") or {}),
            "execution_outcome": str(execution_runtime.get("execution_outcome") or ""),
        }
    )
    plan.update(intelligence)
    authority = build_runtime_authority(
        {
            **plan,
            "capability_snapshot": dict(plan["browser_runtime_bridge"].get("capability_snapshot") or compatibility),
        }
    )
    plan.update(authority)
    identity = build_runtime_identity(
        {
            **plan,
            "capability_snapshot": dict(plan["browser_runtime_bridge"].get("capability_snapshot") or compatibility),
        }
    )
    plan.update(identity)
    ecosystem = build_runtime_ecosystem(
        {
            **plan,
            "capability_snapshot": dict(plan["browser_runtime_bridge"].get("capability_snapshot") or compatibility),
        }
    )
    plan.update(ecosystem)
    cinema = build_runtime_cinema(
        {
            **plan,
            "capability_snapshot": dict(plan["browser_runtime_bridge"].get("capability_snapshot") or compatibility),
        }
    )
    plan.update(cinema)
    consciousness = build_runtime_consciousness(
        {
            **plan,
            "capability_snapshot": dict(plan["browser_runtime_bridge"].get("capability_snapshot") or compatibility),
        }
    )
    plan.update(consciousness)
    instinct = build_runtime_instinct(
        {
            **plan,
            "capability_snapshot": dict(plan["browser_runtime_bridge"].get("capability_snapshot") or compatibility),
        }
    )
    plan.update(instinct)
    subconscious = build_runtime_subconscious(
        {
            **plan,
            "capability_snapshot": dict(plan["browser_runtime_bridge"].get("capability_snapshot") or compatibility),
        }
    )
    plan.update(subconscious)
    dreaming = build_runtime_dreaming(
        {
            **plan,
            "capability_snapshot": dict(plan["browser_runtime_bridge"].get("capability_snapshot") or compatibility),
        }
    )
    plan.update(dreaming)
    federation = build_runtime_federation(
        {
            **plan,
            "capability_snapshot": dict(plan["browser_runtime_bridge"].get("capability_snapshot") or compatibility),
        }
    )
    plan.update(federation)
    temporal = build_runtime_temporal(
        {
            **plan,
            "capability_snapshot": dict(plan["browser_runtime_bridge"].get("capability_snapshot") or compatibility),
        }
    )
    plan.update(temporal)
    resonance = build_runtime_resonance(
        {
            **plan,
            "capability_snapshot": dict(plan["browser_runtime_bridge"].get("capability_snapshot") or compatibility),
        }
    )
    plan.update(resonance)
    symbiosis = build_runtime_symbiosis(
        {
            **plan,
            "capability_snapshot": dict(plan["browser_runtime_bridge"].get("capability_snapshot") or compatibility),
        }
    )
    plan.update(symbiosis)
    original_runtime = str(plan.get("playback_runtime") or "")
    approved_runtime = str(authority.get("approved_runtime") or original_runtime)
    if approved_runtime and approved_runtime != original_runtime:
        plan["governance_override"] = {
            "from_runtime": original_runtime,
            "to_runtime": approved_runtime,
            "reason": list(authority.get("governance_actions") or []) or list(authority.get("authority_reasoning") or []),
        }
        plan["playback_runtime"] = approved_runtime
        plan["runtime_mode"] = approved_runtime
        if approved_runtime == "external_runtime":
            plan["playback_readiness"] = "external_recommended"
        elif approved_runtime == "browser_runtime" and str(plan.get("playback_readiness") or "").startswith("external"):
            plan["playback_readiness"] = "browser_ready"
    plan["readiness_snapshot"] = build_playback_readiness_snapshot(plan)
    plan["session_payload"] = build_playback_session_payload(plan)
    return plan


def _resolve_profile(
    *,
    requested_profile: str,
    default_profile: Mapping[str, Any],
    source: Mapping[str, Any],
    compatibility: Mapping[str, Any],
) -> dict[str, Any]:
    requested = str(requested_profile or "").strip()
    if requested:
        return evaluate_runtime_profile(source, requested, compatibility=compatibility)
    return dict(default_profile or evaluate_runtime_profile(source, "external_player_only", compatibility=compatibility))


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


def _movie_name(movie: Mapping[str, Any]) -> str:
    return str(movie.get("title") or movie.get("name") or "").strip() or "unknown"


def _readiness_meter(plan: Mapping[str, Any]) -> int:
    confidence = str(plan.get("startup_confidence") or "low")
    runtime = str(plan.get("playback_runtime") or "")
    if confidence == "high" and runtime == "browser_runtime":
        return 92
    if confidence == "medium" and runtime == "browser_runtime":
        return 74
    if runtime == "external_runtime":
        return 56 if confidence != "low" else 38
    return 28


def _resolve_runtime_state(preflight: Mapping[str, Any]) -> str:
    current_state = evolve_runtime_state("idle", "preflight")
    runtime_mode = str(preflight.get("runtime_mode") or "").strip()
    if runtime_mode == "blocked":
        return evolve_runtime_state(current_state, "runtime_blocked")
    if runtime_mode == "external_runtime":
        return evolve_runtime_state(current_state, "external_only")
    if preflight.get("runtime_allowed"):
        return evolve_runtime_state(current_state, "runtime_ready")
    return evolve_runtime_state(current_state, "runtime_limited")
