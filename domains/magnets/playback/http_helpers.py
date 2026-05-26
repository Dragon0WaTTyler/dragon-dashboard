from __future__ import annotations

from typing import Any, Mapping


def parse_playback_runtime_request(payload: Mapping[str, Any] | None, *, include_source: bool = False) -> dict[str, Any]:
    data = dict(payload or {})
    result = {
        "movie": data.get("movie") if isinstance(data.get("movie"), dict) else {},
        "sources": data.get("sources") if isinstance(data.get("sources"), list) else [],
        "player_sources": data.get("player_sources") if isinstance(data.get("player_sources"), list) else [],
        "fallback_urls": data.get("fallback_urls") if isinstance(data.get("fallback_urls"), list) else [],
        "requested_profile": str(data.get("runtime_profile") or "").strip(),
    }
    if include_source:
        result["source"] = data.get("source") if isinstance(data.get("source"), dict) else {}
        result["handoff_mode"] = str(data.get("handoff_mode") or "").strip()
    return result


def serialize_playback_runtime(plan: Mapping[str, Any] | None) -> dict[str, Any]:
    payload = dict(plan or {})
    payload["selected_source"] = dict(payload.get("selected_source") or {})
    payload["browser_runtime"] = dict(payload.get("browser_runtime") or {})
    payload["external_runtime"] = dict(payload.get("external_runtime") or {})
    payload["runtime_diagnostics"] = dict(payload.get("runtime_diagnostics") or {})
    payload["runtime_preflight"] = dict(payload.get("runtime_preflight") or {})
    payload["runtime_manifest"] = dict(payload.get("runtime_manifest") or {})
    payload["runtime_events"] = [dict(item) for item in payload.get("runtime_events") or [] if isinstance(item, Mapping)]
    payload["runtime_transport"] = dict(payload.get("runtime_transport") or {})
    payload["browser_runtime_bridge"] = dict(payload.get("browser_runtime_bridge") or {})
    payload["readiness_snapshot"] = dict(payload.get("readiness_snapshot") or {})
    payload["runtime_grade"] = dict(payload.get("runtime_grade") or {})
    payload["runtime_warnings"] = [str(value or "").strip() for value in payload.get("runtime_warnings") or [] if str(value or "").strip()]
    payload["fallbacks"] = [dict(item) for item in payload.get("fallbacks") or [] if isinstance(item, Mapping)]
    payload["profiles"] = [dict(item) for item in payload.get("profiles") or [] if isinstance(item, Mapping)]
    payload["coordination_metrics"] = dict(payload.get("coordination_metrics") or {})
    payload["orchestration_graph"] = dict(payload.get("orchestration_graph") or {})
    payload["runtime_negotiation"] = dict(payload.get("runtime_negotiation") or {})
    payload["adaptive_strategy"] = dict(payload.get("adaptive_strategy") or {})
    payload["runtime_switch_history"] = [dict(item) for item in payload.get("runtime_switch_history") or [] if isinstance(item, Mapping)]
    payload["fallback_negotiation"] = dict(payload.get("fallback_negotiation") or {})
    payload["coordination_events"] = [dict(item) for item in payload.get("coordination_events") or [] if isinstance(item, Mapping)]
    payload["authority_reasoning"] = [str(value or "").strip() for value in payload.get("authority_reasoning") or [] if str(value or "").strip()]
    payload["runtime_risk"] = dict(payload.get("runtime_risk") or {})
    payload["arbitration_result"] = dict(payload.get("arbitration_result") or {})
    payload["arbitration_trace"] = [dict(item) for item in payload.get("arbitration_trace") or [] if isinstance(item, Mapping)]
    payload["governance_actions"] = [str(value or "").strip() for value in payload.get("governance_actions") or [] if str(value or "").strip()]
    payload["stability_state"] = dict(payload.get("stability_state") or {})
    payload["execution_policy"] = dict(payload.get("execution_policy") or {})
    payload["forced_constraints"] = [dict(item) for item in payload.get("forced_constraints") or [] if isinstance(item, Mapping)]
    payload["blocked_paths"] = [str(value or "").strip() for value in payload.get("blocked_paths") or [] if str(value or "").strip()]
    payload["fallback_authority"] = dict(payload.get("fallback_authority") or {})
    payload["confidence_governance"] = dict(payload.get("confidence_governance") or {})
    payload["authority_memory_summary"] = dict(payload.get("authority_memory_summary") or {})
    payload["authority_events"] = [dict(item) for item in payload.get("authority_events") or [] if isinstance(item, Mapping)]
    payload["authority_metrics"] = dict(payload.get("authority_metrics") or {})
    payload["runtime_identity"] = dict(payload.get("runtime_identity") or {})
    payload["behavioral_drift"] = dict(payload.get("behavioral_drift") or {})
    payload["continuity_state"] = dict(payload.get("continuity_state") or {})
    payload["identity_forecast"] = dict(payload.get("identity_forecast") or {})
    payload["identity_metrics"] = dict(payload.get("identity_metrics") or {})
    payload["persistent_traits"] = [str(value or "").strip() for value in payload.get("persistent_traits") or [] if str(value or "").strip()]
    payload["orchestration_traits"] = [str(value or "").strip() for value in payload.get("orchestration_traits") or [] if str(value or "").strip()]
    payload["identity_warnings"] = [str(value or "").strip() for value in payload.get("identity_warnings") or [] if str(value or "").strip()]
    payload["identity_events"] = [dict(item) for item in payload.get("identity_events") or [] if isinstance(item, Mapping)]
    payload["runtime_ecosystem"] = dict(payload.get("runtime_ecosystem") or {})
    payload["ecosystem_balance"] = dict(payload.get("ecosystem_balance") or {})
    payload["orchestration_pressure"] = dict(payload.get("orchestration_pressure") or {})
    payload["runtime_clusters"] = dict(payload.get("runtime_clusters") or {})
    payload["stability_zone"] = dict(payload.get("stability_zone") or {})
    payload["ecosystem_climate"] = dict(payload.get("ecosystem_climate") or {})
    payload["degradation_currents"] = dict(payload.get("degradation_currents") or {})
    payload["resilience_topology"] = dict(payload.get("resilience_topology") or {})
    payload["adaptive_equilibrium"] = dict(payload.get("adaptive_equilibrium") or {})
    payload["ecosystem_forecast"] = dict(payload.get("ecosystem_forecast") or {})
    payload["ecosystem_governance"] = dict(payload.get("ecosystem_governance") or {})
    payload["ecosystem_metrics"] = dict(payload.get("ecosystem_metrics") or {})
    payload["ecosystem_events"] = [dict(item) for item in payload.get("ecosystem_events") or [] if isinstance(item, Mapping)]
    payload["ecosystem_memory"] = dict(payload.get("ecosystem_memory") or {})
    payload["session_payload"] = dict(payload.get("session_payload") or {})
    return payload


def build_playback_response_payload(playback_plan: Mapping[str, Any], *, session: Mapping[str, Any] | None = None) -> dict[str, Any]:
    response = {
        "ok": True,
        "playback": serialize_playback_runtime(playback_plan),
    }
    if session is not None:
        response["session"] = dict(session)
    return response
