from __future__ import annotations

from typing import Any, Mapping


def build_coordination_persistence_payload(coordination: Mapping[str, Any] | None) -> dict[str, Any]:
    payload = dict(coordination or {})
    return {
        "coordination_state": str(payload.get("coordination_state") or "").strip(),
        "coordination_metrics": dict(payload.get("coordination_metrics") or {}),
        "orchestration_graph": dict(payload.get("orchestration_graph") or {}),
        "runtime_negotiation": dict(payload.get("runtime_negotiation") or {}),
        "adaptive_strategy": dict(payload.get("adaptive_strategy") or {}),
        "runtime_switch_history": [dict(item) for item in payload.get("runtime_switch_history") or [] if isinstance(item, Mapping)],
        "fallback_negotiation": dict(payload.get("fallback_negotiation") or {}),
        "coordination_events": [dict(item) for item in payload.get("coordination_events") or [] if isinstance(item, Mapping)],
    }
