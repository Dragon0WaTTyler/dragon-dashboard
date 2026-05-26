from __future__ import annotations

from typing import Any


COORDINATION_STATES = (
    "coordination_pending",
    "runtime_negotiated",
    "adaptation_required",
    "fallback_negotiated",
    "recovery_negotiated",
    "runtime_rebalanced",
    "coordination_failed",
)

_ALLOWED_TRANSITIONS = {
    "coordination_pending": {"runtime_negotiated", "coordination_failed"},
    "runtime_negotiated": {"adaptation_required", "fallback_negotiated", "recovery_negotiated", "runtime_rebalanced", "coordination_failed"},
    "adaptation_required": {"fallback_negotiated", "recovery_negotiated", "runtime_rebalanced", "coordination_failed"},
    "fallback_negotiated": {"recovery_negotiated", "runtime_rebalanced", "coordination_failed"},
    "recovery_negotiated": {"runtime_rebalanced", "coordination_failed"},
    "runtime_rebalanced": {"recovery_negotiated", "coordination_failed"},
    "coordination_failed": set(),
}

_ROLLBACK_PATHS = {
    "runtime_negotiated": ["coordination_pending"],
    "adaptation_required": ["runtime_negotiated", "coordination_pending"],
    "fallback_negotiated": ["adaptation_required", "runtime_negotiated", "coordination_pending"],
    "recovery_negotiated": ["fallback_negotiated", "adaptation_required", "runtime_negotiated", "coordination_pending"],
    "runtime_rebalanced": ["recovery_negotiated", "fallback_negotiated", "runtime_negotiated", "coordination_pending"],
    "coordination_failed": ["recovery_negotiated", "fallback_negotiated", "runtime_negotiated", "coordination_pending"],
}


def normalize_coordination_state(value: Any) -> str:
    state = str(value or "").strip().lower()
    if state in COORDINATION_STATES:
        return state
    return "coordination_pending"


def can_transition_coordination_state(current_state: Any, next_state: Any) -> bool:
    current = normalize_coordination_state(current_state)
    candidate = normalize_coordination_state(next_state)
    return candidate == current or candidate in _ALLOWED_TRANSITIONS.get(current, set())


def rollback_path_for_state(state: Any) -> list[str]:
    return list(_ROLLBACK_PATHS.get(normalize_coordination_state(state), []))


def validate_coordination_transition(current_state: Any, next_state: Any) -> dict[str, Any]:
    current = normalize_coordination_state(current_state)
    candidate = normalize_coordination_state(next_state)
    allowed = can_transition_coordination_state(current, candidate)
    return {
        "current_state": current,
        "requested_state": candidate,
        "allowed": allowed,
        "rollback_path": rollback_path_for_state(candidate if not allowed else current),
        "downgrade_safe": candidate in {"fallback_negotiated", "recovery_negotiated", "runtime_rebalanced", "coordination_failed"},
    }


def evolve_coordination_state(current_state: Any, next_state: Any) -> str:
    validation = validate_coordination_transition(current_state, next_state)
    if validation["allowed"]:
        return validation["requested_state"]
    rollback_path = list(validation.get("rollback_path") or [])
    if rollback_path:
        return rollback_path[0]
    return "coordination_failed"
