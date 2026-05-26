from __future__ import annotations

from typing import Any


EXECUTION_STATES = {
    "idle",
    "bootstrapping",
    "preparing_transport",
    "validating_runtime",
    "startup_pending",
    "startup_degraded",
    "runtime_active",
    "runtime_unstable",
    "runtime_recovering",
    "fallback_transition",
    "runtime_failed",
    "runtime_completed",
}

_TRANSITIONS = {
    "idle": {"bootstrapping", "runtime_failed"},
    "bootstrapping": {"preparing_transport", "startup_degraded", "runtime_failed"},
    "preparing_transport": {"validating_runtime", "startup_degraded", "fallback_transition", "runtime_failed"},
    "validating_runtime": {"startup_pending", "startup_degraded", "fallback_transition", "runtime_failed"},
    "startup_pending": {"runtime_active", "startup_degraded", "fallback_transition", "runtime_failed"},
    "startup_degraded": {"runtime_active", "runtime_unstable", "fallback_transition", "runtime_failed"},
    "runtime_active": {"runtime_unstable", "runtime_completed", "fallback_transition", "runtime_failed"},
    "runtime_unstable": {"runtime_recovering", "fallback_transition", "runtime_failed"},
    "runtime_recovering": {"runtime_active", "runtime_unstable", "fallback_transition", "runtime_failed"},
    "fallback_transition": {"runtime_completed", "runtime_failed"},
    "runtime_failed": {"runtime_completed"},
    "runtime_completed": set(),
}

_FAILURE_SAFE_DOWNGRADE = {
    "bootstrapping": "startup_degraded",
    "preparing_transport": "startup_degraded",
    "validating_runtime": "startup_degraded",
    "startup_pending": "startup_degraded",
    "startup_degraded": "fallback_transition",
    "runtime_active": "runtime_unstable",
    "runtime_unstable": "runtime_recovering",
    "runtime_recovering": "fallback_transition",
    "fallback_transition": "runtime_failed",
    "runtime_failed": "runtime_completed",
}


def normalize_execution_state(value: Any) -> str:
    state = str(value or "").strip().lower()
    if state in EXECUTION_STATES:
        return state
    return "idle"


def can_transition_execution_state(current_state: Any, next_state: Any) -> bool:
    current = normalize_execution_state(current_state)
    target = normalize_execution_state(next_state)
    return target in _TRANSITIONS.get(current, set())


def validate_execution_transition(current_state: Any, next_state: Any) -> dict[str, Any]:
    current = normalize_execution_state(current_state)
    target = normalize_execution_state(next_state)
    safe_target = failure_safe_downgrade_state(current)
    return {
        "current_state": current,
        "next_state": target,
        "allowed": current == target or can_transition_execution_state(current, target),
        "fallback_state": safe_target,
    }


def failure_safe_downgrade_state(current_state: Any) -> str:
    current = normalize_execution_state(current_state)
    return _FAILURE_SAFE_DOWNGRADE.get(current, "runtime_failed")


def evolve_execution_state(current_state: Any, next_state: Any, *, allow_downgrade: bool = True) -> str:
    current = normalize_execution_state(current_state)
    target = normalize_execution_state(next_state)
    if current == target:
        return current
    if can_transition_execution_state(current, target):
        return target
    if allow_downgrade:
        safe_target = failure_safe_downgrade_state(current)
        if can_transition_execution_state(current, safe_target):
            return safe_target
    raise ValueError(f"Invalid execution state transition: {current} -> {target}")
