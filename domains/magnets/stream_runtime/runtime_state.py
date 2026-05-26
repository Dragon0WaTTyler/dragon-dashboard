from __future__ import annotations

from typing import Any


RUNTIME_STATES = {
    "idle",
    "preflight",
    "runtime_ready",
    "handoff_ready",
    "runtime_limited",
    "runtime_blocked",
    "external_only",
    "failed",
    "expired",
}

_TRANSITIONS = {
    "idle": {"preflight", "runtime_blocked", "external_only", "failed", "expired"},
    "preflight": {"runtime_ready", "runtime_limited", "runtime_blocked", "external_only", "failed", "expired"},
    "runtime_ready": {"handoff_ready", "runtime_limited", "runtime_blocked", "failed", "expired"},
    "handoff_ready": {"runtime_limited", "failed", "expired"},
    "runtime_limited": {"handoff_ready", "runtime_blocked", "external_only", "failed", "expired"},
    "runtime_blocked": {"external_only", "failed", "expired"},
    "external_only": {"handoff_ready", "failed", "expired"},
    "failed": {"expired"},
    "expired": set(),
}


def normalize_runtime_state(value: Any) -> str:
    state = str(value or "").strip().lower()
    if state in RUNTIME_STATES:
        return state
    return "idle"


def can_transition_runtime_state(current_state: Any, next_state: Any) -> bool:
    current = normalize_runtime_state(current_state)
    target = normalize_runtime_state(next_state)
    return target in _TRANSITIONS.get(current, set())


def evolve_runtime_state(current_state: Any, next_state: Any) -> str:
    current = normalize_runtime_state(current_state)
    target = normalize_runtime_state(next_state)
    if current == target:
        return current
    if not can_transition_runtime_state(current, target):
        raise ValueError(f"Invalid runtime state transition: {current} -> {target}")
    return target
