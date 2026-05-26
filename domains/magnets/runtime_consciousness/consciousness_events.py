from __future__ import annotations


def build_consciousness_events(
    *,
    awareness_state: str,
    focus: str,
    cognitive_balance: str,
    continuity_awareness: str,
    runtime_presence: str,
    orchestration_clarity: int,
    previous_awareness_state: str = "",
    previous_focus: str = "",
    previous_clarity: int = 0,
) -> list[dict[str, str | int]]:
    events: list[dict[str, str | int]] = []
    if previous_awareness_state and previous_awareness_state != awareness_state:
        events.append({"event": "awareness_shift_detected", "from": previous_awareness_state, "to": awareness_state})
    if previous_focus and previous_focus != focus:
        events.append({"event": "orchestration_focus_changed", "from": previous_focus, "to": focus})
    if cognitive_balance == "fragmented_cognition":
        events.append({"event": "cognitive_balance_fragmented", "state": cognitive_balance})
    if continuity_awareness in {"preserved_awareness", "resilient_awareness"}:
        events.append({"event": "continuity_awareness_stabilized", "state": continuity_awareness})
    if runtime_presence in {"strong_presence", "resilient_presence", "cinematic_presence"}:
        events.append({"event": "runtime_presence_elevated", "state": runtime_presence})
    if orchestration_clarity > previous_clarity and orchestration_clarity >= 70:
        events.append({"event": "orchestration_clarity_recovered", "score": orchestration_clarity})
    return events
