from __future__ import annotations

from typing import Any


def build_silent_adaptation(
    *,
    switch_frequency: int = 0,
    adaptation_pressure: int = 0,
    latent_pattern: str = "",
    hidden_equilibrium_state: str = "",
) -> dict[str, Any]:
    if hidden_equilibrium_state == "hidden_fragmentation" or switch_frequency >= 4:
        state = "silent_fragmentation"
    elif "recover" in latent_pattern:
        state = "silent_recovery"
    elif hidden_equilibrium_state in {"hidden_resilience", "hidden_balance"} and adaptation_pressure <= 28:
        state = "silent_stabilization"
    elif adaptation_pressure <= 42:
        state = "silent_resilience"
    else:
        state = "silent_equilibrium"
    return {
        "state": state,
        "adaptation_trace": "deterministic_silent_adaptation",
    }
