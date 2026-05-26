from __future__ import annotations

from typing import Any


def build_subconscious_events(
    *,
    latent_pattern: str = "",
    pressure_score: int = 0,
    dormant_resilience_state: str = "",
    residue_pattern: str = "",
    cinematic_underflow_state: str = "",
    hidden_equilibrium_state: str = "",
    previous_latent_pattern: str = "",
    previous_hidden_equilibrium: str = "",
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    if previous_latent_pattern and previous_latent_pattern != latent_pattern:
        events.append({"event": "latent_pattern_shift_detected", "from": previous_latent_pattern, "to": latent_pattern})
    if pressure_score >= 72:
        events.append({"event": "subconscious_pressure_elevated", "pressure_score": pressure_score})
    if dormant_resilience_state in {"dormant_recovering", "dormant_balanced"}:
        events.append({"event": "dormant_resilience_recovered", "dormant_resilience": dormant_resilience_state})
    if residue_pattern in {"degradation_residue", "fallback_residue"}:
        events.append({"event": "orchestration_residue_fragmented", "residue_pattern": residue_pattern})
    if cinematic_underflow_state in {"cinematic_underflow_stable", "cinematic_underflow_resilient"}:
        events.append({"event": "cinematic_underflow_stabilized", "cinematic_underflow": cinematic_underflow_state})
    if hidden_equilibrium_state in {"hidden_balance", "hidden_resilience"} and previous_hidden_equilibrium and previous_hidden_equilibrium != hidden_equilibrium_state:
        events.append({"event": "hidden_equilibrium_preserved", "hidden_equilibrium": hidden_equilibrium_state})
    return events
