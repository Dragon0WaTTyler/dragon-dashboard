from __future__ import annotations

from typing import Any


def build_dream_events(
    *,
    cinematic_dream: str = "",
    latent_projection: str = "",
    dormant_pathway: str = "",
    adaptive_dreaming: str = "",
    runtime_mirroring: str = "",
    dream_balance: int = 0,
    previous_cinematic_dream: str = "",
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    if previous_cinematic_dream and previous_cinematic_dream != cinematic_dream:
        events.append({"event": "cinematic_dream_shift_detected", "from": previous_cinematic_dream, "to": cinematic_dream})
    if latent_projection == "latent_fragmentation_projection":
        events.append({"event": "latent_projection_fragmented", "latent_projection": latent_projection})
    if dormant_pathway in {"dormant_recovery_path", "dormant_resilience_path", "dormant_cinematic_path"}:
        events.append({"event": "dormant_pathway_awakened", "dormant_pathway": dormant_pathway})
    if adaptive_dreaming in {"adaptive_equilibrium", "adaptive_resilience", "adaptive_cinematic_preservation"}:
        events.append({"event": "adaptive_dreaming_stabilized", "adaptive_dreaming": adaptive_dreaming})
    if runtime_mirroring in {"stabilized_mirroring", "resilient_mirroring", "cinematic_mirroring", "continuity_mirroring"}:
        events.append({"event": "runtime_mirroring_preserved", "runtime_mirroring": runtime_mirroring})
    if dream_balance >= 76:
        events.append({"event": "orchestration_dream_converged", "dream_balance": dream_balance})
    return events
