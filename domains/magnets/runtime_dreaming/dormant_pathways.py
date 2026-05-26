from __future__ import annotations

from typing import Any

from .dream_metrics import clamp


def build_dormant_pathways(
    *,
    dormant_resilience: str = "",
    continuity_dream: str = "",
    cinematic_dream: str = "",
    latent_projection: str = "",
) -> dict[str, Any]:
    strength = clamp(
        (18 if dormant_resilience in {"dormant_resilient", "dormant_balanced"} else 10)
        + (16 if continuity_dream in {"continuity_preservation", "continuity_balance"} else 8)
        + (16 if cinematic_dream in {"immersive_dream", "resilient_cinema_dream", "stabilized_cinema_dream"} else 8)
        + (16 if latent_projection in {"latent_resilience_projection", "latent_stability_projection"} else 8)
        + 24
    )
    if cinematic_dream in {"immersive_dream", "resilient_cinema_dream", "stabilized_cinema_dream"}:
        state = "dormant_cinematic_path"
    elif continuity_dream == "continuity_recovery":
        state = "dormant_recovery_path"
    elif dormant_resilience in {"dormant_resilient", "dormant_balanced"}:
        state = "dormant_resilience_path"
    elif latent_projection == "latent_stability_projection":
        state = "dormant_stability_path"
    else:
        state = "dormant_adaptation_path"
    return {"state": state, "dormant_pathway_strength": strength}
