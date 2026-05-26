from __future__ import annotations

from typing import Any


def build_federation_metrics(
    *,
    coherence: int,
    harmony: int,
    pressure: int,
    integrity: int,
    resilience: int,
    alignment: int,
    divergence: int,
    convergence_count: int,
    divergence_count: int,
    adaptive_balance: int,
) -> dict[str, Any]:
    return {
        "federation_coherence": coherence,
        "federation_harmony": harmony,
        "federation_pressure": pressure,
        "federation_integrity": integrity,
        "federation_resilience": resilience,
        "federation_alignment": alignment,
        "federation_divergence": divergence,
        "convergence_count": convergence_count,
        "divergence_count": divergence_count,
        "adaptive_federation_balance": adaptive_balance,
    }
