from __future__ import annotations

from typing import Any


def build_symbiosis_balance(
    *,
    federation_coherence: int = 0,
    federation_alignment: int = 0,
    resonance_alignment: int = 0,
    temporal_alignment: int = 0,
    ecosystem_integrity: int = 0,
    pressure: int = 0,
    fragmentation: int = 0,
) -> dict[str, Any]:
    balance_score = _clamp(
        int(
            round(
                (federation_coherence * 0.2)
                + (federation_alignment * 0.16)
                + (resonance_alignment * 0.18)
                + (temporal_alignment * 0.18)
                + (ecosystem_integrity * 0.18)
                + ((100 - pressure) * 0.06)
                + ((100 - fragmentation) * 0.04)
            )
        )
    )
    if fragmentation >= 68 or pressure >= 72:
        balance_state = "fractured_balance"
    elif balance_score >= 72:
        balance_state = "balanced_mutualism"
    elif balance_score >= 52:
        balance_state = "pressured_balance"
    else:
        balance_state = "unstable_balance"
    return {
        "balance_score": balance_score,
        "balance_state": balance_state,
        "adaptive_mutual_balance": "balanced" if balance_score >= 72 else "pressured" if balance_score >= 48 else "fragile",
    }


def _clamp(value: int) -> int:
    return max(0, min(100, int(value or 0)))
