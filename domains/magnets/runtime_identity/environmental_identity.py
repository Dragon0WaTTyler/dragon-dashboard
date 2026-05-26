from __future__ import annotations

from typing import Any, Mapping


def build_environmental_identity(
    identity_memory_summary: Mapping[str, Any] | None,
    *,
    current_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    summary = dict(identity_memory_summary or {})
    counts = dict(summary.get("environmental_identities") or {})
    total = max(int(summary.get("total_observations", 0) or 0), 1)
    dominant = max(
        ((str(key), int(value or 0)) for key, value in counts.items()),
        key=lambda item: (item[1], item[0]),
        default=("stable_runtime_identity", 0),
    )
    return {
        "environmental_identity": dominant[0],
        "environmental_memory_strength": max(0, min(100, int(round(dominant[1] * 100 / total)))),
    }
