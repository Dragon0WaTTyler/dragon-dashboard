from __future__ import annotations

from typing import Any, Mapping


def build_behavioral_drift(
    identity_memory_summary: Mapping[str, Any] | None,
    *,
    current_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    summary = dict(identity_memory_summary or {})
    drift_counts = dict(summary.get("drift_signals") or {})
    total = max(int(summary.get("total_observations", 0) or 0), 1)
    dominant = max(
        ((str(key), int(value or 0)) for key, value in drift_counts.items()),
        key=lambda item: (item[1], item[0]),
        default=("stability_bias", 0),
    )
    severity = max(0, min(100, int(round(dominant[1] * 100 / total))))
    return {
        "drift_state": dominant[0],
        "drift_score": severity,
        "drift_direction": "elevated" if severity >= 55 else "gradual" if severity >= 26 else "stable",
        "drift_signals": [name for name, value in sorted(drift_counts.items(), key=lambda item: (-int(item[1] or 0), item[0])) if int(value or 0) > 0][:4] or ["stability_bias"],
    }
