from __future__ import annotations

from typing import Any, Mapping


def build_continuity_state(
    identity_memory_summary: Mapping[str, Any] | None,
    *,
    current_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    summary = dict(identity_memory_summary or {})
    current = dict(current_context or {})
    average_continuity = float(summary.get("average_continuity", 0) or 0.0)
    average_maturity = float(summary.get("average_maturity", 0) or 0.0)
    current_confidence = str(current.get("startup_confidence") or "").strip()
    continuity_confidence = max(0, min(100, int(round((average_continuity * 45) + (average_maturity * 35)))))
    continuity_confidence += 10 if current_confidence == "high" else 0
    continuity_confidence = max(0, min(100, continuity_confidence))
    fragmentation = max(0, min(100, 100 - int(round(average_continuity * 100))))
    stability = max(0, min(100, int(round((average_maturity * 55) + (average_continuity * 35)))))
    persistence = max(0, min(100, int(round((float(summary.get("average_identity_confidence", 0) or 0.0) * 70) + (average_continuity * 20)))))
    state = "stable" if continuity_confidence >= 70 else "developing" if continuity_confidence >= 42 else "fragmented"
    return {
        "continuity_state": state,
        "continuity_confidence": continuity_confidence,
        "runtime_consistency": max(0, min(100, int(round(average_continuity * 100)))),
        "orchestration_fragmentation": fragmentation,
        "behavioral_stability": stability,
        "adaptation_persistence": persistence,
    }
