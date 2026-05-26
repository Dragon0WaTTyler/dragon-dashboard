from __future__ import annotations

from typing import Any


def build_orchestration_mood(
    *,
    authority_state: str = "",
    immersion_state: str = "",
    tension: str = "",
    forecast_risk: str = "",
) -> dict[str, Any]:
    if authority_state == "approved" and immersion_state in {"immersive", "resilient_immersion"}:
        mood = "confident_mood"
    elif forecast_risk == "high" and "degradation" in tension:
        mood = "degraded_mood"
    elif "resilience" in tension or immersion_state == "resilient_immersion":
        mood = "resilient_mood"
    elif "adaptive" in immersion_state or forecast_risk == "moderate":
        mood = "adaptive_mood"
    elif "fragile" in immersion_state:
        mood = "volatile_mood"
    else:
        mood = "cinematic_mood"
    return {
        "mood": mood,
        "mood_polarity": "positive" if mood in {"confident_mood", "resilient_mood", "cinematic_mood"} else "guarded",
    }
