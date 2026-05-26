from __future__ import annotations


def build_temporal_continuity(
    *,
    federation_continuity: str,
    subconscious_state: str,
    continuity_awareness: str,
    temporal_stability: int,
    temporal_alignment: int,
    prior_continuity: str,
) -> dict[str, object]:
    continuity_text = " ".join(
        [
            str(federation_continuity or "").lower(),
            str(subconscious_state or "").lower(),
            str(continuity_awareness or "").lower(),
        ]
    )
    if "fragment" in continuity_text or "unstable" in continuity_text:
        state = "temporal_fragility"
    elif temporal_stability >= 66 and temporal_alignment >= 68:
        state = "persistent_temporal_continuity"
    elif prior_continuity and prior_continuity == "persistent_temporal_continuity":
        state = "retained_temporal_continuity"
    else:
        state = "adaptive_temporal_continuity"

    persistence = max(
        0,
        min(
            100,
            int(round((temporal_stability * 0.55) + (temporal_alignment * 0.45) + (8 if "retained" in state or "persistent" in state else 0))),
        ),
    )
    return {
        "state": state,
        "continuity_persistence": persistence,
        "continuity_reference": str(federation_continuity or "measured_continuity"),
    }
