from __future__ import annotations


def clamp(value: int | float, low: int = 0, high: int = 100) -> int:
    return max(low, min(high, int(round(value))))


def normalized_quality_weight(quality_label: str = "") -> int:
    label = str(quality_label or "").lower()
    if "2160" in label or "4k" in label:
        return 92
    if "1080" in label:
        return 82
    if "720" in label:
        return 70
    return 58


def cinematic_profile_bias(runtime_profile: str = "") -> int:
    profile = str(runtime_profile or "").strip().lower()
    if "cinematic" in profile:
        return 12
    if "balanced" in profile:
        return 6
    if "external" in profile:
        return -4
    return 0


def preferred_cinematic_runtime(playback_runtime: str = "") -> bool:
    return str(playback_runtime or "").strip() == "browser_runtime"
