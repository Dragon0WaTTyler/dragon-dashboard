from __future__ import annotations

from typing import Any, Mapping

from ..runtime.observability import emit_event
from .capability_matrix import evaluate_capability_matrix
from .runtime_policy import browser_codec_friendly, mobile_codec_friendly, preferred_codecs, startup_confidence_level


RUNTIME_PROFILE_ORDER = (
    "browser_light",
    "browser_balanced",
    "browser_cinematic",
    "mobile_safe",
    "external_player_only",
)


RUNTIME_PROFILES = {
    "browser_light": {
        "label": "Browser Light",
        "bandwidth": "low",
        "max_size_gb": 4.5,
        "preferred_codecs": {"x264", "AV1"},
        "allow_hdr": False,
        "allow_dolby_vision": False,
        "mobile_friendly": True,
        "browser_required": True,
    },
    "browser_balanced": {
        "label": "Browser Balanced",
        "bandwidth": "medium",
        "max_size_gb": 9.5,
        "preferred_codecs": {"x264", "AV1"},
        "allow_hdr": False,
        "allow_dolby_vision": False,
        "mobile_friendly": False,
        "browser_required": True,
    },
    "browser_cinematic": {
        "label": "Browser Cinematic",
        "bandwidth": "high",
        "max_size_gb": 16.0,
        "preferred_codecs": {"x264", "AV1"},
        "allow_hdr": True,
        "allow_dolby_vision": False,
        "mobile_friendly": False,
        "browser_required": True,
    },
    "mobile_safe": {
        "label": "Mobile Safe",
        "bandwidth": "low",
        "max_size_gb": 6.5,
        "preferred_codecs": {"x264"},
        "allow_hdr": False,
        "allow_dolby_vision": False,
        "mobile_friendly": True,
        "browser_required": True,
    },
    "external_player_only": {
        "label": "External Player Only",
        "bandwidth": "variable",
        "max_size_gb": 120.0,
        "preferred_codecs": set(preferred_codecs()),
        "allow_hdr": True,
        "allow_dolby_vision": True,
        "mobile_friendly": False,
        "browser_required": False,
    },
}


def get_runtime_profiles_catalog() -> list[dict[str, Any]]:
    profiles: list[dict[str, Any]] = []
    for name in RUNTIME_PROFILE_ORDER:
        meta = dict(RUNTIME_PROFILES[name])
        profiles.append(
            {
                "id": name,
                "label": meta["label"],
                "bandwidth": meta["bandwidth"],
                "browser_required": bool(meta["browser_required"]),
                "mobile_friendly": bool(meta["mobile_friendly"]),
                "supports_hdr": bool(meta["allow_hdr"]),
                "supports_dolby_vision": bool(meta["allow_dolby_vision"]),
                "max_size_gb": float(meta["max_size_gb"]),
            }
        )
    return profiles


def evaluate_runtime_profile(
    source: Mapping[str, Any],
    profile_name: str,
    *,
    compatibility: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    profile = dict(RUNTIME_PROFILES.get(profile_name) or RUNTIME_PROFILES["external_player_only"])
    data = dict(source or {})
    comp = dict(compatibility or evaluate_capability_matrix(data))
    codec = str(data.get("codec") or "").strip()
    size_gb = _float_value(data.get("size_gb"))
    hdr = bool(data.get("hdr"))
    dolby_vision = bool(data.get("dolby_vision"))
    high_bandwidth = bool(comp.get("high_bandwidth_required"))
    browser_ok = bool(comp.get("browser_friendly"))
    mobile_ok = bool(comp.get("mobile_friendly"))
    warnings: list[str] = []
    score = 45

    if codec in profile["preferred_codecs"]:
        score += 18
    else:
        score -= 12
        warnings.append("codec_mismatch")

    if size_gb <= float(profile["max_size_gb"]):
        score += 12
    else:
        score -= 14
        warnings.append("size_heavy")

    if high_bandwidth:
        score -= 12
        warnings.append("high_bandwidth")
    else:
        score += 8

    if hdr and not profile["allow_hdr"]:
        score -= 10
        warnings.append("hdr_penalty")
    if dolby_vision and not profile["allow_dolby_vision"]:
        score -= 18
        warnings.append("dolby_vision_penalty")

    if profile["browser_required"]:
        if browser_ok:
            score += 18
        else:
            score -= 26
            warnings.append("browser_incompatible")

    if profile["mobile_friendly"]:
        if mobile_ok and mobile_codec_friendly(codec):
            score += 16
        else:
            score -= 20
            warnings.append("mobile_risk")

    startup_reliability = startup_confidence_level(
        score=score,
        browser_ok=browser_ok,
        mobile_ok=mobile_ok,
        high_bandwidth=high_bandwidth,
        codec=codec,
    )
    return {
        "id": profile_name,
        "label": profile["label"],
        "score": max(min(score, 100), 0),
        "bandwidth": profile["bandwidth"],
        "browser_compatible": browser_ok if profile["browser_required"] else browser_codec_friendly(codec),
        "mobile_friendly": mobile_ok if profile["mobile_friendly"] else False,
        "hdr": hdr,
        "dolby_vision": dolby_vision,
        "startup_reliability": startup_reliability,
        "likely_startup_reliability": startup_reliability,
        "warnings": warnings,
    }


def recommend_runtime_profile(
    source: Mapping[str, Any],
    *,
    compatibility: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    evaluations = [
        evaluate_runtime_profile(source, profile_name, compatibility=compatibility)
        for profile_name in RUNTIME_PROFILE_ORDER
    ]
    evaluations.sort(
        key=lambda item: (
            int(item.get("score", 0) or 0),
            1 if item.get("id") != "external_player_only" else 0,
        ),
        reverse=True,
    )
    recommended = evaluations[0] if evaluations else evaluate_runtime_profile(source, "external_player_only", compatibility=compatibility)
    emit_event(
        "[runtime-profile]",
        profile=str(recommended.get("id") or "external_player_only"),
        score=int(recommended.get("score", 0) or 0),
        startup=str(recommended.get("startup_reliability") or "low"),
    )
    return {
        "recommended": recommended,
        "profiles": evaluations,
    }


def _float_value(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
