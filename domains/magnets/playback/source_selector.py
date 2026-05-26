from __future__ import annotations

from typing import Any, Mapping

from ..runtime.identifiers import source_fingerprint
from ..runtime.observability import emit_event
from .capability_matrix import evaluate_capability_matrix
from .runtime_policy import (
    codec_weight,
    int_value,
    is_remux_heavy,
    release_type_weight,
    strongly_trusted_release_group,
    trusted_release_group_score,
)
from .runtime_profile import recommend_runtime_profile


def select_playback_candidates(
    candidates: list[Mapping[str, Any]],
    *,
    movie: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    decorated = [_decorate_source(candidate, movie=movie) for candidate in candidates if isinstance(candidate, Mapping)]
    decorated.sort(
        key=lambda item: (
            int(item.get("playback_priority", 0) or 0),
            int(item.get("seeders", 0) or 0),
            int(item.get("estimated_quality_score", 0) or 0),
        ),
        reverse=True,
    )
    if decorated:
        decorated[0]["auto_selected"] = True

    recommended = decorated[0] if decorated else {}
    emit_event(
        "[runtime-selection]",
        movie=_movie_name(movie or recommended),
        selected=str(recommended.get("source_fingerprint") or "none"),
        runtime=str(recommended.get("runtime_recommended") or "none"),
        browser_ready=1 if recommended.get("browser_playable_candidate") else 0,
    )
    return {
        "sources": decorated,
        "selected_source": recommended,
    }


def _decorate_source(candidate: Mapping[str, Any], *, movie: Mapping[str, Any] | None = None) -> dict[str, Any]:
    data = dict(candidate or {})
    capability = evaluate_capability_matrix(data)
    recommended_profile_meta = recommend_runtime_profile(data, compatibility=capability)
    recommended_profile = dict(recommended_profile_meta.get("recommended") or {})
    codec = str(data.get("codec") or "").strip()
    seeders = int_value(data.get("seeders"))
    size_gb = _float_value(data.get("size_gb"))
    quality_score = int_value(data.get("estimated_quality_score"))
    fake_penalty = _fake_penalty(data)
    priority = quality_score
    priority += trusted_release_group_score(data) * 6
    priority += release_type_weight(data.get("source_type"))
    priority += codec_weight(codec)
    priority += _seeder_weight(seeders)
    priority += _size_weight(size_gb=size_gb, browser_friendly=bool(capability.get("browser_friendly")))
    priority -= fake_penalty
    if data.get("hdr") or data.get("dolby_vision") or is_remux_heavy(data):
        priority -= 6

    browser_candidate = bool(capability.get("browser_friendly"))
    external_only = bool(not browser_candidate and capability.get("magnet_valid"))
    data.update(
        {
            "source_fingerprint": str(data.get("source_fingerprint") or source_fingerprint(data) or "").strip(),
            "trusted_group": strongly_trusted_release_group(data),
            "playback_priority": priority,
            "browser_playable_candidate": browser_candidate,
            "external_runtime_only": external_only,
            "runtime_recommended": "browser_runtime" if browser_candidate else "external_runtime",
            "runtime_profile": str(recommended_profile.get("id") or "external_player_only"),
            "runtime_profile_label": str(recommended_profile.get("label") or "External Player Only"),
            "playback_profile_score": int(recommended_profile.get("score", 0) or 0),
            "startup_confidence": str(recommended_profile.get("startup_reliability") or "low"),
            "playback_warnings": list(recommended_profile.get("warnings") or []),
            "auto_selected": False,
            "browser_friendly": bool(capability.get("browser_friendly")),
            "external_player_ready": bool(capability.get("external_player_ready")),
            "mobile_friendly": bool(capability.get("mobile_friendly")),
            "high_bandwidth_required": bool(capability.get("high_bandwidth_required")),
            "bandwidth_class": str(capability.get("bandwidth_class") or "unknown"),
            "startup_risk": str(capability.get("startup_risk") or "high"),
            "container": str(capability.get("container") or ""),
        }
    )
    return data


def _seeder_weight(seeders: int) -> int:
    if seeders >= 100:
        return 24
    if seeders >= 40:
        return 16
    if seeders >= 12:
        return 8
    if seeders > 0:
        return 2
    return -10


def _size_weight(*, size_gb: float, browser_friendly: bool) -> int:
    if size_gb <= 0:
        return -6
    if browser_friendly:
        if size_gb <= 4.5:
            return 18
        if size_gb <= 9.5:
            return 10
        if size_gb <= 16:
            return 2
        return -12
    if size_gb <= 25:
        return 4
    if size_gb <= 60:
        return 1
    return -10


def _fake_penalty(source: Mapping[str, Any]) -> int:
    title = str(source.get("title") or "").lower()
    indicators = list(source.get("low_quality_indicators") or []) + list(source.get("fake_indicators") or [])
    penalty = 0
    for token in indicators:
        if token:
            penalty += 28
    if "sample" in title or "fake" in title:
        penalty += 18
    return penalty


def _float_value(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _movie_name(movie: Mapping[str, Any]) -> str:
    return str(movie.get("title") or movie.get("name") or "").strip() or "unknown"
