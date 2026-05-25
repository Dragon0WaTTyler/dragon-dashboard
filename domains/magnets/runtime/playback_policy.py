from __future__ import annotations

from typing import Any, Mapping

from ..handoff.diagnostics import evaluate_streamability
from ..runtime.compatibility import is_high_bandwidth_profile
from ..runtime.magnet import parse_magnet_uri
from ..runtime.observability import emit_event


def build_compatibility_snapshot(
    source: Mapping[str, Any],
    *,
    movie: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    data = dict(source or {})
    diagnostics = evaluate_streamability(data, movie=movie)
    magnet = parse_magnet_uri(data.get("magnet"))
    resolution = str(data.get("resolution") or "").strip()
    codec = str(data.get("codec") or "").strip()
    source_type = str(data.get("source_type") or "").strip()
    try:
        size_gb = float(data.get("size_gb") or 0.0)
    except (TypeError, ValueError):
        size_gb = 0.0
    return {
        "magnet": magnet,
        "diagnostics": diagnostics.get("summary") or {},
        "compatibility": diagnostics.get("compatibility") or {},
        "quality": {
            "estimated_quality_score": int(data.get("estimated_quality_score", 0) or 0),
            "confidence": str(data.get("confidence") or "").strip(),
            "likely_streamable": bool(data.get("likely_streamable")),
            "resolution": resolution,
            "codec": codec,
            "source_type": source_type,
            "size_gb": size_gb,
            "seeders": _int_value(data.get("seeders")),
            "release_group": str(data.get("release_group") or "").strip(),
            "ranking_penalties": list(data.get("ranking_penalties") or []),
        },
        "warnings": list((diagnostics.get("summary") or {}).get("warnings") or []),
    }


def evaluate_playback_admission(
    source: Mapping[str, Any],
    *,
    movie: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    snapshot = build_compatibility_snapshot(source, movie=movie)
    compatibility = dict(snapshot.get("compatibility") or {})
    diagnostics = dict(snapshot.get("diagnostics") or {})
    quality = dict(snapshot.get("quality") or {})
    magnet = dict(snapshot.get("magnet") or {})

    quality_score = int(quality.get("estimated_quality_score", 0) or 0)
    likely_streamable = bool(quality.get("likely_streamable"))
    source_type = str(quality.get("source_type") or "").strip()
    resolution = str(quality.get("resolution") or "").strip()
    size_gb = _float_value(quality.get("size_gb"))
    requires_high_bandwidth = bool(
        compatibility.get("high_bandwidth_required")
        or is_high_bandwidth_profile(resolution=resolution, size_gb=size_gb, source_type=source_type)
    )

    blocked_reason = ""
    allowed_for_browser = bool(compatibility.get("browser_friendly"))
    external_only = False
    mobile_safe = bool(compatibility.get("mobile_friendly"))
    hard_blocked = False

    if not magnet.get("is_valid"):
        blocked_reason = "invalid_magnet"
        hard_blocked = True
    elif not likely_streamable:
        blocked_reason = "low_streamability_confidence"
        hard_blocked = True
    elif quality_score < 35:
        blocked_reason = "low_quality_source"
        hard_blocked = True
    elif source_type in {"CAM", "TS"}:
        blocked_reason = "unsupported_release_type"
        hard_blocked = True
    elif requires_high_bandwidth and source_type == "REMUX":
        blocked_reason = "high_bandwidth_remux"

    if hard_blocked:
        allowed_for_browser = False
        external_only = False
        mobile_safe = False
    elif blocked_reason == "high_bandwidth_remux":
        allowed_for_browser = False
        external_only = bool(compatibility.get("external_player_ready")) or bool(magnet.get("is_valid"))
        mobile_safe = False
    elif not blocked_reason and not allowed_for_browser:
        external_only = bool(compatibility.get("external_player_ready")) or bool(magnet.get("is_valid"))
    elif allowed_for_browser:
        external_only = False
    elif compatibility.get("external_player_ready"):
        external_only = True

    policy = {
        "allowed_for_browser": allowed_for_browser,
        "external_only": external_only,
        "mobile_safe": mobile_safe and not blocked_reason,
        "blocked_reason": blocked_reason,
        "requires_high_bandwidth": requires_high_bandwidth,
        "compatibility_status": str(diagnostics.get("status") or "").strip(),
    }
    emit_event(
        "[playback-policy]",
        movie=_movie_name(movie or source),
        allowed_for_browser=1 if policy["allowed_for_browser"] else 0,
        external_only=1 if policy["external_only"] else 0,
        mobile_safe=1 if policy["mobile_safe"] else 0,
        reason=policy["blocked_reason"] or "none",
    )
    return {
        "policy": policy,
        "snapshot": snapshot,
    }


def _int_value(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _float_value(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _movie_name(movie: Mapping[str, Any]) -> str:
    return str(movie.get("title") or movie.get("name") or "").strip() or "unknown"
