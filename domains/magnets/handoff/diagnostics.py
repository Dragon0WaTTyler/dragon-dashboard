from __future__ import annotations

from typing import Any, Mapping

from ..runtime.compatibility import (
    browser_codec_friendly,
    external_player_codec_friendly,
    is_high_bandwidth_profile,
    mobile_codec_friendly,
)
from ..runtime.magnet import parse_magnet_uri
from ..runtime.observability import emit_event


def evaluate_streamability(candidate: Mapping[str, Any], *, movie: Mapping[str, Any] | None = None) -> dict[str, Any]:
    data = dict(candidate or {})
    magnet_meta = parse_magnet_uri(data.get("magnet"))
    resolution = str(data.get("resolution") or "").strip()
    codec = str(data.get("codec") or "").strip()
    source_type = str(data.get("source_type") or "").strip()
    likely_streamable = bool(data.get("likely_streamable"))
    size_gb = _float_value(data.get("size_gb"))
    size_sanity = _file_size_sanity(resolution=resolution, source_type=source_type, size_gb=size_gb)
    high_bandwidth_required = is_high_bandwidth_profile(
        resolution=resolution,
        size_gb=size_gb,
        source_type=source_type,
    )
    codec_supported = bool(codec) and external_player_codec_friendly(codec)
    browser_friendly = bool(
        magnet_meta["is_valid"]
        and browser_codec_friendly(codec)
        and size_sanity["is_sane"]
        and not high_bandwidth_required
        and likely_streamable
        and source_type not in {"REMUX", "CAM", "TS"}
    )
    external_player_ready = bool(
        magnet_meta["is_valid"]
        and codec_supported
        and size_sanity["is_sane"]
        and likely_streamable
        and source_type not in {"CAM", "TS"}
    )
    mobile_friendly = bool(
        browser_friendly
        and mobile_codec_friendly(codec)
        and resolution in {"720p", "1080p", ""}
        and size_gb <= 8.5
    )
    warnings = []
    if not magnet_meta["is_valid"]:
        warnings.append("invalid_magnet")
    if not size_sanity["is_sane"]:
        warnings.extend(size_sanity["reasons"])
    if not codec_supported:
        warnings.append("codec_unverified")
    if high_bandwidth_required:
        warnings.append("high_bandwidth_required")
    if not likely_streamable:
        warnings.append("low_streamability_confidence")

    compatibility = {
        "browser_friendly": browser_friendly,
        "external_player_ready": external_player_ready,
        "mobile_friendly": mobile_friendly,
        "high_bandwidth_required": high_bandwidth_required,
    }
    summary = {
        "magnet_valid": bool(magnet_meta["is_valid"]),
        "file_size_sane": bool(size_sanity["is_sane"]),
        "codec_compatible": codec_supported,
        "browser_friendly": browser_friendly,
        "external_player_friendly": external_player_ready,
        "status": _diagnostic_status(
            magnet_valid=bool(magnet_meta["is_valid"]),
            external_player_ready=external_player_ready,
            browser_friendly=browser_friendly,
        ),
        "warnings": warnings,
    }
    emit_event(
        "[streamability-check]",
        movie=_movie_name(movie or data),
        candidate=str(data.get("release_group") or data.get("provider") or data.get("source") or "unknown"),
        magnet_valid=1 if magnet_meta["is_valid"] else 0,
        browser_friendly=1 if browser_friendly else 0,
        mobile_friendly=1 if mobile_friendly else 0,
        external_player_ready=1 if external_player_ready else 0,
    )
    return {
        "magnet": magnet_meta,
        "size": size_sanity,
        "compatibility": compatibility,
        "summary": summary,
    }


def _file_size_sanity(*, resolution: str, source_type: str, size_gb: float) -> dict[str, Any]:
    reasons: list[str] = []
    if size_gb <= 0:
        return {"is_sane": False, "reasons": ["unknown_size"]}

    sane = True
    if resolution == "2160p":
        if source_type == "REMUX":
            sane = 25 <= size_gb <= 110
            if size_gb < 25:
                reasons.append("undersized_4k_remux")
            elif size_gb > 110:
                reasons.append("oversized_4k_remux")
        else:
            sane = 6 <= size_gb <= 55
            if size_gb < 6:
                reasons.append("undersized_4k")
            elif size_gb > 55:
                reasons.append("oversized_4k")
    elif resolution == "1080p":
        if source_type == "REMUX":
            sane = 12 <= size_gb <= 60
            if size_gb < 12:
                reasons.append("undersized_1080p_remux")
            elif size_gb > 60:
                reasons.append("oversized_1080p_remux")
        else:
            sane = 2 <= size_gb <= 30
            if size_gb < 2:
                reasons.append("undersized_1080p")
            elif size_gb > 30:
                reasons.append("oversized_1080p")
    elif resolution == "720p":
        sane = 1 <= size_gb <= 14
        if size_gb < 1:
            reasons.append("undersized_720p")
        elif size_gb > 14:
            reasons.append("oversized_720p")
    else:
        sane = size_gb <= 80
        if size_gb > 80:
            reasons.append("oversized_unknown_profile")
    return {"is_sane": sane, "reasons": reasons}


def _diagnostic_status(*, magnet_valid: bool, external_player_ready: bool, browser_friendly: bool) -> str:
    if browser_friendly:
        return "browser_ready"
    if external_player_ready:
        return "external_ready"
    if magnet_valid:
        return "limited"
    return "invalid"


def _float_value(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _movie_name(movie: Mapping[str, Any]) -> str:
    return str(movie.get("title") or movie.get("name") or "").strip() or "unknown"
