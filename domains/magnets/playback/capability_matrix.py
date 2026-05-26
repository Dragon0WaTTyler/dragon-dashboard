from __future__ import annotations

from typing import Any, Mapping

from ..runtime.magnet import parse_magnet_uri
from .runtime_policy import (
    browser_codec_friendly,
    browser_hard_fail_codec,
    bandwidth_class,
    external_player_codec_friendly,
    float_value,
    is_browser_rejected_source_type,
    is_external_rejected_source_type,
    is_high_bandwidth_source,
    is_remux_heavy,
    max_browser_safe_size_for_source,
    mobile_codec_friendly,
    mobile_safe_resolution,
    mobile_safe_size_limit,
    normalize_container,
    normalize_codec,
    normalize_resolution,
    normalize_source_type,
    startup_risk_from_capability,
)


SUPPORTED_BROWSER_CONTAINERS = {"mp4", "mkv", "webm"}
SUPPORTED_EXTERNAL_CONTAINERS = {"mp4", "mkv", "webm", "avi"}


def evaluate_capability_matrix(source: Mapping[str, Any]) -> dict[str, Any]:
    data = dict(source or {})
    magnet_meta = parse_magnet_uri(data.get("magnet"))
    codec = normalize_codec(data.get("codec"))
    resolution = normalize_resolution(data.get("resolution"))
    source_type = normalize_source_type(data.get("source_type"))
    size_gb = float_value(data.get("size_gb"))
    likely_streamable = bool(data.get("likely_streamable", True))
    container = _resolve_container(data)
    container_supported = container in SUPPORTED_BROWSER_CONTAINERS or container in SUPPORTED_EXTERNAL_CONTAINERS or not container
    browser_container_supported = container in SUPPORTED_BROWSER_CONTAINERS or not container
    external_container_supported = container in SUPPORTED_EXTERNAL_CONTAINERS or not container
    size_sanity = _file_size_sanity(resolution=resolution, source_type=source_type, size_gb=size_gb)
    high_bandwidth_required = is_high_bandwidth_source(data)
    remux_heavy = is_remux_heavy(data)
    hdr = bool(data.get("hdr"))
    dolby_vision = bool(data.get("dolby_vision"))
    codec_supported = bool(codec) and external_player_codec_friendly(codec)
    browser_allowed = bool(
        magnet_meta.get("is_valid")
        and likely_streamable
        and size_sanity["is_sane"]
        and browser_codec_friendly(codec)
        and not browser_hard_fail_codec(codec)
        and browser_container_supported
        and not is_browser_rejected_source_type(source_type)
        and size_gb <= max_browser_safe_size_for_source(data)
        and not high_bandwidth_required
        and not remux_heavy
    )
    external_ready = bool(
        magnet_meta.get("is_valid")
        and likely_streamable
        and size_sanity["is_sane"]
        and codec_supported
        and external_container_supported
        and not is_external_rejected_source_type(source_type)
    )
    mobile_safe = bool(
        browser_allowed
        and mobile_codec_friendly(codec)
        and mobile_safe_resolution(data)
        and size_gb <= mobile_safe_size_limit()
        and not hdr
        and not dolby_vision
    )
    capability = {
        "magnet_valid": bool(magnet_meta.get("is_valid")),
        "codec": codec,
        "codec_compatible": codec_supported,
        "codec_browser_friendly": browser_codec_friendly(codec),
        "browser_hard_fail_codec": browser_hard_fail_codec(codec),
        "resolution": resolution,
        "container": container,
        "container_supported": container_supported,
        "browser_container_supported": browser_container_supported,
        "external_container_supported": external_container_supported,
        "source_type": source_type,
        "hdr": hdr,
        "dolby_vision": dolby_vision,
        "remux_heavy": remux_heavy,
        "high_bandwidth_required": high_bandwidth_required,
        "bandwidth_class": estimate_bandwidth_class(data),
        "startup_risk": estimate_startup_risk(data),
        "browser_friendly": browser_allowed,
        "external_player_ready": external_ready,
        "mobile_friendly": mobile_safe,
        "size_gb": size_gb,
        "size_sanity": size_sanity,
        "likely_streamable": likely_streamable,
    }
    capability["notes"] = _build_capability_notes(capability)
    return capability


def evaluate_browser_capability(source: Mapping[str, Any]) -> dict[str, Any]:
    capability = evaluate_capability_matrix(source)
    return {
        "browser_friendly": bool(capability["browser_friendly"]),
        "browser_hard_fail_codec": bool(capability["browser_hard_fail_codec"]),
        "browser_container_supported": bool(capability["browser_container_supported"]),
        "high_bandwidth_required": bool(capability["high_bandwidth_required"]),
        "remux_heavy": bool(capability["remux_heavy"]),
        "hdr": bool(capability["hdr"]),
        "dolby_vision": bool(capability["dolby_vision"]),
        "notes": list(capability.get("notes") or []),
    }


def evaluate_mobile_capability(source: Mapping[str, Any]) -> dict[str, Any]:
    capability = evaluate_capability_matrix(source)
    return {
        "mobile_friendly": bool(capability["mobile_friendly"]),
        "bandwidth_class": str(capability["bandwidth_class"]),
        "startup_risk": str(capability["startup_risk"]),
        "notes": list(capability.get("notes") or []),
    }


def estimate_bandwidth_class(source: Mapping[str, Any]) -> str:
    return bandwidth_class(source)


def estimate_startup_risk(source: Mapping[str, Any]) -> str:
    capability = {
        "magnet_valid": bool(parse_magnet_uri(source.get("magnet")).get("is_valid")),
        "container_supported": bool(_resolve_container(source) in SUPPORTED_BROWSER_CONTAINERS or _resolve_container(source) in SUPPORTED_EXTERNAL_CONTAINERS or not _resolve_container(source)),
        "browser_friendly": bool(
            bool(parse_magnet_uri(source.get("magnet")).get("is_valid"))
            and browser_codec_friendly(source.get("codec"))
            and not browser_hard_fail_codec(source.get("codec"))
            and not is_high_bandwidth_source(source)
            and not is_remux_heavy(source)
        ),
        "external_player_ready": bool(
            bool(parse_magnet_uri(source.get("magnet")).get("is_valid"))
            and external_player_codec_friendly(source.get("codec"))
            and not is_external_rejected_source_type(source.get("source_type"))
        ),
        "high_bandwidth_required": bool(is_high_bandwidth_source(source)),
        "remux_heavy": bool(is_remux_heavy(source)),
    }
    return startup_risk_from_capability(capability)


def _resolve_container(source: Mapping[str, Any]) -> str:
    direct = normalize_container(source.get("container") or source.get("file_ext"))
    if direct:
        return direct
    title = str(source.get("title") or "").lower()
    for candidate in ("mkv", "mp4", "webm", "avi"):
        if f".{candidate}" in title:
            return candidate
    return ""


def _build_capability_notes(capability: Mapping[str, Any]) -> list[str]:
    notes: list[str] = []
    if not capability.get("magnet_valid"):
        notes.append("invalid_magnet")
    if not capability.get("size_sanity", {}).get("is_sane", True):
        notes.extend(capability.get("size_sanity", {}).get("reasons", []))
    if capability.get("browser_hard_fail_codec"):
        notes.append("browser_hard_fail_codec")
    if not capability.get("codec_compatible"):
        notes.append("codec_unverified")
    if not capability.get("browser_container_supported"):
        notes.append("browser_container_unsupported")
    if capability.get("hdr"):
        notes.append("hdr_present")
    if capability.get("dolby_vision"):
        notes.append("dolby_vision_present")
    if capability.get("remux_heavy"):
        notes.append("remux_heavy")
    if capability.get("high_bandwidth_required"):
        notes.append("high_bandwidth_required")
    if not capability.get("likely_streamable"):
        notes.append("low_streamability_confidence")
    return notes


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
