from __future__ import annotations

from typing import Any, Mapping

from ..ranking.trusted_groups import is_trusted_group, trusted_group_score


BROWSER_HARD_FAIL_CODECS = {"x265", "hevc", "xvid", "divx", "mpeg2", "vc1", ""}
BROWSER_FRIENDLY_CODECS = {"x264", "av1"}
EXTERNAL_PLAYER_CODECS = {"x264", "x265", "av1", "xvid", ""}
MOBILE_FRIENDLY_CODECS = {"x264"}
CODEC_PREFERENCE_ORDER = ("x264", "AV1", "x265", "XviD", "")
TRUSTED_RELEASE_GROUP_MIN_SCORE = 6
STARTUP_CONFIDENCE_THRESHOLDS = {
    "high": 80,
    "medium": 62,
}
MAX_BROWSER_SAFE_SIZE_GB = {
    "default": 9.5,
    "browser_light": 4.5,
    "browser_balanced": 9.5,
    "browser_cinematic": 16.0,
}
MOBILE_SAFE_MAX_SIZE_GB = 6.5
MOBILE_SAFE_MAX_RESOLUTION = {"", "720p", "1080p"}
BROWSER_REMUX_REJECT_SOURCE_TYPES = {"REMUX", "CAM", "TS"}
EXTERNAL_REJECT_SOURCE_TYPES = {"CAM", "TS"}
HIGH_BANDWIDTH_SIZE_THRESHOLD_GB = 18.0
REMUX_HEAVY_SIZE_THRESHOLD_GB = 25.0

RELEASE_TYPE_WEIGHTS = {
    "WEBDL": 18,
    "WEBRIP": 12,
    "BLURAY": 8,
    "HDTV": 3,
    "DVD": 0,
    "REMUX": -8,
    "CAM": -70,
    "TS": -70,
}

CODEC_WEIGHTS = {
    "X264": 20,
    "AV1": 14,
    "X265": 10,
    "XVID": -8,
    "": -6,
}


def normalize_codec(value: Any) -> str:
    text = str(value or "").strip()
    return text.upper() if text else ""


def normalize_resolution(value: Any) -> str:
    return str(value or "").strip().lower()


def normalize_source_type(value: Any) -> str:
    return str(value or "").strip().upper()


def normalize_container(value: Any) -> str:
    return str(value or "").strip().lower().lstrip(".")


def normalize_release_group(value: Any) -> str:
    return str(value or "").strip().lower()


def browser_hard_fail_codec(codec: Any) -> bool:
    return normalize_codec(codec).lower() in BROWSER_HARD_FAIL_CODECS


def browser_codec_friendly(codec: Any) -> bool:
    return normalize_codec(codec).lower() in BROWSER_FRIENDLY_CODECS


def external_player_codec_friendly(codec: Any) -> bool:
    return normalize_codec(codec).lower() in EXTERNAL_PLAYER_CODECS


def mobile_codec_friendly(codec: Any) -> bool:
    return normalize_codec(codec).lower() in MOBILE_FRIENDLY_CODECS


def preferred_codec_rank(codec: Any) -> int:
    normalized = normalize_codec(codec)
    for index, preferred in enumerate(CODEC_PREFERENCE_ORDER):
        if normalized == preferred.upper():
            return index
    return len(CODEC_PREFERENCE_ORDER)


def preferred_codecs() -> list[str]:
    return list(CODEC_PREFERENCE_ORDER)


def browser_safe_size_limit(profile_name: str = "") -> float:
    key = str(profile_name or "").strip()
    return float(MAX_BROWSER_SAFE_SIZE_GB.get(key) or MAX_BROWSER_SAFE_SIZE_GB["default"])


def mobile_safe_size_limit() -> float:
    return float(MOBILE_SAFE_MAX_SIZE_GB)


def startup_confidence_threshold(level: str) -> int:
    return int(STARTUP_CONFIDENCE_THRESHOLDS.get(level, 0) or 0)


def is_browser_rejected_source_type(source_type: Any) -> bool:
    return normalize_source_type(source_type) in BROWSER_REMUX_REJECT_SOURCE_TYPES


def is_external_rejected_source_type(source_type: Any) -> bool:
    return normalize_source_type(source_type) in EXTERNAL_REJECT_SOURCE_TYPES


def is_remux_heavy(source: Mapping[str, Any]) -> bool:
    return normalize_source_type(source.get("source_type")) == "REMUX" or float_value(source.get("size_gb")) >= REMUX_HEAVY_SIZE_THRESHOLD_GB


def is_high_bandwidth_source(source: Mapping[str, Any]) -> bool:
    resolution = normalize_resolution(source.get("resolution"))
    source_type = normalize_source_type(source.get("source_type"))
    size_gb = float_value(source.get("size_gb"))
    return resolution == "2160p" or size_gb >= HIGH_BANDWIDTH_SIZE_THRESHOLD_GB or source_type == "REMUX"


def mobile_safe_resolution(source: Mapping[str, Any]) -> bool:
    return normalize_resolution(source.get("resolution")) in MOBILE_SAFE_MAX_RESOLUTION


def max_browser_safe_size_for_source(source: Mapping[str, Any]) -> float:
    if normalize_resolution(source.get("resolution")) == "2160p":
        return browser_safe_size_limit("browser_cinematic")
    return browser_safe_size_limit()


def release_type_weight(source_type: Any) -> int:
    return int(RELEASE_TYPE_WEIGHTS.get(normalize_source_type(source_type), 0) or 0)


def codec_weight(codec: Any) -> int:
    return int(CODEC_WEIGHTS.get(normalize_codec(codec), -10) or -10)


def trusted_release_group(source: Mapping[str, Any]) -> bool:
    return is_trusted_group(normalize_release_group(source.get("release_group")))


def trusted_release_group_score(source: Mapping[str, Any]) -> int:
    return trusted_group_score(normalize_release_group(source.get("release_group")))


def strongly_trusted_release_group(source: Mapping[str, Any]) -> bool:
    return trusted_release_group_score(source) >= TRUSTED_RELEASE_GROUP_MIN_SCORE


def startup_confidence_level(score: int, *, browser_ok: bool, mobile_ok: bool, high_bandwidth: bool, codec: Any) -> str:
    codec_text = normalize_codec(codec)
    if score >= startup_confidence_threshold("high") and browser_ok and not high_bandwidth:
        return "high"
    if score >= startup_confidence_threshold("medium") and browser_ok:
        return "medium"
    if mobile_ok and codec_text == "X264":
        return "medium"
    return "low"


def bandwidth_class(source: Mapping[str, Any]) -> str:
    size_gb = float_value(source.get("size_gb"))
    if is_high_bandwidth_source(source):
        return "high"
    if size_gb >= 8.0:
        return "medium"
    if size_gb > 0:
        return "low"
    return "unknown"


def startup_risk_from_capability(capability: Mapping[str, Any]) -> str:
    if not capability.get("magnet_valid") or not capability.get("container_supported"):
        return "high"
    if capability.get("browser_friendly") and not capability.get("high_bandwidth_required") and not capability.get("remux_heavy"):
        return "low"
    if capability.get("external_player_ready"):
        return "medium"
    return "high"


def float_value(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def int_value(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0
