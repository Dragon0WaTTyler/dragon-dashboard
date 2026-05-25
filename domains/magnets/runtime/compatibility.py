from __future__ import annotations

from typing import Any


BROWSER_FRIENDLY_CODECS = {"x264", "AV1"}
EXTERNAL_PLAYER_CODECS = {"x264", "x265", "AV1", "XviD", ""}
MOBILE_FRIENDLY_CODECS = {"x264"}


def normalize_resolution(value: Any) -> str:
    return str(value or "").strip()


def normalize_codec(value: Any) -> str:
    return str(value or "").strip()


def is_high_bandwidth_profile(*, resolution: Any, size_gb: Any, source_type: Any) -> bool:
    resolution_text = normalize_resolution(resolution)
    source_type_text = str(source_type or "").strip()
    try:
        size_value = float(size_gb or 0.0)
    except (TypeError, ValueError):
        size_value = 0.0
    return resolution_text == "2160p" or size_value >= 18 or source_type_text == "REMUX"


def browser_codec_friendly(codec: Any) -> bool:
    return normalize_codec(codec) in BROWSER_FRIENDLY_CODECS


def external_player_codec_friendly(codec: Any) -> bool:
    return normalize_codec(codec) in EXTERNAL_PLAYER_CODECS


def mobile_codec_friendly(codec: Any) -> bool:
    return normalize_codec(codec) in MOBILE_FRIENDLY_CODECS
