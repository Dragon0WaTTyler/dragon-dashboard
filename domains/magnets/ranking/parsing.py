from __future__ import annotations

import re
from typing import Any


RESOLUTION_PATTERNS = (
    ("2160p", re.compile(r"\b(2160p|4k|uhd)\b", re.IGNORECASE)),
    ("1080p", re.compile(r"\b1080p\b", re.IGNORECASE)),
    ("720p", re.compile(r"\b720p\b", re.IGNORECASE)),
    ("480p", re.compile(r"\b(480p|dvd|dvdrip)\b", re.IGNORECASE)),
)

CODEC_PATTERNS = (
    ("AV1", re.compile(r"\bav1\b", re.IGNORECASE)),
    ("x265", re.compile(r"\b(x265|h[\.\s-]?265|hevc)\b", re.IGNORECASE)),
    ("x264", re.compile(r"\b(x264|h[\.\s-]?264|avc)\b", re.IGNORECASE)),
    ("XviD", re.compile(r"\b(xvid|divx)\b", re.IGNORECASE)),
)

SOURCE_PATTERNS = (
    ("REMUX", re.compile(r"\b(remux)\b", re.IGNORECASE)),
    ("BluRay", re.compile(r"\b(blu[ .-]?ray|bdrip|bdremux|bdmux)\b", re.IGNORECASE)),
    ("WebDL", re.compile(r"\b(web[ .-]?dl|webdl|nf|amzn|dsnp|hmax|itunes)\b", re.IGNORECASE)),
    ("WebRip", re.compile(r"\b(web[ .-]?rip|webrip)\b", re.IGNORECASE)),
    ("HDTV", re.compile(r"\b(hdtv|pdtv)\b", re.IGNORECASE)),
    ("DVD", re.compile(r"\b(dvd|dvdrip)\b", re.IGNORECASE)),
    ("CAM", re.compile(r"\b(cam|hdcam)\b", re.IGNORECASE)),
    ("TS", re.compile(r"\b(ts|telesync|hdts)\b", re.IGNORECASE)),
)

AUDIO_PATTERNS = (
    ("TrueHD Atmos", re.compile(r"\b(truehd[ .-]?atmos|atmos[ .-]?truehd|atmos)\b", re.IGNORECASE)),
    ("DTS-HD MA", re.compile(r"\b(dts[ .-]?hd(?:[ .-]?ma)?|dts[- ]?x)\b", re.IGNORECASE)),
    ("TrueHD", re.compile(r"\btruehd\b", re.IGNORECASE)),
    ("DDP 5.1", re.compile(r"\b(ddp|eac3|dd\+)\s*(?:5\.1|7\.1)?\b", re.IGNORECASE)),
    ("DD 5.1", re.compile(r"\b(ac3|dd)\s*(?:5\.1|7\.1)?\b", re.IGNORECASE)),
    ("AAC", re.compile(r"\baac(?:2\.0|5\.1|7\.1)?\b", re.IGNORECASE)),
    ("MP3", re.compile(r"\bmp3\b", re.IGNORECASE)),
)

SEASON_PACK_PATTERN = re.compile(r"\b(s\d{1,2}(?:e\d{1,2})?|season\s*\d+|complete(?:\s+series|\s+season)?|episode\s*\d+)\b", re.IGNORECASE)
PACK_PATTERN = re.compile(r"\b(pack|collection|trilogy|duology|anthology|bundle|boxset|multi[- ]?movie|3in1|2in1)\b", re.IGNORECASE)
HDR_PATTERN = re.compile(r"\b(hdr10\+|hdr10|hdr)\b", re.IGNORECASE)
DV_PATTERN = re.compile(r"\b(dv|dolby[ .-]?vision)\b", re.IGNORECASE)
NOISE_PATTERN = re.compile(r"\b(readnfo|proper|repack|internal|dubbed|subbed|dual[ -]?audio|multi|imax|extended|uncut|criterion|regraded)\b", re.IGNORECASE)
FAKE_PATTERN = re.compile(r"\b(fake|upscale|upscaled|ai[- ]?upscale|camrip|line|workprint|sample)\b", re.IGNORECASE)
LOW_QUALITY_PATTERN = re.compile(r"\b(cam|ts|tc|telecine|hdcam|hdts|workprint|screener)\b", re.IGNORECASE)
MULTI_MOVIE_RANGE_PATTERN = re.compile(r"\b\d+\s*[-:]\s*\d+\b")


def _first_match(patterns: tuple[tuple[str, re.Pattern[str]], ...], title: str) -> str:
    for label, pattern in patterns:
        if pattern.search(title):
            return label
    return ""


def _extract_release_group(title: str) -> str:
    cleaned = title.strip()
    bracket_match = re.search(r"\[([A-Za-z][A-Za-z0-9_-]{1,20})\]\s*$", cleaned)
    if bracket_match:
        return bracket_match.group(1).strip().lower()

    if "-" not in cleaned:
        return ""
    value = cleaned.rsplit("-", 1)[-1].strip().strip("[](). ").lower()
    if not re.fullmatch(r"[a-z][a-z0-9_-]{1,20}", value):
        return ""
    if value.isdigit():
        return ""
    return value


def _extract_noise_tokens(title: str) -> list[str]:
    return sorted({match.group(0).strip().lower() for match in NOISE_PATTERN.finditer(title)})


def _extract_indicators(pattern: re.Pattern[str], title: str) -> list[str]:
    return sorted({match.group(0).strip().lower() for match in pattern.finditer(title)})


def parse_release_title(title: Any) -> dict[str, Any]:
    raw_title = str(title or "").strip()
    lowered = raw_title.lower()
    resolution = _first_match(RESOLUTION_PATTERNS, raw_title)
    codec = _first_match(CODEC_PATTERNS, raw_title)
    source = _first_match(SOURCE_PATTERNS, raw_title)
    hdr = bool(HDR_PATTERN.search(raw_title))
    dolby_vision = bool(DV_PATTERN.search(raw_title))
    audio = _first_match(AUDIO_PATTERNS, raw_title)
    release_group = _extract_release_group(raw_title)
    noise_tokens = _extract_noise_tokens(raw_title)
    fake_indicators = _extract_indicators(FAKE_PATTERN, raw_title)
    low_quality_indicators = _extract_indicators(LOW_QUALITY_PATTERN, raw_title)
    has_season_pack_noise = bool(SEASON_PACK_PATTERN.search(raw_title))
    has_multi_movie_pack = bool(PACK_PATTERN.search(raw_title) or MULTI_MOVIE_RANGE_PATTERN.search(raw_title))
    token_count = len([token for token in re.split(r"[^a-z0-9]+", lowered) if token])
    return {
        "raw_title": raw_title,
        "resolution": resolution,
        "codec": codec,
        "source_type": source,
        "hdr": hdr,
        "dolby_vision": dolby_vision,
        "audio_format": audio,
        "release_group": release_group,
        "noise_tokens": noise_tokens,
        "noise_count": len(noise_tokens),
        "token_count": token_count,
        "has_season_pack_noise": has_season_pack_noise,
        "has_multi_movie_pack": has_multi_movie_pack,
        "fake_indicators": fake_indicators,
        "low_quality_indicators": low_quality_indicators,
    }
