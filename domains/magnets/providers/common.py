from __future__ import annotations

import re
import urllib.parse
from typing import Any

from ..models import MagnetCandidate
from ..runtime.config import DEFAULT_TRACKERS


RESOLUTION_PATTERNS = (
    ("2160p", re.compile(r"\b(2160p|4k|uhd)\b", re.IGNORECASE)),
    ("1080p", re.compile(r"\b1080p\b", re.IGNORECASE)),
    ("720p", re.compile(r"\b720p\b", re.IGNORECASE)),
    ("480p", re.compile(r"\b480p\b", re.IGNORECASE)),
)

CODEC_PATTERNS = (
    ("AV1", re.compile(r"\bav1\b", re.IGNORECASE)),
    ("x265", re.compile(r"\b(x265|h\.?265|hevc)\b", re.IGNORECASE)),
    ("x264", re.compile(r"\b(x264|h\.?264|avc)\b", re.IGNORECASE)),
)


def normalize_text(value: Any) -> str:
    return str(value or "").strip()


def normalize_imdb_id(value: Any) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    lowered = text.lower()
    return lowered if lowered.startswith("tt") else text


def title_for_movie(movie: dict[str, Any] | Any) -> str:
    item = movie if isinstance(movie, dict) else {}
    return normalize_text(item.get("title") or item.get("name"))


def year_for_movie(movie: dict[str, Any] | Any) -> str:
    item = movie if isinstance(movie, dict) else {}
    return normalize_text(item.get("year"))


def parse_size_gb(value: Any = None, *, size_bytes: Any = None) -> float:
    try:
        if size_bytes not in (None, ""):
            return round(float(size_bytes) / (1024 ** 3), 3)
    except (TypeError, ValueError):
        pass

    text = normalize_text(value).upper()
    if not text:
        return 0.0

    match = re.search(r"(\d+(?:\.\d+)?)\s*(GB|MB|TB)", text)
    if not match:
        return 0.0

    amount = float(match.group(1))
    unit = match.group(2)
    if unit == "TB":
        amount *= 1024
    elif unit == "MB":
        amount /= 1024
    return round(amount, 3)


def infer_resolution(*values: Any) -> str:
    haystack = " ".join(normalize_text(value) for value in values if value is not None)
    for label, pattern in RESOLUTION_PATTERNS:
        if pattern.search(haystack):
            return label
    return ""


def infer_codec(*values: Any) -> str:
    haystack = " ".join(normalize_text(value) for value in values if value is not None)
    for label, pattern in CODEC_PATTERNS:
        if pattern.search(haystack):
            return label
    return ""


def infer_language(*values: Any) -> str:
    haystack = " ".join(normalize_text(value) for value in values if value is not None).lower()
    if not haystack:
        return ""
    if "multi" in haystack:
        return "multi"
    if re.search(r"\b(ita|italian)\b", haystack):
        return "it"
    if re.search(r"\b(eng|english)\b", haystack):
        return "en"
    if re.search(r"\b(fr|fre|french)\b", haystack):
        return "fr"
    if re.search(r"\b(es|spa|spanish)\b", haystack):
        return "es"
    if re.search(r"\b(pt|por|portuguese)\b", haystack):
        return "pt"
    if re.search(r"\b(ja|jpn|japanese)\b", haystack):
        return "ja"
    if re.search(r"\b(ko|kor|korean)\b", haystack):
        return "ko"
    if re.search(r"\b(de|ger|german)\b", haystack):
        return "de"
    return ""


def build_magnet(hash_value: Any, title: Any, trackers: list[str] | tuple[str, ...] | None = None) -> str:
    info_hash = normalize_text(hash_value).upper()
    if not info_hash:
        return ""
    encoded_title = urllib.parse.quote(normalize_text(title), safe="")
    tracker_list = list(trackers or DEFAULT_TRACKERS)
    tracker_query = "&".join(f"tr={urllib.parse.quote(tracker, safe='')}" for tracker in tracker_list if tracker)
    suffix = f"&{tracker_query}" if tracker_query else ""
    return f"magnet:?xt=urn:btih:{info_hash}&dn={encoded_title}{suffix}"


def make_candidate(
    *,
    source: str,
    title: Any,
    magnet: Any,
    size_gb: Any = 0.0,
    resolution: Any = "",
    codec: Any = "",
    seeders: Any = 0,
    language: Any = "",
    imdb_id: Any = "",
) -> dict[str, Any]:
    try:
        size_value = round(float(size_gb or 0.0), 3)
    except (TypeError, ValueError):
        size_value = 0.0
    try:
        seed_value = int(seeders or 0)
    except (TypeError, ValueError):
        seed_value = 0

    candidate = MagnetCandidate(
        source=normalize_text(source),
        title=normalize_text(title),
        magnet=normalize_text(magnet),
        size_gb=size_value,
        resolution=normalize_text(resolution),
        codec=normalize_text(codec),
        seeders=seed_value,
        language=normalize_text(language),
        imdb_id=normalize_imdb_id(imdb_id),
    )
    return candidate.to_dict()
