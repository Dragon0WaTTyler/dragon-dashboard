from __future__ import annotations

import math
import re
from typing import Any, Mapping

from .parsing import parse_release_title
from .trusted_groups import is_trusted_group, trusted_group_score


RESOLUTION_SCORES = {
    "2160p": 18,
    "1080p": 14,
    "720p": 8,
    "480p": 2,
    "": 0,
}

SOURCE_SCORES = {
    "REMUX": 30,
    "BluRay": 22,
    "WebDL": 18,
    "WebRip": 12,
    "HDTV": 4,
    "DVD": 2,
    "CAM": -55,
    "TS": -50,
    "": 0,
}

CODEC_SCORES = {
    "AV1": 9,
    "x265": 8,
    "x264": 4,
    "XviD": -8,
    "": 0,
}

AUDO_SCORES = {
    "TrueHD Atmos": 10,
    "DTS-HD MA": 8,
    "TrueHD": 7,
    "DDP 5.1": 5,
    "DD 5.1": 4,
    "AAC": 1,
    "MP3": -4,
    "": 0,
}


def score_candidate(candidate: Mapping[str, Any], *, movie: Mapping[str, Any] | None = None) -> dict[str, Any]:
    parsed = parse_release_title(candidate.get("title"))
    resolution = parsed["resolution"] or str(candidate.get("resolution") or "").strip()
    codec = parsed["codec"] or str(candidate.get("codec") or "").strip()
    source_type = parsed["source_type"]
    audio_format = parsed["audio_format"]
    release_group = parsed["release_group"]
    size_gb = _float_value(candidate.get("size_gb"))
    seeders = _int_value(candidate.get("seeders"))

    score = 10
    score += RESOLUTION_SCORES.get(resolution, 0)
    score += SOURCE_SCORES.get(source_type, 0)
    score += CODEC_SCORES.get(codec, 0)
    score += AUDO_SCORES.get(audio_format, 0)
    if parsed["hdr"]:
        score += 4
    if parsed["dolby_vision"]:
        score += 4
    score += trusted_group_score(release_group)
    score += _seeder_score(seeders)

    penalties: list[str] = []
    bonuses: list[str] = []

    if source_type == "REMUX":
        bonuses.append("remux")
    elif source_type == "BluRay":
        bonuses.append("bluray")
    elif source_type == "WebDL":
        bonuses.append("webdl")

    if codec == "x265":
        bonuses.append("x265")
    if is_trusted_group(release_group):
        bonuses.append(f"group:{release_group}")

    size_adjustment, size_flags = _size_adjustment(size_gb=size_gb, resolution=resolution, source_type=source_type)
    score += size_adjustment
    penalties.extend(size_flags["penalties"])
    bonuses.extend(size_flags["bonuses"])

    fake_penalty, fake_flags = _anti_fake_adjustment(
        parsed=parsed,
        resolution=resolution,
        source_type=source_type,
        codec=codec,
        size_gb=size_gb,
    )
    score += fake_penalty
    penalties.extend(fake_flags)

    if parsed["has_multi_movie_pack"]:
        score -= 35
        penalties.append("multi_movie_pack")
    if parsed["has_season_pack_noise"]:
        score -= 28
        penalties.append("season_noise")
    if parsed["noise_count"] >= 4:
        score -= 8
        penalties.append("noisy_title")
    elif parsed["noise_count"] >= 2:
        score -= 3

    if parsed["low_quality_indicators"]:
        score -= 22
        penalties.extend(parsed["low_quality_indicators"])

    if movie and _looks_like_wrong_movie(candidate.get("title"), movie):
        score -= 25
        penalties.append("title_mismatch")

    clamped_score = max(0, min(100, int(round(score))))
    confidence = _confidence(parsed=parsed, score=clamped_score, penalties=penalties)
    likely_streamable = _likely_streamable(
        parsed=parsed,
        score=clamped_score,
        seeders=seeders,
        size_gb=size_gb,
        penalties=penalties,
    )

    return {
        "estimated_quality_score": clamped_score,
        "likely_streamable": likely_streamable,
        "confidence": confidence,
        "resolution": resolution,
        "codec": codec,
        "source_type": source_type,
        "audio_format": audio_format,
        "release_group": release_group,
        "hdr": parsed["hdr"],
        "dolby_vision": parsed["dolby_vision"],
        "has_season_pack_noise": parsed["has_season_pack_noise"],
        "has_multi_movie_pack": parsed["has_multi_movie_pack"],
        "noise_count": parsed["noise_count"],
        "noise_tokens": parsed["noise_tokens"],
        "fake_indicators": parsed["fake_indicators"],
        "low_quality_indicators": parsed["low_quality_indicators"],
        "ranking_penalties": penalties,
        "ranking_bonuses": bonuses,
    }


def _float_value(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _int_value(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _seeder_score(seeders: int) -> int:
    if seeders <= 0:
        return -8
    if seeders < 5:
        return 1
    if seeders < 20:
        return 4
    if seeders < 80:
        return 7
    return min(12, 8 + int(math.log10(max(seeders, 1)) * 2))


def _size_adjustment(*, size_gb: float, resolution: str, source_type: str) -> tuple[int, dict[str, list[str]]]:
    penalties: list[str] = []
    bonuses: list[str] = []
    score = 0
    if size_gb <= 0:
        penalties.append("unknown_size")
        return (-3, {"penalties": penalties, "bonuses": bonuses})

    if resolution == "2160p":
        if source_type == "REMUX":
            if 40 <= size_gb <= 90:
                score += 8
                bonuses.append("healthy_size")
            elif size_gb < 25:
                score -= 25
                penalties.append("tiny_remux")
            elif size_gb > 110:
                score -= 10
                penalties.append("bloated_file")
        else:
            if 8 <= size_gb <= 35:
                score += 5
                bonuses.append("healthy_size")
            elif size_gb < 6:
                score -= 22
                penalties.append("tiny_4k")
            elif size_gb > 55:
                score -= 10
                penalties.append("bloated_file")
    elif resolution == "1080p":
        if source_type == "REMUX":
            if 18 <= size_gb <= 45:
                score += 7
                bonuses.append("healthy_size")
            elif size_gb < 12:
                score -= 18
                penalties.append("tiny_remux")
            elif size_gb > 60:
                score -= 8
                penalties.append("bloated_file")
        else:
            if 4 <= size_gb <= 18:
                score += 5
                bonuses.append("healthy_size")
            elif size_gb < 2.0:
                score -= 16
                penalties.append("tiny_1080p")
            elif size_gb > 30:
                score -= 8
                penalties.append("bloated_file")
    elif resolution == "720p":
        if 2 <= size_gb <= 8:
            score += 4
            bonuses.append("healthy_size")
        elif size_gb < 1.0:
            score -= 10
            penalties.append("tiny_720p")
        elif size_gb > 14:
            score -= 8
            penalties.append("bloated_file")
    else:
        if size_gb > 80:
            score -= 10
            penalties.append("bloated_file")

    return score, {"penalties": penalties, "bonuses": bonuses}


def _anti_fake_adjustment(
    *,
    parsed: Mapping[str, Any],
    resolution: str,
    source_type: str,
    codec: str,
    size_gb: float,
) -> tuple[int, list[str]]:
    penalty = 0
    flags: list[str] = []
    for token in list(parsed.get("fake_indicators") or []):
        penalty -= 16
        flags.append(token)

    if resolution == "2160p" and source_type in {"CAM", "TS", "HDTV"}:
        penalty -= 35
        flags.append("fake_4k_source")
    if resolution == "2160p" and size_gb and size_gb < 6:
        penalty -= 22
        flags.append("fake_4k_size")
    if resolution == "2160p" and codec == "XviD":
        penalty -= 18
        flags.append("fake_4k_codec")
    if source_type == "REMUX" and size_gb and size_gb < 12:
        penalty -= 30
        flags.append("fake_remux")
    if parsed.get("hdr") and codec == "XviD":
        penalty -= 12
        flags.append("implausible_hdr_codec")
    return penalty, flags


def _confidence(*, parsed: Mapping[str, Any], score: int, penalties: list[str]) -> str:
    if score >= 80 and not parsed.get("fake_indicators") and not parsed.get("has_multi_movie_pack"):
        return "high"
    if score >= 55 and len(penalties) <= 3:
        return "medium"
    return "low"


def _likely_streamable(
    *,
    parsed: Mapping[str, Any],
    score: int,
    seeders: int,
    size_gb: float,
    penalties: list[str],
) -> bool:
    if parsed.get("has_multi_movie_pack") or parsed.get("has_season_pack_noise"):
        return False
    if parsed.get("low_quality_indicators") or parsed.get("fake_indicators"):
        return False
    if any(flag in penalties for flag in ("title_mismatch", "fake_remux", "fake_4k_source", "fake_4k_size")):
        return False
    if seeders <= 0 or size_gb <= 0:
        return False
    return score >= 50


def _looks_like_wrong_movie(title: Any, movie: Mapping[str, Any]) -> bool:
    requested_title = str(movie.get("title") or movie.get("name") or "").strip().lower()
    candidate_title = str(title or "").strip().lower()
    if not requested_title or not candidate_title:
        return False
    requested_tokens = {token for token in re.findall(r"[a-z0-9]+", requested_title) if len(token) > 2}
    candidate_tokens = {token for token in re.findall(r"[a-z0-9]+", candidate_title) if len(token) > 2}
    if not requested_tokens:
        return False
    overlap = requested_tokens.intersection(candidate_tokens)
    return len(overlap) < max(1, len(requested_tokens) // 2)
