from __future__ import annotations

from typing import Any


TRUSTED_GROUP_SCORES = {
    "framestor": 10,
    "ctrlhd": 9,
    "don": 9,
    "ebp": 8,
    "hdbits": 8,
    "ntb": 8,
    "qxr": 8,
    "tigole": 8,
    "d-z0n3": 7,
    "evo": 6,
    "ffm": 7,
    "flux": 7,
    "mkvcage": 4,
    "playbd": 6,
    "rarbg": 5,
    "sbc": 6,
    "sparks": 5,
    "termi": 6,
    "trolluhd": 7,
    "vyndros": 6,
    "yify": 3,
    "yts": 3,
}


GROUP_ALIASES = {
    "framestor": "framestor",
    "fra_mestor": "framestor",
    "qxr": "qxr",
    "tigole": "tigole",
    "yts.mx": "yts",
    "yts": "yts",
    "yify": "yify",
}


def normalize_group_name(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    return GROUP_ALIASES.get(text, text)


def trusted_group_score(value: Any) -> int:
    return TRUSTED_GROUP_SCORES.get(normalize_group_name(value), 0)


def is_trusted_group(value: Any) -> bool:
    return trusted_group_score(value) > 0
