from __future__ import annotations

import hashlib
from typing import Any, Mapping


def normalize_token(value: Any) -> str:
    return str(value or "").strip().lower()


def source_fingerprint(source: Mapping[str, Any]) -> str:
    payload = "|".join(
        [
            normalize_token(source.get("magnet")),
            normalize_token(source.get("title")),
            normalize_token(source.get("source") or source.get("provider")),
            normalize_token(source.get("release_group")),
            normalize_token(source.get("resolution")),
            normalize_token(source.get("codec")),
            normalize_token(source.get("source_type")),
        ]
    )
    if not payload.strip("|"):
        return ""
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]

