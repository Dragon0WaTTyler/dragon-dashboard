from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from .mime_helpers import is_playable_video_path


MIN_VIDEO_BYTES = 50 * 1024 * 1024
REJECT_TOKENS = ("sample", "trailer")


def select_playable_media_file(files: list[Mapping[str, Any]] | None) -> dict[str, Any] | None:
    candidates: list[dict[str, Any]] = []
    for item in files or []:
        candidate = dict(item or {})
        path = str(candidate.get("path") or candidate.get("name") or "").strip()
        size = _to_int(candidate.get("length") or candidate.get("size"))
        if not path or size < MIN_VIDEO_BYTES or not is_playable_video_path(path):
            continue
        name = Path(path).name.lower()
        if any(token in name for token in REJECT_TOKENS):
            continue
        candidate["path"] = path
        candidate["length"] = size
        candidate["extension_rank"] = _extension_rank(path)
        candidates.append(candidate)
    if not candidates:
        return None
    candidates.sort(
        key=lambda item: (
            item["extension_rank"],
            -int(item["length"]),
            str(item["path"]).lower(),
        )
    )
    selected = dict(candidates[0])
    selected.pop("extension_rank", None)
    return selected


def _extension_rank(path: str) -> int:
    suffix = Path(path).suffix.lower()
    if suffix == ".mp4":
        return 0
    if suffix == ".mkv":
        return 1
    return 9


def _to_int(value: Any) -> int:
    try:
        return max(int(value or 0), 0)
    except (TypeError, ValueError):
        return 0
