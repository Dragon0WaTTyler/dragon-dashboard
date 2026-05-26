from __future__ import annotations

from pathlib import Path


VIDEO_MIME_TYPES = {
    ".mp4": "video/mp4",
    ".m4v": "video/mp4",
    ".mkv": "video/x-matroska",
}


def guess_media_mime_type(path: str) -> str:
    suffix = Path(str(path or "")).suffix.lower()
    return VIDEO_MIME_TYPES.get(suffix, "application/octet-stream")


def is_playable_video_path(path: str) -> bool:
    return Path(str(path or "")).suffix.lower() in {".mp4", ".mkv"}
