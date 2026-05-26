from __future__ import annotations

from typing import Any, Mapping


READY_BYTES = 2 * 1024 * 1024
PLAYBACK_BUFFER_BYTES = 8 * 1024 * 1024


def evaluate_buffer_health(session: Mapping[str, Any], status: Mapping[str, Any]) -> dict[str, Any]:
    selected_file = dict(status.get("selectedFile") or {})
    downloaded_bytes = int(selected_file.get("downloaded", session.get("downloaded_bytes", 0)) or 0)
    file_size = int(selected_file.get("length", session.get("file_size", 0)) or 0)
    complete = bool(status.get("complete", session.get("complete")))
    buffered_ratio = (downloaded_bytes / file_size) if file_size > 0 else 0.0
    buffering = not complete and downloaded_bytes < READY_BYTES
    starvation_risk = not complete and downloaded_bytes < PLAYBACK_BUFFER_BYTES

    return {
        "buffered_bytes": downloaded_bytes,
        "buffered_mb": round(downloaded_bytes / (1024 * 1024), 2),
        "buffered_ratio": round(buffered_ratio, 4),
        "playback_ready": complete or downloaded_bytes >= READY_BYTES,
        "buffering": buffering,
        "starvation_risk": starvation_risk,
    }
