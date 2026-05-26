from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from flask import Response, request

from .runtime_manager import PlaybackRuntimeError, PlaybackRuntimeManager


STREAM_CHUNK_SIZE = 1024 * 1024


def build_stream_response(manager: PlaybackRuntimeManager, session_id: str, http_request=None) -> Response:
    current_request = http_request or request
    session = manager.wait_for_bytes(session_id, 0 if "Range" not in current_request.headers else _range_start(current_request.headers.get("Range")), timeout_seconds=12.0)
    file_path = Path(str(session.get("file_path") or ""))
    file_size = int(session.get("file_size", 0) or 0)
    mime_type = str(session.get("mime_type") or "application/octet-stream")
    if not file_path.exists() or file_size <= 0:
        raise PlaybackRuntimeError("stream_unavailable", "Playable media file is not available yet.")

    range_header = str(current_request.headers.get("Range") or "").strip()
    start, end = _resolve_range(range_header, file_size)
    session = manager.wait_for_bytes(session_id, start, timeout_seconds=12.0)
    downloaded_bytes = int(session.get("downloaded_bytes", 0) or 0)
    complete = bool(session.get("complete"))

    if downloaded_bytes <= start and not complete:
        raise PlaybackRuntimeError("buffering", "Torrent data is still buffering for this playback range.")
    if start >= file_size:
        return Response(status=416, headers={"Content-Range": f"bytes */{file_size}"})

    available_end = min(end, file_size - 1) if complete else min(end, max(downloaded_bytes - 1, start))
    content_length = max(available_end - start + 1, 0)
    status_code = 206 if range_header or available_end < file_size - 1 else 200

    headers = {
        "Accept-Ranges": "bytes",
        "Content-Type": mime_type,
        "Content-Length": str(content_length),
    }
    if status_code == 206:
        headers["Content-Range"] = f"bytes {start}-{available_end}/{file_size}"

    def generate():
        with file_path.open("rb") as handle:
            handle.seek(start)
            remaining = content_length
            while remaining > 0:
                chunk = handle.read(min(STREAM_CHUNK_SIZE, remaining))
                if not chunk:
                    break
                remaining -= len(chunk)
                yield chunk

    return Response(generate(), status=status_code, headers=headers, direct_passthrough=True)


def _range_start(range_header: str) -> int:
    if not range_header.startswith("bytes="):
        return 0
    start_text = range_header[6:].split("-", 1)[0].strip()
    try:
        return max(int(start_text), 0)
    except (TypeError, ValueError):
        return 0


def _resolve_range(range_header: str, total_size: int) -> tuple[int, int]:
    if total_size <= 0 or not range_header.startswith("bytes="):
        return 0, max(total_size - 1, 0)
    start_text, _, end_text = range_header[6:].partition("-")
    try:
        start = max(int(start_text or 0), 0)
    except (TypeError, ValueError):
        start = 0
    try:
        end = min(int(end_text), total_size - 1) if end_text else total_size - 1
    except (TypeError, ValueError):
        end = total_size - 1
    if end < start:
        end = start
    return start, end
