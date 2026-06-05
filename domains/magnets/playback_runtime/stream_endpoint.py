from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from flask import Response, current_app, request

from .runtime_manager import PlaybackRuntimeError, PlaybackRuntimeManager


STREAM_CHUNK_SIZE = 1024 * 1024


def build_stream_response(manager: PlaybackRuntimeManager, session_id: str, http_request=None) -> Response:
    current_request = http_request or request
    range_header = str(current_request.headers.get("Range") or "").strip()
    range_start = 0 if "Range" not in current_request.headers else _range_start(current_request.headers.get("Range"))
    try:
        session = manager.wait_for_bytes(session_id, range_start, timeout_seconds=12.0)
    except PlaybackRuntimeError as exc:
        raise _augment_stream_error(exc, session_id=session_id, range_header=range_header) from exc
    file_path, file_exists, disk_size = _local_file_snapshot(str(session.get("file_path") or ""))
    file_size = int(session.get("file_size", 0) or 0)
    mime_type = str(session.get("mime_type") or "application/octet-stream")
    selected_file = dict(session.get("selected_file") or {})
    selected_file_name = str(selected_file.get("name") or session.get("file_name") or "").strip()
    if not selected_file_name or file_size <= 0:
        raise _stream_error(
            "selected_file_missing",
            "A playable file has not been selected for this stream yet.",
            session_id=session_id,
            range_header=range_header,
            selected_file_name=selected_file_name,
            file_path=file_path,
            file_exists=file_exists,
            disk_size=disk_size,
            selected_length=file_size,
        )
    if not file_path or not file_exists:
        raise _stream_error(
            "file_not_found",
            "The selected media file is not available on disk yet.",
            session_id=session_id,
            range_header=range_header,
            selected_file_name=selected_file_name,
            file_path=file_path,
            file_exists=file_exists,
            disk_size=disk_size,
            selected_length=file_size,
        )

    range_request = _resolve_range(range_header, file_size)
    if not range_request["ok"]:
        return Response(status=416, headers={"Accept-Ranges": "bytes", "Content-Range": f"bytes */{file_size}"})
    start = int(range_request["start"])
    end = int(range_request["end"])
    try:
        session = manager.wait_for_bytes(session_id, start, timeout_seconds=12.0)
    except PlaybackRuntimeError as exc:
        raise _augment_stream_error(exc, session_id=session_id, range_header=range_header, session=session) from exc
    file_path, file_exists, disk_size = _local_file_snapshot(str(session.get("file_path") or file_path))
    downloaded_bytes = int(session.get("downloaded_bytes", 0) or 0)
    complete = bool(session.get("complete"))
    selected_file = dict(session.get("selected_file") or selected_file)
    selected_file_name = str(selected_file.get("name") or selected_file_name).strip()
    file_size = int(session.get("file_size", file_size) or 0)
    stream_readiness = dict(session.get("stream_readiness") or {})
    if not file_exists:
        raise _stream_error(
            "file_not_found",
            "The selected media file is not available on disk yet.",
            session_id=session_id,
            range_header=range_header,
            selected_file_name=selected_file_name,
            file_path=file_path,
            file_exists=file_exists,
            disk_size=disk_size,
            selected_length=file_size,
            near_tail=_is_near_tail_range(start, end, file_size),
            browser_range_blocked=bool(start >= disk_size),
        )

    if downloaded_bytes <= start and not complete:
        raise _stream_error(
            "file_not_ready",
            "The requested playback range has not been downloaded yet.",
            session_id=session_id,
            range_header=range_header,
            selected_file_name=selected_file_name,
            file_path=file_path,
            file_exists=file_exists,
            disk_size=disk_size,
            selected_length=file_size,
            near_tail=_is_near_tail_range(start, end, file_size),
            browser_range_blocked=bool(start >= disk_size),
            tail_probe_range=str(stream_readiness.get("tail_probe_range") or "").strip(),
            tail_probe_code=str(stream_readiness.get("tail_probe_code") or "").strip(),
        )
    if start >= file_size:
        return Response(status=416, headers={"Content-Range": f"bytes */{file_size}"})

    readable_end = file_size - 1 if complete else min(file_size - 1, max(downloaded_bytes - 1, start))
    available_end = min(end, readable_end)
    if available_end < start:
        raise _stream_error(
            "file_not_ready",
            "The selected media file is not ready for this playback range yet.",
            session_id=session_id,
            range_header=range_header,
            selected_file_name=selected_file_name,
            file_path=file_path,
            file_exists=file_exists,
            disk_size=disk_size,
            selected_length=file_size,
            near_tail=_is_near_tail_range(start, end, file_size),
            browser_range_blocked=bool(start >= disk_size),
            tail_probe_range=str(stream_readiness.get("tail_probe_range") or "").strip(),
            tail_probe_code=str(stream_readiness.get("tail_probe_code") or "").strip(),
        )
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
        try:
            with Path(file_path).open("rb") as handle:
                handle.seek(start)
                remaining = content_length
                while remaining > 0:
                    chunk = handle.read(min(STREAM_CHUNK_SIZE, remaining))
                    if not chunk:
                        break
                    remaining -= len(chunk)
                    yield chunk
        except OSError as exc:
            raise _stream_error(
                "range_read_failed",
                "The selected media file could not be read for this playback range.",
                session_id=session_id,
                range_header=range_header,
                selected_file_name=selected_file_name,
                file_path=file_path,
                file_exists=file_exists,
                disk_size=disk_size,
                selected_length=file_size,
                cause=str(exc),
            ) from exc

    return Response(generate(), status=status_code, headers=headers, direct_passthrough=True)


def _local_file_snapshot(file_path_text: str) -> tuple[str, bool, int]:
    file_path_value = str(file_path_text or "").strip()
    if not file_path_value:
        return "", False, 0
    candidate = Path(file_path_value)
    try:
        exists = candidate.exists()
        size = int(candidate.stat().st_size) if exists else 0
    except OSError:
        exists = False
        size = 0
    return str(candidate), exists, size


def _stream_error(
    code: str,
    message: str,
    *,
    session_id: str,
    range_header: str,
    selected_file_name: str,
    file_path: str,
    file_exists: bool,
    disk_size: int,
    selected_length: int,
    cause: str = "",
    near_tail: bool = False,
    browser_range_blocked: bool = False,
    tail_probe_range: str = "",
    tail_probe_code: str = "",
) -> PlaybackRuntimeError:
    _log_stream_failure(
        session_id=session_id,
        code=code,
        range_header=range_header,
        selected_file_name=selected_file_name,
        file_path=file_path,
        file_exists=file_exists,
        disk_size=disk_size,
        selected_length=selected_length,
        cause=cause or message,
        near_tail=near_tail,
        browser_range_blocked=browser_range_blocked,
        tail_probe_range=tail_probe_range,
        tail_probe_code=tail_probe_code,
    )
    return PlaybackRuntimeError(
        code,
        message,
        details={
            "session_id": str(session_id or "").strip(),
            "selected_file_name": selected_file_name,
            "requested_range": str(range_header or "").strip(),
            "disk_size": int(disk_size or 0),
            "selected_length": int(selected_length or 0),
            "near_tail": bool(near_tail),
            "browser_range_blocked": bool(browser_range_blocked),
            "tail_probe_range": str(tail_probe_range or "").strip(),
            "tail_probe_code": str(tail_probe_code or "").strip(),
        },
    )


def _augment_stream_error(
    exc: PlaybackRuntimeError,
    *,
    session_id: str,
    range_header: str,
    session: Mapping[str, Any] | None = None,
) -> PlaybackRuntimeError:
    session_payload = dict(session or {})
    selected_file = dict(session_payload.get("selected_file") or {})
    selected_file_name = str(selected_file.get("name") or session_payload.get("file_name") or "").strip()
    file_path, file_exists, disk_size = _local_file_snapshot(str(session_payload.get("file_path") or ""))
    code = exc.code if exc.code not in {"buffering", "stream_unavailable"} else "file_not_ready"
    message = exc.message
    selected_length = int(session_payload.get("file_size", 0) or 0)
    stream_readiness = dict(session_payload.get("stream_readiness") or {})
    range_start = _range_start(range_header)
    range_end = _range_end(range_header, selected_length)
    _log_stream_failure(
        session_id=session_id,
        code=code,
        range_header=range_header,
        selected_file_name=selected_file_name,
        file_path=file_path,
        file_exists=file_exists,
        disk_size=disk_size,
        selected_length=selected_length,
        cause=message,
        near_tail=_is_near_tail_range(range_start, range_end, selected_length),
        browser_range_blocked=bool(range_start >= disk_size and range_start > 0),
        tail_probe_range=str(stream_readiness.get("tail_probe_range") or "").strip(),
        tail_probe_code=str(stream_readiness.get("tail_probe_code") or "").strip(),
    )
    return PlaybackRuntimeError(
        code,
        message,
        details={
            "session_id": str(session_id or "").strip(),
            "selected_file_name": selected_file_name,
            "requested_range": str(range_header or "").strip(),
            "disk_size": int(disk_size or 0),
            "selected_length": selected_length,
            "near_tail": bool(_is_near_tail_range(range_start, range_end, selected_length)),
            "browser_range_blocked": bool(range_start >= disk_size and range_start > 0),
            "tail_probe_range": str(stream_readiness.get("tail_probe_range") or "").strip(),
            "tail_probe_code": str(stream_readiness.get("tail_probe_code") or "").strip(),
        },
    )


def _log_stream_failure(
    *,
    session_id: str,
    code: str,
    range_header: str,
    selected_file_name: str,
    file_path: str,
    file_exists: bool,
    disk_size: int,
    selected_length: int,
    cause: str,
    near_tail: bool,
    browser_range_blocked: bool,
    tail_probe_range: str,
    tail_probe_code: str,
) -> None:
    try:
        current_app.logger.warning(
            "runtime_stream_failure session_id=%s code=%s range=%s selected_file=%s file_path=%s file_exists=%s disk_size=%s selected_length=%s near_tail=%s browser_range_blocked=%s tail_probe_range=%s tail_probe_code=%s reason=%s",
            str(session_id or "").strip(),
            str(code or "").strip(),
            str(range_header or "").strip(),
            str(selected_file_name or "").strip(),
            str(file_path or "").strip(),
            bool(file_exists),
            int(disk_size or 0),
            int(selected_length or 0),
            bool(near_tail),
            bool(browser_range_blocked),
            str(tail_probe_range or "").strip(),
            str(tail_probe_code or "").strip(),
            str(cause or "").strip(),
        )
    except Exception:
        pass


def _range_start(range_header: str) -> int:
    if not range_header.startswith("bytes="):
        return 0
    start_text = range_header[6:].split(",", 1)[0].split("-", 1)[0].strip()
    try:
        return max(int(start_text), 0)
    except (TypeError, ValueError):
        return 0


def _range_end(range_header: str, total_size: int) -> int:
    resolved = _resolve_range(range_header, total_size)
    return int(resolved.get("end", 0) or 0) if resolved.get("ok") else 0


def _is_near_tail_range(start: int, end: int, total_size: int) -> bool:
    if total_size <= 0:
        return False
    tail_probe_bytes = min(total_size, 1024 * 1024)
    tail_start = max(total_size - tail_probe_bytes, 0)
    return max(int(start or 0), 0) >= tail_start or max(int(end or 0), 0) >= tail_start


def _resolve_range(range_header: str, total_size: int) -> dict[str, int | bool]:
    if total_size <= 0 or not range_header:
        return {"ok": True, "start": 0, "end": max(total_size - 1, 0)}
    if not range_header.startswith("bytes="):
        return {"ok": False, "start": 0, "end": 0}
    unit_range = range_header[6:].split(",", 1)[0].strip()
    if "-" not in unit_range:
        return {"ok": False, "start": 0, "end": 0}
    start_text, _, end_text = unit_range.partition("-")
    start_text = start_text.strip()
    end_text = end_text.strip()
    if not start_text and not end_text:
        return {"ok": False, "start": 0, "end": 0}
    if not start_text:
        try:
            suffix_length = int(end_text)
        except (TypeError, ValueError):
            return {"ok": False, "start": 0, "end": 0}
        if suffix_length <= 0:
            return {"ok": False, "start": 0, "end": 0}
        suffix_length = min(suffix_length, total_size)
        start = max(total_size - suffix_length, 0)
        return {"ok": True, "start": start, "end": max(total_size - 1, 0)}
    try:
        start = max(int(start_text), 0)
    except (TypeError, ValueError):
        return {"ok": False, "start": 0, "end": 0}
    try:
        end = min(int(end_text), total_size - 1) if end_text else total_size - 1
    except (TypeError, ValueError):
        return {"ok": False, "start": 0, "end": 0}
    if end < start:
        return {"ok": False, "start": start, "end": end}
    return {"ok": True, "start": start, "end": end}
