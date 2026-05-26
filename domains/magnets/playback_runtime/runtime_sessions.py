from __future__ import annotations

import threading
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass(slots=True)
class PlaybackRuntimeSession:
    session_id: str
    movie_id: str
    movie_title: str
    source_fingerprint: str
    magnet: str
    torrent_name: str = ""
    status: str = "initializing"
    error: str = ""
    stream_url: str = ""
    download_dir: str = ""
    file_index: int = -1
    file_name: str = ""
    file_path: str = ""
    file_size: int = 0
    mime_type: str = "application/octet-stream"
    downloaded_bytes: int = 0
    progress: float = 0.0
    download_speed: float = 0.0
    peer_count: int = 0
    ready: bool = False
    complete: bool = False
    helper_pid: int | None = None
    helper_running: bool = False
    playback_state: str = "initializing"
    last_activity_at: str = field(default_factory=utc_now_iso)
    last_stream_at: str = ""
    last_error_at: str = ""
    cleanup_reason: str = ""
    startup_failures: int = 0
    recovery_attempts: int = 0
    runtime_errors: list[str] = field(default_factory=list)
    created_at: str = field(default_factory=utc_now_iso)
    updated_at: str = field(default_factory=utc_now_iso)
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class InMemoryPlaybackRuntimeSessions:
    def __init__(self) -> None:
        self._sessions: dict[str, PlaybackRuntimeSession] = {}
        self._lock = threading.RLock()

    def save(self, session: PlaybackRuntimeSession) -> dict[str, Any]:
        with self._lock:
            session.updated_at = utc_now_iso()
            self._sessions[session.session_id] = session
            return session.to_dict()

    def get(self, session_id: str) -> PlaybackRuntimeSession | None:
        with self._lock:
            return self._sessions.get(str(session_id or "").strip())

    def delete(self, session_id: str) -> PlaybackRuntimeSession | None:
        with self._lock:
            return self._sessions.pop(str(session_id or "").strip(), None)

    def all(self) -> list[PlaybackRuntimeSession]:
        with self._lock:
            return list(self._sessions.values())
