from __future__ import annotations

import secrets
import shutil
import threading
import time
from pathlib import Path
from typing import Any, Mapping

from dragon.paths import CACHE_DIR

from ..runtime.identifiers import source_fingerprint
from .media_selection import select_playable_media_file
from .mime_helpers import guess_media_mime_type
from .runtime_health import (
    RuntimeSessionCleaner,
    build_recovery_decision,
    build_runtime_metrics,
    evaluate_buffer_health,
    evaluate_peer_health,
)
from .runtime_sessions import InMemoryPlaybackRuntimeSessions, PlaybackRuntimeSession, utc_now_iso
from .torrent_runtime import TorrentRuntimeError, WebTorrentRuntimeClient


INITIAL_READY_BYTES = 2 * 1024 * 1024
METADATA_TIMEOUT_MS = 20000
READY_TIMEOUT_MS = 45000
SESSION_INACTIVE_SECONDS = 30 * 60
FAILED_STARTUP_EXPIRY_SECONDS = 5 * 60


class PlaybackRuntimeError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = str(code or "").strip() or "playback_runtime_error"
        self.message = str(message or "").strip() or "Playback runtime failed"

    def to_dict(self) -> dict[str, str]:
        return {"code": self.code, "message": self.message}


class PlaybackRuntimeManager:
    def __init__(
        self,
        *,
        sessions: InMemoryPlaybackRuntimeSessions | None = None,
        torrent_client: WebTorrentRuntimeClient | None = None,
        runtime_root: Path | None = None,
        cleanup_interval_seconds: float = 60.0,
    ) -> None:
        self.sessions = sessions or InMemoryPlaybackRuntimeSessions()
        self.torrent_client = torrent_client or WebTorrentRuntimeClient()
        self.runtime_root = Path(runtime_root or (CACHE_DIR / "magnets" / "playback_runtime"))
        self.runtime_root.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._stagnant_polls: dict[str, int] = {}
        self.cleaner = RuntimeSessionCleaner(self, cleanup_interval_seconds=cleanup_interval_seconds)
        self.cleaner.start()

    def create_session(
        self,
        *,
        movie: Mapping[str, Any] | None,
        source: Mapping[str, Any] | None,
        stream_base_url: str,
    ) -> dict[str, Any]:
        movie_data = dict(movie or {})
        source_data = dict(source or {})
        magnet = str(source_data.get("magnet") or "").strip()
        if not magnet.lower().startswith("magnet:?xt=urn:btih:"):
            raise PlaybackRuntimeError("invalid_magnet", "Invalid magnet link for playback runtime.")

        session_id = secrets.token_urlsafe(12)
        download_dir = self.runtime_root / session_id
        session = PlaybackRuntimeSession(
            session_id=session_id,
            movie_id=str(movie_data.get("movie_id") or movie_data.get("entry_id") or "").strip(),
            movie_title=str(movie_data.get("title") or movie_data.get("name") or "").strip(),
            source_fingerprint=str(source_data.get("source_fingerprint") or source_fingerprint(source_data) or "").strip(),
            magnet=magnet,
            status="metadata_fetching",
            playback_state="preparing",
            stream_url=f"{stream_base_url.rstrip('/')}/api/runtime/stream/{session_id}",
            download_dir=str(download_dir),
            details={"selected_codec": str(source_data.get("codec") or "unknown").strip()},
        )
        self.sessions.save(session)

        try:
            started = self.torrent_client.start(
                session_id=session_id,
                magnet=magnet,
                download_dir=download_dir,
                metadata_timeout_ms=METADATA_TIMEOUT_MS,
            )
        except TorrentRuntimeError as exc:
            session.startup_failures += 1
            self._mark_failed(session, "metadata_timeout", str(exc))
            raise PlaybackRuntimeError("metadata_timeout", str(exc)) from exc

        selected_file = select_playable_media_file(started.get("files"))
        if not selected_file:
            self.teardown_session(session_id, reason="no_playable_media")
            raise PlaybackRuntimeError("no_playable_media", "No playable MP4 or MKV file was found in the torrent.")

        session.torrent_name = str(started.get("torrentName") or "").strip()
        session.status = "selecting_media"
        session.helper_pid = self.torrent_client.helper_pid()
        session.helper_running = self.torrent_client.helper_running()
        self.sessions.save(session)

        try:
            selection = self.torrent_client.select(
                session_id=session_id,
                file_index=int(selected_file.get("index", -1)),
                file_path=str(selected_file.get("path") or ""),
                min_ready_bytes=INITIAL_READY_BYTES,
                ready_timeout_ms=READY_TIMEOUT_MS,
            )
        except TorrentRuntimeError as exc:
            session.startup_failures += 1
            self._mark_failed(session, "stream_initialization_failed", str(exc))
            self.teardown_session(session_id, reason="stream_initialization_failed")
            raise PlaybackRuntimeError("stream_initialization_failed", str(exc)) from exc

        self._apply_status(session, dict(selection.get("status") or {}), selected_file=dict(selection.get("selectedFile") or selected_file))
        session.status = "ready_to_play" if session.ready else "buffering_video"
        session.playback_state = "ready" if session.ready else "buffering"
        return self._build_session_payload(self.sessions.save(session))

    def get_session(self, session_id: str, *, refresh: bool = False) -> dict[str, Any] | None:
        session = self.sessions.get(session_id)
        if session is None:
            return None
        payload = self.refresh_session(session_id, allow_recovery=False) if refresh else session.to_dict()
        return self._build_session_payload(payload)

    def refresh_session(self, session_id: str, *, allow_recovery: bool = True) -> dict[str, Any]:
        with self._lock:
            session = self.sessions.get(session_id)
            if session is None:
                raise PlaybackRuntimeError("unknown_session", "Playback session was not found.")
            previous_downloaded = int(session.downloaded_bytes or 0)
            try:
                status_payload = self.torrent_client.status(session_id=session_id)
            except TorrentRuntimeError as exc:
                if allow_recovery and self._attempt_recovery(session, reason=str(exc)):
                    status_payload = self.torrent_client.status(session_id=session_id)
                else:
                    self._mark_failed(session, "torrent_unavailable", str(exc))
                    raise PlaybackRuntimeError("torrent_unavailable", str(exc)) from exc

            status = dict(status_payload.get("status") or {})
            stagnant_polls = self._update_stagnation(session_id, previous_downloaded, status)
            self._apply_status(session, status)
            peer_health = evaluate_peer_health(session.to_dict(), status, stagnant_polls=stagnant_polls)
            buffer_health = evaluate_buffer_health(session.to_dict(), status)
            session.details["peer_health"] = peer_health
            session.details["buffer_health"] = buffer_health

            if allow_recovery:
                decision = build_recovery_decision(
                    session.to_dict(),
                    peer_health,
                    buffer_health,
                    helper_running=self.torrent_client.helper_running(),
                )
                if decision.should_retry and decision.action == "restart_session":
                    if self._attempt_recovery(session, reason=decision.reason):
                        status = dict(self.torrent_client.status(session_id=session_id).get("status") or {})
                        self._apply_status(session, status)
                        session.status = "recovering_stream"
                        session.playback_state = "recovering"

            payload = self.sessions.save(session)
            return self._build_session_payload(payload)

    def wait_for_bytes(self, session_id: str, start_offset: int, *, timeout_seconds: float = 12.0) -> dict[str, Any]:
        deadline = time.monotonic() + max(float(timeout_seconds or 0.0), 0.0)
        while True:
            session = self.refresh_session(session_id)
            self.mark_activity(session_id, playback_state="streaming", last_stream=True)
            if int(session.get("downloaded_bytes", 0) or 0) > int(start_offset or 0) or session.get("complete"):
                return session
            if time.monotonic() >= deadline:
                return session
            time.sleep(0.25)

    def mark_activity(self, session_id: str, *, playback_state: str | None = None, last_stream: bool = False) -> dict[str, Any] | None:
        session = self.sessions.get(session_id)
        if session is None:
            return None
        session.last_activity_at = utc_now_iso()
        if playback_state:
            session.playback_state = str(playback_state or "").strip() or session.playback_state
        if last_stream:
            session.last_stream_at = utc_now_iso()
        return self._build_session_payload(self.sessions.save(session))

    def teardown_session(self, session_id: str, *, reason: str) -> bool:
        with self._lock:
            session = self.sessions.get(session_id)
            if session is None:
                return False
            session.cleanup_reason = reason
            try:
                self.torrent_client.close(session_id=session_id)
            except TorrentRuntimeError:
                pass
            self.sessions.delete(session_id)
            self._stagnant_polls.pop(session_id, None)
            self._safe_remove_dir(Path(session.download_dir or ""))
            if not self.sessions.all():
                self.torrent_client.terminate()
            return True

    def cleanup_expired_sessions(self) -> None:
        now = time.time()
        active_ids = {session.session_id for session in self.sessions.all()}
        for session in self.sessions.all():
            last_activity = self._session_timestamp(session.last_activity_at, default=session.created_at)
            inactive_seconds = max(now - last_activity, 0.0)
            expiry_seconds = FAILED_STARTUP_EXPIRY_SECONDS if session.startup_failures > 0 or session.status == "stream_failed" else SESSION_INACTIVE_SECONDS
            if inactive_seconds >= expiry_seconds:
                self.teardown_session(session.session_id, reason="inactive_expired")
        for child in self.runtime_root.iterdir():
            if not child.is_dir() or child.name in active_ids:
                continue
            try:
                child_mtime = child.stat().st_mtime
            except OSError:
                continue
            if max(now - child_mtime, 0.0) >= SESSION_INACTIVE_SECONDS:
                self._safe_remove_dir(child)
        if not active_ids:
            self.torrent_client.terminate()

    def _attempt_recovery(self, session: PlaybackRuntimeSession, *, reason: str) -> bool:
        if session.recovery_attempts >= 2:
            return False
        session.recovery_attempts += 1
        session.status = "recovering_stream"
        session.playback_state = "recovering"
        self._append_error(session, reason)
        self.sessions.save(session)
        try:
            self.torrent_client.start(
                session_id=session.session_id,
                magnet=session.magnet,
                download_dir=Path(session.download_dir),
                metadata_timeout_ms=METADATA_TIMEOUT_MS,
            )
            self.torrent_client.select(
                session_id=session.session_id,
                file_index=int(session.file_index),
                file_path=session.file_path,
                min_ready_bytes=INITIAL_READY_BYTES,
                ready_timeout_ms=READY_TIMEOUT_MS,
            )
            session.helper_pid = self.torrent_client.helper_pid()
            session.helper_running = self.torrent_client.helper_running()
            return True
        except TorrentRuntimeError as exc:
            self._mark_failed(session, "stream_failed", str(exc))
            return False

    def _apply_status(self, session: PlaybackRuntimeSession, status: Mapping[str, Any], *, selected_file: Mapping[str, Any] | None = None) -> None:
        runtime_file = dict(selected_file or status.get("selectedFile") or {})
        session.helper_pid = self.torrent_client.helper_pid()
        session.helper_running = self.torrent_client.helper_running()
        session.downloaded_bytes = int(runtime_file.get("downloaded", session.downloaded_bytes) or 0)
        session.progress = float(status.get("progress", session.progress) or 0.0)
        session.download_speed = float(status.get("downloadSpeed", session.download_speed) or 0.0)
        session.peer_count = int(status.get("numPeers", session.peer_count) or 0)
        session.complete = bool(status.get("complete"))
        session.ready = session.complete or session.downloaded_bytes >= INITIAL_READY_BYTES
        session.error = str(status.get("error") or session.error or "").strip()
        session.file_index = int(runtime_file.get("index", session.file_index) or -1)
        session.file_name = str(runtime_file.get("name") or session.file_name or "").strip()
        session.file_path = str(runtime_file.get("localPath") or session.file_path or "").strip()
        session.file_size = int(runtime_file.get("length", session.file_size) or 0)
        session.mime_type = guess_media_mime_type(str(runtime_file.get("name") or runtime_file.get("path") or session.file_name or ""))

        if session.error:
            self._append_error(session, session.error)
        if session.complete:
            session.status = "completed"
            session.playback_state = "ready"
        elif session.ready:
            session.status = "ready_to_play"
            if session.playback_state not in {"playing", "paused"}:
                session.playback_state = "ready"
        elif session.peer_count > 0:
            session.status = "buffering_video"
            session.playback_state = "buffering"
        else:
            session.status = "connecting_peers"
            session.playback_state = "buffering"

        session.details["selected_file"] = {
            "index": session.file_index,
            "name": session.file_name,
            "path": str(runtime_file.get("path") or "").strip(),
            "length": session.file_size,
        }

    def _build_session_payload(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        response = dict(payload)
        response["runtime_metrics"] = build_runtime_metrics(response)
        return response

    def _update_stagnation(self, session_id: str, previous_downloaded: int, status: Mapping[str, Any]) -> int:
        downloaded = int(((status.get("selectedFile") or {}).get("downloaded")) or previous_downloaded)
        if downloaded <= previous_downloaded and not bool(status.get("complete")):
            self._stagnant_polls[session_id] = int(self._stagnant_polls.get(session_id, 0) or 0) + 1
        else:
            self._stagnant_polls[session_id] = 0
        return self._stagnant_polls[session_id]

    def _mark_failed(self, session: PlaybackRuntimeSession, code: str, message: str) -> None:
        session.status = "stream_failed"
        session.playback_state = "failed"
        session.error = message
        session.last_error_at = utc_now_iso()
        self._append_error(session, f"{code}:{message}")
        self.sessions.save(session)

    def _append_error(self, session: PlaybackRuntimeSession, message: str) -> None:
        text = str(message or "").strip()
        if not text:
            return
        errors = [item for item in session.runtime_errors if item]
        errors.append(text)
        session.runtime_errors = errors[-5:]

    def _session_timestamp(self, value: str, *, default: str) -> float:
        from datetime import datetime

        text = str(value or default or "").replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(text).timestamp()
        except ValueError:
            return time.time()

    def _safe_remove_dir(self, target: Path) -> None:
        try:
            resolved_root = self.runtime_root.resolve()
            resolved_target = target.resolve()
        except OSError:
            return
        if resolved_target == resolved_root:
            return
        try:
            resolved_target.relative_to(resolved_root)
        except ValueError:
            return
        if resolved_target.exists():
            shutil.rmtree(resolved_target, ignore_errors=True)
