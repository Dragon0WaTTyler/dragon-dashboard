from __future__ import annotations

import logging
import re
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
HEAD_PROBE_BYTES = 1024
MP4_FAST_START_SCAN_BYTES = 1024 * 1024
MP4_TAIL_PROBE_BYTES = 1024 * 1024
METADATA_TIMEOUT_MS = 20000
READY_TIMEOUT_MS = 45000
SESSION_INACTIVE_SECONDS = 30 * 60
FAILED_STARTUP_EXPIRY_SECONDS = 5 * 60
LOGGER = logging.getLogger(__name__)


def _runtime_tracker_issue_message(webtorrent: Mapping[str, Any] | None) -> str:
    payload = dict(webtorrent or {})
    messages: list[str] = []
    for key in ("trackerMessages", "warningMessages", "errorMessages"):
        raw_items = payload.get(key)
        if isinstance(raw_items, (list, tuple)):
            messages.extend(str(item or "").strip() for item in raw_items if str(item or "").strip())
    helper_error = str(payload.get("helperError") or "").strip()
    if helper_error:
        messages.append(helper_error)
    for message in messages:
        if re.search(r"tracker|announce|enotfound|timed out|timeout|dns", message, re.IGNORECASE):
            return message
    return ""


def build_runtime_source_quality(
    payload: Mapping[str, Any] | None,
    *,
    error_code: str = "",
    error_message: str = "",
    magnet_available: bool = False,
    source_kind: str = "",
) -> dict[str, Any]:
    runtime_payload = dict(payload or {})
    details = dict(runtime_payload.get("details") or {})
    stream_readiness = dict(runtime_payload.get("stream_readiness") or details.get("stream_readiness") or {})
    materialization = dict(runtime_payload.get("materialization") or details.get("materialization") or {})
    webtorrent = dict(runtime_payload.get("webtorrent") or details.get("webtorrent") or {})
    selected_file = dict(runtime_payload.get("selected_file") or details.get("selected_file") or {})
    normalized_error_code = str(error_code or runtime_payload.get("code") or "").strip()
    normalized_error_message = str(error_message or runtime_payload.get("error") or runtime_payload.get("message") or "").strip()
    normalized_source_kind = str(source_kind or details.get("source_type") or "").strip().lower()
    if not normalized_source_kind:
        normalized_source_kind = "local_file" if bool(details.get("local_file_test")) else "magnet"
    local_file_exists = bool(stream_readiness.get("local_file_exists") or materialization.get("local_file_exists"))
    local_file_size = int(stream_readiness.get("local_file_size", materialization.get("local_file_size", 0)) or 0)
    first_byte_readable = bool(stream_readiness.get("first_byte_readable") or materialization.get("first_byte_readable"))
    stream_openable = bool(stream_readiness.get("stream_openable"))
    browser_ready = bool(stream_readiness.get("stream_openable_for_browser", stream_openable))
    metadata_ready = bool(stream_readiness.get("metadata_ready"))
    selected_file_ready = bool(stream_readiness.get("selected_file_ready")) or bool(selected_file.get("name") or selected_file.get("path"))
    bytes_written = int(materialization.get("bytes_written", webtorrent.get("bytesWritten", 0)) or 0)
    downloaded_bytes = int(webtorrent.get("downloaded", runtime_payload.get("downloaded_bytes", 0)) or 0)
    first_data_received = bool(materialization.get("first_data_received", webtorrent.get("firstDataReceived")))
    writer_active = bool(materialization.get("writer_active", webtorrent.get("writerActive")))
    num_peers = int(webtorrent.get("numPeers", runtime_payload.get("peer_count", 0)) or 0)
    progress = float(webtorrent.get("progress", runtime_payload.get("progress", 0.0)) or 0.0)
    tracker_issue = _runtime_tracker_issue_message(webtorrent)
    has_data = bool(local_file_exists and local_file_size > 0 and first_byte_readable and max(bytes_written, downloaded_bytes, local_file_size) > 0)
    can_open_stream = bool(stream_openable and has_data and browser_ready)
    materialization_code = str(materialization.get("code") or "").strip()

    state = "metadata_failed"
    code = normalized_error_code or materialization_code or "metadata_failed"
    label = "Metadata Failed"
    message = normalized_error_message or "Dragon could not load enough runtime metadata to prepare playback."
    recommended_action = "Try another legal/personal/public-domain source."

    metadata_failure_codes = {
        "metadata_timeout",
        "magnet_metadata_timeout",
        "torrent_file_metadata_timeout",
        "torrent_file_metadata_failed",
        "torrent_file_missing",
        "torrent_file_empty",
        "torrent_file_read_failed",
        "torrent_file_add_failed",
        "torrent_file_no_files",
        "invalid_torrent_file",
        "invalid_magnet",
        "missing_magnet",
        "no_playable_media",
    }

    if can_open_stream:
        state = "playable"
        code = "playable"
        label = "Playable"
        message = "Source is playable. Open Stream is available."
        recommended_action = "Open Stream now."
    elif normalized_error_code in metadata_failure_codes or (not metadata_ready and not selected_file_ready):
        state = "metadata_failed"
        code = normalized_error_code or "metadata_failed"
        label = "Metadata Failed"
        message = normalized_error_message or "Dragon could not load torrent metadata or choose a playable file."
        recommended_action = (
            "Use external qBittorrent handoff or try another legal/personal/public-domain source."
            if magnet_available
            else "Try another legal/personal/public-domain source or a known-good .torrent file."
        )
    elif tracker_issue and not first_data_received and bytes_written <= 0 and downloaded_bytes <= 0:
        state = "tracker_unavailable"
        code = "external_recommended"
        label = "Tracker Unavailable"
        message = "Metadata loaded, but tracker reachability failed and no peers are sending data. Dragon cannot stream this source right now."
        recommended_action = (
            "Use external qBittorrent handoff or try another legal/personal/public-domain source."
            if magnet_available
            else "Try another legal/personal/public-domain source or a different .torrent file."
        )
    elif metadata_ready and selected_file_ready and num_peers <= 0 and downloaded_bytes <= 0 and bytes_written <= 0 and not first_data_received:
        state = "no_peers"
        code = "external_recommended"
        label = "No Peers"
        message = "Metadata loaded, but no reachable peers are sending data. Dragon cannot stream this source right now."
        recommended_action = (
            "Use external qBittorrent handoff or try another legal/personal/public-domain source."
            if magnet_available
            else "Try another legal/personal/public-domain source or a different .torrent file."
        )
    elif metadata_ready and selected_file_ready and num_peers > 0 and bytes_written <= 0 and not first_data_received:
        state = "peer_connected_but_no_data"
        code = "external_recommended"
        label = "Peer Connected, No Data"
        message = "Peers connected, but the selected file has not started delivering bytes yet."
        recommended_action = (
            "Wait briefly, then retry the probe. If no bytes arrive, use external qBittorrent handoff."
            if magnet_available
            else "Wait briefly, then retry the probe or try another legal/personal/public-domain source."
        )
    elif metadata_ready and selected_file_ready and (
        writer_active
        or local_file_size > 0
        or bytes_written > 0
        or downloaded_bytes > 0
        or progress > 0
    ):
        state = "buffering"
        code = materialization_code or "buffering"
        label = "Buffering"
        message = "Source is materializing. Wait and retry the probe."
        recommended_action = "Wait for local bytes to grow, then retry the stream probe."

    return {
        "state": state,
        "code": code,
        "label": label,
        "message": message,
        "can_open_stream": can_open_stream,
        "recommended_action": recommended_action,
        "show_qbittorrent_fallback": bool(magnet_available and not can_open_stream and state in {"metadata_failed", "no_peers", "tracker_unavailable", "peer_connected_but_no_data"}),
        "show_local_file_option": bool(not can_open_stream and normalized_source_kind != "local_file"),
        "show_torrent_file_option": bool(not can_open_stream and normalized_source_kind == "magnet"),
    }


class PlaybackRuntimeError(RuntimeError):
    def __init__(self, code: str, message: str, *, details: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code = str(code or "").strip() or "playback_runtime_error"
        self.message = str(message or "").strip() or "Playback runtime failed"
        self.details = dict(details or {})

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"code": self.code, "message": self.message}
        payload.update(self.details)
        return payload


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
        torrent_file_path = str(source_data.get("torrent_file_path") or "").strip()
        source_type = "magnet"
        torrent_input = magnet
        torrent_file_size = 0
        startup_error_code = "metadata_timeout"
        startup_error_message = "Torrent metadata timeout"
        if torrent_file_path:
            candidate = Path(torrent_file_path).expanduser()
            if str(candidate.suffix or "").lower() != ".torrent":
                raise PlaybackRuntimeError("invalid_torrent_file", "The torrent file path must end with .torrent.")
            if not candidate.exists() or not candidate.is_file():
                raise PlaybackRuntimeError("torrent_file_missing", "The torrent file path does not exist.")
            try:
                candidate = candidate.resolve(strict=True)
            except OSError as exc:
                raise PlaybackRuntimeError("torrent_file_missing", f"The torrent file path could not be resolved: {exc}") from exc
            try:
                torrent_file_size = int(candidate.stat().st_size)
            except OSError as exc:
                raise PlaybackRuntimeError("torrent_file_read_failed", f"The torrent file could not be read: {exc}") from exc
            if torrent_file_size <= 0:
                raise PlaybackRuntimeError("torrent_file_empty", "The torrent file is empty.")
            source_type = "torrent_file"
            torrent_input = str(candidate)
            startup_error_code = "torrent_file_metadata_timeout"
            startup_error_message = "Torrent file metadata could not be loaded"
        elif magnet.lower().startswith("magnet:?xt=urn:btih:"):
            source_type = "magnet"
            torrent_input = magnet
            startup_error_code = "metadata_timeout"
            startup_error_message = "Torrent metadata timeout"
        else:
            raise PlaybackRuntimeError("invalid_magnet", "Invalid magnet link for playback runtime.")

        session_id = secrets.token_urlsafe(12)
        download_dir = self.runtime_root / session_id
        session = PlaybackRuntimeSession(
            session_id=session_id,
            movie_id=str(movie_data.get("movie_id") or movie_data.get("entry_id") or "").strip(),
            movie_title=str(movie_data.get("title") or movie_data.get("name") or "").strip(),
            source_fingerprint=str(source_data.get("source_fingerprint") or source_fingerprint(source_data) or "").strip(),
            magnet=magnet if source_type == "magnet" else "",
            status="metadata_fetching",
            playback_state="preparing",
            stream_url=f"{stream_base_url.rstrip('/')}/api/runtime/stream/{session_id}",
            download_dir=str(download_dir),
            details={
                "selected_codec": str(source_data.get("codec") or "unknown").strip(),
                "source_type": source_type,
                "torrent_input": torrent_input,
                "torrent_file_path": str(torrent_input if source_type == "torrent_file" else "").strip(),
                "torrent_file_size": int(torrent_file_size or 0),
            },
        )
        self.sessions.save(session)

        try:
            started = self.torrent_client.start(
                session_id=session_id,
                torrent_input=torrent_input,
                source_kind=source_type,
                download_dir=download_dir,
                metadata_timeout_ms=METADATA_TIMEOUT_MS,
            )
        except TorrentRuntimeError as exc:
            session.startup_failures += 1
            mapped_code = self._classify_startup_error(source_type=source_type, fallback_code=startup_error_code, error_message=str(exc))
            self._mark_failed(session, mapped_code, str(exc))
            raise PlaybackRuntimeError(mapped_code, str(exc) or startup_error_message) from exc

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

    def create_local_file_session(
        self,
        *,
        file_path: str,
        title: str,
        stream_base_url: str,
    ) -> dict[str, Any]:
        normalized_file_path = str(file_path or "").strip()
        if not normalized_file_path:
            raise PlaybackRuntimeError("invalid_local_file", "A local file path is required for the stream self-test.")
        candidate = Path(normalized_file_path).expanduser()
        if not candidate.exists() or not candidate.is_file():
            raise PlaybackRuntimeError("invalid_local_file", "The local file path does not exist.")
        try:
            file_size = int(candidate.stat().st_size)
        except OSError as exc:
            raise PlaybackRuntimeError("invalid_local_file", f"The local file could not be read: {exc}") from exc
        if file_size <= 0:
            raise PlaybackRuntimeError("invalid_local_file", "The local file is empty.")

        session_id = secrets.token_urlsafe(12)
        session = PlaybackRuntimeSession(
            session_id=session_id,
            movie_id=f"local-file-{session_id}",
            movie_title=str(title or candidate.name).strip() or candidate.name,
            source_fingerprint=source_fingerprint({"title": title or candidate.name, "file_path": str(candidate)}),
            magnet="",
            torrent_name="Local File Stream Self-Test",
            status="buffering_video",
            playback_state="buffering",
            stream_url=f"{stream_base_url.rstrip('/')}/api/runtime/stream/{session_id}",
            download_dir=str(candidate.parent),
            file_index=0,
            file_name=candidate.name,
            file_path=str(candidate),
            file_size=file_size,
            mime_type=guess_media_mime_type(candidate.name),
            downloaded_bytes=file_size,
            progress=1.0,
            complete=True,
            details={"local_file_test": True},
        )
        self._refresh_local_file_session(session)
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
            if bool((session.details or {}).get("local_file_test")):
                self._refresh_local_file_session(session)
                payload = self.sessions.save(session)
                return self._build_session_payload(payload)
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
                torrent_input=str((session.details or {}).get("torrent_input") or session.magnet or "").strip(),
                source_kind=str((session.details or {}).get("source_type") or "magnet").strip() or "magnet",
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
        materialization = dict(status.get("materialization") or {})
        webtorrent = dict(status.get("webtorrent") or {})
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
        local_file_exists, local_file_size, first_byte_readable = self._inspect_local_file(session.file_path)
        helper_download_root = str(materialization.get("helperDownloadRoot") or status.get("downloadDir") or session.download_dir or "").strip()
        selected_relative_path = str(
            materialization.get("selectedFileRelativePath")
            or runtime_file.get("relativePath")
            or runtime_file.get("path")
            or ""
        ).strip()
        expected_absolute_path = str(
            materialization.get("selectedFileExpectedPath")
            or runtime_file.get("localPath")
            or session.file_path
            or ""
        ).strip()
        selected_file_prioritized = bool(materialization.get("selectedFilePrioritized") or session.file_index >= 0)
        materialization_code = str(materialization.get("code") or "").strip()
        materialization_reason = str(materialization.get("reason") or "").strip()
        materialization_state = self._materialization_state(
            local_file_exists=local_file_exists,
            local_file_size=local_file_size,
            first_byte_readable=first_byte_readable,
            materialization_code=materialization_code,
            selected_relative_path=selected_relative_path,
            helper_state=str(materialization.get("state") or "").strip(),
        )
        bytes_written = int(materialization.get("bytesWritten", 0) or 0)
        writer_active = bool(materialization.get("writerActive"))
        read_stream_started = bool(materialization.get("readStreamStarted"))
        read_stream_active = bool(materialization.get("readStreamActive"))
        first_data_received = bool(materialization.get("firstDataReceived"))
        last_data_at = str(materialization.get("lastDataAt") or "").strip()
        time_since_last_data_ms = int(materialization.get("timeSinceLastDataMs", 0) or 0)
        materialization_timeout_ms = int(materialization.get("materializationTimeoutMs", 0) or 0)
        available_local_bytes = max(local_file_size, bytes_written, int(session.downloaded_bytes or 0))
        mp4_readiness = self._inspect_browser_mp4_readiness(
            file_path=session.file_path,
            file_size=session.file_size,
            mime_type=session.mime_type,
            local_file_exists=local_file_exists,
            local_file_size=local_file_size,
            first_byte_readable=first_byte_readable,
        )
        session.ready = session.complete or (
            available_local_bytes > 0
            and local_file_exists
            and local_file_size > 0
            and first_byte_readable
            and bool(mp4_readiness["stream_openable_for_browser"])
        )

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
            "path": selected_relative_path,
            "length": session.file_size,
        }
        session.details["materialization"] = {
            "helper_download_root": helper_download_root,
            "selected_file_relative_path": selected_relative_path,
            "selected_file_expected_path": expected_absolute_path,
            "selected_file_prioritized": selected_file_prioritized,
            "local_file_exists": local_file_exists,
            "local_file_size": local_file_size,
            "first_byte_readable": first_byte_readable,
            "bytes_written": bytes_written,
            "writer_active": writer_active,
            "read_stream_started": read_stream_started,
            "read_stream_active": read_stream_active,
            "first_data_received": first_data_received,
            "last_data_at": last_data_at,
            "time_since_last_data_ms": time_since_last_data_ms,
            "materialization_timeout_ms": materialization_timeout_ms,
            "state": materialization_state,
            "code": materialization_code,
            "reason": materialization_reason,
        }
        session.details["webtorrent"] = webtorrent
        session.details["stream_readiness"] = {
            "metadata_ready": session.status != "metadata_fetching",
            "selected_file_ready": bool(session.file_name or session.file_path) and session.file_size > 0,
            "stream_openable": bool(
                local_file_exists
                and local_file_size > 0
                and first_byte_readable
                and available_local_bytes > 0
                and bool(mp4_readiness["stream_openable_for_browser"])
            ),
            "waiting_for_bytes": bool(
                not session.complete and (
                    not local_file_exists
                    or local_file_size <= 0
                    or not first_byte_readable
                    or available_local_bytes <= 0
                    or not bool(mp4_readiness["stream_openable_for_browser"])
                )
            ),
            "failed": bool(session.status == "stream_failed" or materialization_state == "materialization_failed"),
            "local_file_exists": local_file_exists,
            "local_file_size": local_file_size,
            "first_byte_readable": first_byte_readable,
            "head_ready": bool(mp4_readiness["head_ready"]),
            "tail_ready": bool(mp4_readiness["tail_ready"]),
            "fast_start_confirmed": bool(mp4_readiness["fast_start_confirmed"]),
            "tail_probe_range": str(mp4_readiness["tail_probe_range"] or "").strip(),
            "tail_probe_code": str(mp4_readiness["tail_probe_code"] or "").strip(),
            "stream_openable_for_browser": bool(mp4_readiness["stream_openable_for_browser"]),
        }
        LOGGER.info(
            "playback_runtime_materialization session_id=%s helper_download_root=%s selected_relative_path=%s expected_absolute_path=%s selected_file_exists=%s local_file_size=%s selected_file_prioritized=%s materialization_code=%s materialization_state=%s",
            session.session_id,
            helper_download_root,
            selected_relative_path,
            expected_absolute_path,
            local_file_exists,
            local_file_size,
            selected_file_prioritized,
            materialization_code,
            str((session.details.get("materialization") or {}).get("state") or "").strip(),
        )

    def _build_session_payload(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        response = dict(payload)
        materialization = dict((response.get("details") or {}).get("materialization") or {})
        response["selected_file"] = {
            "index": int(response.get("file_index", -1) or -1),
            "name": str(response.get("file_name") or "").strip(),
            "path": str(response.get("file_path") or "").strip(),
            "length": int(response.get("file_size", 0) or 0),
            "mime_type": str(response.get("mime_type") or "application/octet-stream"),
            "relative_path": str(materialization.get("selected_file_relative_path") or "").strip(),
            "expected_path": str(materialization.get("selected_file_expected_path") or response.get("file_path") or "").strip(),
        }
        response["state"] = self._public_state(response)
        response["runtime_metrics"] = build_runtime_metrics(response)
        response["stream_readiness"] = dict((response.get("details") or {}).get("stream_readiness") or {})
        response["materialization"] = materialization
        response["webtorrent"] = dict((response.get("details") or {}).get("webtorrent") or {})
        response["source_quality"] = build_runtime_source_quality(
            response,
            magnet_available=bool(str(response.get("magnet") or "").strip()),
            source_kind=str(((response.get("details") or {}).get("source_type") or "")).strip(),
        )
        return response

    def _public_state(self, payload: Mapping[str, Any]) -> str:
        status = str(payload.get("status") or "").strip()
        if status in {"ready_to_play", "completed"}:
            return "ready"
        if status == "stream_failed":
            return "failed"
        if status == "metadata_fetching":
            return "metadata_fetching"
        if status in {"selecting_media", "connecting_peers", "buffering_video"}:
            return "buffering"
        return status or str(payload.get("playback_state") or "").strip() or "preparing"

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

    def _classify_startup_error(self, *, source_type: str, fallback_code: str, error_message: str) -> str:
        message = str(error_message or "").strip().lower()
        if source_type != "torrent_file":
            return fallback_code
        if "torrent file is missing" in message:
            return "torrent_file_missing"
        if "torrent file is empty" in message:
            return "torrent_file_empty"
        if "torrent file could not be read" in message:
            return "torrent_file_read_failed"
        if "torrent file add failed" in message:
            return "torrent_file_add_failed"
        if "torrent file contains no files" in message:
            return "torrent_file_no_files"
        if "metadata timeout" in message:
            return "torrent_file_metadata_timeout"
        return fallback_code

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

    def _inspect_local_file(self, file_path: str) -> tuple[bool, int, bool]:
        candidate_path = str(file_path or "").strip()
        if not candidate_path:
            return False, 0, False
        try:
            file_candidate = Path(candidate_path)
            if not file_candidate.exists():
                return False, 0, False
            local_file_size = int(file_candidate.stat().st_size)
            if local_file_size <= 0:
                return True, local_file_size, False
            with file_candidate.open("rb") as handle:
                first_byte = handle.read(1)
            return True, local_file_size, bool(first_byte)
        except OSError:
            return False, 0, False

    def _refresh_local_file_session(self, session: PlaybackRuntimeSession) -> None:
        local_file_exists, local_file_size, first_byte_readable = self._inspect_local_file(session.file_path)
        session.downloaded_bytes = local_file_size
        session.complete = bool(local_file_exists and local_file_size > 0 and first_byte_readable)
        session.ready = session.complete
        session.progress = 1.0 if session.complete else 0.0
        session.status = "ready_to_play" if session.ready else "buffering_video"
        session.playback_state = "ready" if session.ready else "buffering"
        session.details["selected_file"] = {
            "index": session.file_index,
            "name": session.file_name,
            "path": session.file_path,
            "length": session.file_size,
        }
        session.details["materialization"] = {
            "helper_download_root": str(Path(session.file_path).parent if session.file_path else ""),
            "selected_file_relative_path": session.file_name,
            "selected_file_expected_path": session.file_path,
            "selected_file_prioritized": True,
            "local_file_exists": local_file_exists,
            "local_file_size": local_file_size,
            "first_byte_readable": first_byte_readable,
            "bytes_written": local_file_size,
            "writer_active": False,
            "read_stream_started": False,
            "read_stream_active": False,
            "first_data_received": bool(local_file_exists and local_file_size > 0 and first_byte_readable),
            "last_data_at": "",
            "time_since_last_data_ms": 0,
            "materialization_timeout_ms": 0,
            "state": "file_ready" if local_file_exists and local_file_size > 0 and first_byte_readable else "materializing",
            "code": "" if local_file_exists and local_file_size > 0 and first_byte_readable else "waiting_for_bytes",
            "reason": "",
        }
        session.details["stream_readiness"] = {
            "metadata_ready": True,
            "selected_file_ready": bool(session.file_name or session.file_path) and session.file_size > 0,
            "stream_openable": bool(local_file_exists and local_file_size > 0 and first_byte_readable),
            "waiting_for_bytes": not bool(local_file_exists and local_file_size > 0 and first_byte_readable),
            "failed": False,
            "local_file_exists": local_file_exists,
            "local_file_size": local_file_size,
            "first_byte_readable": first_byte_readable,
            "head_ready": bool(local_file_exists and local_file_size >= min(session.file_size or HEAD_PROBE_BYTES, HEAD_PROBE_BYTES) and first_byte_readable),
            "tail_ready": bool(local_file_exists and local_file_size >= session.file_size and first_byte_readable),
            "fast_start_confirmed": False,
            "tail_probe_range": self._tail_probe_range_text(session.file_size),
            "tail_probe_code": "" if local_file_exists and local_file_size >= session.file_size and first_byte_readable else "tail_not_ready",
            "stream_openable_for_browser": bool(local_file_exists and local_file_size > 0 and first_byte_readable),
        }

    def _materialization_state(
        self,
        *,
        local_file_exists: bool,
        local_file_size: int,
        first_byte_readable: bool,
        materialization_code: str,
        selected_relative_path: str,
        helper_state: str = "",
    ) -> str:
        normalized_helper_state = str(helper_state or "").strip()
        normalized_code = str(materialization_code or "").strip()
        if normalized_helper_state:
            return normalized_helper_state
        if normalized_code in {"materialization_timeout", "unsafe_path", "read_stream_error", "write_stream_error"}:
            return "materialization_failed"
        if normalized_code in {"no_peers", "no_data_received", "read_stream_waiting_for_pieces", "peer_connected_but_no_data", "tracker_unavailable"}:
            return "materialization_failed"
        if local_file_exists and local_file_size > 0 and first_byte_readable:
            return "file_ready"
        if local_file_exists and local_file_size > 0:
            return "materializing"
        if selected_relative_path:
            return "metadata_loaded_but_file_missing"
        return "idle"

    def _inspect_browser_mp4_readiness(
        self,
        *,
        file_path: str,
        file_size: int,
        mime_type: str,
        local_file_exists: bool,
        local_file_size: int,
        first_byte_readable: bool,
    ) -> dict[str, Any]:
        if str(mime_type or "").strip().lower() != "video/mp4":
            return {
                "head_ready": bool(local_file_exists and local_file_size > 0 and first_byte_readable),
                "tail_ready": True,
                "fast_start_confirmed": False,
                "tail_probe_range": "",
                "tail_probe_code": "",
                "stream_openable_for_browser": bool(local_file_exists and local_file_size > 0 and first_byte_readable),
            }
        head_ready = bool(local_file_exists and local_file_size >= min(max(file_size, 1), HEAD_PROBE_BYTES) and first_byte_readable)
        fast_start_confirmed = self._mp4_fast_start_confirmed(file_path, local_file_exists, local_file_size)
        tail_range = self._tail_probe_range(file_size)
        tail_ready = bool(local_file_exists and first_byte_readable and file_size > 0 and local_file_size >= file_size)
        tail_probe_code = ""
        if not tail_ready and not fast_start_confirmed:
            tail_probe_code = "tail_not_ready"
        return {
            "head_ready": head_ready,
            "tail_ready": tail_ready,
            "fast_start_confirmed": fast_start_confirmed,
            "tail_probe_range": self._tail_probe_range_text(file_size),
            "tail_probe_code": tail_probe_code,
            # Keep MP4 browser readiness conservative: if the browser later seeks
            # near the tail for metadata, we should stay buffering until those
            # bytes are actually materialized on disk.
            "stream_openable_for_browser": bool(head_ready and tail_ready),
            "tail_range": tail_range,
        }

    def _mp4_fast_start_confirmed(self, file_path: str, local_file_exists: bool, local_file_size: int) -> bool:
        if not local_file_exists or local_file_size <= 0:
            return False
        scan_bytes = min(local_file_size, MP4_FAST_START_SCAN_BYTES)
        if scan_bytes <= 0:
            return False
        try:
            with Path(file_path).open("rb") as handle:
                header = handle.read(scan_bytes)
            if not header:
                return False
            moov_index = header.find(b"moov")
            mdat_index = header.find(b"mdat")
            return moov_index >= 0 and (mdat_index < 0 or moov_index < mdat_index)
        except OSError:
            return False

    def _tail_probe_range(self, file_size: int) -> tuple[int, int]:
        normalized_size = max(int(file_size or 0), 0)
        if normalized_size <= 0:
            return (0, 0)
        probe_bytes = min(normalized_size, MP4_TAIL_PROBE_BYTES)
        start = max(normalized_size - probe_bytes, 0)
        end = max(normalized_size - 1, 0)
        return (start, end)

    def _tail_probe_range_text(self, file_size: int) -> str:
        start, end = self._tail_probe_range(file_size)
        if file_size <= 0:
            return ""
        return f"bytes={start}-{end}"
