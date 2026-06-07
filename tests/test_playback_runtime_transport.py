import tempfile
import unittest
from pathlib import Path
from unittest import mock

from flask import Flask

from domains.magnets.playback_runtime.media_selection import select_playable_media_file
from domains.magnets.playback_runtime.runtime_manager import PlaybackRuntimeError, PlaybackRuntimeManager
from domains.magnets.playback_runtime.runtime_sessions import InMemoryPlaybackRuntimeSessions
from domains.magnets.playback_runtime.stream_endpoint import build_stream_response
from domains.magnets.playback_runtime.torrent_runtime import TorrentRuntimeError, WEBTORRENT_HELPER_SOURCE


class FakeTorrentClient:
    def __init__(self):
        self.closed_sessions = []
        self.running = True
        self.last_start_kwargs = None
        self.temp_dir = tempfile.TemporaryDirectory()
        self.local_file_path = Path(self.temp_dir.name) / "Film.2026.1080p.mp4"
        self.local_file_path.write_bytes((b"\x00\x00\x00\x18ftypisom" + b"\x00\x00\x00\x08moov" + b"\x00\x00\x00\x08mdat") + (b"x" * ((4 * 1024 * 1024) - 28)))
        self.status_payload = {
            "status": {
                "progress": 0.24,
                "downloadSpeed": 1024 * 512,
                "numPeers": 18,
                "complete": False,
                "downloadDir": self.temp_dir.name,
                "materialization": {
                    "helperDownloadRoot": self.temp_dir.name,
                    "selectedFileRelativePath": "Film.2026.1080p.mp4",
                    "selectedFileExpectedPath": str(self.local_file_path),
                    "selectedFilePrioritized": True,
                    "localFileExists": True,
                    "localFileSize": 4 * 1024 * 1024,
                    "firstByteReadable": True,
                    "bytesWritten": 4 * 1024 * 1024,
                    "writerActive": False,
                    "state": "file_ready",
                    "code": "",
                    "reason": "",
                },
                "selectedFile": {
                    "index": 1,
                    "name": "Film.2026.1080p.mp4",
                    "path": "Film.2026.1080p.mp4",
                    "relativePath": "Film.2026.1080p.mp4",
                    "length": 4 * 1024 * 1024,
                    "downloaded": 4 * 1024 * 1024,
                    "localPath": str(self.local_file_path),
                },
            }
        }

    def start(self, **kwargs):
        self.last_start_kwargs = dict(kwargs)
        return {
            "torrentName": "Film Torrent",
            "files": [
                {"index": 0, "name": "sample.mkv", "path": "sample.mkv", "length": 80 * 1024 * 1024},
                {"index": 1, "name": "Film.2026.1080p.mp4", "path": "Film.2026.1080p.mp4", "length": 700 * 1024 * 1024},
            ],
        }

    def select(self, **kwargs):
        return {
            "selectedFile": dict(self.status_payload["status"]["selectedFile"]),
            "status": dict(self.status_payload["status"]),
        }

    def status(self, **kwargs):
        return self.status_payload

    def close(self, **kwargs):
        self.closed_sessions.append(kwargs.get("session_id"))
        return {"closed": True}

    def helper_pid(self):
        return 4242

    def helper_running(self):
        return self.running

    def terminate(self):
        self.running = False

    def __del__(self):
        try:
            self.temp_dir.cleanup()
        except Exception:
            pass


class PlaybackRuntimeTransportTests(unittest.TestCase):
    def test_webtorrent_helper_primes_output_file_before_tail_writer_opens(self):
        mkdir_index = WEBTORRENT_HELPER_SOURCE.index("await mkdir(path.dirname(resolvedPath.expectedPath), { recursive: true })")
        prime_index = WEBTORRENT_HELPER_SOURCE.index("const fileHandle = await openFile(resolvedPath.expectedPath, 'w')")
        tail_writer_index = WEBTORRENT_HELPER_SOURCE.index("const tailWriteStream = tailPriority.requested")

        self.assertLess(mkdir_index, prime_index)
        self.assertLess(prime_index, tail_writer_index)
        self.assertIn("flags: 'r+'", WEBTORRENT_HELPER_SOURCE)

    def test_media_selection_prefers_mp4_and_rejects_sample(self):
        selected = select_playable_media_file(
            [
                {"index": 0, "path": "Film.sample.mkv", "length": 200 * 1024 * 1024},
                {"index": 1, "path": "Film.2026.1080p.mkv", "length": 2_000_000_000},
                {"index": 2, "path": "Film.2026.1080p.mp4", "length": 1_500_000_000},
            ]
        )

        self.assertIsNotNone(selected)
        self.assertEqual(selected["index"], 2)
        self.assertTrue(str(selected["path"]).endswith(".mp4"))

    def test_runtime_manager_creates_stream_session(self):
        manager = PlaybackRuntimeManager(
            sessions=InMemoryPlaybackRuntimeSessions(),
            torrent_client=FakeTorrentClient(),
            runtime_root=Path(tempfile.gettempdir()) / "dragon-playback-tests",
            cleanup_interval_seconds=3600,
        )

        session = manager.create_session(
            movie={"movie_id": "film-1", "title": "Film"},
            source={"magnet": "magnet:?xt=urn:btih:1234567890ABCDEF1234567890ABCDEF12345678", "source_fingerprint": "src123"},
            stream_base_url="http://localhost:5000",
        )

        self.assertTrue(session["stream_url"].endswith(f"/api/runtime/stream/{session['session_id']}"))
        self.assertEqual(session["status"], "ready_to_play")
        self.assertEqual(session["state"], "ready")
        self.assertEqual(session["file_name"], "Film.2026.1080p.mp4")
        self.assertEqual(session["selected_file"]["path"], str(manager.torrent_client.local_file_path))
        self.assertEqual(session["selected_file"]["relative_path"], "Film.2026.1080p.mp4")
        self.assertEqual(session["selected_file"]["expected_path"], str(manager.torrent_client.local_file_path))
        self.assertEqual(session["runtime_metrics"]["selected_container"], "mp4")
        self.assertEqual(session["helper_pid"], 4242)
        self.assertTrue(session["stream_readiness"]["local_file_exists"])
        self.assertTrue(session["stream_readiness"]["first_byte_readable"])
        self.assertTrue(session["stream_readiness"]["stream_openable"])
        self.assertEqual(session["source_quality"]["state"], "playable")
        self.assertTrue(session["source_quality"]["can_open_stream"])
        self.assertTrue(session["stream_readiness"]["head_ready"])
        self.assertTrue(session["stream_readiness"]["tail_ready"])
        self.assertTrue(session["stream_readiness"]["fast_start_confirmed"])
        self.assertTrue(session["stream_readiness"]["initial_window_ready"])
        self.assertEqual(session["stream_readiness"]["initial_window_bytes_required"], 4 * 1024 * 1024)
        self.assertEqual(session["stream_readiness"]["initial_window_range"], f"bytes=0-{(4 * 1024 * 1024) - 1}")
        self.assertEqual(session["selected_file"]["container"], "mp4")
        self.assertEqual(session["selected_file"]["audio_codec_risk"], "unknown")
        self.assertEqual(session["selected_file"]["video_codec_risk"], "unknown")
        self.assertEqual(session["materialization"]["helper_download_root"], manager.torrent_client.temp_dir.name)
        self.assertEqual(session["materialization"]["selected_file_relative_path"], "Film.2026.1080p.mp4")
        self.assertEqual(session["materialization"]["selected_file_expected_path"], str(manager.torrent_client.local_file_path))
        self.assertTrue(session["materialization"]["selected_file_prioritized"])
        self.assertEqual(session["materialization"]["state"], "file_ready")
        self.assertEqual(session["materialization"]["bytes_written"], 4 * 1024 * 1024)
        self.assertFalse(session["materialization"]["writer_active"])

    def test_runtime_manager_accepts_local_torrent_file_input(self):
        manager = PlaybackRuntimeManager(
            sessions=InMemoryPlaybackRuntimeSessions(),
            torrent_client=FakeTorrentClient(),
            runtime_root=Path(tempfile.gettempdir()) / "dragon-playback-tests-torrent-file",
            cleanup_interval_seconds=3600,
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            torrent_path = Path(temp_dir) / "legal-sample.torrent"
            torrent_path.write_bytes(b"d8:announce0:e")
            session = manager.create_session(
                movie={"movie_id": "film-1", "title": "Film"},
                source={"torrent_file_path": str(torrent_path), "source_fingerprint": "src123"},
                stream_base_url="http://localhost:5000",
            )

        self.assertEqual(session["status"], "ready_to_play")
        self.assertEqual(
            str(manager.torrent_client.last_start_kwargs.get("torrent_input") or ""),
            str(torrent_path.resolve()),
        )
        self.assertEqual(
            str(manager.torrent_client.last_start_kwargs.get("source_kind") or ""),
            "torrent_file",
        )
        self.assertEqual(session["metadata_diagnostics"]["metadata_retry_count"], 0)
        self.assertEqual(session["metadata_diagnostics"]["metadata_timeout_ms"], 20000)

    def test_runtime_manager_retries_magnet_metadata_timeout_once_before_success(self):
        class RetryThenSuccessClient(FakeTorrentClient):
            def __init__(self):
                super().__init__()
                self.start_calls = 0
                self.close_calls = 0

            def start(self, **kwargs):
                self.start_calls += 1
                self.last_start_kwargs = dict(kwargs)
                if self.start_calls == 1:
                    raise TorrentRuntimeError("Torrent metadata timeout")
                return super().start(**kwargs)

            def close(self, **kwargs):
                self.close_calls += 1
                return super().close(**kwargs)

        client = RetryThenSuccessClient()
        manager = PlaybackRuntimeManager(
            sessions=InMemoryPlaybackRuntimeSessions(),
            torrent_client=client,
            runtime_root=Path(tempfile.gettempdir()) / "dragon-playback-tests-metadata-retry-success",
            cleanup_interval_seconds=3600,
        )

        session = manager.create_session(
            movie={"movie_id": "film-1", "title": "Film"},
            source={"magnet": "magnet:?xt=urn:btih:1234567890ABCDEF1234567890ABCDEF12345678", "source_fingerprint": "src123"},
            stream_base_url="http://localhost:5000",
        )

        self.assertEqual(client.start_calls, 2)
        self.assertEqual(client.close_calls, 1)
        self.assertEqual(client.last_start_kwargs["metadata_timeout_ms"], 25000)
        self.assertEqual(session["status"], "ready_to_play")
        self.assertEqual(session["metadata_diagnostics"]["metadata_retry_count"], 1)
        self.assertFalse(session["metadata_diagnostics"]["metadata_retry_in_progress"])
        self.assertFalse(session["metadata_diagnostics"]["metadata_retry_exhausted"])
        self.assertEqual(session["metadata_diagnostics"]["metadata_timeout_ms"], 25000)
        self.assertEqual(session["metadata_diagnostics"]["last_metadata_error"], "Torrent metadata timeout")
        self.assertEqual(session["webtorrent"]["metadataRetryCount"], 1)
        self.assertEqual(session["webtorrent"]["metadataTimeoutMs"], 25000)
        self.assertEqual(session["webtorrent"]["lastMetadataError"], "Torrent metadata timeout")

    def test_runtime_manager_fails_cleanly_after_metadata_retry_is_exhausted(self):
        class AlwaysMetadataTimeoutClient(FakeTorrentClient):
            def __init__(self):
                super().__init__()
                self.start_calls = 0
                self.close_calls = 0

            def start(self, **kwargs):
                self.start_calls += 1
                self.last_start_kwargs = dict(kwargs)
                raise TorrentRuntimeError("Torrent metadata timeout")

            def close(self, **kwargs):
                self.close_calls += 1
                return super().close(**kwargs)

        client = AlwaysMetadataTimeoutClient()
        sessions = InMemoryPlaybackRuntimeSessions()
        manager = PlaybackRuntimeManager(
            sessions=sessions,
            torrent_client=client,
            runtime_root=Path(tempfile.gettempdir()) / "dragon-playback-tests-metadata-retry-failure",
            cleanup_interval_seconds=3600,
        )

        with self.assertRaises(PlaybackRuntimeError) as ctx:
            manager.create_session(
                movie={"movie_id": "film-1", "title": "Film"},
                source={"magnet": "magnet:?xt=urn:btih:1234567890ABCDEF1234567890ABCDEF12345678", "source_fingerprint": "src123"},
                stream_base_url="http://localhost:5000",
            )

        self.assertEqual(ctx.exception.code, "metadata_timeout")
        self.assertEqual(str(ctx.exception), "Torrent metadata timeout")
        self.assertEqual(client.start_calls, 2)
        self.assertEqual(client.close_calls, 1)
        failed_session = sessions.all()[0].to_dict()
        self.assertEqual(failed_session["status"], "stream_failed")
        self.assertEqual(failed_session["metadata_retry_count"], 1)
        self.assertEqual(failed_session["metadata_timeout_ms"], 25000)
        self.assertEqual(failed_session["last_metadata_error"], "Torrent metadata timeout")
        self.assertEqual(
            failed_session["details"]["metadata_diagnostics"]["metadata_retry_count"],
            1,
        )
        self.assertTrue(
            failed_session["details"]["metadata_diagnostics"]["metadata_retry_exhausted"]
        )
        self.assertFalse(
            failed_session["details"]["metadata_diagnostics"]["metadata_retry_in_progress"]
        )

    def test_runtime_manager_rejects_empty_torrent_file_input(self):
        manager = PlaybackRuntimeManager(
            sessions=InMemoryPlaybackRuntimeSessions(),
            torrent_client=FakeTorrentClient(),
            runtime_root=Path(tempfile.gettempdir()) / "dragon-playback-tests-empty-torrent-file",
            cleanup_interval_seconds=3600,
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            torrent_path = Path(temp_dir) / "empty.torrent"
            torrent_path.write_bytes(b"")
            with self.assertRaises(PlaybackRuntimeError) as ctx:
                manager.create_session(
                    movie={"movie_id": "film-1", "title": "Film"},
                    source={"torrent_file_path": str(torrent_path), "source_fingerprint": "src123"},
                    stream_base_url="http://localhost:5000",
                )

        self.assertEqual(ctx.exception.code, "torrent_file_empty")

    def test_runtime_manager_rejects_non_torrent_file_input(self):
        manager = PlaybackRuntimeManager(
            sessions=InMemoryPlaybackRuntimeSessions(),
            torrent_client=FakeTorrentClient(),
            runtime_root=Path(tempfile.gettempdir()) / "dragon-playback-tests-invalid-torrent-file",
            cleanup_interval_seconds=3600,
        )
        with self.assertRaises(PlaybackRuntimeError) as ctx:
            manager.create_session(
                movie={"movie_id": "film-1", "title": "Film"},
                source={"torrent_file_path": "C:/tmp/not-a-torrent.txt", "source_fingerprint": "src123"},
                stream_base_url="http://localhost:5000",
            )

        self.assertEqual(ctx.exception.code, "invalid_torrent_file")

    def test_runtime_manager_maps_torrent_file_add_failure(self):
        class BrokenTorrentFileClient(FakeTorrentClient):
            def start(self, **kwargs):
                raise TorrentRuntimeError("Torrent file add failed: Invalid torrent identifier")

        manager = PlaybackRuntimeManager(
            sessions=InMemoryPlaybackRuntimeSessions(),
            torrent_client=BrokenTorrentFileClient(),
            runtime_root=Path(tempfile.gettempdir()) / "dragon-playback-tests-broken-torrent-file",
            cleanup_interval_seconds=3600,
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            torrent_path = Path(temp_dir) / "broken.torrent"
            torrent_path.write_bytes(b"not-a-real-torrent")
            with self.assertRaises(PlaybackRuntimeError) as ctx:
                manager.create_session(
                    movie={"movie_id": "film-1", "title": "Film"},
                    source={"torrent_file_path": str(torrent_path), "source_fingerprint": "src123"},
                    stream_base_url="http://localhost:5000",
                )

        self.assertEqual(ctx.exception.code, "torrent_file_add_failed")

    def test_runtime_manager_reports_no_peers_diagnostic_on_zero_peer_timeout(self):
        class ZeroPeerTorrentClient(FakeTorrentClient):
            def __init__(self):
                super().__init__()
                self.local_file_path.unlink(missing_ok=True)
                self.status_payload["status"]["numPeers"] = 0
                self.status_payload["status"]["downloadSpeed"] = 0
                self.status_payload["status"]["progress"] = 0
                self.status_payload["status"]["materialization"] = {
                    "helperDownloadRoot": self.temp_dir.name,
                    "selectedFileRelativePath": "Film.2026.1080p.mp4",
                    "selectedFileExpectedPath": str(self.local_file_path),
                    "selectedFilePrioritized": True,
                    "localFileExists": False,
                    "localFileSize": 0,
                    "firstByteReadable": False,
                    "bytesWritten": 0,
                    "writerActive": True,
                    "readStreamStarted": True,
                    "readStreamActive": True,
                    "firstDataReceived": False,
                    "lastDataAt": "",
                    "timeSinceLastDataMs": 0,
                    "materializationTimeoutMs": 45000,
                    "state": "materialization_failed",
                    "code": "no_peers",
                    "reason": "No peers were connected before materialization timed out.",
                }
                self.status_payload["status"]["webtorrent"] = {
                    "numPeers": 0,
                    "downloaded": 0,
                    "downloadSpeed": 0,
                    "progress": 0,
                    "ready": False,
                    "paused": False,
                    "torrentLength": 700 * 1024 * 1024,
                    "filesCount": 2,
                    "wiresCount": 0,
                    "selectedFileIndex": 1,
                    "selectedFileName": "Film.2026.1080p.mp4",
                    "selectedFileLength": 700 * 1024 * 1024,
                    "readStreamStarted": True,
                    "readStreamActive": True,
                    "firstDataReceived": False,
                    "bytesWritten": 0,
                    "lastDataAt": "",
                    "timeSinceLastDataMs": 0,
                    "materializationTimeoutMs": 45000,
                    "warningMessages": [],
                    "errorMessages": [],
                    "trackerMessages": [],
                }

        manager = PlaybackRuntimeManager(
            sessions=InMemoryPlaybackRuntimeSessions(),
            torrent_client=ZeroPeerTorrentClient(),
            runtime_root=Path(tempfile.gettempdir()) / "dragon-playback-tests-zero-peers",
            cleanup_interval_seconds=3600,
        )

        session = manager.create_session(
            movie={"movie_id": "film-1", "title": "Film"},
            source={"magnet": "magnet:?xt=urn:btih:1234567890ABCDEF1234567890ABCDEF12345678", "source_fingerprint": "src123"},
            stream_base_url="http://localhost:5000",
        )

        self.assertEqual(session["materialization"]["state"], "materialization_failed")
        self.assertEqual(session["materialization"]["code"], "no_peers")
        self.assertFalse(session["materialization"]["first_data_received"])
        self.assertEqual(session["webtorrent"]["numPeers"], 0)
        self.assertFalse(session["webtorrent"]["firstDataReceived"])
        self.assertEqual(session["source_quality"]["state"], "no_peers")
        self.assertFalse(session["source_quality"]["can_open_stream"])

    def test_runtime_manager_keeps_buffering_when_local_file_is_missing(self):
        class MissingLocalFileClient(FakeTorrentClient):
            def __init__(self):
                super().__init__()
                self.local_file_path.unlink(missing_ok=True)
                self.status_payload["status"]["materialization"] = {
                    "helperDownloadRoot": self.temp_dir.name,
                    "selectedFileRelativePath": "Film.2026.1080p.mp4",
                    "selectedFileExpectedPath": str(self.local_file_path),
                    "selectedFilePrioritized": True,
                    "localFileExists": False,
                    "localFileSize": 0,
                    "firstByteReadable": False,
                    "bytesWritten": 0,
                    "writerActive": True,
                    "state": "metadata_loaded_but_file_missing",
                    "code": "selected_file_missing",
                    "reason": "",
                }
                self.status_payload["status"]["selectedFile"]["localPath"] = str(self.local_file_path)

        manager = PlaybackRuntimeManager(
            sessions=InMemoryPlaybackRuntimeSessions(),
            torrent_client=MissingLocalFileClient(),
            runtime_root=Path(tempfile.gettempdir()) / "dragon-playback-tests-missing-file",
            cleanup_interval_seconds=3600,
        )

        session = manager.create_session(
            movie={"movie_id": "film-1", "title": "Film"},
            source={"magnet": "magnet:?xt=urn:btih:1234567890ABCDEF1234567890ABCDEF12345678", "source_fingerprint": "src123"},
            stream_base_url="http://localhost:5000",
        )

        self.assertEqual(session["status"], "buffering_video")
        self.assertEqual(session["state"], "buffering")
        self.assertFalse(session["stream_readiness"]["local_file_exists"])
        self.assertFalse(session["stream_readiness"]["stream_openable"])
        self.assertTrue(session["stream_readiness"]["waiting_for_bytes"])
        self.assertFalse(session["stream_readiness"]["head_ready"])
        self.assertFalse(session["stream_readiness"]["tail_ready"])
        self.assertEqual(session["materialization"]["state"], "metadata_loaded_but_file_missing")
        self.assertEqual(session["materialization"]["code"], "selected_file_missing")
        self.assertTrue(session["materialization"]["writer_active"])
        self.assertEqual(session["source_quality"]["state"], "peer_connected_but_no_data")
        self.assertFalse(session["source_quality"]["can_open_stream"])

    def test_runtime_manager_requires_readable_first_byte_for_stream_openable(self):
        class UnreadableLocalFileClient(FakeTorrentClient):
            def __init__(self):
                super().__init__()
                self.status_payload["status"]["materialization"] = {
                    "helperDownloadRoot": self.temp_dir.name,
                    "selectedFileRelativePath": "Film.2026.1080p.mp4",
                    "selectedFileExpectedPath": str(self.local_file_path),
                    "selectedFilePrioritized": True,
                    "localFileExists": True,
                    "localFileSize": 4 * 1024 * 1024,
                    "firstByteReadable": False,
                    "bytesWritten": 1024,
                    "writerActive": True,
                    "state": "materializing",
                    "code": "waiting_for_bytes",
                    "reason": "",
                }

        manager = PlaybackRuntimeManager(
            sessions=InMemoryPlaybackRuntimeSessions(),
            torrent_client=UnreadableLocalFileClient(),
            runtime_root=Path(tempfile.gettempdir()) / "dragon-playback-tests-unreadable-file",
            cleanup_interval_seconds=3600,
        )

        with mock.patch.object(manager, "_inspect_local_file", return_value=(True, 4 * 1024 * 1024, False)):
            session = manager.create_session(
                movie={"movie_id": "film-1", "title": "Film"},
                source={"magnet": "magnet:?xt=urn:btih:1234567890ABCDEF1234567890ABCDEF12345678", "source_fingerprint": "src123"},
                stream_base_url="http://localhost:5000",
            )

        self.assertFalse(session["stream_readiness"]["stream_openable"])
        self.assertTrue(session["stream_readiness"]["local_file_exists"])
        self.assertFalse(session["stream_readiness"]["first_byte_readable"])
        self.assertEqual(session["materialization"]["state"], "materializing")
        self.assertEqual(session["materialization"]["bytes_written"], 1024)

    def test_runtime_manager_buffers_mp4_when_only_head_bytes_are_ready(self):
        class HeadOnlyMp4Client(FakeTorrentClient):
            def __init__(self):
                super().__init__()
                self.local_file_path.write_bytes(b"\x00\x00\x00\x18ftypisom" + (b"x" * 4096))
                self.status_payload["status"]["selectedFile"]["length"] = 700 * 1024 * 1024
                self.status_payload["status"]["selectedFile"]["downloaded"] = 4 * 1024 * 1024

        manager = PlaybackRuntimeManager(
            sessions=InMemoryPlaybackRuntimeSessions(),
            torrent_client=HeadOnlyMp4Client(),
            runtime_root=Path(tempfile.gettempdir()) / "dragon-playback-tests-head-only",
            cleanup_interval_seconds=3600,
        )

        session = manager.create_session(
            movie={"movie_id": "film-1", "title": "Film"},
            source={"magnet": "magnet:?xt=urn:btih:1234567890ABCDEF1234567890ABCDEF12345678", "source_fingerprint": "src123"},
            stream_base_url="http://localhost:5000",
        )

        self.assertEqual(session["status"], "buffering_video")
        self.assertFalse(session["stream_readiness"]["stream_openable"])
        self.assertTrue(session["stream_readiness"]["head_ready"])
        self.assertFalse(session["stream_readiness"]["tail_ready"])
        self.assertEqual(session["stream_readiness"]["tail_probe_code"], "tail_not_ready")

    def test_runtime_manager_keeps_fast_start_mp4_buffering_until_tail_bytes_exist(self):
        class FastStartHeadOnlyMp4Client(FakeTorrentClient):
            def __init__(self):
                super().__init__()
                self.local_file_path.write_bytes(
                    b"\x00\x00\x00\x18ftypisom"
                    + b"\x00\x00\x00\x08moov"
                    + b"\x00\x00\x00\x08mdat"
                    + (b"x" * 4096)
                )
                self.status_payload["status"]["selectedFile"]["length"] = 700 * 1024 * 1024
                self.status_payload["status"]["selectedFile"]["downloaded"] = 4 * 1024 * 1024

        manager = PlaybackRuntimeManager(
            sessions=InMemoryPlaybackRuntimeSessions(),
            torrent_client=FastStartHeadOnlyMp4Client(),
            runtime_root=Path(tempfile.gettempdir()) / "dragon-playback-tests-fast-start-head-only",
            cleanup_interval_seconds=3600,
        )

        session = manager.create_session(
            movie={"movie_id": "film-1", "title": "Film"},
            source={"magnet": "magnet:?xt=urn:btih:1234567890ABCDEF1234567890ABCDEF12345678", "source_fingerprint": "src123"},
            stream_base_url="http://localhost:5000",
        )

        self.assertEqual(session["status"], "buffering_video")
        self.assertEqual(session["state"], "buffering")
        self.assertTrue(session["stream_readiness"]["head_ready"])
        self.assertFalse(session["stream_readiness"]["tail_ready"])
        self.assertTrue(session["stream_readiness"]["fast_start_confirmed"])
        self.assertFalse(session["stream_readiness"]["stream_openable"])
        self.assertFalse(session["source_quality"]["can_open_stream"])

    def test_runtime_manager_keeps_mp4_buffering_until_initial_playback_window_is_ready(self):
        class TailReadyButShortHeadClient(FakeTorrentClient):
            def __init__(self):
                super().__init__()
                self.local_file_path.write_bytes(
                    b"\x00\x00\x00\x18ftypisom"
                    + b"\x00\x00\x00\x08moov"
                    + b"\x00\x00\x00\x08mdat"
                    + (b"x" * ((4 * 1024 * 1024) - 28))
                )
                self.status_payload["status"]["selectedFile"]["length"] = 700 * 1024 * 1024
                self.status_payload["status"]["selectedFile"]["downloaded"] = 4 * 1024 * 1024
                self.status_payload["status"]["materialization"].update(
                    {
                        "tailPriorityRequested": True,
                        "tailWindowReady": True,
                        "tailWindowStart": (700 * 1024 * 1024) - (1024 * 1024),
                        "tailWindowEnd": (700 * 1024 * 1024) - 1,
                        "tailWindowLength": 1024 * 1024,
                        "tailWindowRange": f"bytes={(700 * 1024 * 1024) - (1024 * 1024)}-{(700 * 1024 * 1024) - 1}",
                    }
                )

        manager = PlaybackRuntimeManager(
            sessions=InMemoryPlaybackRuntimeSessions(),
            torrent_client=TailReadyButShortHeadClient(),
            runtime_root=Path(tempfile.gettempdir()) / "dragon-playback-tests-initial-window-wait",
            cleanup_interval_seconds=3600,
        )

        session = manager.create_session(
            movie={"movie_id": "film-1", "title": "Film"},
            source={"magnet": "magnet:?xt=urn:btih:1234567890ABCDEF1234567890ABCDEF12345678", "source_fingerprint": "src123"},
            stream_base_url="http://localhost:5000",
        )

        self.assertEqual(session["status"], "buffering_video")
        self.assertTrue(session["stream_readiness"]["head_ready"])
        self.assertTrue(session["stream_readiness"]["tail_ready"])
        self.assertFalse(session["stream_readiness"]["initial_window_ready"])
        self.assertEqual(session["stream_readiness"]["initial_window_bytes_required"], 8 * 1024 * 1024)
        self.assertEqual(session["stream_readiness"]["initial_window_range"], f"bytes=0-{(8 * 1024 * 1024) - 1}")
        self.assertFalse(session["stream_readiness"]["stream_openable"])

    def test_runtime_manager_marks_mp4_ready_when_head_tail_and_initial_window_are_ready(self):
        class TailReadyAndWideHeadClient(FakeTorrentClient):
            def __init__(self):
                super().__init__()
                self.local_file_path.write_bytes(
                    b"\x00\x00\x00\x18ftypisom"
                    + b"\x00\x00\x00\x08moov"
                    + b"\x00\x00\x00\x08mdat"
                    + (b"x" * ((8 * 1024 * 1024) - 28))
                )
                self.status_payload["status"]["selectedFile"]["length"] = 700 * 1024 * 1024
                self.status_payload["status"]["selectedFile"]["downloaded"] = 8 * 1024 * 1024
                self.status_payload["status"]["materialization"].update(
                    {
                        "localFileSize": 8 * 1024 * 1024,
                        "bytesWritten": 8 * 1024 * 1024,
                        "tailPriorityRequested": True,
                        "tailWindowReady": True,
                        "tailWindowStart": (700 * 1024 * 1024) - (1024 * 1024),
                        "tailWindowEnd": (700 * 1024 * 1024) - 1,
                        "tailWindowLength": 1024 * 1024,
                        "tailWindowRange": f"bytes={(700 * 1024 * 1024) - (1024 * 1024)}-{(700 * 1024 * 1024) - 1}",
                    }
                )

        manager = PlaybackRuntimeManager(
            sessions=InMemoryPlaybackRuntimeSessions(),
            torrent_client=TailReadyAndWideHeadClient(),
            runtime_root=Path(tempfile.gettempdir()) / "dragon-playback-tests-initial-window-ready",
            cleanup_interval_seconds=3600,
        )

        session = manager.create_session(
            movie={"movie_id": "film-1", "title": "Film"},
            source={"magnet": "magnet:?xt=urn:btih:1234567890ABCDEF1234567890ABCDEF12345678", "source_fingerprint": "src123"},
            stream_base_url="http://localhost:5000",
        )

        self.assertEqual(session["status"], "ready_to_play")
        self.assertTrue(session["stream_readiness"]["head_ready"])
        self.assertTrue(session["stream_readiness"]["tail_ready"])
        self.assertTrue(session["stream_readiness"]["initial_window_ready"])
        self.assertEqual(session["stream_readiness"]["initial_window_bytes_required"], 8 * 1024 * 1024)
        self.assertTrue(session["stream_readiness"]["stream_openable"])

    def test_runtime_manager_classifies_codec_risk_from_selected_filename(self):
        class AacClient(FakeTorrentClient):
            def __init__(self):
                super().__init__()
                target_name = "Film.2026.1080p.x264.AAC.mp4"
                renamed_path = self.local_file_path.with_name(target_name)
                self.local_file_path.replace(renamed_path)
                self.local_file_path = renamed_path
                self.status_payload["status"]["materialization"]["selectedFileRelativePath"] = target_name
                self.status_payload["status"]["materialization"]["selectedFileExpectedPath"] = str(self.local_file_path)
                self.status_payload["status"]["selectedFile"].update(
                    {
                        "name": target_name,
                        "path": target_name,
                        "relativePath": target_name,
                        "localPath": str(self.local_file_path),
                    }
                )

            def start(self, **kwargs):
                started = super().start(**kwargs)
                started["files"][1]["name"] = "Film.2026.1080p.x264.AAC.mp4"
                started["files"][1]["path"] = "Film.2026.1080p.x264.AAC.mp4"
                return started

        class Ac3Client(AacClient):
            def __init__(self):
                super().__init__()
                target_name = "Film.2026.1080p.HEVC.EAC3.mkv"
                renamed_path = self.local_file_path.with_name(target_name)
                self.local_file_path.replace(renamed_path)
                self.local_file_path = renamed_path
                self.status_payload["status"]["materialization"]["selectedFileRelativePath"] = target_name
                self.status_payload["status"]["materialization"]["selectedFileExpectedPath"] = str(self.local_file_path)
                self.status_payload["status"]["selectedFile"].update(
                    {
                        "name": target_name,
                        "path": target_name,
                        "relativePath": target_name,
                        "localPath": str(self.local_file_path),
                    }
                )

            def start(self, **kwargs):
                started = super().start(**kwargs)
                started["files"][1]["name"] = "Film.2026.1080p.HEVC.EAC3.mkv"
                started["files"][1]["path"] = "Film.2026.1080p.HEVC.EAC3.mkv"
                return started

        aac_manager = PlaybackRuntimeManager(
            sessions=InMemoryPlaybackRuntimeSessions(),
            torrent_client=AacClient(),
            runtime_root=Path(tempfile.gettempdir()) / "dragon-playback-tests-aac-risk",
            cleanup_interval_seconds=3600,
        )
        ac3_manager = PlaybackRuntimeManager(
            sessions=InMemoryPlaybackRuntimeSessions(),
            torrent_client=Ac3Client(),
            runtime_root=Path(tempfile.gettempdir()) / "dragon-playback-tests-ac3-risk",
            cleanup_interval_seconds=3600,
        )

        aac_session = aac_manager.create_session(
            movie={"movie_id": "film-1", "title": "Film"},
            source={"magnet": "magnet:?xt=urn:btih:1234567890ABCDEF1234567890ABCDEF12345678", "source_fingerprint": "src123"},
            stream_base_url="http://localhost:5000",
        )
        ac3_session = ac3_manager.create_session(
            movie={"movie_id": "film-1", "title": "Film"},
            source={"magnet": "magnet:?xt=urn:btih:1234567890ABCDEF1234567890ABCDEF12345678", "source_fingerprint": "src123"},
            stream_base_url="http://localhost:5000",
        )

        self.assertEqual(aac_session["selected_file"]["container"], "mp4")
        self.assertEqual(aac_session["selected_file"]["audio_codec_hint"], "aac")
        self.assertEqual(aac_session["selected_file"]["audio_codec_risk"], "low")
        self.assertEqual(aac_session["selected_file"]["video_codec_hint"], "h264")
        self.assertEqual(aac_session["selected_file"]["video_codec_risk"], "low")
        self.assertEqual(ac3_session["selected_file"]["container"], "mkv")
        self.assertEqual(ac3_session["selected_file"]["audio_codec_hint"], "eac3")
        self.assertEqual(ac3_session["selected_file"]["audio_codec_risk"], "high")
        self.assertEqual(ac3_session["selected_file"]["video_codec_hint"], "hevc")
        self.assertEqual(ac3_session["selected_file"]["video_codec_risk"], "high")

    def test_runtime_manager_exposes_tail_priority_window_diagnostics(self):
        class TailPriorityClient(FakeTorrentClient):
            def __init__(self):
                super().__init__()
                self.status_payload["status"]["selectedFile"]["length"] = 700 * 1024 * 1024
                self.status_payload["status"]["selectedFile"]["downloaded"] = 3 * 1024 * 1024
                self.status_payload["status"]["materialization"].update(
                    {
                        "tailPriorityRequested": True,
                        "tailPriorityReason": "",
                        "tailWindowStart": (700 * 1024 * 1024) - (1024 * 1024),
                        "tailWindowEnd": (700 * 1024 * 1024) - 1,
                        "tailWindowLength": 1024 * 1024,
                        "tailWindowRange": f"bytes={(700 * 1024 * 1024) - (1024 * 1024)}-{(700 * 1024 * 1024) - 1}",
                        "tailBytesWritten": 0,
                        "tailWriterActive": True,
                        "tailFirstDataReceived": False,
                        "tailLastDataAt": "",
                        "tailWindowReady": False,
                        "tailErrorCode": "",
                        "tailErrorReason": "",
                    }
                )
                self.status_payload["status"]["webtorrent"] = {
                    "tailPriorityRequested": True,
                    "tailWindowStart": (700 * 1024 * 1024) - (1024 * 1024),
                    "tailWindowEnd": (700 * 1024 * 1024) - 1,
                    "tailWindowLength": 1024 * 1024,
                    "tailBytesWritten": 0,
                    "tailWriterActive": True,
                    "tailFirstDataReceived": False,
                    "tailWindowReady": False,
                    "tailErrorCode": "",
                    "tailErrorReason": "",
                    "selectedFileStartPiece": 0,
                    "selectedFileEndPiece": 10,
                    "pieceLength": 256 * 1024,
                }

        manager = PlaybackRuntimeManager(
            sessions=InMemoryPlaybackRuntimeSessions(),
            torrent_client=TailPriorityClient(),
            runtime_root=Path(tempfile.gettempdir()) / "dragon-playback-tests-tail-priority-diagnostics",
            cleanup_interval_seconds=3600,
        )

        session = manager.create_session(
            movie={"movie_id": "film-1", "title": "Film"},
            source={"magnet": "magnet:?xt=urn:btih:1234567890ABCDEF1234567890ABCDEF12345678", "source_fingerprint": "src123"},
            stream_base_url="http://localhost:5000",
        )

        self.assertTrue(session["materialization"]["tail_priority_requested"])
        self.assertFalse(session["materialization"]["tail_window_ready"])
        self.assertTrue(session["materialization"]["tail_writer_active"])
        self.assertIn("bytes=", session["materialization"]["tail_window_range"])
        self.assertTrue(session["webtorrent"]["tailPriorityRequested"])
        self.assertEqual(session["webtorrent"]["pieceLength"], 256 * 1024)

    def test_runtime_manager_reports_unsafe_selected_path(self):
        class UnsafePathClient(FakeTorrentClient):
            def __init__(self):
                super().__init__()
                self.status_payload["status"]["materialization"] = {
                    "helperDownloadRoot": self.temp_dir.name,
                    "selectedFileRelativePath": "../escape/movie.mp4",
                    "selectedFileExpectedPath": "",
                    "selectedFilePrioritized": True,
                    "localFileExists": False,
                    "localFileSize": 0,
                    "firstByteReadable": False,
                    "bytesWritten": 0,
                    "writerActive": False,
                    "state": "materialization_failed",
                    "code": "unsafe_path",
                    "reason": "Selected file path resolves outside the helper download root.",
                }
                self.status_payload["status"]["selectedFile"]["path"] = "../escape/movie.mp4"
                self.status_payload["status"]["selectedFile"]["relativePath"] = "../escape/movie.mp4"
                self.status_payload["status"]["selectedFile"]["localPath"] = ""

        manager = PlaybackRuntimeManager(
            sessions=InMemoryPlaybackRuntimeSessions(),
            torrent_client=UnsafePathClient(),
            runtime_root=Path(tempfile.gettempdir()) / "dragon-playback-tests-unsafe-path",
            cleanup_interval_seconds=3600,
        )

        session = manager.create_session(
            movie={"movie_id": "film-1", "title": "Film"},
            source={"magnet": "magnet:?xt=urn:btih:1234567890ABCDEF1234567890ABCDEF12345678", "source_fingerprint": "src123"},
            stream_base_url="http://localhost:5000",
        )

        self.assertEqual(session["status"], "buffering_video")
        self.assertEqual(session["materialization"]["state"], "materialization_failed")
        self.assertEqual(session["materialization"]["code"], "unsafe_path")
        self.assertTrue(session["stream_readiness"]["failed"])
        self.assertEqual(session["selected_file"]["expected_path"], "")

    def test_runtime_manager_creates_local_file_session(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            video_path = Path(temp_dir) / "local.mp4"
            video_path.write_bytes(b"x" * 4096)
            manager = PlaybackRuntimeManager(
                sessions=InMemoryPlaybackRuntimeSessions(),
                torrent_client=FakeTorrentClient(),
                runtime_root=Path(tempfile.gettempdir()) / "dragon-playback-tests-local-file",
                cleanup_interval_seconds=3600,
            )

            session = manager.create_local_file_session(
                file_path=str(video_path),
                title="Local File Test",
                stream_base_url="http://localhost:5000",
            )

        self.assertEqual(session["status"], "ready_to_play")
        self.assertEqual(session["state"], "ready")
        self.assertTrue(session["stream_readiness"]["stream_openable"])
        self.assertTrue(session["stream_readiness"]["local_file_exists"])
        self.assertEqual(session["materialization"]["state"], "file_ready")
        self.assertTrue(session["stream_readiness"]["head_ready"])
        self.assertTrue(session["stream_readiness"]["tail_ready"])
        self.assertEqual(session["source_quality"]["state"], "playable")
        self.assertTrue(session["source_quality"]["can_open_stream"])

    def test_stream_endpoint_serves_range(self):
        app = Flask(__name__)
        with tempfile.TemporaryDirectory() as temp_dir:
            video_path = Path(temp_dir) / "movie.mp4"
            video_path.write_bytes(b"0123456789abcdefghijklmnopqrstuvwxyz")

            class StubManager:
                def wait_for_bytes(self, session_id, start_offset, timeout_seconds=12.0):
                    return {
                        "file_path": str(video_path),
                        "file_size": video_path.stat().st_size,
                        "mime_type": "video/mp4",
                        "downloaded_bytes": video_path.stat().st_size,
                        "complete": True,
                        "selected_file": {"name": "movie.mp4", "length": video_path.stat().st_size},
                        "file_name": "movie.mp4",
                    }

            with app.test_request_context(
                "/api/runtime/stream/session-1",
                headers={"Range": "bytes=5-14"},
            ):
                response = build_stream_response(StubManager(), "session-1")
                self.assertEqual(response.status_code, 206)
                self.assertEqual(response.headers["Content-Range"], f"bytes 5-14/{video_path.stat().st_size}")
                self.assertEqual(response.headers["Accept-Ranges"], "bytes")
                self.assertEqual(response.headers["Content-Length"], "10")
                self.assertEqual(response.headers["Content-Type"], "video/mp4")
                body = b"".join(response.response)
                self.assertEqual(body, b"56789abcde")

    def test_stream_endpoint_rejects_invalid_reverse_range(self):
        app = Flask(__name__)
        with tempfile.TemporaryDirectory() as temp_dir:
            video_path = Path(temp_dir) / "movie.mp4"
            video_path.write_bytes(b"0123456789abcdefghijklmnopqrstuvwxyz")

            class StubManager:
                def wait_for_bytes(self, session_id, start_offset, timeout_seconds=12.0):
                    return {
                        "file_path": str(video_path),
                        "file_size": video_path.stat().st_size,
                        "mime_type": "video/mp4",
                        "downloaded_bytes": video_path.stat().st_size,
                        "complete": True,
                        "selected_file": {"name": "movie.mp4", "length": video_path.stat().st_size},
                        "file_name": "movie.mp4",
                    }

            with app.test_request_context(
                "/api/runtime/stream/session-1",
                headers={"Range": "bytes=20-10"},
            ):
                response = build_stream_response(StubManager(), "session-1")
                self.assertEqual(response.status_code, 416)
                self.assertEqual(response.headers["Content-Range"], f"bytes */{video_path.stat().st_size}")
                self.assertEqual(response.headers["Accept-Ranges"], "bytes")

    def test_stream_endpoint_raises_when_active_video_is_missing(self):
        app = Flask(__name__)

        class StubManager:
            def wait_for_bytes(self, session_id, start_offset, timeout_seconds=12.0):
                return {
                    "file_path": "C:/missing/movie.mp4",
                    "file_size": 123,
                    "mime_type": "video/mp4",
                    "downloaded_bytes": 123,
                    "complete": True,
                    "selected_file": {"name": "movie.mp4", "length": 123},
                    "file_name": "movie.mp4",
                }

        with app.test_request_context("/api/runtime/stream/session-1"):
            with self.assertRaises(PlaybackRuntimeError) as error_context:
                build_stream_response(StubManager(), "session-1")
        self.assertEqual(error_context.exception.code, "file_not_found")
        self.assertIn("not available on disk yet", error_context.exception.message)

    def test_stream_endpoint_reports_file_not_ready_for_later_range(self):
        app = Flask(__name__)
        with tempfile.TemporaryDirectory() as temp_dir:
            video_path = Path(temp_dir) / "movie.mp4"
            video_path.write_bytes(b"0123456789abcdefghijklmnopqrstuvwxyz")

            class StubManager:
                def wait_for_bytes(self, session_id, start_offset, timeout_seconds=12.0):
                    return {
                        "file_path": str(video_path),
                        "file_size": 4 * 1024 * 1024,
                        "mime_type": "video/mp4",
                        "downloaded_bytes": 2048,
                        "complete": False,
                        "selected_file": {"name": "movie.mp4", "length": 4 * 1024 * 1024},
                        "file_name": "movie.mp4",
                    }

            with app.test_request_context(
                "/api/runtime/stream/session-1",
                headers={"Range": "bytes=1048576-1050623"},
            ):
                with self.assertRaises(PlaybackRuntimeError) as error_context:
                    build_stream_response(StubManager(), "session-1")
        self.assertEqual(error_context.exception.code, "file_not_ready")
        self.assertEqual(error_context.exception.details.get("session_id"), "session-1")
        self.assertEqual(error_context.exception.details.get("selected_file_name"), "movie.mp4")
        self.assertEqual(error_context.exception.details.get("requested_range"), "bytes=1048576-1050623")
        self.assertFalse(error_context.exception.details.get("near_tail"))

    def test_stream_endpoint_reports_browser_tail_range_blocked(self):
        app = Flask(__name__)
        with tempfile.TemporaryDirectory() as temp_dir:
            video_path = Path(temp_dir) / "movie.mp4"
            video_path.write_bytes(b"0123456789abcdefghijklmnopqrstuvwxyz")

            class StubManager:
                def wait_for_bytes(self, session_id, start_offset, timeout_seconds=12.0):
                    return {
                        "file_path": str(video_path),
                        "file_size": 890_203_389,
                        "mime_type": "video/mp4",
                        "downloaded_bytes": 188_743_221,
                        "complete": False,
                        "selected_file": {"name": "movie.mp4", "length": 890_203_389},
                        "file_name": "movie.mp4",
                        "stream_readiness": {
                            "tail_probe_range": "bytes=889154813-890203388",
                            "tail_probe_code": "tail_not_ready",
                        },
                    }

            with app.test_request_context(
                "/api/runtime/stream/session-1",
                headers={"Range": "bytes=890175488-"},
            ):
                with self.assertRaises(PlaybackRuntimeError) as error_context:
                    build_stream_response(StubManager(), "session-1")

        self.assertEqual(error_context.exception.code, "file_not_ready")
        self.assertTrue(error_context.exception.details.get("near_tail"))
        self.assertTrue(error_context.exception.details.get("browser_range_blocked"))
        self.assertEqual(error_context.exception.details.get("tail_probe_code"), "tail_not_ready")

    def test_stream_endpoint_serves_ready_tail_window_even_when_downloaded_bytes_are_low(self):
        app = Flask(__name__)
        with tempfile.TemporaryDirectory() as temp_dir:
            file_size = 2 * 1024 * 1024
            tail_window = 1024 * 1024
            tail_start = file_size - tail_window
            video_path = Path(temp_dir) / "movie.mp4"
            with video_path.open("wb") as handle:
                handle.seek(file_size - 1)
                handle.write(b"\x00")
            with video_path.open("r+b") as handle:
                handle.seek(tail_start)
                handle.write(b"z" * tail_window)

            class StubManager:
                def wait_for_bytes(self, session_id, start_offset, timeout_seconds=12.0):
                    return {
                        "file_path": str(video_path),
                        "file_size": file_size,
                        "mime_type": "video/mp4",
                        "downloaded_bytes": 2 * 1024,
                        "complete": False,
                        "selected_file": {"name": "movie.mp4", "length": file_size},
                        "file_name": "movie.mp4",
                        "materialization": {
                            "tail_window_ready": True,
                            "tail_window_start": tail_start,
                            "tail_window_end": file_size - 1,
                        },
                        "stream_readiness": {
                            "tail_probe_range": f"bytes={tail_start}-{file_size - 1}",
                            "tail_probe_code": "",
                        },
                    }

            with app.test_request_context(
                "/api/runtime/stream/session-1",
                headers={"Range": f"bytes={tail_start}-{tail_start + 1023}"},
            ):
                response = build_stream_response(StubManager(), "session-1")
                self.assertEqual(response.status_code, 206)
                self.assertEqual(response.headers["Content-Range"], f"bytes {tail_start}-{tail_start + 1023}/{file_size}")
                self.assertEqual(response.headers["Content-Type"], "video/mp4")
                body = b"".join(response.response)
                self.assertEqual(body, b"z" * 1024)

    def test_cleanup_expires_inactive_sessions_and_removes_runtime_dir(self):
        client = FakeTorrentClient()
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_root = Path(temp_dir) / "runtime"
            manager = PlaybackRuntimeManager(
                sessions=InMemoryPlaybackRuntimeSessions(),
                torrent_client=client,
                runtime_root=runtime_root,
                cleanup_interval_seconds=3600,
            )
            session = manager.create_session(
                movie={"movie_id": "film-1", "title": "Film"},
                source={"magnet": "magnet:?xt=urn:btih:1234567890ABCDEF1234567890ABCDEF12345678", "source_fingerprint": "src123"},
                stream_base_url="http://localhost:5000",
            )
            session_id = session["session_id"]
            (runtime_root / session_id).mkdir(parents=True, exist_ok=True)
            stale = manager.sessions.get(session_id)
            stale.last_activity_at = "2000-01-01T00:00:00+00:00"
            manager.sessions.save(stale)

            manager.cleanup_expired_sessions()

            self.assertIsNone(manager.sessions.get(session_id))
            self.assertFalse((runtime_root / session_id).exists())
            self.assertIn(session_id, client.closed_sessions)

    def test_refresh_session_recovers_when_helper_is_restarted(self):
        class RecoveringClient(FakeTorrentClient):
            def __init__(self):
                super().__init__()
                self.fail_next_status = True
                self.restart_calls = 0

            def status(self, **kwargs):
                if self.fail_next_status:
                    self.fail_next_status = False
                    self.running = False
                    raise TorrentRuntimeError("helper crashed")
                self.running = True
                return self.status_payload

            def start(self, **kwargs):
                self.restart_calls += 1
                self.running = True
                return super().start(**kwargs)

        client = RecoveringClient()
        manager = PlaybackRuntimeManager(
            sessions=InMemoryPlaybackRuntimeSessions(),
            torrent_client=client,
            runtime_root=Path(tempfile.gettempdir()) / "dragon-playback-tests-recovery",
            cleanup_interval_seconds=3600,
        )

        session = manager.create_session(
            movie={"movie_id": "film-1", "title": "Film"},
            source={"magnet": "magnet:?xt=urn:btih:1234567890ABCDEF1234567890ABCDEF12345678", "source_fingerprint": "src123"},
            stream_base_url="http://localhost:5000",
        )
        refreshed = manager.refresh_session(session["session_id"])

        self.assertGreaterEqual(client.restart_calls, 2)
        self.assertIn(refreshed["status"], {"ready_to_play", "recovering_stream"})
        self.assertGreaterEqual(refreshed["recovery_attempts"], 1)


if __name__ == "__main__":
    unittest.main()
