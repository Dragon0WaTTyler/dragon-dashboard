import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import app as dragon_app
from flask import Response
from domains.magnets.playback_runtime.runtime_manager import PlaybackRuntimeError


TEST_MAGNET = "magnet:?xt=urn:btih:1234567890ABCDEF1234567890ABCDEF12345678"


class RuntimeDiagnosticUiTests(unittest.TestCase):
    def setUp(self):
        self.client = dragon_app.app.test_client()

    def test_runtime_test_page_renders(self):
        response = self.client.get("/runtime-test")

        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("Runtime Test", html)
        self.assertIn("Test Dragon Runtime", html)
        self.assertIn("Magnet Link", html)
        self.assertIn("Local File Stream Self-Test", html)
        self.assertIn("Torrent File Metadata Test", html)

    def test_runtime_test_missing_magnet_is_safe(self):
        response = self.client.post("/runtime-test", data={"title": "Test Film"})

        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("A magnet link is required.", html)
        self.assertIn("missing_magnet", html)
        self.assertIn("Copy Report", html)
        self.assertIn("Active result: magnet", html)
        self.assertEqual(html.count("Active result:"), 1)

    def test_runtime_test_missing_torrent_file_path_is_safe(self):
        response = self.client.post(
            "/runtime-test",
            data={"test_mode": "torrent-file", "title": "Test Film"},
        )

        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("torrent_file_missing", html)
        self.assertIn("A local .torrent file path is required.", html)
        self.assertIn("Active result: torrent-file", html)
        self.assertEqual(html.count("Active result:"), 1)

    def test_runtime_test_empty_torrent_file_is_safe(self):
        with TemporaryDirectory() as temp_dir:
            torrent_path = Path(temp_dir) / "empty.torrent"
            torrent_path.write_bytes(b"")
            response = self.client.post(
                "/runtime-test",
                data={"test_mode": "torrent-file", "torrent_file_path": str(torrent_path), "title": "Test Film"},
            )

        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("torrent_file_empty", html)
        self.assertIn("The torrent file is empty.", html)
        self.assertIn("Active result: torrent-file", html)
        self.assertEqual(html.count("Active result:"), 1)

    def test_runtime_test_rejects_non_torrent_path_safely(self):
        response = self.client.post(
            "/runtime-test",
            data={"test_mode": "torrent-file", "torrent_file_path": "C:/tmp/not-a-torrent.txt", "title": "Test Film"},
        )

        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("invalid_torrent_file", html)
        self.assertIn("must end with .torrent", html)
        self.assertIn("Active result: torrent-file", html)
        self.assertEqual(html.count("Active result:"), 1)

    def test_runtime_test_torrent_file_routes_into_runtime_manager(self):
        runtime_session = {
            "state": "buffering",
            "status": "buffering_video",
            "selected_file": {
                "path": "C:/tmp/Test.Film.2026.mp4",
                "name": "Test.Film.2026.mp4",
                "relative_path": "Test.Film.2026.mp4",
                "expected_path": "C:/tmp/Test.Film.2026.mp4",
            },
            "materialization": {
                "helper_download_root": "C:/tmp",
                "selected_file_relative_path": "Test.Film.2026.mp4",
                "selected_file_expected_path": "C:/tmp/Test.Film.2026.mp4",
                "selected_file_prioritized": True,
                "local_file_exists": False,
                "local_file_size": 0,
                "first_byte_readable": False,
                "bytes_written": 0,
                "writer_active": True,
                "state": "materialization_failed",
                "code": "no_peers",
                "reason": "No peers were connected before materialization timed out.",
            },
            "stream_readiness": {
                "metadata_ready": True,
                "selected_file_ready": True,
                "stream_openable": False,
                "waiting_for_bytes": True,
                "failed": False,
                "head_ready": False,
                "tail_ready": False,
                "tail_probe_range": "bytes=1024-2047",
                "tail_probe_code": "tail_not_ready",
            },
            "stream_url": "http://127.0.0.1:5000/api/runtime/stream/sess-1",
            "session_id": "sess-1",
            "webtorrent": {
                "sourceKind": "torrent_file",
                "torrentFilePath": "",
                "torrentFileExists": True,
                "torrentFileSize": 14,
                "torrentAddMode": "buffer",
                "clientAddStarted": True,
                "metadataEventReceived": True,
                "readyEventReceived": False,
                "helperError": "",
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
                "selectedFileName": "Test.Film.2026.mp4",
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
            },
        }
        with TemporaryDirectory() as temp_dir:
            torrent_path = Path(temp_dir) / "legal-sample.torrent"
            torrent_path.write_bytes(b"d8:announce0:e")
            with patch.object(dragon_app.PLAYBACK_RUNTIME_MANAGER, "create_session", return_value=runtime_session) as create_session_mock, \
                 patch.object(dragon_app.PLAYBACK_RUNTIME_MANAGER, "get_session", return_value=runtime_session), \
                 patch.object(
                     dragon_app,
                     "build_stream_response",
                     side_effect=PlaybackRuntimeError(
                         "file_not_found",
                         "The selected media file is not available on disk yet.",
                         details={"session_id": "sess-1", "selected_file_name": "Test.Film.2026.mp4"},
                     ),
                 ):
                response = self.client.post(
                    "/runtime-test",
                    data={
                        "test_mode": "torrent-file",
                        "torrent_file_path": str(torrent_path),
                        "title": "Torrent File Test",
                        "movie_id": "film-test",
                        "entry_id": "film-test",
                    },
                )

        self.assertEqual(response.status_code, 200)
        create_session_mock.assert_called_once()
        self.assertEqual(
            str(create_session_mock.call_args.kwargs["source"].get("torrent_file_path") or ""),
            str(torrent_path),
        )
        html = response.get_data(as_text=True)
        self.assertIn("torrent_file_loaded", html)
        self.assertIn("no_peers", html)
        self.assertIn("Runtime Source Quality", html)
        self.assertIn("No Peers", html)
        self.assertIn("external_recommended", html)
        self.assertIn("Stream Probe", html)
        self.assertIn("WebTorrent diagnostics", html)
        self.assertIn("num_peers", html)
        self.assertIn("source_kind", html)
        self.assertIn("torrent_add_mode", html)
        self.assertIn("client_add_started", html)
        self.assertIn("first_data_received", html)
        self.assertIn("Active result: torrent-file", html)
        self.assertEqual(html.count("Active result:"), 1)
        self.assertNotIn("Local File Result", html)
        self.assertEqual(html.count("Runtime Source Quality"), 1)

    def test_runtime_test_torrent_file_add_failure_is_shown_cleanly(self):
        with TemporaryDirectory() as temp_dir:
            torrent_path = Path(temp_dir) / "broken.torrent"
            torrent_path.write_bytes(b"not-a-real-torrent")
            with patch.object(
                dragon_app.PLAYBACK_RUNTIME_MANAGER,
                "create_session",
                side_effect=PlaybackRuntimeError("torrent_file_add_failed", "Torrent file add failed: Invalid torrent identifier"),
            ):
                response = self.client.post(
                    "/runtime-test",
                    data={
                        "test_mode": "torrent-file",
                        "torrent_file_path": str(torrent_path),
                        "title": "Torrent File Test",
                    },
                )

        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("torrent_file_add_failed", html)
        self.assertIn("Invalid torrent identifier", html)
        self.assertIn("Active result: torrent-file", html)
        self.assertEqual(html.count("Active result:"), 1)

    def test_runtime_test_tracker_unavailable_hides_open_stream(self):
        runtime_session = {
            "state": "buffering",
            "status": "connecting_peers",
            "selected_file": {
                "path": "C:/tmp/Test.Film.2026.mp4",
                "name": "Test.Film.2026.mp4",
                "relative_path": "Test.Film.2026.mp4",
                "expected_path": "C:/tmp/Test.Film.2026.mp4",
            },
            "materialization": {
                "helper_download_root": "C:/tmp",
                "selected_file_relative_path": "Test.Film.2026.mp4",
                "selected_file_expected_path": "C:/tmp/Test.Film.2026.mp4",
                "selected_file_prioritized": True,
                "local_file_exists": False,
                "local_file_size": 0,
                "first_byte_readable": False,
                "bytes_written": 0,
                "writer_active": True,
                "state": "materialization_failed",
                "code": "tracker_unavailable",
                "reason": "udp://tracker.example/announce timed out",
            },
            "stream_readiness": {
                "metadata_ready": True,
                "selected_file_ready": True,
                "stream_openable": False,
                "waiting_for_bytes": True,
                "failed": False,
                "head_ready": False,
                "tail_ready": False,
                "tail_probe_range": "bytes=1024-2047",
                "tail_probe_code": "tail_not_ready",
            },
            "stream_url": "http://127.0.0.1:5000/api/runtime/stream/sess-2",
            "session_id": "sess-2",
            "webtorrent": {
                "sourceKind": "torrent_file",
                "torrentAddMode": "buffer",
                "torrentFileExists": True,
                "torrentFileSize": 14,
                "clientAddStarted": True,
                "metadataEventReceived": True,
                "readyEventReceived": False,
                "helperError": "udp://tracker.example/announce timed out",
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
                "selectedFileName": "Test.Film.2026.mp4",
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
                "trackerMessages": ["udp://tracker.example/announce timed out", "getaddrinfo ENOTFOUND tracker.example"],
            },
        }
        with TemporaryDirectory() as temp_dir:
            torrent_path = Path(temp_dir) / "legal-sample.torrent"
            torrent_path.write_bytes(b"d8:announce0:e")
            with patch.object(dragon_app.PLAYBACK_RUNTIME_MANAGER, "create_session", return_value=runtime_session), \
                 patch.object(dragon_app.PLAYBACK_RUNTIME_MANAGER, "get_session", return_value=runtime_session):
                response = self.client.post(
                    "/runtime-test",
                    data={"test_mode": "torrent-file", "torrent_file_path": str(torrent_path), "title": "Torrent File Test"},
                )

        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("Tracker Unavailable", html)
        self.assertIn("external_recommended", html)
        self.assertNotIn(">Open Stream<", html)

    def test_runtime_test_post_renders_real_result_panel(self):
        with TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "Test.Film.2026.mp4"
            file_path.write_bytes(b"x" * 2048)
            runtime_session = {
                "state": "ready",
                "status": "ready_to_play",
                "selected_file": {
                    "path": str(file_path),
                    "name": "Test.Film.2026.mp4",
                    "relative_path": "Test.Film.2026.mp4",
                    "expected_path": str(file_path),
                },
                "materialization": {
                    "helper_download_root": temp_dir,
                    "selected_file_relative_path": "Test.Film.2026.mp4",
                    "selected_file_expected_path": str(file_path),
                    "selected_file_prioritized": True,
                    "local_file_exists": True,
                    "local_file_size": 2048,
                    "first_byte_readable": True,
                    "bytes_written": 2048,
                    "writer_active": False,
                    "read_stream_started": True,
                    "read_stream_active": False,
                    "first_data_received": True,
                    "last_data_at": "2026-06-05T21:00:00Z",
                    "time_since_last_data_ms": 1250,
                    "materialization_timeout_ms": 45000,
                    "state": "file_ready",
                    "code": "",
                    "reason": "",
                },
                "stream_readiness": {
                    "metadata_ready": True,
                    "selected_file_ready": True,
                    "stream_openable": True,
                    "waiting_for_bytes": False,
                    "failed": False,
                    "head_ready": True,
                    "tail_ready": True,
                    "tail_probe_range": "bytes=1024-2047",
                    "tail_probe_code": "",
                },
                "stream_url": "http://127.0.0.1:5000/api/runtime/stream/sess-1",
                "session_id": "sess-1",
                "webtorrent": {
                    "numPeers": 18,
                    "downloaded": 4 * 1024 * 1024,
                    "downloadSpeed": 1024 * 512,
                    "progress": 0.24,
                    "ready": False,
                    "paused": False,
                    "torrentLength": 700 * 1024 * 1024,
                    "filesCount": 2,
                    "wiresCount": 12,
                    "selectedFileIndex": 1,
                    "selectedFileName": "Test.Film.2026.mp4",
                    "selectedFileLength": 700 * 1024 * 1024,
                    "readStreamStarted": True,
                    "readStreamActive": False,
                    "firstDataReceived": True,
                    "bytesWritten": 2048,
                    "lastDataAt": "2026-06-05T21:00:00Z",
                    "timeSinceLastDataMs": 1250,
                    "materializationTimeoutMs": 45000,
                    "warningMessages": [],
                    "errorMessages": [],
                    "trackerMessages": [],
                },
            }

            probe_responses = [
                Response(
                    [b"x" * 1024],
                    status=206,
                    headers={
                        "Accept-Ranges": "bytes",
                        "Content-Range": "bytes 0-1023/2048",
                        "Content-Length": "1024",
                        "Content-Type": "video/mp4",
                    },
                ),
            ]

            with patch.object(dragon_app.PLAYBACK_RUNTIME_MANAGER, "create_session", return_value=runtime_session), \
                 patch.object(dragon_app.PLAYBACK_RUNTIME_MANAGER, "get_session", return_value=runtime_session), \
                 patch.object(dragon_app, "build_stream_response", side_effect=probe_responses):
                response = self.client.post(
                    "/runtime-test",
                    data={
                        "magnet": TEST_MAGNET,
                        "title": "Test Film",
                        "movie_id": "film-test",
                        "entry_id": "film-test",
                    },
                )

        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("Active result: magnet", html)
        self.assertEqual(html.count("Active result:"), 1)
        self.assertIn("Runtime Source Quality", html)
        self.assertIn("Playable", html)
        self.assertIn("Open Stream", html)
        self.assertIn("Open Watch Flow", html)
        self.assertIn("sess-1", html)
        self.assertIn("ready_to_play", html)
        self.assertIn("file_ready", html)
        self.assertIn("/watch?magnet=", html)
        self.assertIn("Test.Film.2026.mp4", html)
        self.assertIn("Stream Probe", html)
        self.assertIn("WebTorrent diagnostics", html)
        self.assertIn("bytes_written", html)
        self.assertIn("bytes 0-1023/2048", html)
        self.assertNotIn("Local File Result", html)

    def test_runtime_test_hides_open_stream_while_waiting_for_local_file(self):
        runtime_session = {
            "state": "buffering",
            "status": "buffering_video",
            "selected_file": {
                "path": "C:/tmp/Test.Film.2026.mp4",
                "name": "Test.Film.2026.mp4",
                "relative_path": "Test.Film.2026.mp4",
                "expected_path": "C:/tmp/Test.Film.2026.mp4",
            },
            "materialization": {
                "helper_download_root": "C:/tmp",
                "selected_file_relative_path": "Test.Film.2026.mp4",
                "selected_file_expected_path": "C:/tmp/Test.Film.2026.mp4",
                "selected_file_prioritized": True,
                "local_file_exists": False,
                "local_file_size": 0,
                "first_byte_readable": False,
                "bytes_written": 0,
                "writer_active": True,
                "state": "metadata_loaded_but_file_missing",
                "code": "selected_file_missing",
                "reason": "",
            },
            "stream_readiness": {
                "metadata_ready": True,
                "selected_file_ready": True,
                "stream_openable": False,
                "waiting_for_bytes": True,
                "failed": False,
                "local_file_exists": False,
                "local_file_size": 0,
                "first_byte_readable": False,
                "head_ready": False,
                "tail_ready": False,
                "tail_probe_range": "bytes=1024-2047",
                "tail_probe_code": "tail_not_ready",
            },
            "stream_url": "http://127.0.0.1:5000/api/runtime/stream/sess-1",
            "session_id": "sess-1",
            "webtorrent": {
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
                "selectedFileName": "Test.Film.2026.mp4",
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
            },
        }

        with patch.object(dragon_app.PLAYBACK_RUNTIME_MANAGER, "create_session", return_value=runtime_session), \
             patch.object(dragon_app.PLAYBACK_RUNTIME_MANAGER, "get_session", return_value=runtime_session), \
             patch.object(
                 dragon_app,
                 "build_stream_response",
                 side_effect=PlaybackRuntimeError(
                     "file_not_found",
                     "The selected media file is not available on disk yet.",
                     details={"session_id": "sess-1", "selected_file_name": "Test.Film.2026.mp4"},
                 ),
             ):
            response = self.client.post(
                "/runtime-test",
                data={
                    "magnet": TEST_MAGNET,
                    "title": "Test Film",
                    "movie_id": "film-test",
                    "entry_id": "film-test",
                },
            )

        html = response.get_data(as_text=True)
        self.assertIn("Active result: magnet", html)
        self.assertEqual(html.count("Active result:"), 1)
        self.assertNotIn("Open Stream", html)
        self.assertIn("no reachable peers are sending data", html.lower())
        self.assertIn("buffering_video", html)
        self.assertIn("selected_file_missing", html)
        self.assertIn("metadata_loaded_but_file_missing", html)
        self.assertIn("selected_file_missing", html)
        self.assertIn("tail_not_ready", html)

    def test_stream_probe_handles_missing_session_safely(self):
        with patch.object(dragon_app.PLAYBACK_RUNTIME_MANAGER, "get_session", return_value=None):
            probe = dragon_app._run_stream_probe("missing-session")

        self.assertFalse(probe["ok"])
        self.assertEqual(probe["code"], "session_missing")

    def test_stream_probe_reports_success_for_small_local_file(self):
        with TemporaryDirectory() as temp_dir:
            file_path = f"{temp_dir}/movie.mp4"
            runtime_session = {
                "session_id": "sess-1",
                "selected_file": {"path": file_path, "name": "movie.mp4", "length": 2048},
                "materialization": {
                    "helper_download_root": temp_dir,
                    "selected_file_relative_path": "movie.mp4",
                    "selected_file_expected_path": file_path,
                    "selected_file_prioritized": True,
                    "local_file_exists": True,
                    "local_file_size": 2048,
                    "first_byte_readable": True,
                    "bytes_written": 2048,
                    "writer_active": False,
                    "state": "file_ready",
                    "code": "",
                    "reason": "",
                },
            }
            Path(file_path).write_bytes(b"x" * 2048)
            probe_responses = [
                Response(
                    [b"x" * 1024],
                    status=206,
                    headers={
                        "Accept-Ranges": "bytes",
                        "Content-Range": "bytes 0-1023/2048",
                        "Content-Length": "1024",
                        "Content-Type": "video/mp4",
                    },
                ),
            ]

            with patch.object(dragon_app.PLAYBACK_RUNTIME_MANAGER, "get_session", return_value=runtime_session), \
                 patch.object(dragon_app, "build_stream_response", side_effect=probe_responses):
                probe = dragon_app._run_stream_probe("sess-1")

        self.assertTrue(probe["ok"])
        self.assertTrue(probe["local_path_exists"])
        self.assertEqual(probe["materialization_state"], "file_ready")
        self.assertEqual(probe["selected_file_expected_path"], file_path)
        self.assertEqual(probe["checks"][0]["status_code"], 206)

    def test_stream_probe_does_not_claim_success_for_zero_byte_local_file(self):
        with TemporaryDirectory() as temp_dir:
            file_path = f"{temp_dir}/movie.mp4"
            Path(file_path).touch()
            runtime_session = {
                "session_id": "sess-1",
                "selected_file": {"path": file_path, "name": "movie.mp4", "length": 2048},
                "materialization": {
                    "helper_download_root": temp_dir,
                    "selected_file_relative_path": "movie.mp4",
                    "selected_file_expected_path": file_path,
                    "selected_file_prioritized": True,
                    "local_file_exists": True,
                    "local_file_size": 0,
                    "first_byte_readable": False,
                    "bytes_written": 0,
                    "writer_active": True,
                    "state": "materialization_failed",
                    "code": "no_peers",
                    "reason": "No peers were connected before materialization timed out.",
                },
                "webtorrent": {
                    "numPeers": 0,
                    "downloaded": 0,
                    "downloadSpeed": 0,
                    "progress": 0,
                    "ready": False,
                    "paused": False,
                    "torrentLength": 2048,
                    "filesCount": 1,
                    "wiresCount": 0,
                    "selectedFileIndex": 0,
                    "selectedFileName": "movie.mp4",
                    "selectedFileLength": 2048,
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
                },
                "stream_readiness": {
                    "head_ready": False,
                    "tail_ready": False,
                    "tail_probe_range": "bytes=1024-2047",
                    "tail_probe_code": "tail_not_ready",
                },
            }

            with patch.object(dragon_app.PLAYBACK_RUNTIME_MANAGER, "get_session", return_value=runtime_session), \
                 patch.object(dragon_app, "build_stream_response", side_effect=AssertionError("stream probe should not run for zero-byte local files")):
                probe = dragon_app._run_stream_probe("sess-1")

        self.assertFalse(probe["ok"])
        self.assertEqual(probe["materialization_code"], "no_peers")
        self.assertEqual(probe["local_file_size"], 0)
        self.assertEqual(probe["checks"], [])
        self.assertFalse(probe.get("browser_range_blocked", True))

    def test_stream_probe_exposes_materialization_timeout_fields(self):
        runtime_session = {
            "session_id": "sess-1",
            "selected_file": {"path": "C:/tmp/movie.mp4", "name": "movie.mp4", "length": 2048},
            "materialization": {
                "helper_download_root": "C:/tmp",
                "selected_file_relative_path": "movie.mp4",
                "selected_file_expected_path": "C:/tmp/movie.mp4",
                "selected_file_prioritized": True,
                "local_file_exists": False,
                "local_file_size": 0,
                "first_byte_readable": False,
                "bytes_written": 0,
                "writer_active": False,
                "state": "materialization_failed",
                "code": "materialization_timeout",
                "reason": "Selected file materialization timeout",
            },
            "stream_readiness": {
                "head_ready": False,
                "tail_ready": False,
                "tail_probe_range": "bytes=889154813-890203388",
                "tail_probe_code": "tail_not_ready",
            },
        }

        with patch.object(dragon_app.PLAYBACK_RUNTIME_MANAGER, "get_session", return_value=runtime_session), \
             patch.object(
                 dragon_app,
                 "build_stream_response",
                side_effect=PlaybackRuntimeError(
                    "file_not_found",
                    "The selected media file is not available on disk yet.",
                    details={
                        "session_id": "sess-1",
                        "selected_file_name": "movie.mp4",
                        "requested_range": "bytes=890175488-",
                        "disk_size": 188743221,
                        "selected_length": 890203389,
                        "near_tail": True,
                        "browser_range_blocked": True,
                        "tail_probe_range": "bytes=889154813-890203388",
                        "tail_probe_code": "tail_not_ready",
                    },
                ),
             ):
            probe = dragon_app._run_stream_probe("sess-1")

        self.assertFalse(probe["ok"])
        self.assertEqual(probe["materialization_state"], "materialization_failed")
        self.assertEqual(probe["materialization_code"], "materialization_timeout")
        self.assertEqual(probe["selected_file_relative_path"], "movie.mp4")
        self.assertEqual(probe["bytes_written"], 0)
        self.assertFalse(probe["writer_active"])
        self.assertFalse(probe["browser_range_blocked"])
        self.assertEqual(probe["tail_probe_code"], "tail_not_ready")
        self.assertEqual(probe["checks"], [])

    def test_local_file_self_test_invalid_path_is_safe(self):
        response = self.client.post(
            "/runtime-test",
            data={
                "test_mode": "local-file",
                "local_file_path": "C:/definitely/missing/local-test.mp4",
                "local_file_title": "Local Test",
            },
        )

        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("Active result: local-file", html)
        self.assertEqual(html.count("Active result:"), 1)
        self.assertIn("invalid_local_file", html)
        self.assertIn("does not exist", html)

    def test_local_file_self_test_valid_temp_file_creates_session_and_probe(self):
        with TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "local.mp4"
            file_path.write_bytes(b"x" * 4096)

            response = self.client.post(
                "/runtime-test",
                data={
                    "test_mode": "local-file",
                    "local_file_path": str(file_path),
                    "local_file_title": "Local Test",
                },
            )

        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("Active result: local-file", html)
        self.assertEqual(html.count("Active result:"), 1)
        self.assertIn("local_file_test", html)
        self.assertIn("Runtime Source Quality", html)
        self.assertIn("Playable", html)
        self.assertIn("Open Stream", html)
        self.assertIn("Stream Probe", html)
        self.assertIn("bytes 0-1023/", html)

    def test_watch_browser_metadata_timeout_renders_html_error_page(self):
        with patch.object(
            dragon_app.PLAYBACK_RUNTIME_MANAGER,
            "create_session",
            side_effect=PlaybackRuntimeError("metadata_timeout", "Torrent metadata timeout"),
        ):
            response = self.client.get(
                "/watch",
                query_string={
                    "magnet": TEST_MAGNET,
                    "title": "Test Film",
                    "movie_id": "film-test",
                    "entry_id": "film-test",
                },
            )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.content_type, "text/html; charset=utf-8")
        html = response.get_data(as_text=True)
        self.assertIn("Dragon Runtime could not start this stream.", html)
        self.assertIn("metadata_timeout", html)
        self.assertIn("Torrent metadata timeout", html)
        self.assertIn("Metadata Failed", html)
        self.assertIn("Use external qBittorrent handoff", html)

    def test_watch_json_metadata_timeout_preserves_json_behavior(self):
        with patch.object(
            dragon_app.PLAYBACK_RUNTIME_MANAGER,
            "create_session",
            side_effect=PlaybackRuntimeError("metadata_timeout", "Torrent metadata timeout"),
        ):
            response = self.client.get(
                "/watch",
                query_string={
                    "magnet": TEST_MAGNET,
                    "title": "Test Film",
                    "movie_id": "film-test",
                    "entry_id": "film-test",
                    "format": "json",
                },
            )

        self.assertEqual(response.status_code, 400)
        self.assertTrue(response.is_json)
        self.assertEqual(
            response.get_json(),
            {"ok": False, "error": "Torrent metadata timeout", "code": "metadata_timeout"},
        )

    def test_api_runtime_stream_503_includes_stable_error_code(self):
        with patch.object(
            dragon_app,
            "build_stream_response",
            side_effect=PlaybackRuntimeError(
                "file_not_ready",
                "The requested playback range has not been downloaded yet.",
                details={"session_id": "sess-1", "selected_file_name": "movie.mp4"},
            ),
        ):
            response = self.client.get("/api/runtime/stream/sess-1", headers={"Accept": "application/json"})

        self.assertEqual(response.status_code, 503)
        self.assertEqual(
            response.get_json(),
            {
                "ok": False,
                "error": "The requested playback range has not been downloaded yet.",
                "code": "file_not_ready",
                "session_id": "sess-1",
                "selected_file_name": "movie.mp4",
                "requested_range": "",
                "disk_size": 0,
                "selected_length": 0,
                "near_tail": False,
                "browser_range_blocked": False,
                "tail_probe_range": "",
                "tail_probe_code": "",
            },
        )


if __name__ == "__main__":
    unittest.main()
