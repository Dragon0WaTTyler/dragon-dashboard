import tempfile
import unittest
from pathlib import Path

from flask import Flask

from domains.magnets.playback_runtime.media_selection import select_playable_media_file
from domains.magnets.playback_runtime.runtime_manager import PlaybackRuntimeManager
from domains.magnets.playback_runtime.runtime_sessions import InMemoryPlaybackRuntimeSessions
from domains.magnets.playback_runtime.stream_endpoint import build_stream_response
from domains.magnets.playback_runtime.torrent_runtime import TorrentRuntimeError


class FakeTorrentClient:
    def __init__(self):
        self.closed_sessions = []
        self.running = True
        self.status_payload = {
            "status": {
                "progress": 0.24,
                "downloadSpeed": 1024 * 512,
                "numPeers": 18,
                "complete": False,
                "selectedFile": {
                    "index": 1,
                    "name": "Film.2026.1080p.mp4",
                    "path": "Film.2026.1080p.mp4",
                    "length": 700 * 1024 * 1024,
                    "downloaded": 4 * 1024 * 1024,
                    "localPath": "C:/tmp/Film.2026.1080p.mp4",
                },
            }
        }

    def start(self, **kwargs):
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


class PlaybackRuntimeTransportTests(unittest.TestCase):
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
        self.assertEqual(session["file_name"], "Film.2026.1080p.mp4")
        self.assertEqual(session["runtime_metrics"]["selected_container"], "mp4")
        self.assertEqual(session["helper_pid"], 4242)

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
                    }

            with app.test_request_context(
                "/api/runtime/stream/session-1",
                headers={"Range": "bytes=5-14"},
            ):
                response = build_stream_response(StubManager(), "session-1")
                self.assertEqual(response.status_code, 206)
                self.assertEqual(response.headers["Content-Range"], f"bytes 5-14/{video_path.stat().st_size}")
                body = b"".join(response.response)
                self.assertEqual(body, b"56789abcde")

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
