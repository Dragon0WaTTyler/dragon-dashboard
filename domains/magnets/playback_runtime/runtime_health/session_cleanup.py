from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..runtime_manager import PlaybackRuntimeManager


class RuntimeSessionCleaner:
    def __init__(
        self,
        manager: "PlaybackRuntimeManager",
        *,
        cleanup_interval_seconds: float = 60.0,
    ) -> None:
        self.manager = manager
        self.cleanup_interval_seconds = max(float(cleanup_interval_seconds or 0.0), 5.0)
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, name="playback-runtime-cleaner", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()

    def _run(self) -> None:
        while not self._stop_event.wait(self.cleanup_interval_seconds):
            try:
                self.manager.cleanup_expired_sessions()
            except Exception:
                time.sleep(1.0)
