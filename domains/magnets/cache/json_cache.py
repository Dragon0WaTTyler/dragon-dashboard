from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Any

from dragon.cache import load_json_file, save_json_file
from dragon.paths import CACHE_DIR


class JsonMagnetCache:
    def __init__(self, path: Path | None = None, *, ttl_seconds: int = 21600) -> None:
        self.path = Path(path or (CACHE_DIR / "magnets" / "search_cache.json"))
        self.ttl_seconds = max(int(ttl_seconds or 0), 0)
        self._lock = threading.Lock()

    def get(self, key: str) -> list[dict[str, Any]] | None:
        now = time.time()
        with self._lock:
            payload = self._load()
            entry = payload.get(key)
            if not isinstance(entry, dict):
                return None
            cached_at = float(entry.get("cached_at", 0) or 0)
            if self.ttl_seconds and cached_at and (now - cached_at) > self.ttl_seconds:
                return None
            results = entry.get("results")
            return results if isinstance(results, list) else None

    def set(self, key: str, results: list[dict[str, Any]]) -> None:
        with self._lock:
            payload = self._load()
            payload[key] = {
                "cached_at": time.time(),
                "results": list(results or []),
            }
            save_json_file(self.path, payload)

    def _load(self) -> dict[str, Any]:
        payload = load_json_file(self.path, {})
        return payload if isinstance(payload, dict) else {}
