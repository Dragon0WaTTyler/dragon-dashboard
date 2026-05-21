from __future__ import annotations

import copy
import json
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from .constants import CACHE_BUCKETS, DEFAULT_RUNTIME_CACHE


def clone_json_compatible(value: Any) -> Any:
    return json.loads(json.dumps(value))


def load_json_file(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def save_json_file(path: Path, payload: Any) -> bool:
    path = Path(path)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_name(f"{path.name}.tmp")
        temp_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        temp_path.replace(path)
        return True
    except OSError as exc:
        if getattr(exc, "errno", None) in {28, 122}:
            print(f"[warn] Skipping write for {path.name}: disk quota exceeded")
            return False
        raise


class DragonCache:
    def __init__(
        self,
        path: Path,
        *,
        default_factory: Callable[[], Any] | None = None,
        buckets: tuple[str, ...] | None = None,
        lock: threading.Lock | None = None,
    ) -> None:
        self.path = Path(path)
        self.default_factory = default_factory or dict
        self.buckets = tuple(buckets or ())
        self.lock = lock or threading.Lock()

    def _normalize(self, data: Any) -> Any:
        default_value = self.default_factory()
        if isinstance(default_value, dict):
            data = data if isinstance(data, dict) else {}
            for bucket in self.buckets:
                data.setdefault(bucket, {})
            return data
        return data if data is not None else default_value

    def load(self) -> Any:
        with self.lock:
            payload = load_json_file(self.path, self.default_factory())
            return self._normalize(payload)

    def save(self, payload: Any) -> bool:
        with self.lock:
            return save_json_file(self.path, self._normalize(payload))

    def exists(self) -> bool:
        return self.path.exists()

    def invalidate(self, *keys: str) -> bool:
        if not keys:
            return self.save(self.default_factory())
        payload = self.load()
        if not isinstance(payload, dict):
            return False
        changed = False
        for key in keys:
            if key in payload:
                payload.pop(key, None)
                changed = True
        return self.save(payload) if changed else False

    def snapshot(self) -> Any:
        return copy.deepcopy(self.load())

    def index(self) -> dict[str, Any]:
        payload = self.load()
        try:
            stat_result = self.path.stat()
            updated_at = datetime.fromtimestamp(stat_result.st_mtime).isoformat()
            size_bytes = stat_result.st_size
        except OSError:
            updated_at = ""
            size_bytes = 0
        keys = list(payload.keys()) if isinstance(payload, dict) else []
        counts = {key: len(value) for key, value in payload.items() if isinstance(value, (dict, list))} if isinstance(payload, dict) else {}
        return {
            "path": str(self.path),
            "exists": self.path.exists(),
            "updated_at": updated_at,
            "size_bytes": size_bytes,
            "keys": keys,
            "counts": counts,
        }


def build_cache_store(path: Path, *, buckets: tuple[str, ...] | None = None) -> DragonCache:
    return DragonCache(path, default_factory=dict, buckets=buckets or CACHE_BUCKETS)


def build_runtime_cache() -> dict[str, Any]:
    return copy.deepcopy(DEFAULT_RUNTIME_CACHE)
