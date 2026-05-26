from __future__ import annotations

import hashlib
import threading
from datetime import datetime, timezone
from typing import Any, Mapping

from .runtime_state import evolve_runtime_state, normalize_runtime_state


class InMemoryRuntimeRegistry:
    def __init__(self) -> None:
        self._contexts: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()

    def create(self, context: Mapping[str, Any]) -> dict[str, Any]:
        payload = self._normalize_context(context)
        runtime_id = str(payload.get("runtime_id") or "").strip()
        if not runtime_id:
            raise ValueError("runtime_id is required")
        with self._lock:
            self._contexts[runtime_id] = payload
            return dict(self._contexts[runtime_id])

    def get(self, runtime_id: str) -> dict[str, Any] | None:
        normalized_id = str(runtime_id or "").strip()
        if not normalized_id:
            return None
        with self._lock:
            context = self._contexts.get(normalized_id)
            return dict(context) if isinstance(context, dict) else None

    def list(self) -> list[dict[str, Any]]:
        with self._lock:
            items = [dict(item) for item in self._contexts.values()]
        items.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
        return items

    def update(self, runtime_id: str, updates: Mapping[str, Any]) -> dict[str, Any] | None:
        normalized_id = str(runtime_id or "").strip()
        if not normalized_id:
            return None
        with self._lock:
            current = self._contexts.get(normalized_id)
            if not isinstance(current, dict):
                return None
            next_payload = dict(current)
            next_payload.update(dict(updates or {}))
            if "runtime_state" in updates:
                next_payload["runtime_state"] = evolve_runtime_state(current.get("runtime_state"), updates.get("runtime_state"))
            next_payload["updated_at"] = _now_iso8601()
            self._contexts[normalized_id] = self._normalize_context(next_payload)
            return dict(self._contexts[normalized_id])

    def delete(self, runtime_id: str) -> bool:
        normalized_id = str(runtime_id or "").strip()
        if not normalized_id:
            return False
        with self._lock:
            return self._contexts.pop(normalized_id, None) is not None

    def expire(self, runtime_id: str) -> dict[str, Any] | None:
        return self.update(runtime_id, {"runtime_state": "expired"})

    def build_runtime_id(self, *, session_id: str, source_fingerprint: str, runtime_profile: str) -> str:
        payload = "|".join(
            [
                str(session_id or "").strip().lower(),
                str(source_fingerprint or "").strip().lower(),
                str(runtime_profile or "").strip().lower(),
            ]
        ) or "runtime"
        return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:20]

    def _normalize_context(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        created_at = str(payload.get("created_at") or "").strip() or _now_iso8601()
        updated_at = str(payload.get("updated_at") or "").strip() or created_at
        return {
            "runtime_id": str(payload.get("runtime_id") or "").strip(),
            "session_id": str(payload.get("session_id") or "").strip(),
            "selected_source": dict(payload.get("selected_source") or {}),
            "runtime_profile": str(payload.get("runtime_profile") or "").strip(),
            "runtime_state": normalize_runtime_state(payload.get("runtime_state")),
            "startup_confidence": str(payload.get("startup_confidence") or "").strip(),
            "created_at": created_at,
            "updated_at": updated_at,
        }


_DEFAULT_RUNTIME_REGISTRY = InMemoryRuntimeRegistry()


def get_runtime_registry() -> InMemoryRuntimeRegistry:
    return _DEFAULT_RUNTIME_REGISTRY


def _now_iso8601() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
