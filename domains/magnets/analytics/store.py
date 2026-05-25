from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dragon.cache import save_json_file
from dragon.paths import CACHE_DIR


_STORE_LOCK = threading.Lock()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def default_session_analytics_payload() -> dict[str, Any]:
    return {
        "version": 1,
        "events": [],
        "aggregate": {
            "event_counts": {},
            "preferred_runtime_frequency": {},
            "browser_failure_reasons": {},
            "external_success_patterns": {},
            "mobile_compatibility_patterns": {},
            "source_type_stats": {},
            "codec_stats": {},
            "release_pattern_stats": {},
            "runtime_stats": {},
            "invalid_magnet_frequency": {
                "invalid": 0,
                "total": 0,
            },
            "high_bandwidth_failure_frequency": {
                "failed": 0,
                "total": 0,
            },
        },
        "meta": {
            "created_at": utc_now_iso(),
            "updated_at": utc_now_iso(),
            "recovery_count": 0,
            "last_recovered_at": "",
            "last_corrupted_path": "",
        },
    }


class SessionAnalyticsStore:
    def __init__(self, path: Path | None = None, *, max_events: int = 250) -> None:
        self.path = Path(path or (CACHE_DIR / "magnets" / "session_analytics.json"))
        self.max_events = max(int(max_events or 0), 25)
        self._lock = _STORE_LOCK

    def load(self) -> dict[str, Any]:
        with self._lock:
            return self._load()

    def save(self, payload: dict[str, Any]) -> bool:
        normalized = self._normalize_payload(payload)
        normalized["meta"]["updated_at"] = utc_now_iso()
        with self._lock:
            return save_json_file(self.path, normalized)

    def update(self, updater) -> dict[str, Any]:
        with self._lock:
            payload = self._load()
            updated = updater(payload)
            normalized = self._normalize_payload(updated)
            normalized["meta"]["updated_at"] = utc_now_iso()
            save_json_file(self.path, normalized)
            return normalized

    def _load(self) -> dict[str, Any]:
        default = default_session_analytics_payload()
        if not self.path.exists():
            return default
        try:
            raw_text = self.path.read_text(encoding="utf-8")
            payload = json.loads(raw_text)
        except Exception:
            recovered = self._recover_from_corruption(default)
            save_json_file(self.path, recovered)
            return recovered
        return self._normalize_payload(payload)

    def _recover_from_corruption(self, default: dict[str, Any]) -> dict[str, Any]:
        recovered = self._normalize_payload(default)
        recovered["meta"]["recovery_count"] = int(recovered["meta"].get("recovery_count", 0) or 0) + 1
        recovered["meta"]["last_recovered_at"] = utc_now_iso()
        recovered["meta"]["last_corrupted_path"] = str(self.path)
        try:
            corrupt_path = self.path.with_suffix(f"{self.path.suffix}.corrupt")
            if self.path.exists():
                self.path.replace(corrupt_path)
        except OSError:
            pass
        return recovered

    def _normalize_payload(self, payload: Any) -> dict[str, Any]:
        default = default_session_analytics_payload()
        if not isinstance(payload, dict):
            payload = {}
        events = payload.get("events")
        if not isinstance(events, list):
            events = []
        normalized_events = [dict(item) for item in events if isinstance(item, dict)][-self.max_events :]

        aggregate = payload.get("aggregate")
        if not isinstance(aggregate, dict):
            aggregate = {}
        normalized_aggregate = dict(default["aggregate"])
        normalized_aggregate.update({key: value for key, value in aggregate.items() if isinstance(value, (dict, int, float, str, list))})
        for key in (
            "event_counts",
            "preferred_runtime_frequency",
            "browser_failure_reasons",
            "external_success_patterns",
            "mobile_compatibility_patterns",
            "source_type_stats",
            "codec_stats",
            "release_pattern_stats",
            "runtime_stats",
        ):
            normalized_aggregate[key] = self._dict_of_dicts_or_scalars(normalized_aggregate.get(key))
        normalized_aggregate["invalid_magnet_frequency"] = self._counter_pair(
            normalized_aggregate.get("invalid_magnet_frequency")
        )
        normalized_aggregate["high_bandwidth_failure_frequency"] = self._counter_pair(
            normalized_aggregate.get("high_bandwidth_failure_frequency"),
            first_key="failed",
            second_key="total",
        )

        meta = payload.get("meta")
        if not isinstance(meta, dict):
            meta = {}
        normalized_meta = dict(default["meta"])
        normalized_meta.update({
            "created_at": str(meta.get("created_at") or normalized_meta["created_at"]).strip(),
            "updated_at": str(meta.get("updated_at") or normalized_meta["updated_at"]).strip(),
            "recovery_count": int(meta.get("recovery_count", 0) or 0),
            "last_recovered_at": str(meta.get("last_recovered_at") or "").strip(),
            "last_corrupted_path": str(meta.get("last_corrupted_path") or "").strip(),
        })

        return {
            "version": 1,
            "events": normalized_events,
            "aggregate": normalized_aggregate,
            "meta": normalized_meta,
        }

    def _counter_pair(self, value: Any, *, first_key: str = "invalid", second_key: str = "total") -> dict[str, int]:
        data = dict(value or {}) if isinstance(value, dict) else {}
        return {
            first_key: int(data.get(first_key, 0) or 0),
            second_key: int(data.get(second_key, 0) or 0),
        }

    def _dict_of_dicts_or_scalars(self, value: Any) -> dict[str, Any]:
        data = dict(value or {}) if isinstance(value, dict) else {}
        normalized: dict[str, Any] = {}
        for key, item in data.items():
            normalized_key = str(key or "").strip()
            if not normalized_key:
                continue
            if isinstance(item, dict):
                normalized[normalized_key] = {
                    str(sub_key or "").strip(): int(sub_value or 0)
                    for sub_key, sub_value in item.items()
                    if str(sub_key or "").strip()
                }
            elif isinstance(item, (int, float)):
                normalized[normalized_key] = int(item)
            elif isinstance(item, str):
                normalized[normalized_key] = item
        return normalized
