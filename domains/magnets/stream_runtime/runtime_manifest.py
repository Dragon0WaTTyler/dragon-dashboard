from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping


def build_runtime_manifest(
    *,
    runtime_id: str = "",
    session_id: str = "",
    selected_source: Mapping[str, Any] | None = None,
    runtime_mode: str = "",
    runtime_state: str = "",
    startup_confidence: str = "",
    capability_snapshot: Mapping[str, Any] | None = None,
    diagnostics: Mapping[str, Any] | None = None,
    fallbacks: list[Mapping[str, Any]] | None = None,
    preflight: Mapping[str, Any] | None = None,
    transport: Mapping[str, Any] | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    source = dict(selected_source or {})
    return {
        "runtime_id": str(runtime_id or "").strip(),
        "session_id": str(session_id or "").strip(),
        "selected_source": {
            "source_fingerprint": str(source.get("source_fingerprint") or "").strip(),
            "title": str(source.get("title") or "").strip(),
            "quality_label": str(source.get("quality_label") or source.get("resolution") or "").strip(),
            "provider": str(source.get("provider") or source.get("source") or "").strip(),
            "magnet": str(source.get("magnet") or "").strip(),
        },
        "runtime_mode": str(runtime_mode or "").strip(),
        "runtime_state": str(runtime_state or "").strip(),
        "startup_confidence": str(startup_confidence or "").strip(),
        "capability_snapshot": dict(capability_snapshot or {}),
        "diagnostics": dict(diagnostics or {}),
        "preflight": dict(preflight or {}),
        "transport": dict(transport or {}),
        "fallbacks": [dict(item) for item in (fallbacks or []) if isinstance(item, Mapping)],
        "created_at": str(created_at or _now_iso8601()),
    }


def _now_iso8601() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
