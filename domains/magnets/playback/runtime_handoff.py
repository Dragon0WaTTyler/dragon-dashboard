from __future__ import annotations

from typing import Any, Mapping
from urllib.parse import urlencode

import requests

from dragon.config import config_value

from ..runtime.observability import emit_event


def resolve_runtime_base_url(request=None, *, fallback: str | None = None) -> str:
    configured = str(config_value("DRAGON_RUNTIME_BASE_URL", "") or "").strip().rstrip("/")
    if configured:
        return configured
    if fallback:
        fallback_url = str(fallback or "").strip().rstrip("/")
        if fallback_url:
            return fallback_url
    if request is not None:
        request_url = str(getattr(request, "host_url", "") or "").strip().rstrip("/")
        if request_url:
            return request_url
    return ""


def build_runtime_watch_url(
    runtime_base_url: str,
    magnet: str,
    *,
    title: str = "",
    movie_id: str = "",
    entry_id: str = "",
    source_fingerprint: str = "",
) -> str:
    base_url = str(runtime_base_url or "").strip().rstrip("/")
    if not base_url:
        raise ValueError("runtime_base_url is required")
    payload = {"magnet": str(magnet or "").strip()}
    for key, value in (
        ("title", title),
        ("movie_id", movie_id),
        ("entry_id", entry_id),
        ("source_fingerprint", source_fingerprint),
    ):
        text = str(value or "").strip()
        if text:
            payload[key] = text
    return f"{base_url}/watch?{urlencode(payload, doseq=True)}"


def probe_runtime_reachability(runtime_base_url: str, *, timeout_seconds: float = 1.5) -> bool:
    base_url = str(runtime_base_url or "").strip().rstrip("/")
    if not base_url:
        return False
    health_url = f"{base_url}/healthz"
    try:
        response = requests.get(health_url, timeout=max(float(timeout_seconds or 0.0), 0.25))
        return 200 <= int(response.status_code or 0) < 500
    except Exception:
        return False


def log_runtime_handoff(
    *,
    runtime_base_url: str,
    watch_url: str,
    stream_url: str,
    magnet: str,
    runtime_reachable: bool,
    selected_file: Mapping[str, Any] | None = None,
    session_id: str = "",
) -> None:
    file_data = dict(selected_file or {})
    emit_event(
        "[playback-runtime-handoff]",
        runtime_base_url=str(runtime_base_url or "").strip() or "unset",
        handoff_url=str(watch_url or "").strip() or "unset",
        runtime_reachable="reachable" if runtime_reachable else "unreachable",
        magnet_accepted=1 if str(magnet or "").strip() else 0,
        selected_file=str(file_data.get("name") or file_data.get("file_name") or file_data.get("path") or "").strip() or "unknown",
        stream_url=str(stream_url or "").strip() or "unset",
        session_id=str(session_id or "").strip() or "unset",
    )
