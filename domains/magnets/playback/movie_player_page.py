from __future__ import annotations

from typing import Any, Mapping
from urllib.parse import urlencode


READY_STATES = {"ready", "ready_to_play"}
BUFFERING_STATES = {
    "buffering",
    "metadata_fetching",
    "metadata_retrying",
    "selecting_media",
    "connecting_peers",
    "buffering_video",
}


def build_watch_status_copy(*, runtime_state: str, source_quality: Mapping[str, Any] | None = None) -> dict[str, str]:
    quality_payload = dict(source_quality or {})
    label = str(quality_payload.get("label") or "").strip()
    message = str(quality_payload.get("message") or "").strip()
    recommended_action = str(quality_payload.get("recommended_action") or "").strip()
    fallback_available = bool(
        quality_payload.get("fallback_available") or quality_payload.get("show_qbittorrent_fallback")
    )

    if not label:
        if runtime_state in READY_STATES:
            label = "Ready"
            message = message or "This source is ready. Press play to start playback."
        elif runtime_state in BUFFERING_STATES:
            label = "Buffering"
            message = message or "Dragon is still waiting for playable bytes."
        else:
            label = "Runtime unavailable"
            message = message or "Dragon could not prepare playback for this source right now."

    if fallback_available and not recommended_action:
        recommended_action = "Open the external handoff or try another source."

    return {
        "label": label,
        "message": message,
        "recommended_action": recommended_action,
        "fallback_text": "Fallback available" if fallback_available else "",
    }


def build_watch_retry_url(
    *,
    magnet: str,
    title: str,
    movie_id: str,
    entry_id: str,
    source_fingerprint: str,
    tmdb_id: str = "",
    poster_url: str = "",
    fallback_url: str = "",
) -> str:
    payload = {
        "retry": "1",
        "magnet": str(magnet or "").strip(),
        "title": str(title or "").strip(),
        "movie_id": str(movie_id or "").strip(),
        "entry_id": str(entry_id or "").strip(),
        "tmdb_id": str(tmdb_id or "").strip(),
        "source_fingerprint": str(source_fingerprint or "").strip(),
    }
    poster_text = str(poster_url or "").strip()
    fallback_text = str(fallback_url or "").strip()
    if poster_text:
        payload["poster"] = poster_text
    if fallback_text:
        payload["fallback_url"] = fallback_text
    return f"/watch?{urlencode(payload, doseq=True)}"


def build_watch_refresh_url(
    *,
    session_id: str,
    magnet: str,
    title: str,
    movie_id: str,
    entry_id: str,
    source_fingerprint: str,
    tmdb_id: str = "",
    poster_url: str = "",
    fallback_url: str = "",
) -> str:
    payload = {
        "session_id": str(session_id or "").strip(),
        "magnet": str(magnet or "").strip(),
        "title": str(title or "").strip(),
        "movie_id": str(movie_id or "").strip(),
        "entry_id": str(entry_id or "").strip(),
        "tmdb_id": str(tmdb_id or "").strip(),
        "source_fingerprint": str(source_fingerprint or "").strip(),
    }
    poster_text = str(poster_url or "").strip()
    fallback_text = str(fallback_url or "").strip()
    if poster_text:
        payload["poster"] = poster_text
    if fallback_text:
        payload["fallback_url"] = fallback_text
    return f"/watch?{urlencode(payload, doseq=True)}"


def build_movie_player_page_context(
    *,
    page_title: str,
    page_heading: str,
    page_state: str,
    message: str,
    recommended_action: str,
    movie_title: str,
    movie_id: str = "",
    tmdb_id: str = "",
    poster_url: str,
    stream_url: str,
    retry_url: str,
    retry_label: str = "Retry playback",
    open_stream_url: str,
    fallback_url: str,
    external_handoff_url: str,
    elapsed_seconds: int = 0,
    auto_refresh_url: str = "",
    auto_refresh_seconds: int = 0,
    pause_notice: str = "",
    show_video: bool = False,
    show_debug_details: bool = False,
    debug_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_state = str(page_state or "buffering").strip().lower() or "buffering"
    status_label = {
        "ready": "Ready",
        "failed": "Failed",
    }.get(normalized_state, "Buffering")
    return {
        "page_title": str(page_title or "Dragon Movie Player").strip(),
        "page_heading": str(page_heading or status_label).strip(),
        "page_state": normalized_state,
        "status_label": status_label,
        "message": str(message or "").strip(),
        "recommended_action": str(recommended_action or "").strip(),
        "movie_title": str(movie_title or "").strip(),
        "movie_id": str(movie_id or "").strip(),
        "tmdb_id": str(tmdb_id or "").strip(),
        "poster_url": str(poster_url or "").strip(),
        "stream_url": str(stream_url or "").strip(),
        "retry_url": str(retry_url or "").strip(),
        "retry_label": str(retry_label or "Retry playback").strip(),
        "open_stream_url": str(open_stream_url or "").strip(),
        "fallback_url": str(fallback_url or "").strip(),
        "external_handoff_url": str(external_handoff_url or "").strip(),
        "elapsed_seconds": max(int(elapsed_seconds or 0), 0),
        "auto_refresh_url": str(auto_refresh_url or "").strip(),
        "auto_refresh_seconds": max(int(auto_refresh_seconds or 0), 0),
        "pause_notice": str(pause_notice or "").strip(),
        "show_video": bool(show_video and str(stream_url or "").strip()),
        "show_debug_details": bool(show_debug_details),
        "debug_payload": dict(debug_payload or {}),
    }
