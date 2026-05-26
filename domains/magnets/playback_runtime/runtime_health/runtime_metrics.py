from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


def _parse_iso(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return None


def runtime_state_label(runtime_state: str) -> str:
    labels = {
        "metadata_fetching": "Fetching metadata",
        "connecting_peers": "Connecting to peers",
        "selecting_media": "Selecting media",
        "buffering_video": "Buffering video",
        "ready_to_play": "Ready to play",
        "buffering": "Buffering...",
        "playback_stalled": "Playback stalled",
        "recovering_stream": "Recovering stream",
        "stream_failed": "Playback stalled",
        "completed": "Ready to play",
        "expired": "Playback stalled",
    }
    return labels.get(str(runtime_state or "").strip(), "Preparing...")


def build_runtime_metrics(session: Mapping[str, Any]) -> dict[str, Any]:
    created_at = _parse_iso(str(session.get("created_at") or ""))
    age_seconds = 0
    if created_at is not None:
        age_seconds = max(int((datetime.now(timezone.utc) - created_at).total_seconds()), 0)

    file_name = str(session.get("file_name") or session.get("file_path") or "").strip()
    extension = Path(file_name).suffix.lower().lstrip(".")
    playback_state = str(session.get("playback_state") or session.get("status") or "initializing").strip()
    runtime_state = str(session.get("status") or "initializing").strip()

    return {
        "peers_connected": int(session.get("peer_count", 0) or 0),
        "download_speed": float(session.get("download_speed", 0.0) or 0.0),
        "buffered_mb": round(int(session.get("downloaded_bytes", 0) or 0) / (1024 * 1024), 2),
        "playback_ready": bool(session.get("ready")),
        "runtime_errors": list(session.get("runtime_errors") or []),
        "selected_container": extension or "unknown",
        "selected_codec": str(((session.get("details") or {}).get("selected_codec")) or "unknown").strip(),
        "session_age": age_seconds,
        "playback_state": playback_state,
        "runtime_state_label": runtime_state_label(runtime_state),
        "helper_running": bool(session.get("helper_running")),
    }
