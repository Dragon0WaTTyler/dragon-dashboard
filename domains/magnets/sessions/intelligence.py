from __future__ import annotations

from typing import Any, Mapping

from ..runtime.intelligence import build_release_pattern


def build_session_intelligence_context(
    session: Mapping[str, Any],
    *,
    event_name: str,
    source: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    session_data = dict(session or {})
    snapshot = dict(session_data.get("compatibility_snapshot") or {})
    quality = dict(snapshot.get("quality") or {})
    compatibility = dict(snapshot.get("compatibility") or {})
    diagnostics = dict(snapshot.get("diagnostics") or {})
    magnet = dict(snapshot.get("magnet") or {})
    source_data = dict(source or {})

    context = {
        "event_name": str(event_name or "").strip(),
        "session_id": str(session_data.get("session_id") or "").strip(),
        "movie_id": str(session_data.get("movie_id") or "").strip(),
        "movie_title": str(session_data.get("movie_title") or "unknown").strip() or "unknown",
        "runtime_intent": str(session_data.get("runtime_intent") or "").strip(),
        "preferred_runtime": str(session_data.get("preferred_runtime") or "").strip(),
        "handoff_mode": str(session_data.get("handoff_mode") or "").strip(),
        "session_state": str(session_data.get("session_state") or "").strip(),
        "failure_reason": str(session_data.get("failure_reason") or session_data.get("blocked_reason") or "").strip(),
        "blocked_reason": str((session_data.get("admission_policy") or {}).get("blocked_reason") or "").strip(),
        "codec": str(quality.get("codec") or source_data.get("codec") or "unknown").strip() or "unknown",
        "resolution": str(quality.get("resolution") or source_data.get("resolution") or "unknown").strip() or "unknown",
        "source_type": str(quality.get("source_type") or source_data.get("source_type") or "unknown").strip() or "unknown",
        "release_group": str(quality.get("release_group") or source_data.get("release_group") or "unknown").strip() or "unknown",
        "provider": str(source_data.get("source") or source_data.get("provider") or "unknown").strip() or "unknown",
        "source_fingerprint": str(session_data.get("source_fingerprint") or "").strip(),
        "browser_friendly": bool(compatibility.get("browser_friendly")),
        "external_player_ready": bool(compatibility.get("external_player_ready")),
        "mobile_friendly": bool(compatibility.get("mobile_friendly")),
        "high_bandwidth_required": bool(compatibility.get("high_bandwidth_required")),
        "magnet_valid": bool(magnet.get("is_valid")),
        "likely_streamable": bool(quality.get("likely_streamable")),
        "quality_score": int(quality.get("estimated_quality_score", 0) or 0),
        "warnings": list(diagnostics.get("warnings") or snapshot.get("warnings") or []),
    }
    context["release_pattern"] = build_release_pattern(context)
    return context
