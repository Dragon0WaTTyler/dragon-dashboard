from __future__ import annotations

import hashlib
from typing import Any, Mapping

from ..analytics import SessionAnalyticsService
from ..runtime.identifiers import normalize_token, source_fingerprint
from ..runtime.observability import emit_event
from ..runtime.playback_policy import evaluate_playback_admission
from ..runtime.session_runtime import normalize_runtime_intent, resolve_runtime_intent
from ..sessions import StreamSession, normalize_session_state, utc_now_iso
from .session_store import StreamSessionStore


HARD_BLOCK_REASONS = {
    "invalid_magnet",
    "low_streamability_confidence",
    "low_quality_source",
    "unsupported_release_type",
}


class StreamSessionService:
    def __init__(
        self,
        *,
        store: StreamSessionStore | None = None,
        analytics_service: SessionAnalyticsService | None = None,
    ) -> None:
        self.store = store or StreamSessionStore()
        self.analytics_service = analytics_service or SessionAnalyticsService()

    def create_session(
        self,
        *,
        movie: Mapping[str, Any] | None = None,
        source: Mapping[str, Any] | None = None,
        handoff_mode: str = "",
        preferred_runtime: str = "",
    ) -> dict[str, Any]:
        movie_data = dict(movie or {})
        source_data = dict(source or {})
        movie_id = self._movie_id(movie_data)
        fingerprint = source_fingerprint(source_data) or self._fallback_source_fingerprint(source_data)
        admission = evaluate_playback_admission(source_data, movie=movie_data)
        runtime_preference = normalize_runtime_intent(preferred_runtime)
        session = StreamSession(
            session_id=self._session_id(
                movie_id=movie_id,
                source_fingerprint=fingerprint,
                handoff_mode=handoff_mode,
                preferred_runtime=runtime_preference,
            ),
            movie_id=movie_id,
            source_fingerprint=fingerprint,
            handoff_mode=self._handoff_mode(handoff_mode, admission["policy"]),
            preferred_runtime=runtime_preference,
            session_state="created",
            compatibility_snapshot=admission["snapshot"],
            created_at=utc_now_iso(),
            updated_at=utc_now_iso(),
            runtime_intent=resolve_runtime_intent(
                preferred_runtime=runtime_preference,
                handoff_mode=handoff_mode,
                admission_policy=admission["policy"],
            ),
            admission_policy=admission["policy"],
            movie_title=self._movie_name(movie_data),
        )
        saved = self.store.save_session(session)
        self.analytics_service.track_session_event("session_created", session=saved, source=source_data)
        self._emit_session_event(saved)
        return {"ok": True, "session": saved}

    def prepare_session(self, session_id: str) -> dict[str, Any]:
        session = self.store.get_session(session_id)
        if not session:
            return {"ok": False, "error": "Unknown session."}
        session = dict(session)
        policy = dict(session.get("admission_policy") or {})
        if self._is_hard_blocked(policy):
            session["session_state"] = "failed"
            session["failure_reason"] = str(policy.get("blocked_reason") or "").strip()
        else:
            session["session_state"] = "prepared"
        session["updated_at"] = utc_now_iso()
        saved = self.store.save_session(session)
        self.analytics_service.track_session_event("session_prepared", session=saved)
        self._emit_session_event(saved)
        return {"ok": True, "session": saved}

    def handoff_session(self, session_id: str, *, runtime_intent: str = "") -> dict[str, Any]:
        session = self.store.get_session(session_id)
        if not session:
            return {"ok": False, "error": "Unknown session."}
        session = dict(session)
        policy = dict(session.get("admission_policy") or {})
        resolved_intent = normalize_runtime_intent(runtime_intent) or str(session.get("runtime_intent") or "").strip()
        session["runtime_intent"] = resolved_intent or "external_player"
        if self._is_hard_blocked(policy):
            session["session_state"] = "failed"
            session["failure_reason"] = str(policy.get("blocked_reason") or "").strip()
        else:
            session["session_state"] = "handed_off"
        session["updated_at"] = utc_now_iso()
        saved = self.store.save_session(session)
        self._track_handoff_analytics(saved)
        self._emit_session_event(saved)
        return {"ok": True, "session": saved}

    def fail_session(self, session_id: str, *, reason: str = "") -> dict[str, Any]:
        session = self.store.get_session(session_id)
        if not session:
            return {"ok": False, "error": "Unknown session."}
        session = dict(session)
        session["session_state"] = "failed"
        session["failure_reason"] = str(reason or session.get("failure_reason") or "failed").strip()
        session["updated_at"] = utc_now_iso()
        saved = self.store.save_session(session)
        self.analytics_service.track_session_event("session_handoff_failed", session=saved)
        self._emit_session_event(saved)
        return {"ok": True, "session": saved}

    def expire_session(self, session_id: str) -> dict[str, Any]:
        session = self.store.get_session(session_id)
        if not session:
            return {"ok": False, "error": "Unknown session."}
        session = dict(session)
        session["session_state"] = "expired"
        session["updated_at"] = utc_now_iso()
        saved = self.store.save_session(session)
        self.analytics_service.track_session_event("session_expired", session=saved)
        self._emit_session_event(saved)
        return {"ok": True, "session": saved}

    def get_session(self, session_id: str) -> dict[str, Any]:
        session = self.store.get_session(session_id)
        if not session:
            return {"ok": False, "error": "Unknown session."}
        return {"ok": True, "session": session}

    def list_sessions(self, *, movie_id: str = "") -> dict[str, Any]:
        normalized_movie_id = str(movie_id or "").strip()
        sessions = self.store.list_sessions()
        if normalized_movie_id:
            sessions = [item for item in sessions if str(item.get("movie_id") or "").strip() == normalized_movie_id]
        return {"ok": True, "sessions": sessions}

    def _session_id(
        self,
        *,
        movie_id: str,
        source_fingerprint: str,
        handoff_mode: str,
        preferred_runtime: str,
    ) -> str:
        payload = "|".join(
            [
                normalize_token(movie_id),
                normalize_token(source_fingerprint),
                normalize_token(handoff_mode),
                normalize_token(preferred_runtime),
            ]
        )
        if not payload.strip("|"):
            payload = "session"
        return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:20]

    def _movie_id(self, movie: Mapping[str, Any]) -> str:
        explicit = str(movie.get("movie_id") or movie.get("id") or movie.get("imdb_id") or "").strip()
        if explicit:
            return explicit
        title = self._movie_name(movie)
        year = str(movie.get("year") or "").strip()
        slug = normalize_token(f"{title}-{year}").replace(" ", "-")
        return slug or "unknown-movie"

    def _movie_name(self, movie: Mapping[str, Any]) -> str:
        return str(movie.get("title") or movie.get("name") or "").strip() or "unknown"

    def _fallback_source_fingerprint(self, source: Mapping[str, Any]) -> str:
        payload = "|".join(
            [
                normalize_token(source.get("magnet")),
                normalize_token(source.get("title")),
                normalize_token(source.get("source")),
            ]
        )
        if not payload.strip("|"):
            return "malformed-source"
        return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]

    def _handoff_mode(self, requested_mode: str, policy: Mapping[str, Any]) -> str:
        normalized_mode = str(requested_mode or "").strip().lower()
        if normalized_mode:
            return normalized_mode
        if policy.get("allowed_for_browser"):
            return "browser_handoff"
        if policy.get("mobile_safe"):
            return "mobile_handoff"
        if policy.get("external_only"):
            return "external_handoff"
        if policy.get("blocked_reason"):
            return "blocked"
        return "external_handoff"

    def _emit_session_event(self, session: Mapping[str, Any]) -> None:
        emit_event(
            "[stream-session]",
            movie=str(session.get("movie_title") or "unknown"),
            state=normalize_session_state(session.get("session_state")),
            runtime=str(session.get("runtime_intent") or "unknown"),
            session=str(session.get("session_id") or "").strip(),
        )

    def _is_hard_blocked(self, policy: Mapping[str, Any]) -> bool:
        return str(policy.get("blocked_reason") or "").strip() in HARD_BLOCK_REASONS

    def _track_handoff_analytics(self, session: Mapping[str, Any]) -> None:
        runtime_intent = str(session.get("runtime_intent") or "").strip()
        if runtime_intent == "browser_stream":
            self.analytics_service.track_session_event("browser_attempted", session=session)
        if normalize_session_state(session.get("session_state")) == "failed":
            self.analytics_service.track_session_event("session_handoff_failed", session=session)
            return
        self.analytics_service.track_session_event("session_handoff_success", session=session)
        if runtime_intent == "external_player":
            self.analytics_service.track_session_event("external_player_used", session=session)
        elif runtime_intent == "mobile_handoff":
            self.analytics_service.track_session_event("mobile_handoff_used", session=session)
