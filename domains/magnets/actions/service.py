from __future__ import annotations

from typing import Any, Mapping

from ..preferences import MagnetPreferenceService
from ..runtime.identifiers import source_fingerprint
from ..runtime.observability import emit_event
from ..services.source_handoff_service import SourceHandoffService


class SourceActionService:
    def __init__(
        self,
        *,
        preference_service: MagnetPreferenceService | None = None,
        handoff_service: SourceHandoffService | None = None,
    ) -> None:
        self.preference_service = preference_service or MagnetPreferenceService()
        self.handoff_service = handoff_service or SourceHandoffService()

    def handle_action(
        self,
        *,
        action: str,
        movie: Mapping[str, Any] | None = None,
        source: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_action = str(action or "").strip().lower()
        source_data = dict(source or {})
        movie_data = dict(movie or {})

        if normalized_action in {"copy_magnet", "open_magnet", "open_external"}:
            emit_event(
                "[source-action]",
                movie=self._movie_name(movie_data),
                action=normalized_action,
                fingerprint=source_fingerprint(source_data),
            )
            return self.handoff_service.handle_action(
                action=normalized_action,
                movie=movie_data,
                source=source_data,
            )
        if normalized_action == "save_source":
            return self.preference_service.save_source(source_data, movie=movie_data)
        if normalized_action == "favorite_release":
            return self.preference_service.favorite_release(source_data, movie=movie_data)
        if normalized_action == "hide_release":
            return self.preference_service.hide_release(source_data, movie=movie_data)
        return {"ok": False, "error": "Unsupported source action."}

    def _movie_name(self, movie: Mapping[str, Any]) -> str:
        return str(movie.get("title") or movie.get("name") or "").strip() or "unknown"
