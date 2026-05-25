from __future__ import annotations

from typing import Any, Mapping

from ..handoff import build_handoff_profile, resolve_handoff_action
from ..runtime.identifiers import source_fingerprint
from ..runtime.observability import emit_event


class SourceHandoffService:
    def describe_source(
        self,
        source: Mapping[str, Any],
        *,
        movie: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        return build_handoff_profile(source, movie=movie)

    def handle_action(
        self,
        *,
        action: str,
        source: Mapping[str, Any],
        movie: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        result = resolve_handoff_action(action, source, movie=movie)
        handoff = dict(result.get("handoff") or {})
        selected_action = dict(handoff.get("selected_action") or {})
        emit_event(
            "[source-handoff]",
            movie=self._movie_name(movie or source),
            strategy=str(selected_action.get("strategy") or handoff.get("open_strategy") or "unknown"),
            compatibility=str(handoff.get("likely_compatibility") or "limited"),
            action=str(action or "").strip().lower(),
            fingerprint=source_fingerprint(source),
        )
        return result

    def _movie_name(self, movie: Mapping[str, Any]) -> str:
        return str(movie.get("title") or movie.get("name") or "").strip() or "unknown"
