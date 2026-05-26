from __future__ import annotations

from typing import Any, Mapping

from ..playback import prepare_playback_runtime, select_playback_candidates
from ..preferences import MagnetPreferenceService
from .source_handoff_service import SourceHandoffService
from .search_service import MagnetSearchService


PROVIDER_LABELS = {
    "torrentio": "Torrentio",
    "yts": "YTS",
}


class MovieSourcesService:
    def __init__(
        self,
        *,
        magnet_search_service: MagnetSearchService | None = None,
        preference_service: MagnetPreferenceService | None = None,
        handoff_service: SourceHandoffService | None = None,
        max_sources: int = 6,
    ) -> None:
        self.magnet_search_service = magnet_search_service or MagnetSearchService()
        self.preference_service = preference_service or MagnetPreferenceService()
        self.handoff_service = handoff_service or SourceHandoffService()
        self.max_sources = max(int(max_sources or 0), 1)

    def get_movie_sources(
        self,
        movie: Mapping[str, Any],
        *,
        force_refresh: bool = False,
    ) -> dict[str, Any]:
        movie_data = dict(movie or {})
        if not self._is_movie_candidate(movie_data):
            return self._empty_payload()

        try:
            search_meta = self.magnet_search_service.search_movie_magnets_with_meta(
                movie_data,
                force_refresh=force_refresh,
            )
        except Exception:
            return self._empty_payload(
                cache_status="error",
                message="Sources are temporarily unavailable.",
            )

        indexed_results = list(search_meta.get("results") or [])
        results, preference_meta = self.preference_service.apply_candidate_preferences(
            indexed_results,
            movie=movie_data,
        )
        provider_counts = dict(search_meta.get("provider_counts") or {})
        visible_sources = [
            self._to_ui_source(result, movie=movie_data)
            for result in results[: self.max_sources]
            if isinstance(result, dict)
        ]
        selection = select_playback_candidates(visible_sources, movie=movie_data)
        selected_source = dict(selection.get("selected_source") or {})
        selected_fingerprint = str(selected_source.get("source_fingerprint") or "").strip()
        visible_sources = list(selection.get("sources") or visible_sources)
        playback_plan = prepare_playback_runtime(
            movie=movie_data,
            sources=visible_sources,
            selected_source=selected_source,
        )
        provider_labels = [
            self._provider_label(provider)
            for provider, count in provider_counts.items()
            if count
        ]
        cache_status = str(search_meta.get("cache") or "miss")

        return {
            "sources": visible_sources,
            "has_sources": bool(visible_sources),
            "total_count": len(results),
            "indexed_count": len(indexed_results),
            "visible_count": len(visible_sources),
            "hidden_count": int(preference_meta.get("hidden_count", 0) or 0),
            "provider_labels": provider_labels,
            "provider_summary": " / ".join(provider_labels) if provider_labels else "No indexed providers",
            "cache_status": cache_status,
            "cache_label": self._cache_label(cache_status),
            "status_message": self._status_message(visible_sources=visible_sources, hidden_count=int(preference_meta.get("hidden_count", 0) or 0)),
            "auto_selected_source_fingerprint": selected_fingerprint,
            "playback_runtime": str(playback_plan.get("playback_runtime") or "").strip(),
            "runtime_profile": str(playback_plan.get("runtime_profile") or "").strip(),
            "playback_readiness": str(playback_plan.get("playback_readiness") or "").strip(),
            "startup_confidence": str(playback_plan.get("startup_confidence") or "").strip(),
            "runtime_warnings": list(playback_plan.get("runtime_warnings") or []),
            "runtime_fallbacks": list(playback_plan.get("fallbacks") or []),
            "preference_summary": {
                "favorite_group_count": int(preference_meta.get("favorite_group_count", 0) or 0),
                "saved_source_count": int(preference_meta.get("saved_source_count", 0) or 0),
            },
        }

    def _is_movie_candidate(self, movie: Mapping[str, Any]) -> bool:
        title = str(movie.get("title") or movie.get("name") or "").strip()
        category = str(movie.get("category") or "").strip().lower()
        return bool(title) and "tv" not in category

    def _to_ui_source(self, candidate: Mapping[str, Any], *, movie: Mapping[str, Any] | None = None) -> dict[str, Any]:
        resolution = str(candidate.get("resolution") or "").strip()
        codec = str(candidate.get("codec") or "").strip()
        seeders = self._int_value(candidate.get("seeders"))
        size_gb = self._float_value(candidate.get("size_gb"))
        provider = str(candidate.get("source") or "").strip().lower()
        handoff = self.handoff_service.describe_source(candidate, movie=movie)
        compatibility = dict(handoff.get("compatibility") or {})
        diagnostics = dict(handoff.get("diagnostics") or {})
        return {
            "quality": resolution or "Source",
            "quality_label": resolution or "Source",
            "resolution": resolution,
            "codec": codec or "Unknown codec",
            "codec_label": codec or "Unknown codec",
            "size_label": f"{size_gb:.1f} GB" if size_gb > 0 else "Size unknown",
            "magnet": str(candidate.get("magnet") or "").strip(),
            "seeders": seeders,
            "seeders_label": f"{seeders} seeders" if seeders > 0 else "Seeder count unknown",
            "provider": provider,
            "source": provider,
            "provider_label": self._provider_label(provider),
            "title": str(candidate.get("title") or "").strip(),
            "likely_streamable": bool(candidate.get("likely_streamable")),
            "estimated_quality_score": int(candidate.get("estimated_quality_score", 0) or 0),
            "base_quality_score": int(candidate.get("base_quality_score", candidate.get("estimated_quality_score", 0)) or 0),
            "preference_boost": int(candidate.get("preference_boost", 0) or 0),
            "preference_reasons": list(candidate.get("preference_reasons") or []),
            "confidence": str(candidate.get("confidence") or "").strip(),
            "release_group": str(candidate.get("release_group") or "").strip(),
            "source_type": str(candidate.get("source_type") or "").strip(),
            "source_type_label": str(candidate.get("source_type") or "").strip() or "Unknown source",
            "audio_format": str(candidate.get("audio_format") or "").strip(),
            "hdr": bool(candidate.get("hdr")),
            "dolby_vision": bool(candidate.get("dolby_vision")),
            "source_fingerprint": str(candidate.get("source_fingerprint") or "").strip(),
            "is_saved_source": bool(candidate.get("is_saved_source")),
            "is_favorite_group": bool(candidate.get("is_favorite_group")),
            "open_strategy": str(handoff.get("open_strategy") or "").strip(),
            "handoff_type": str(handoff.get("handoff_type") or "").strip(),
            "likely_compatibility": str(handoff.get("likely_compatibility") or "").strip(),
            "external_player_hints": list(handoff.get("external_player_hints") or []),
            "handoff_actions": list(handoff.get("actions") or []),
            "browser_friendly": bool(compatibility.get("browser_friendly")),
            "external_player_ready": bool(compatibility.get("external_player_ready")),
            "mobile_friendly": bool(compatibility.get("mobile_friendly")),
            "high_bandwidth_required": bool(compatibility.get("high_bandwidth_required")),
            "streamability": diagnostics,
            "handoff_fallback_strategy": str(handoff.get("fallback_strategy") or "").strip(),
        }

    def _provider_label(self, provider: str) -> str:
        key = str(provider or "").strip().lower()
        return PROVIDER_LABELS.get(key, key.title() or "Unknown")

    def _cache_label(self, cache_status: str) -> str:
        if cache_status == "hit":
            return "Cached"
        if cache_status == "error":
            return "Unavailable"
        return "Live lookup"

    def _empty_payload(self, *, cache_status: str = "miss", message: str = "") -> dict[str, Any]:
        return {
            "sources": [],
            "has_sources": False,
            "total_count": 0,
            "indexed_count": 0,
            "visible_count": 0,
            "hidden_count": 0,
            "provider_labels": [],
            "provider_summary": "No indexed providers",
            "cache_status": cache_status,
            "cache_label": self._cache_label(cache_status),
            "preference_summary": {
                "favorite_group_count": 0,
                "saved_source_count": 0,
            },
            "status_message": message or "No cached sources available for this movie yet.",
        }

    def _status_message(self, *, visible_sources: list[dict[str, Any]], hidden_count: int) -> str:
        if visible_sources:
            return ""
        if hidden_count > 0:
            return "All indexed sources are currently hidden by your preferences."
        return "No cached sources available for this movie yet."

    def _int_value(self, value: Any) -> int:
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    def _float_value(self, value: Any) -> float:
        try:
            return float(value or 0.0)
        except (TypeError, ValueError):
            return 0.0
