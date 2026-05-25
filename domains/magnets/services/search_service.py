from __future__ import annotations

import re
import time
from typing import Any, Mapping

from ..cache import JsonMagnetCache
from ..providers import TorrentioProvider, YtsProvider
from ..ranking import rank_candidates
from ..runtime.config import TORRENTIO_BASE_URL, YTS_API_URLS


class MagnetSearchService:
    def __init__(self, *, providers=None, cache: JsonMagnetCache | None = None) -> None:
        self.providers = list(
            providers
            or [
                YtsProvider(api_urls=YTS_API_URLS),
                TorrentioProvider(base_url=TORRENTIO_BASE_URL),
            ]
        )
        self.cache = cache or JsonMagnetCache()

    def search_movie_magnets(self, movie: Mapping[str, Any], *, force_refresh: bool = False) -> list[dict[str, Any]]:
        return self.search_movie_magnets_with_meta(movie, force_refresh=force_refresh).get("results", [])

    def search_movie_magnets_with_meta(
        self,
        movie: Mapping[str, Any],
        *,
        force_refresh: bool = False,
    ) -> dict[str, Any]:
        movie_data = dict(movie or {})
        cache_key = self._cache_key(movie_data)
        started_at = time.monotonic()
        if cache_key and not force_refresh:
            cached = self.cache.get(cache_key)
            if isinstance(cached, list):
                provider_counts = self._count_results_by_provider(cached)
                elapsed_ms = (time.monotonic() - started_at) * 1000
                self._log_provider_counts(
                    movie_data,
                    provider_counts,
                    cache="hit",
                    elapsed_ms=elapsed_ms,
                )
                return {
                    "results": cached,
                    "cache": "hit",
                    "cache_key": cache_key,
                    "provider_counts": provider_counts,
                    "elapsed_ms": round(elapsed_ms, 2),
                }

        candidates = []
        provider_counts: dict[str, int] = {}
        for provider in self.providers:
            provider_name = str(getattr(provider, "source", provider.__class__.__name__) or "unknown").strip().lower()
            provider_started_at = time.monotonic()
            try:
                provider_results = provider.search_movie_magnets(movie_data)
            except Exception as exc:
                provider_results = []
                self._emit_log(
                    movie_data,
                    provider=provider_name,
                    results=0,
                    cache="miss",
                    elapsed_ms=(time.monotonic() - provider_started_at) * 1000,
                    error=exc,
                )
            else:
                if not isinstance(provider_results, list):
                    provider_results = []
                self._emit_log(
                    movie_data,
                    provider=provider_name,
                    results=len(provider_results),
                    cache="miss",
                    elapsed_ms=(time.monotonic() - provider_started_at) * 1000,
                )
            provider_counts[provider_name] = len(provider_results)
            if provider_results:
                candidates.extend(provider_results)

        ranked = rank_candidates(candidates, movie=movie_data)
        if cache_key:
            self.cache.set(cache_key, ranked)
        return {
            "results": ranked,
            "cache": "miss" if cache_key else "bypass",
            "cache_key": cache_key,
            "provider_counts": provider_counts,
            "elapsed_ms": round((time.monotonic() - started_at) * 1000, 2),
        }

    def get_cached_movie_magnets(self, movie: Mapping[str, Any]) -> list[dict[str, Any]] | None:
        movie_data = dict(movie or {})
        cache_key = self._cache_key(movie_data)
        if not cache_key:
            return None
        cached = self.cache.get(cache_key)
        return cached if isinstance(cached, list) else None

    def _cache_key(self, movie: Mapping[str, Any]) -> str:
        imdb_id = str(movie.get("imdb_id") or "").strip().lower()
        if imdb_id:
            return f"imdb:{imdb_id}"

        title = str(movie.get("title") or movie.get("name") or "").strip().lower()
        year = str(movie.get("year") or "").strip()
        normalized_title = re.sub(r"[^a-z0-9]+", "-", title).strip("-")
        if normalized_title:
            return f"title:{normalized_title}:{year}"
        return ""

    def _count_results_by_provider(self, results: list[dict[str, Any]]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for result in list(results or []):
            if not isinstance(result, dict):
                continue
            provider = str(result.get("source") or "unknown").strip().lower() or "unknown"
            counts[provider] = counts.get(provider, 0) + 1
        return counts

    def _log_provider_counts(
        self,
        movie: Mapping[str, Any],
        provider_counts: Mapping[str, int],
        *,
        cache: str,
        elapsed_ms: float,
    ) -> None:
        for provider, count in dict(provider_counts or {}).items():
            self._emit_log(
                movie,
                provider=provider,
                results=count,
                cache=cache,
                elapsed_ms=elapsed_ms,
            )

    def _emit_log(
        self,
        movie: Mapping[str, Any],
        *,
        provider: str,
        results: int,
        cache: str,
        elapsed_ms: float,
        error: Exception | None = None,
    ) -> None:
        movie_name = str(movie.get("title") or movie.get("name") or "").strip() or "unknown"
        parts = [
            "[magnet-search]",
            f"movie={movie_name}",
            f"provider={provider or 'unknown'}",
            f"results={int(results or 0)}",
            f"cache={cache}",
            f"elapsed_ms={int(max(elapsed_ms, 0))}",
        ]
        if error is not None:
            parts.append(f"error={error.__class__.__name__}")
        message = " ".join(parts)
        print(message.encode("ascii", errors="backslashreplace").decode("ascii"))
