from __future__ import annotations

from typing import Any, Mapping

from .base import MagnetProvider
from .common import (
    build_magnet,
    infer_codec,
    infer_language,
    infer_resolution,
    make_candidate,
    normalize_imdb_id,
    parse_size_gb,
    title_for_movie,
    year_for_movie,
)
from ..runtime.http import build_session, safe_json_get


class YtsProvider(MagnetProvider):
    source = "yts"

    def __init__(self, *, api_urls: list[str] | None = None, session=None, timeout: int = 15) -> None:
        self.api_urls = list(api_urls or ["https://movies-api.accel.li/api/v2", "https://yts.rs/api/v2"])
        self.session = session or build_session()
        self.timeout = timeout

    def search_movie_magnets(self, movie: Mapping[str, Any]) -> list[dict[str, Any]]:
        imdb_id = normalize_imdb_id(movie.get("imdb_id"))
        title = title_for_movie(movie)
        year = year_for_movie(movie)

        if not imdb_id and not title:
            return []

        payload = self._request({"query_term": imdb_id or title, "limit": 20})
        movies = payload.get("data", {}).get("movies", []) if isinstance(payload, dict) else []
        if not isinstance(movies, list):
            return []

        selected = self._pick_movie(movies, imdb_id=imdb_id, title=title, year=year)
        if not selected:
            return []

        resolved_imdb_id = normalize_imdb_id(selected.get("imdb_code") or imdb_id)
        normalized = []
        for torrent in list(selected.get("torrents", []) or []):
            if not isinstance(torrent, dict):
                continue
            magnet = build_magnet(torrent.get("hash"), selected.get("title_long") or selected.get("title") or title)
            if not magnet:
                continue
            normalized.append(
                make_candidate(
                    source=self.source,
                    title=selected.get("title_long") or selected.get("title") or title,
                    magnet=magnet,
                    size_gb=parse_size_gb(torrent.get("size"), size_bytes=torrent.get("size_bytes")),
                    resolution=infer_resolution(torrent.get("quality"), torrent.get("type")),
                    codec=infer_codec(torrent.get("video_codec"), torrent.get("type")),
                    seeders=torrent.get("seeds", 0),
                    language=infer_language(selected.get("language")),
                    imdb_id=resolved_imdb_id,
                )
            )
        return normalized

    def _request(self, params: Mapping[str, Any]) -> dict[str, Any]:
        for base_url in self.api_urls:
            data = safe_json_get(
                self.session,
                f"{str(base_url).rstrip('/')}/list_movies.json",
                params=dict(params),
                timeout=self.timeout,
            )
            if isinstance(data, dict) and data.get("status") == "ok":
                return data
        return {}

    def _pick_movie(
        self,
        movies: list[dict[str, Any]],
        *,
        imdb_id: str,
        title: str,
        year: str,
    ) -> dict[str, Any]:
        if imdb_id:
            for movie in movies:
                if normalize_imdb_id(movie.get("imdb_code")) == imdb_id:
                    return movie

        wanted_title = str(title or "").strip().lower()
        wanted_year = str(year or "").strip()
        ranked = []
        for movie in movies:
            candidate_title = str(movie.get("title") or "").strip().lower()
            candidate_year = str(movie.get("year") or "").strip()
            score = 0
            if wanted_title and candidate_title == wanted_title:
                score += 100
            elif wanted_title and wanted_title in candidate_title:
                score += 50
            if wanted_year and candidate_year == wanted_year:
                score += 25
            seed_count = max(int((torrent or {}).get("seeds", 0) or 0) for torrent in list(movie.get("torrents", []) or []) or [{}])
            ranked.append((score, seed_count, movie))

        ranked.sort(key=lambda item: (-item[0], -item[1]))
        return ranked[0][2] if ranked and ranked[0][0] > 0 else {}
