from __future__ import annotations

import re
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
)
from ..runtime.http import build_session, safe_json_get


STREAM_SIZE_PATTERN = re.compile(r"💾\s*([\d.]+\s*(?:GB|MB|TB))", re.IGNORECASE)
STREAM_SEEDERS_PATTERN = re.compile(r"👤\s*(\d+)", re.IGNORECASE)


class TorrentioProvider(MagnetProvider):
    source = "torrentio"

    def __init__(self, *, base_url: str = "https://torrentio.strem.fun", session=None, timeout: int = 15) -> None:
        self.base_url = str(base_url).rstrip("/")
        self.session = session or build_session()
        self.timeout = timeout

    def search_movie_magnets(self, movie: Mapping[str, Any]) -> list[dict[str, Any]]:
        imdb_id = normalize_imdb_id(movie.get("imdb_id"))
        title = title_for_movie(movie)
        year = str(movie.get("year") or "").strip()
        if not imdb_id:
            return []

        payload = safe_json_get(
            self.session,
            f"{self.base_url}/stream/movie/{imdb_id}.json",
            timeout=self.timeout,
        )
        streams = payload.get("streams", []) if isinstance(payload, dict) else []
        if not isinstance(streams, list):
            return []

        normalized = []
        for stream in streams:
            if not isinstance(stream, dict):
                continue
            info_hash = str(stream.get("infoHash") or "").strip()
            if not info_hash:
                continue
            title_blob = str(stream.get("title") or "")
            display_name = str(stream.get("name") or "").replace("\n", " ").strip()
            candidate_title = self._candidate_title(stream, fallback=title or display_name)
            if title and not self._looks_like_movie_match(candidate_title, requested_title=title, requested_year=year):
                continue
            normalized.append(
                make_candidate(
                    source=self.source,
                    title=candidate_title,
                    magnet=build_magnet(info_hash, title or candidate_title or display_name),
                    size_gb=parse_size_gb(self._extract_size(title_blob)),
                    resolution=infer_resolution(display_name, title_blob, stream.get("behaviorHints", {}).get("bingeGroup", "")),
                    codec=infer_codec(title_blob, stream.get("behaviorHints", {}).get("filename", ""), stream.get("behaviorHints", {}).get("bingeGroup", "")),
                    seeders=self._extract_seeders(title_blob),
                    language=infer_language(title_blob, display_name, stream.get("behaviorHints", {}).get("filename", "")),
                    imdb_id=imdb_id,
                )
            )
        return normalized

    def _extract_size(self, title_blob: str) -> str:
        match = STREAM_SIZE_PATTERN.search(str(title_blob or ""))
        return match.group(1) if match else ""

    def _extract_seeders(self, title_blob: str) -> int:
        match = STREAM_SEEDERS_PATTERN.search(str(title_blob or ""))
        if not match:
            return 0
        try:
            return int(match.group(1))
        except (TypeError, ValueError):
            return 0

    def _candidate_title(self, stream: Mapping[str, Any], *, fallback: str = "") -> str:
        title_blob = str(stream.get("title") or "")
        first_line = title_blob.splitlines()[0].strip() if title_blob else ""
        return first_line or str(fallback or "").strip()

    def _looks_like_movie_match(self, candidate_title: str, *, requested_title: str, requested_year: str) -> bool:
        requested_key = self._normalize_title_key(requested_title)
        candidate_key = self._normalize_title_key(candidate_title)
        if not requested_key or not candidate_key:
            return False
        if self._is_collection_release(candidate_title):
            return False
        if requested_key in candidate_key:
            return True

        requested_tokens = [token for token in requested_key.split(" ") if len(token) > 2]
        overlap = [token for token in requested_tokens if token in candidate_key.split(" ")]
        return len(overlap) >= max(2, len(requested_tokens) - 1)

    def _normalize_title_key(self, value: str) -> str:
        lowered = str(value or "").lower()
        lowered = re.sub(r"[^a-z0-9]+", " ", lowered)
        return re.sub(r"\s+", " ", lowered).strip()

    def _is_collection_release(self, candidate_title: str) -> bool:
        lowered = str(candidate_title or "").lower()
        if any(token in lowered for token in ("collection", "pack", "saga", "trilogy", "movies")):
            return True
        if re.search(r"\b\d+\s*[-:]\s*\d+\b", lowered):
            return True
        return False
