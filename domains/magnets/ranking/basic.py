from __future__ import annotations

from typing import Any

from .heuristics import score_candidate

def rank_candidates(
    candidates: list[dict[str, Any]],
    *,
    movie: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    deduped = {}
    for candidate in list(candidates or []):
        if not isinstance(candidate, dict):
            continue
        enriched = _enrich_candidate(candidate, movie=movie)
        magnet = str(candidate.get("magnet") or "").strip()
        title = str(candidate.get("title") or "").strip().lower()
        key = magnet or f"{candidate.get('source', '')}:{title}"
        current = deduped.get(key)
        if current is None or _score(enriched) > _score(current):
            deduped[key] = enriched
    return sorted(deduped.values(), key=_sort_key, reverse=True)


def _score(candidate: dict[str, Any]) -> float:
    return float(candidate.get("estimated_quality_score", 0) or 0)


def _sort_key(candidate: dict[str, Any]) -> tuple[float, int, int, float]:
    return (
        _score(candidate),
        1 if candidate.get("likely_streamable") else 0,
        int(candidate.get("seeders", 0) or 0),
        float(candidate.get("size_gb", 0.0) or 0.0),
    )


def _enrich_candidate(candidate: dict[str, Any], *, movie: dict[str, Any] | None = None) -> dict[str, Any]:
    enriched = dict(candidate)
    enriched.update(score_candidate(enriched, movie=movie))
    _emit_ranking_log(movie=movie, candidate=enriched)
    return enriched


def _emit_ranking_log(*, movie: dict[str, Any] | None, candidate: dict[str, Any]) -> None:
    movie_name = str((movie or {}).get("title") or (movie or {}).get("name") or "").strip() or "unknown"
    parts = [
        "[magnet-ranking]",
        f"movie={movie_name}",
        f"candidate_score={int(candidate.get('estimated_quality_score', 0) or 0)}",
        f"group={str(candidate.get('release_group') or 'unknown')}",
        f"resolution={str(candidate.get('resolution') or 'unknown')}",
        f"streamable={1 if candidate.get('likely_streamable') else 0}",
        f"confidence={str(candidate.get('confidence') or 'low')}",
    ]
    print(" ".join(parts).encode("ascii", errors="backslashreplace").decode("ascii"))
