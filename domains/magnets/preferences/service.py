from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Any, Mapping

from dragon.cache import load_json_file, save_json_file
from dragon.paths import CACHE_DIR

from ..runtime.identifiers import normalize_token, source_fingerprint
from ..runtime.observability import emit_event

DEFAULT_PREFERENCES = {
    "version": 1,
    "favorite_release_groups": [],
    "hidden_releases": [],
    "preferred_codecs": [],
    "preferred_resolutions": [],
    "preferred_source_types": [],
    "saved_sources": [],
}

GROUP_BOOST = 14
CODEC_BOOST = 8
RESOLUTION_BOOST = 6
SOURCE_TYPE_BOOST = 5
SAVED_SOURCE_BOOST = 3


class MagnetPreferenceService:
    def __init__(self, path: Path | None = None) -> None:
        self.path = Path(path or (CACHE_DIR / "magnets" / "preferences.json"))
        self._lock = threading.Lock()

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return self._load()

    def apply_candidate_preferences(
        self,
        candidates: list[dict[str, Any]],
        *,
        movie: Mapping[str, Any] | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        preferences = self.snapshot()
        favorite_groups = {normalize_token(value) for value in preferences["favorite_release_groups"]}
        hidden_releases = {normalize_token(value) for value in preferences["hidden_releases"]}
        preferred_codecs = {normalize_token(value) for value in preferences["preferred_codecs"]}
        preferred_resolutions = {normalize_token(value) for value in preferences["preferred_resolutions"]}
        preferred_source_types = {normalize_token(value) for value in preferences["preferred_source_types"]}
        saved_sources = {
            normalize_token(item.get("fingerprint"))
            for item in preferences["saved_sources"]
            if isinstance(item, dict)
        }

        movie_name = str((movie or {}).get("title") or (movie or {}).get("name") or "").strip() or "unknown"
        visible: list[dict[str, Any]] = []
        hidden_count = 0

        for candidate in list(candidates or []):
            if not isinstance(candidate, dict):
                continue
            fingerprint = source_fingerprint(candidate)
            if fingerprint and normalize_token(fingerprint) in hidden_releases:
                hidden_count += 1
                emit_event(
                    "[source-preference]",
                    movie=movie_name,
                    hidden_release=fingerprint,
                    action="filter_hidden",
                )
                continue

            enriched = dict(candidate)
            enriched["source_fingerprint"] = fingerprint
            enriched["preference_boost"] = 0
            enriched["preference_reasons"] = []
            enriched["is_saved_source"] = normalize_token(fingerprint) in saved_sources if fingerprint else False
            enriched["is_favorite_group"] = normalize_token(candidate.get("release_group")) in favorite_groups

            self._apply_boost(
                enriched,
                movie_name=movie_name,
                active=enriched["is_favorite_group"],
                boost=GROUP_BOOST,
                reason="preferred_group",
                field_key="group",
                field_value=candidate.get("release_group"),
            )
            self._apply_boost(
                enriched,
                movie_name=movie_name,
                active=normalize_token(candidate.get("codec")) in preferred_codecs,
                boost=CODEC_BOOST,
                reason="preferred_codec",
                field_key="preferred_codec",
                field_value=candidate.get("codec"),
            )
            self._apply_boost(
                enriched,
                movie_name=movie_name,
                active=normalize_token(candidate.get("resolution")) in preferred_resolutions,
                boost=RESOLUTION_BOOST,
                reason="preferred_resolution",
                field_key="preferred_resolution",
                field_value=candidate.get("resolution"),
            )
            self._apply_boost(
                enriched,
                movie_name=movie_name,
                active=normalize_token(candidate.get("source_type")) in preferred_source_types,
                boost=SOURCE_TYPE_BOOST,
                reason="preferred_source_type",
                field_key="preferred_source_type",
                field_value=candidate.get("source_type"),
            )
            self._apply_boost(
                enriched,
                movie_name=movie_name,
                active=bool(enriched["is_saved_source"]),
                boost=SAVED_SOURCE_BOOST,
                reason="saved_source",
                field_key="fingerprint",
                field_value=fingerprint,
            )

            base_score = int(candidate.get("estimated_quality_score", 0) or 0)
            adjusted_score = max(0, min(100, base_score + int(enriched["preference_boost"] or 0)))
            enriched["base_quality_score"] = base_score
            enriched["estimated_quality_score"] = adjusted_score
            visible.append(enriched)

        sorted_candidates = sorted(
            visible,
            key=lambda candidate: (
                int(candidate.get("estimated_quality_score", 0) or 0),
                1 if candidate.get("likely_streamable") else 0,
                int(candidate.get("seeders", 0) or 0),
                float(candidate.get("size_gb", 0.0) or 0.0),
            ),
            reverse=True,
        )
        return sorted_candidates, {
            "hidden_count": hidden_count,
            "favorite_group_count": len(favorite_groups),
            "saved_source_count": len(saved_sources),
        }

    def favorite_release(self, source: Mapping[str, Any], *, movie: Mapping[str, Any] | None = None) -> dict[str, Any]:
        group = normalize_token(source.get("release_group"))
        if not group:
            return {"ok": False, "error": "This release has no detectable release group."}
        return self._update_preferences(
            lambda payload: self._merge_preference(payload, "favorite_release_groups", group),
            event_fields={
                "movie": self._movie_name(movie),
                "action": "favorite_group",
                "group": group,
            },
        )

    def hide_release(self, source: Mapping[str, Any], *, movie: Mapping[str, Any] | None = None) -> dict[str, Any]:
        fingerprint = source_fingerprint(source)
        if not fingerprint:
            return {"ok": False, "error": "This release could not be fingerprinted safely."}
        return self._update_preferences(
            lambda payload: self._merge_preference(payload, "hidden_releases", fingerprint),
            event_fields={
                "movie": self._movie_name(movie),
                "action": "hide_release",
                "fingerprint": fingerprint,
            },
        )

    def save_source(self, source: Mapping[str, Any], *, movie: Mapping[str, Any] | None = None) -> dict[str, Any]:
        fingerprint = source_fingerprint(source)
        if not fingerprint:
            return {"ok": False, "error": "This source could not be fingerprinted safely."}

        def mutate(payload: dict[str, Any]) -> bool:
            changed = False
            if self._merge_preference(payload, "preferred_codecs", normalize_token(source.get("codec"))):
                changed = True
            if self._merge_preference(payload, "preferred_resolutions", normalize_token(source.get("resolution"))):
                changed = True
            if self._merge_preference(payload, "preferred_source_types", normalize_token(source.get("source_type"))):
                changed = True
            group = normalize_token(source.get("release_group"))
            if group and self._merge_preference(payload, "favorite_release_groups", group):
                changed = True

            saved_sources = list(payload.get("saved_sources") or [])
            if not any(normalize_token(item.get("fingerprint")) == fingerprint for item in saved_sources if isinstance(item, dict)):
                saved_sources.append(
                    {
                        "fingerprint": fingerprint,
                        "movie_title": self._movie_name(movie),
                        "title": str(source.get("title") or "").strip(),
                        "provider": str(source.get("source") or source.get("provider") or "").strip().lower(),
                        "release_group": str(source.get("release_group") or "").strip().lower(),
                        "resolution": str(source.get("resolution") or "").strip(),
                        "codec": str(source.get("codec") or "").strip(),
                        "source_type": str(source.get("source_type") or "").strip(),
                        "magnet": str(source.get("magnet") or "").strip(),
                        "saved_at": int(time.time()),
                    }
                )
                payload["saved_sources"] = saved_sources[-100:]
                changed = True
            return changed

        return self._update_preferences(
            mutate,
            event_fields={
                "movie": self._movie_name(movie),
                "action": "save_source",
                "fingerprint": fingerprint,
                "codec": str(source.get("codec") or "").strip(),
                "resolution": str(source.get("resolution") or "").strip(),
                "source_type": str(source.get("source_type") or "").strip(),
            },
        )

    def _update_preferences(self, mutate, *, event_fields: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            payload = self._load()
            changed = bool(mutate(payload))
            if changed:
                save_json_file(self.path, payload)
        emit_event("[source-action]", **event_fields)
        return {"ok": True, "changed": changed, "preferences": self._public_snapshot(payload)}

    def _load(self) -> dict[str, Any]:
        payload = load_json_file(self.path, DEFAULT_PREFERENCES)
        if not isinstance(payload, dict):
            payload = {}
        normalized = {
            "version": 1,
            "favorite_release_groups": self._normalize_string_list(payload.get("favorite_release_groups")),
            "hidden_releases": self._normalize_string_list(payload.get("hidden_releases")),
            "preferred_codecs": self._normalize_string_list(payload.get("preferred_codecs")),
            "preferred_resolutions": self._normalize_string_list(payload.get("preferred_resolutions")),
            "preferred_source_types": self._normalize_string_list(payload.get("preferred_source_types")),
            "saved_sources": self._normalize_saved_sources(payload.get("saved_sources")),
        }
        return normalized

    def _public_snapshot(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {
            "favorite_release_groups": list(payload.get("favorite_release_groups") or []),
            "hidden_releases": list(payload.get("hidden_releases") or []),
            "preferred_codecs": list(payload.get("preferred_codecs") or []),
            "preferred_resolutions": list(payload.get("preferred_resolutions") or []),
            "preferred_source_types": list(payload.get("preferred_source_types") or []),
            "saved_source_count": len(payload.get("saved_sources") or []),
        }

    def _movie_name(self, movie: Mapping[str, Any] | None) -> str:
        return str((movie or {}).get("title") or (movie or {}).get("name") or "").strip() or "unknown"

    def _merge_preference(self, payload: dict[str, Any], key: str, value: str) -> bool:
        normalized_value = normalize_token(value)
        if not normalized_value:
            return False
        current = self._normalize_string_list(payload.get(key))
        if normalized_value in current:
            payload[key] = current
            return False
        current.append(normalized_value)
        payload[key] = current
        return True

    def _normalize_string_list(self, values: Any) -> list[str]:
        normalized: list[str] = []
        for value in list(values or []):
            token = normalize_token(value)
            if token and token not in normalized:
                normalized.append(token)
        return normalized

    def _normalize_saved_sources(self, values: Any) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        seen: set[str] = set()
        for item in list(values or []):
            if not isinstance(item, dict):
                continue
            fingerprint = normalize_token(item.get("fingerprint"))
            if not fingerprint or fingerprint in seen:
                continue
            seen.add(fingerprint)
            normalized.append(
                {
                    "fingerprint": fingerprint,
                    "movie_title": str(item.get("movie_title") or "").strip(),
                    "title": str(item.get("title") or "").strip(),
                    "provider": normalize_token(item.get("provider")),
                    "release_group": normalize_token(item.get("release_group")),
                    "resolution": str(item.get("resolution") or "").strip(),
                    "codec": str(item.get("codec") or "").strip(),
                    "source_type": str(item.get("source_type") or "").strip(),
                    "magnet": str(item.get("magnet") or "").strip(),
                    "saved_at": int(item.get("saved_at", 0) or 0),
                }
            )
        return normalized[-100:]

    def _apply_boost(
        self,
        candidate: dict[str, Any],
        *,
        movie_name: str,
        active: bool,
        boost: int,
        reason: str,
        field_key: str,
        field_value: Any,
    ) -> None:
        if not active:
            return
        candidate["preference_boost"] = int(candidate.get("preference_boost", 0) or 0) + boost
        reasons = list(candidate.get("preference_reasons") or [])
        reasons.append(reason)
        candidate["preference_reasons"] = reasons
        emit_event(
            "[source-preference]",
            movie=movie_name,
            **{field_key: str(field_value or "").strip()},
            score_boost=boost,
        )

