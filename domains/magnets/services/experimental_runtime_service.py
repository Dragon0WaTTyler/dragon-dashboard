from __future__ import annotations

import hashlib
from typing import Any, Mapping

from ..experimental import build_runtime_probe
from ..observability import SessionObservabilityService
from ..runtime.identifiers import normalize_token, source_fingerprint
from ..sessions import utc_now_iso
from .experimental_session_store import ExperimentalSessionStore


class ExperimentalRuntimeService:
    def __init__(
        self,
        *,
        store: ExperimentalSessionStore | None = None,
        observability: SessionObservabilityService | None = None,
    ) -> None:
        self.store = store or ExperimentalSessionStore()
        self.observability = observability or SessionObservabilityService()

    def run_probe(
        self,
        *,
        movie: Mapping[str, Any] | None = None,
        source: Mapping[str, Any] | None = None,
        preferred_runtime: str = "",
    ) -> dict[str, Any]:
        movie_data = dict(movie or {})
        source_data = dict(source or {})
        probe = build_runtime_probe(source_data, movie=movie_data)
        session = {
            "session_id": self._session_id(movie_data, source_data, preferred_runtime),
            "movie_id": self._movie_id(movie_data),
            "movie_title": self._movie_name(movie_data),
            "source_fingerprint": source_fingerprint(source_data),
            "preferred_runtime": str(preferred_runtime or "").strip(),
            "sandbox": {
                "experimental_only": True,
                "isolated_runtime": True,
                "no_production_side_effects": True,
            },
            "probe_state": "completed",
            "created_at": utc_now_iso(),
            "updated_at": utc_now_iso(),
            "probe": probe,
        }
        saved = self.store.save_session(session)
        self.observability.emit_experimental_runtime(
            probe="runtime",
            status=str((probe.get("browser_transport_readiness") or {}).get("status") or "unknown"),
        )
        transport_probe = dict((probe.get("runtime_probe_results") or {}).get("transport_probe") or {})
        self.observability.emit_transport_probe(
            candidate=str(source_data.get("release_group") or source_data.get("source") or "unknown"),
            browser_transport=str(transport_probe.get("browser_transport") or "unknown"),
        )
        return {
            "ok": True,
            "session": saved,
        }

    def get_session(self, session_id: str) -> dict[str, Any]:
        session = self.store.get_session(session_id)
        if not session:
            return {"ok": False, "error": "Unknown experimental session."}
        return {"ok": True, "session": session}

    def list_sessions(self) -> dict[str, Any]:
        return {"ok": True, "sessions": self.store.list_sessions()}

    def get_summary(self) -> dict[str, Any]:
        sessions = self.store.list_sessions()
        runtime_probe_results = []
        browser_transport_readiness: dict[str, int] = {}
        mobile_runtime_warnings: dict[str, int] = {}
        support_matrix = {
            "browser_supported": 0,
            "browser_limited": 0,
            "external_supported": 0,
            "mobile_supported": 0,
        }
        for session in sessions:
            probe = dict(session.get("probe") or {})
            runtime_probe_results.append(
                {
                    "session_id": str(session.get("session_id") or "").strip(),
                    "movie_title": str(session.get("movie_title") or "unknown").strip() or "unknown",
                    "browser_transport": str((probe.get("browser_transport_readiness") or {}).get("status") or "unknown"),
                    "readiness_score": int((probe.get("browser_transport_readiness") or {}).get("readiness_score") or 0),
                }
            )
            status = str((probe.get("browser_transport_readiness") or {}).get("status") or "unknown")
            browser_transport_readiness[status] = int(browser_transport_readiness.get(status, 0) or 0) + 1
            for warning in list(probe.get("mobile_runtime_warnings") or []):
                key = str(warning or "").strip()
                if key:
                    mobile_runtime_warnings[key] = int(mobile_runtime_warnings.get(key, 0) or 0) + 1
            matrix = dict(probe.get("experimental_runtime_support_matrix") or {})
            if str((matrix.get("browser") or {}).get("status") or "") == "supported":
                support_matrix["browser_supported"] += 1
            elif str((matrix.get("browser") or {}).get("status") or "") == "limited":
                support_matrix["browser_limited"] += 1
            if str((matrix.get("external_player") or {}).get("status") or "") == "supported":
                support_matrix["external_supported"] += 1
            if str((matrix.get("mobile") or {}).get("status") or "") == "supported":
                support_matrix["mobile_supported"] += 1

        return {
            "ok": True,
            "sandbox": {
                "experimental_only": True,
                "isolated_runtime": True,
                "no_production_side_effects": True,
            },
            "runtime_probe_results": runtime_probe_results[:10],
            "browser_transport_readiness": browser_transport_readiness,
            "mobile_runtime_warnings": mobile_runtime_warnings,
            "experimental_runtime_support_matrix": support_matrix,
        }

    def _session_id(self, movie: Mapping[str, Any], source: Mapping[str, Any], preferred_runtime: str) -> str:
        payload = "|".join(
            [
                normalize_token(self._movie_id(movie)),
                normalize_token(source_fingerprint(source)),
                normalize_token(preferred_runtime),
                normalize_token(utc_now_iso()),
            ]
        )
        return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:20]

    def _movie_id(self, movie: Mapping[str, Any]) -> str:
        return str(movie.get("movie_id") or movie.get("id") or movie.get("imdb_id") or self._movie_name(movie)).strip()

    def _movie_name(self, movie: Mapping[str, Any]) -> str:
        return str(movie.get("title") or movie.get("name") or "").strip() or "unknown"
