from __future__ import annotations

from typing import Any, Mapping

from ..runtime.observability import emit_event


class SessionObservabilityService:
    def emit_session_analytics(self, event_name: str, payload: Mapping[str, Any]) -> None:
        emit_event(
            "[session-analytics]",
            movie=str(payload.get("movie_title") or "unknown"),
            event=event_name,
            runtime=str(payload.get("runtime_intent") or payload.get("preferred_runtime") or "unknown"),
            session=str(payload.get("session_id") or "").strip(),
        )

    def emit_runtime_intelligence(self, summary: Mapping[str, Any]) -> None:
        emit_event(
            "[runtime-intelligence]",
            codec=str(summary.get("codec") or "unknown"),
            browser_success_rate=self._format_ratio(summary.get("browser_success_rate")),
            external_success_rate=self._format_ratio(summary.get("external_success_rate")),
            mobile_success_rate=self._format_ratio(summary.get("mobile_success_rate")),
        )

    def emit_experimental_runtime(self, *, probe: str, status: str) -> None:
        emit_event(
            "[experimental-runtime]",
            probe=probe,
            status=status,
        )

    def emit_transport_probe(self, *, candidate: str, browser_transport: str) -> None:
        emit_event(
            "[transport-probe]",
            candidate=candidate,
            browser_transport=browser_transport,
        )

    def _format_ratio(self, value: Any) -> str:
        try:
            return f"{float(value or 0.0):.2f}"
        except (TypeError, ValueError):
            return "0.00"
