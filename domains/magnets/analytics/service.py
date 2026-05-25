from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from ..observability import SessionObservabilityService
from ..runtime.intelligence import counter_to_ranked_list, failure_ratio_items, top_ratio_items
from ..sessions.intelligence import build_session_intelligence_context
from .store import SessionAnalyticsStore


SESSION_EVENTS = {
    "session_created",
    "session_prepared",
    "session_handoff_success",
    "session_handoff_failed",
    "session_expired",
    "browser_attempted",
    "external_player_used",
    "mobile_handoff_used",
}


class SessionAnalyticsService:
    def __init__(
        self,
        *,
        store: SessionAnalyticsStore | None = None,
        observability: SessionObservabilityService | None = None,
    ) -> None:
        self.store = store or SessionAnalyticsStore()
        self.observability = observability or SessionObservabilityService()

    def track_session_event(
        self,
        event_name: str,
        *,
        session: Mapping[str, Any],
        source: Mapping[str, Any] | None = None,
        extra: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_event = str(event_name or "").strip().lower()
        if normalized_event not in SESSION_EVENTS:
            return {"ok": False, "error": "Unsupported session analytics event."}

        context = build_session_intelligence_context(session, event_name=normalized_event, source=source)
        context.update({str(key or "").strip(): value for key, value in dict(extra or {}).items() if str(key or "").strip()})
        context["tracked_at"] = self._utc_now_iso()

        payload = self.store.update(lambda current: self._apply_event(current, context))
        self.observability.emit_session_analytics(normalized_event, context)
        codec_summary = self._build_codec_runtime_summary(payload, context["codec"])
        if codec_summary:
            self.observability.emit_runtime_intelligence(codec_summary)
        return {"ok": True, "event": normalized_event}

    def get_summary(self) -> dict[str, Any]:
        payload = self.store.load()
        aggregate = dict(payload.get("aggregate") or {})
        source_type_stats = dict(aggregate.get("source_type_stats") or {})
        codec_stats = dict(aggregate.get("codec_stats") or {})
        release_pattern_stats = dict(aggregate.get("release_pattern_stats") or {})
        runtime_stats = dict(aggregate.get("runtime_stats") or {})
        invalid_magnet = dict(aggregate.get("invalid_magnet_frequency") or {})
        high_bandwidth = dict(aggregate.get("high_bandwidth_failure_frequency") or {})
        return {
            "ok": True,
            "meta": dict(payload.get("meta") or {}),
            "event_counts": dict(aggregate.get("event_counts") or {}),
            "deterministic_metrics": {
                "browser_failure_reasons": counter_to_ranked_list(aggregate.get("browser_failure_reasons") or {}, limit=5),
                "external_success_patterns": counter_to_ranked_list(aggregate.get("external_success_patterns") or {}, limit=5),
                "mobile_compatibility_patterns": counter_to_ranked_list(aggregate.get("mobile_compatibility_patterns") or {}, limit=5),
                "high_bandwidth_failure_frequency": {
                    "failed": int(high_bandwidth.get("failed", 0) or 0),
                    "total": int(high_bandwidth.get("total", 0) or 0),
                    "rate": self._safe_ratio(high_bandwidth.get("failed"), high_bandwidth.get("total")),
                },
                "invalid_magnet_frequency": {
                    "invalid": int(invalid_magnet.get("invalid", 0) or 0),
                    "total": int(invalid_magnet.get("total", 0) or 0),
                    "rate": self._safe_ratio(invalid_magnet.get("invalid"), invalid_magnet.get("total")),
                },
                "preferred_runtime_frequency": counter_to_ranked_list(aggregate.get("preferred_runtime_frequency") or {}, limit=5),
            },
            "runtime_summaries": {
                "most_reliable_source_types": top_ratio_items(source_type_stats, success_key="handoff_success", minimum_total=1),
                "most_successful_codecs": top_ratio_items(codec_stats, success_key="handoff_success", minimum_total=1),
                "most_problematic_release_patterns": failure_ratio_items(release_pattern_stats, failure_key="handoff_failed", minimum_total=1),
                "browser_safe_release_tendencies": top_ratio_items(release_pattern_stats, success_key="browser_safe", minimum_total=1),
            },
            "admin_debug": {
                "top_successful_codecs": top_ratio_items(codec_stats, success_key="handoff_success", minimum_total=1, limit=5),
                "top_failed_source_types": failure_ratio_items(source_type_stats, failure_key="handoff_failed", minimum_total=1, limit=5),
                "browser_safe_percentages": top_ratio_items(release_pattern_stats, success_key="browser_safe", minimum_total=1, limit=5),
                "external_player_preference_ratios": counter_to_ranked_list(aggregate.get("preferred_runtime_frequency") or {}, limit=5),
            },
            "runtime_consumption_snapshot": {
                "compatibility_intelligence": {
                    "source_type_stats": source_type_stats,
                    "codec_stats": codec_stats,
                    "release_pattern_stats": release_pattern_stats,
                },
                "runtime_reliability": runtime_stats,
            },
        }

    def _apply_event(self, payload: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
        aggregate = dict(payload.get("aggregate") or {})
        events = list(payload.get("events") or [])
        events.append(context)
        payload["events"] = events[-self.store.max_events :]

        event_counts = self._counter(aggregate, "event_counts")
        self._increment(event_counts, context["event_name"])

        preferred_runtime = self._counter(aggregate, "preferred_runtime_frequency")
        preferred_key = str(context.get("preferred_runtime") or context.get("runtime_intent") or "unknown").strip() or "unknown"
        self._increment(preferred_runtime, preferred_key)

        runtime_stats = self._nested_counter(aggregate, "runtime_stats", str(context.get("runtime_intent") or "unknown"))
        runtime_stats["count"] = int(runtime_stats.get("count", 0) or 0) + 1

        invalid_magnet = dict(aggregate.get("invalid_magnet_frequency") or {})
        invalid_magnet["total"] = int(invalid_magnet.get("total", 0) or 0) + 1
        if not context.get("magnet_valid", False):
            invalid_magnet["invalid"] = int(invalid_magnet.get("invalid", 0) or 0) + 1
        aggregate["invalid_magnet_frequency"] = invalid_magnet

        high_bandwidth = dict(aggregate.get("high_bandwidth_failure_frequency") or {})
        if context.get("high_bandwidth_required", False):
            high_bandwidth["total"] = int(high_bandwidth.get("total", 0) or 0) + 1
            if context["event_name"] == "session_handoff_failed":
                high_bandwidth["failed"] = int(high_bandwidth.get("failed", 0) or 0) + 1
        aggregate["high_bandwidth_failure_frequency"] = high_bandwidth

        source_stats = self._nested_counter(aggregate, "source_type_stats", context["source_type"])
        codec_stats = self._nested_counter(aggregate, "codec_stats", context["codec"])
        release_stats = self._nested_counter(aggregate, "release_pattern_stats", context["release_pattern"])
        for bucket in (source_stats, codec_stats, release_stats):
            bucket["total"] = int(bucket.get("total", 0) or 0) + 1

        if context.get("browser_friendly"):
            release_stats["browser_safe"] = int(release_stats.get("browser_safe", 0) or 0) + 1
            codec_stats["browser_safe"] = int(codec_stats.get("browser_safe", 0) or 0) + 1
        if context.get("mobile_friendly"):
            release_stats["mobile_compatible"] = int(release_stats.get("mobile_compatible", 0) or 0) + 1
            codec_stats["mobile_compatible"] = int(codec_stats.get("mobile_compatible", 0) or 0) + 1
        if context["event_name"] == "browser_attempted":
            codec_stats["browser_attempted"] = int(codec_stats.get("browser_attempted", 0) or 0) + 1
        if context["event_name"] == "session_handoff_success":
            for bucket in (source_stats, codec_stats, release_stats):
                bucket["handoff_success"] = int(bucket.get("handoff_success", 0) or 0) + 1
            runtime_stats["success"] = int(runtime_stats.get("success", 0) or 0) + 1
            if context.get("runtime_intent") == "browser_stream":
                codec_stats["browser_success"] = int(codec_stats.get("browser_success", 0) or 0) + 1
            if context.get("runtime_intent") == "external_player":
                codec_stats["external_success"] = int(codec_stats.get("external_success", 0) or 0) + 1
                patterns = self._counter(aggregate, "external_success_patterns")
                self._increment(patterns, context["release_pattern"])
            if context.get("runtime_intent") == "mobile_handoff":
                codec_stats["mobile_success"] = int(codec_stats.get("mobile_success", 0) or 0) + 1
        if context["event_name"] == "session_handoff_failed":
            for bucket in (source_stats, codec_stats, release_stats):
                bucket["handoff_failed"] = int(bucket.get("handoff_failed", 0) or 0) + 1
            runtime_stats["failed"] = int(runtime_stats.get("failed", 0) or 0) + 1
            if context.get("runtime_intent") == "browser_stream":
                reasons = self._counter(aggregate, "browser_failure_reasons")
                self._increment(reasons, context.get("failure_reason") or context.get("blocked_reason") or "unknown")
        if context["event_name"] == "mobile_handoff_used":
            patterns = self._counter(aggregate, "mobile_compatibility_patterns")
            self._increment(patterns, context["release_pattern"])

        payload["aggregate"] = aggregate
        return payload

    def _build_codec_runtime_summary(self, payload: Mapping[str, Any], codec: str) -> dict[str, Any]:
        codec_key = str(codec or "").strip() or "unknown"
        codec_stats = dict((payload.get("aggregate") or {}).get("codec_stats") or {})
        stats = dict(codec_stats.get(codec_key) or {})
        total = int(stats.get("total", 0) or 0)
        if total <= 0:
            return {}
        return {
            "codec": codec_key,
            "browser_success_rate": self._safe_ratio(stats.get("browser_success"), stats.get("browser_attempted")),
            "external_success_rate": self._safe_ratio(stats.get("external_success"), stats.get("handoff_success")),
            "mobile_success_rate": self._safe_ratio(stats.get("mobile_success"), stats.get("mobile_compatible")),
        }

    def _counter(self, aggregate: dict[str, Any], key: str) -> dict[str, int]:
        counter = aggregate.get(key)
        if not isinstance(counter, dict):
            counter = {}
            aggregate[key] = counter
        return counter

    def _nested_counter(self, aggregate: dict[str, Any], key: str, item_key: str) -> dict[str, int]:
        counter = self._counter(aggregate, key)
        normalized_key = str(item_key or "unknown").strip() or "unknown"
        item = counter.get(normalized_key)
        if not isinstance(item, dict):
            item = {}
            counter[normalized_key] = item
        return item

    def _increment(self, counter: dict[str, int], key: Any) -> None:
        normalized_key = str(key or "unknown").strip() or "unknown"
        counter[normalized_key] = int(counter.get(normalized_key, 0) or 0) + 1

    def _safe_ratio(self, numerator: Any, denominator: Any) -> float:
        try:
            numerator_value = float(numerator or 0.0)
            denominator_value = float(denominator or 0.0)
        except (TypeError, ValueError):
            return 0.0
        if denominator_value <= 0:
            return 0.0
        return round(numerator_value / denominator_value, 4)

    def _utc_now_iso(self) -> str:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
