from __future__ import annotations

from typing import Any, Mapping


def build_historical_patterns(
    runtime_memory_summary: Mapping[str, Any] | None,
    *,
    current_context: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    summary = dict(runtime_memory_summary or {})
    context = dict(current_context or {})
    runtime_profiles = dict(summary.get("runtime_profiles") or {})
    source_characteristics = dict(summary.get("source_characteristics") or {})
    patterns: list[dict[str, Any]] = []

    cinematic = dict(runtime_profiles.get("browser_cinematic") or {})
    if _ratio(cinematic, "degraded") >= 0.5 and int(cinematic.get("selected", 0) or 0) >= 2:
        patterns.append(_pattern("cinematic_profile_often_downgrades", cinematic, "degraded"))

    remux_key = _find_matching_key(source_characteristics, "remux")
    if remux_key and _ratio(dict(source_characteristics.get(remux_key) or {}), "instability") >= 0.5:
        patterns.append(_pattern("browser_runtime_unstable_on_remux", dict(source_characteristics.get(remux_key) or {}), "instability"))

    mobile_key = _find_matching_key(source_characteristics, "x264|1080")
    if mobile_key and _ratio(dict(source_characteristics.get(mobile_key) or {}), "recovered") >= 0.4:
        patterns.append(_pattern("mobile_runtime_prefers_x264_1080p", dict(source_characteristics.get(mobile_key) or {}), "recovered"))

    external = dict((summary.get("playback_runtimes") or {}).get("external_runtime") or {})
    if _ratio(external, "recovered") >= 0.4 and int(external.get("selected", 0) or 0) >= 2:
        patterns.append(_pattern("external_runtime_succeeds_after_startup_degradation", external, "recovered"))

    profile = str(context.get("runtime_profile") or "").strip()
    if profile and profile in runtime_profiles and not patterns:
        payload = dict(runtime_profiles.get(profile) or {})
        patterns.append(_pattern("profile_observed_without_strong_bias", payload, "selected"))

    patterns.sort(key=lambda item: (item["confidence"], item["evidence_count"], item["pattern_type"]), reverse=True)
    return patterns


def _pattern(pattern_type: str, payload: Mapping[str, Any], evidence_key: str) -> dict[str, Any]:
    evidence_count = int(payload.get(evidence_key, 0) or payload.get("selected", 0) or 0)
    total = int(payload.get("selected", 0) or payload.get("total", 0) or 0)
    confidence = max(35, min(96, int(round(40 + (_safe_ratio(evidence_count, total) * 40) + min(total, 8) * 2))))
    return {
        "pattern_type": pattern_type,
        "confidence": confidence,
        "evidence_count": evidence_count,
    }


def _find_matching_key(bucket: Mapping[str, Any], token: str) -> str:
    probe = str(token or "").strip().lower()
    for key in bucket:
        if probe in str(key or "").lower():
            return str(key)
    return ""


def _ratio(payload: Mapping[str, Any], key: str) -> float:
    return _safe_ratio(int(payload.get(key, 0) or 0), int(payload.get("selected", 0) or payload.get("total", 0) or 0))


def _safe_ratio(value: int, total: int) -> float:
    return int(value or 0) / max(int(total or 0), 1)
