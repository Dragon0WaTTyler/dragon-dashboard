from __future__ import annotations

from typing import Any, Mapping

from ..runtime.intelligence import build_release_pattern


def probe_transport_readiness(
    candidate: Mapping[str, Any],
    *,
    diagnostics: Mapping[str, Any] | None = None,
    compatibility: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    data = dict(candidate or {})
    diagnostics_data = dict(diagnostics or {})
    compatibility_data = dict(compatibility or {})
    magnet_valid = bool(diagnostics_data.get("magnet_valid"))
    browser_friendly = bool(compatibility_data.get("browser_friendly"))
    external_ready = bool(compatibility_data.get("external_player_ready"))
    mobile_friendly = bool(compatibility_data.get("mobile_friendly"))
    high_bandwidth = bool(compatibility_data.get("high_bandwidth_required"))

    reasons: list[str] = []
    if not magnet_valid:
        reasons.append("invalid_magnet")
    if high_bandwidth:
        reasons.append("high_bandwidth_runtime_risk")
    if not browser_friendly:
        reasons.append("browser_transport_limited")

    if browser_friendly:
        browser_transport = "supported"
    elif magnet_valid:
        browser_transport = "limited"
    else:
        browser_transport = "unsupported"

    readiness_score = 0
    if magnet_valid:
        readiness_score += 35
    if browser_friendly:
        readiness_score += 35
    if external_ready:
        readiness_score += 15
    if mobile_friendly:
        readiness_score += 15
    if high_bandwidth:
        readiness_score = max(readiness_score - 20, 0)

    return {
        "probe": "transport",
        "candidate": str(data.get("release_group") or data.get("source") or "unknown"),
        "browser_transport": browser_transport,
        "browser_transport_ready": browser_friendly,
        "protocol_compatibility": "magnet_uri" if magnet_valid else "invalid",
        "readiness_score": readiness_score,
        "mobile_runtime_risk": "low" if mobile_friendly else "elevated",
        "high_bandwidth_runtime_risk": "elevated" if high_bandwidth else "low",
        "release_pattern": build_release_pattern(data),
        "reasons": reasons,
    }
