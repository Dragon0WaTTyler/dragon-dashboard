from __future__ import annotations

from typing import Any, Mapping


def probe_webtorrent_viability(
    candidate: Mapping[str, Any],
    *,
    compatibility: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    data = dict(candidate or {})
    compatibility_data = dict(compatibility or {})
    high_bandwidth = bool(compatibility_data.get("high_bandwidth_required"))
    browser_friendly = bool(compatibility_data.get("browser_friendly"))
    codec = str(data.get("codec") or "").strip() or "unknown"
    resolution = str(data.get("resolution") or "").strip() or "unknown"
    size_gb = _float_value(data.get("size_gb"))

    possible = bool(browser_friendly and not high_bandwidth)
    memory_risk = "elevated" if size_gb >= 8.5 or resolution == "2160p" else "low"
    status = "supported" if possible else "limited"
    reasons: list[str] = []
    if not browser_friendly:
        reasons.append("browser_transport_not_friendly")
    if high_bandwidth:
        reasons.append("high_bandwidth_runtime_risk")
    if codec.lower() == "x265":
        reasons.append("codec_browser_risk")

    return {
        "probe": "webtorrent",
        "experimental_only": True,
        "possible": possible,
        "status": status,
        "browser_webtorrent_possible": possible,
        "browser_memory_risk": memory_risk,
        "reasons": reasons,
    }


def _float_value(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
