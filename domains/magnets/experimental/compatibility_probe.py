from __future__ import annotations

from typing import Any, Mapping

from ..handoff.diagnostics import evaluate_streamability
from ..runtime.playback_policy import evaluate_playback_admission


def probe_runtime_compatibility(
    candidate: Mapping[str, Any],
    *,
    movie: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    data = dict(candidate or {})
    diagnostics = evaluate_streamability(data, movie=movie)
    admission = evaluate_playback_admission(data, movie=movie)
    snapshot = dict(admission.get("snapshot") or {})
    compatibility = dict(snapshot.get("compatibility") or {})
    quality = dict(snapshot.get("quality") or {})
    policy = dict(admission.get("policy") or {})

    return {
        "probe": "compatibility",
        "experimental_only": True,
        "magnet_validation": {
            "is_valid": bool((snapshot.get("magnet") or {}).get("is_valid")),
            "warnings": list((diagnostics.get("summary") or {}).get("warnings") or []),
        },
        "protocol_compatibility": {
            "magnet_uri": bool((snapshot.get("magnet") or {}).get("is_valid")),
            "browser_protocol_safe": bool(compatibility.get("browser_friendly")),
            "external_protocol_safe": bool(compatibility.get("external_player_ready")),
        },
        "streamability_estimate": {
            "likely_streamable": bool(quality.get("likely_streamable")),
            "compatibility_status": str(policy.get("compatibility_status") or "unknown"),
            "blocked_reason": str(policy.get("blocked_reason") or "").strip(),
        },
        "runtime_capability_snapshot": {
            "browser_webtorrent_possible": bool(compatibility.get("browser_friendly")),
            "browser_memory_risk": "elevated" if bool(compatibility.get("high_bandwidth_required")) else "low",
            "high_bandwidth_runtime_risk": "elevated" if bool(compatibility.get("high_bandwidth_required")) else "low",
            "mobile_runtime_risk": "low" if bool(compatibility.get("mobile_friendly")) else "elevated",
        },
        "compatibility": compatibility,
        "diagnostics": diagnostics.get("summary") or {},
        "policy": policy,
    }
