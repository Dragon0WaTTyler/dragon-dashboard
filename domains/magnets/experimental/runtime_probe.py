from __future__ import annotations

from typing import Any, Mapping

from .compatibility_probe import probe_runtime_compatibility
from .transport_probe import probe_transport_readiness
from .webtorrent_probe import probe_webtorrent_viability


def build_runtime_probe(
    candidate: Mapping[str, Any],
    *,
    movie: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    compatibility_probe = probe_runtime_compatibility(candidate, movie=movie)
    compatibility = dict(compatibility_probe.get("compatibility") or {})
    diagnostics = dict(compatibility_probe.get("diagnostics") or {})
    transport_probe = probe_transport_readiness(
        candidate,
        diagnostics=diagnostics,
        compatibility=compatibility,
    )
    webtorrent_probe = probe_webtorrent_viability(
        candidate,
        compatibility=compatibility,
    )
    return {
        "probe": "runtime",
        "sandbox": {
            "experimental_only": True,
            "isolated_runtime": True,
            "no_production_side_effects": True,
        },
        "runtime_probe_results": {
            "compatibility_probe": compatibility_probe,
            "transport_probe": transport_probe,
            "webtorrent_probe": webtorrent_probe,
        },
        "runtime_capability_snapshot": dict(compatibility_probe.get("runtime_capability_snapshot") or {}),
        "browser_transport_readiness": {
            "status": transport_probe.get("browser_transport"),
            "readiness_score": transport_probe.get("readiness_score"),
            "reasons": list(transport_probe.get("reasons") or []),
        },
        "mobile_runtime_warnings": _mobile_runtime_warnings(compatibility_probe, transport_probe),
        "experimental_runtime_support_matrix": _support_matrix(compatibility_probe, transport_probe, webtorrent_probe),
    }


def _mobile_runtime_warnings(
    compatibility_probe: Mapping[str, Any],
    transport_probe: Mapping[str, Any],
) -> list[str]:
    warnings: list[str] = []
    snapshot = dict(compatibility_probe.get("runtime_capability_snapshot") or {})
    if snapshot.get("mobile_runtime_risk") != "low":
        warnings.append("mobile_runtime_risk")
    warnings.extend([str(item) for item in list(transport_probe.get("reasons") or []) if "mobile" in str(item)])
    return warnings


def _support_matrix(
    compatibility_probe: Mapping[str, Any],
    transport_probe: Mapping[str, Any],
    webtorrent_probe: Mapping[str, Any],
) -> dict[str, Any]:
    protocol = dict(compatibility_probe.get("protocol_compatibility") or {})
    return {
        "browser": {
            "status": str(transport_probe.get("browser_transport") or "unsupported"),
            "webtorrent_possible": bool(webtorrent_probe.get("browser_webtorrent_possible")),
        },
        "external_player": {
            "status": "supported" if bool(protocol.get("external_protocol_safe")) else "limited",
        },
        "mobile": {
            "status": "supported" if str((compatibility_probe.get("runtime_capability_snapshot") or {}).get("mobile_runtime_risk")) == "low" else "limited",
        },
    }
