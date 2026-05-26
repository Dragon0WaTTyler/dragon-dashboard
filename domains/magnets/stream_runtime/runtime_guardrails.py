from __future__ import annotations

from typing import Any, Mapping

from ..playback.runtime_policy import is_browser_rejected_source_type
from .runtime_failures import build_runtime_failure


def evaluate_runtime_guardrails(
    *,
    source: Mapping[str, Any] | None = None,
    capability_snapshot: Mapping[str, Any] | None = None,
    runtime_mode: str = "",
    startup_confidence: str = "",
    runtime_profile: str = "",
) -> dict[str, Any]:
    source_data = dict(source or {})
    capability = dict(capability_snapshot or {})
    mode = str(runtime_mode or "").strip() or "external_runtime"
    confidence = str(startup_confidence or "").strip().lower() or "low"
    profile = str(runtime_profile or "").strip()
    warnings: list[str] = []
    blocking_reasons: list[str] = []
    failures: list[dict[str, Any]] = []

    def block(code: str, *, details: Mapping[str, Any] | None = None) -> None:
        if code not in blocking_reasons:
            blocking_reasons.append(code)
            failures.append(build_runtime_failure(code, diagnostics=details))

    magnet_valid = bool(capability.get("magnet_valid"))
    browser_friendly = bool(capability.get("browser_friendly"))
    codec_compatible = bool(capability.get("codec_compatible"))
    high_bandwidth_required = bool(capability.get("high_bandwidth_required"))
    size_sanity = dict(capability.get("size_sanity") or {})
    likely_streamable = bool(capability.get("likely_streamable", True))
    source_type = str(capability.get("source_type") or source_data.get("source_type") or "").strip()

    if not magnet_valid:
        block("invalid_magnet", details={"runtime_mode": mode})
    if not codec_compatible:
        block("unsupported_codec", details={"codec": capability.get("codec")})
    if not size_sanity.get("is_sane", True) or not likely_streamable:
        block(
            "source_sanity_failed",
            details={"size_reasons": list(size_sanity.get("reasons") or []), "likely_streamable": likely_streamable},
        )
    if high_bandwidth_required and mode == "browser_runtime":
        block("bandwidth_insufficient", details={"bandwidth_class": capability.get("bandwidth_class")})
    if confidence == "low" and mode == "browser_runtime":
        block("startup_confidence_too_low", details={"startup_confidence": confidence, "runtime_profile": profile})
    if mode == "browser_runtime" and (not browser_friendly or is_browser_rejected_source_type(source_type)):
        block(
            "browser_policy_block",
            details={"source_type": source_type, "browser_friendly": browser_friendly},
        )
    if mode == "external_runtime" and browser_friendly:
        warnings.append("browser_runtime_available")
    if mode == "browser_runtime" and bool(capability.get("remux_heavy")):
        block("browser_policy_block", details={"reason": "remux_heavy"})
    if mode == "external_runtime" and not bool(capability.get("external_player_ready")):
        block("external_runtime_required", details={"external_player_ready": False})

    return {
        "allowed": not blocking_reasons,
        "warnings": warnings,
        "blocking_reasons": blocking_reasons,
        "failures": failures,
    }
