from __future__ import annotations

from typing import Any, Mapping


def build_runtime_diagnostics(
    *,
    source: Mapping[str, Any],
    capability: Mapping[str, Any],
    playback_runtime: str,
    runtime_profile: Mapping[str, Any] | None = None,
    penalties: list[str] | None = None,
    browser_runtime: Mapping[str, Any] | None = None,
    external_runtime: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    source_data = dict(source or {})
    profile = dict(runtime_profile or {})
    browser = dict(browser_runtime or {})
    external = dict(external_runtime or {})
    capability_data = dict(capability or {})
    penalty_list = _unique_strings(list(penalties or []) + list(profile.get("warnings") or []))
    selected_reasoning = []
    compatibility_notes = list(capability_data.get("notes") or [])
    confidence_factors = []
    warnings = []

    if source_data.get("trusted_group"):
        selected_reasoning.append("Trusted release group boosted selection stability.")
    if source_data.get("seeders", 0):
        selected_reasoning.append(f"Seeder depth favored startup reliability ({int(source_data.get('seeders') or 0)} seeders).")
    if source_data.get("browser_playable_candidate"):
        selected_reasoning.append("Source is browser-safe under current runtime policy.")
    else:
        selected_reasoning.append("Source stayed selected for overall quality even though browser playback is constrained.")
    if source_data.get("estimated_quality_score"):
        selected_reasoning.append(f"Estimated quality score remained competitive ({int(source_data.get('estimated_quality_score') or 0)}).")

    if capability_data.get("browser_friendly"):
        compatibility_notes.append("Browser playback allowed by codec, container, size, and transport heuristics.")
    else:
        compatibility_notes.append("Browser playback rejected by capability matrix.")
    if capability_data.get("external_player_ready"):
        compatibility_notes.append("External handoff remains available.")
    else:
        compatibility_notes.append("External handoff confidence is limited.")

    if playback_runtime == "external_runtime":
        fallback_reason = "Browser runtime was rejected or degraded, so external playback is the deterministic fallback."
    elif external.get("readiness") == "external_ready":
        fallback_reason = "External player is still prepared as a fallback if the browser surface fails."
    else:
        fallback_reason = "Fallback path is limited."

    if capability_data.get("startup_risk") == "low":
        confidence_factors.append("Low startup risk from size, codec, and remux profile.")
    elif capability_data.get("startup_risk") == "medium":
        confidence_factors.append("Startup is viable, but heavier source characteristics raise fallback probability.")
    else:
        confidence_factors.append("Startup risk is elevated by transport, codec, or source constraints.")

    if capability_data.get("high_bandwidth_required"):
        confidence_factors.append("High bandwidth expectation reduces startup confidence.")
    if capability_data.get("mobile_friendly"):
        confidence_factors.append("Mobile-safe profile preserved browser confidence.")
    if capability_data.get("hdr") or capability_data.get("dolby_vision"):
        warnings.append("Expanded dynamic range flags reduce browser certainty.")

    for penalty in penalty_list:
        warnings.append(f"Penalty applied: {penalty.replace('_', ' ')}.")

    if browser.get("warnings"):
        warnings.extend(_humanize_warning(value) for value in browser.get("warnings", []))
    if external.get("warnings"):
        warnings.extend(_humanize_warning(value) for value in external.get("warnings", []))

    return {
        "selected_reasoning": _unique_strings(selected_reasoning),
        "warnings": _unique_strings(warnings),
        "compatibility_notes": _unique_strings(_humanize_warning(value) for value in compatibility_notes),
        "fallback_reason": str(fallback_reason),
        "confidence_factors": _unique_strings(confidence_factors),
        "penalties_applied": penalty_list,
    }


def _humanize_warning(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return text.replace("_", " ").capitalize()


def _unique_strings(values) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return ordered
