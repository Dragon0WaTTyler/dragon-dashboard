from __future__ import annotations

from typing import Any, Mapping


_FAILURE_TEMPLATES = {
    "invalid_magnet": {
        "category": "input",
        "message": "The selected source has an invalid magnet link.",
    },
    "unsupported_codec": {
        "category": "compatibility",
        "message": "The selected source uses a codec this runtime cannot safely start.",
    },
    "browser_policy_block": {
        "category": "policy",
        "message": "Browser playback is blocked for this source profile.",
    },
    "startup_confidence_too_low": {
        "category": "stability",
        "message": "Startup confidence is too low for direct runtime handoff.",
    },
    "external_runtime_required": {
        "category": "handoff",
        "message": "This source requires external playback.",
    },
    "bandwidth_insufficient": {
        "category": "bandwidth",
        "message": "The current source requires more bandwidth than the runtime envelope allows.",
    },
    "source_sanity_failed": {
        "category": "source",
        "message": "The selected source failed sanity checks.",
    },
}


def build_runtime_failure(
    code: str,
    *,
    diagnostics: Mapping[str, Any] | None = None,
    message: str = "",
) -> dict[str, Any]:
    normalized_code = str(code or "").strip() or "runtime_failure"
    template = _FAILURE_TEMPLATES.get(normalized_code, {})
    return {
        "code": normalized_code,
        "category": str(template.get("category") or "runtime"),
        "user_safe_message": str(message or template.get("message") or "Runtime preparation failed.").strip(),
        "diagnostics": dict(diagnostics or {}),
    }
