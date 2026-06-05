from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import re
from typing import Callable, Optional


FRESHNESS_STATES = ("fresh", "stale", "refreshing", "failed", "unknown", "disabled")


@dataclass(frozen=True)
class FreshnessStatus:
    state: str
    last_refreshed_at: str = ""
    stale_reason: str = ""
    source_label: str = ""
    safe_error: str = ""
    refresh_available: bool = True
    refresh_in_progress: bool = False
    next_action: str = ""
    age_seconds: Optional[int] = None
    is_stale: bool = False
    display_label: str = ""
    display_message: str = ""

    def to_dict(self):
        return {
            "state": self.state,
            "last_refreshed_at": self.last_refreshed_at,
            "stale_reason": self.stale_reason,
            "source_label": self.source_label,
            "safe_error": self.safe_error,
            "refresh_available": bool(self.refresh_available),
            "refresh_in_progress": bool(self.refresh_in_progress),
            "next_action": self.next_action,
            "age_seconds": self.age_seconds,
            "is_stale": bool(self.is_stale),
            "display_label": self.display_label,
            "display_message": self.display_message,
        }


def _coerce_datetime(value):
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def compute_age_seconds(last_refreshed_at="", now=None):
    refreshed_at = _coerce_datetime(last_refreshed_at)
    now_dt = _coerce_datetime(now) if now else datetime.now(timezone.utc)
    if refreshed_at is None or now_dt is None:
        return None
    return max(0, int((now_dt - refreshed_at).total_seconds()))


def sanitize_freshness_error(error, fallback="Refresh failed. Try again later."):
    message = str(error or "").strip()
    if not message:
        return ""
    lowered = message.lower()
    sensitive_markers = (
        "traceback",
        "token",
        "secret",
        "proxy",
        "forbidden",
        "permission denied",
        "c:\\",
        "/users/",
        "/home/",
        "\\",
    )
    if any(marker in lowered for marker in sensitive_markers):
        return fallback
    collapsed = re.sub(r"\s+", " ", message)
    return collapsed[:160].strip()


def build_freshness(
    *,
    last_refreshed_at="",
    now=None,
    ttl_seconds=24 * 60 * 60,
    stale_reason="",
    source_label="",
    error="",
    refresh_available=True,
    refresh_in_progress=False,
    next_action="",
    format_timestamp_label: Optional[Callable[[str, str], str]] = None,
):
    normalized_last_refreshed_at = str(last_refreshed_at or "").strip()
    normalized_source_label = str(source_label or "").strip()
    normalized_stale_reason = str(stale_reason or "").strip()
    safe_error = sanitize_freshness_error(error)
    age_seconds = compute_age_seconds(normalized_last_refreshed_at, now=now)
    normalized_ttl = max(0, int(ttl_seconds or 0))

    if not refresh_available:
        state = "disabled"
    elif refresh_in_progress:
        state = "refreshing"
    elif safe_error:
        state = "failed"
    elif not normalized_last_refreshed_at:
        state = "unknown"
    elif age_seconds is not None and normalized_ttl and age_seconds > normalized_ttl:
        state = "stale"
    else:
        state = "fresh"

    display_label_map = {
        "fresh": "Fresh",
        "stale": "Stale",
        "refreshing": "Refreshing",
        "failed": "Failed",
        "unknown": "Unknown",
        "disabled": "Disabled",
    }
    display_label = display_label_map[state]

    refreshed_display = ""
    if normalized_last_refreshed_at and callable(format_timestamp_label):
        refreshed_display = str(format_timestamp_label(normalized_last_refreshed_at, "") or "").strip()

    label_prefix = normalized_source_label or "Source"
    if state == "fresh":
        display_message = f"{label_prefix} is fresh."
        if refreshed_display:
            display_message = f"{display_message} Last refreshed {refreshed_display}."
    elif state == "stale":
        display_message = f"{label_prefix} may be stale."
        if refreshed_display:
            display_message = f"{display_message} Last refreshed {refreshed_display}."
    elif state == "refreshing":
        display_message = f"{label_prefix} refresh is in progress."
        if refreshed_display:
            display_message = f"{display_message} Last refreshed {refreshed_display}."
    elif state == "failed":
        display_message = safe_error or "Refresh failed. Try again later."
        if refreshed_display:
            display_message = f"{display_message} Last refreshed {refreshed_display}."
    elif state == "disabled":
        display_message = f"{label_prefix} refresh is unavailable."
    else:
        display_message = f"{label_prefix} freshness is unknown."

    if not next_action:
        if state in {"stale", "failed", "unknown"} and refresh_available:
            next_action = "refresh"
        elif state == "refreshing":
            next_action = "wait"
        else:
            next_action = "none"

    return FreshnessStatus(
        state=state,
        last_refreshed_at=normalized_last_refreshed_at,
        stale_reason=normalized_stale_reason,
        source_label=normalized_source_label,
        safe_error=safe_error,
        refresh_available=bool(refresh_available),
        refresh_in_progress=bool(refresh_in_progress),
        next_action=str(next_action or "").strip() or "none",
        age_seconds=age_seconds,
        is_stale=state in {"stale", "failed", "unknown"},
        display_label=display_label,
        display_message=display_message,
    )
