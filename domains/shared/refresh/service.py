from .types import RefreshAction, RefreshState, StaleInfo


class RefreshService:
    """Small shared helper for describing refresh state without owning refresh I/O."""

    def __init__(self, *, format_timestamp_label):
        self.format_timestamp_label = format_timestamp_label

    def detect_stale(
        self,
        *,
        last_refreshed_at="",
        age_seconds=None,
        missing=False,
        aging_after_seconds=6 * 60 * 60,
        stale_after_seconds=24 * 60 * 60,
        missing_label="Snapshot missing",
        fresh_label="Fresh snapshot",
        aging_label="Snapshot aging",
        stale_label="Stale snapshot",
    ):
        if missing:
            return StaleInfo(
                state="missing",
                label=missing_label,
                is_stale=True,
                age_seconds=None,
                last_refreshed_at=str(last_refreshed_at or "").strip(),
            )
        normalized_age = None if age_seconds is None else max(0, int(age_seconds))
        if normalized_age is None:
            state = "missing"
            label = missing_label
            is_stale = True
        elif normalized_age <= int(aging_after_seconds):
            state = "fresh"
            label = fresh_label
            is_stale = False
        elif normalized_age <= int(stale_after_seconds):
            state = "aging"
            label = aging_label
            is_stale = False
        else:
            state = "stale"
            label = stale_label
            is_stale = True
        return StaleInfo(
            state=state,
            label=label,
            is_stale=is_stale,
            age_seconds=normalized_age,
            last_refreshed_at=str(last_refreshed_at or "").strip(),
        )

    def build_state(
        self,
        *,
        last_refreshed_at="",
        age_seconds=None,
        missing=False,
        refresh_status="idle",
        refresh_error="",
        refresh_now_enabled=True,
        background_revalidate_enabled=False,
        background_revalidate_placeholder=True,
    ):
        stale = self.detect_stale(
            last_refreshed_at=last_refreshed_at,
            age_seconds=age_seconds,
            missing=missing,
        )
        normalized_last_refreshed_at = stale.last_refreshed_at
        return RefreshState(
            last_refreshed_at=normalized_last_refreshed_at,
            last_refreshed_at_display=self.format_timestamp_label(normalized_last_refreshed_at, default="Unknown"),
            refresh_status=str(refresh_status or "").strip() or ("missing" if stale.state == "missing" else "idle"),
            refresh_error=str(refresh_error or "").strip(),
            stale=stale,
            refresh_now=RefreshAction(key="refresh_now", enabled=bool(refresh_now_enabled), placeholder=False),
            background_revalidate=RefreshAction(
                key="background_revalidate",
                enabled=bool(background_revalidate_enabled),
                placeholder=bool(background_revalidate_placeholder),
            ),
        )

    def apply_result(self, state, result):
        state_payload = state.to_dict() if hasattr(state, "to_dict") else {}
        result_payload = result.to_dict() if hasattr(result, "to_dict") else {}
        next_last_refreshed_at = str(
            result_payload.get("last_refreshed_at", "") or state_payload.get("last_refreshed_at", "") or ""
        ).strip()
        next_refresh_error = str(result_payload.get("refresh_error", "") or "").strip()
        return self.build_state(
            last_refreshed_at=next_last_refreshed_at,
            age_seconds=0 if next_last_refreshed_at else None,
            missing=not bool(next_last_refreshed_at),
            refresh_status=str(result_payload.get("status", "") or state_payload.get("refresh_status", "") or "idle").strip(),
            refresh_error=next_refresh_error,
            refresh_now_enabled=bool((state_payload.get("refresh_now", {}) or {}).get("enabled", True)),
            background_revalidate_enabled=bool((state_payload.get("background_revalidate", {}) or {}).get("enabled", False)),
            background_revalidate_placeholder=bool((state_payload.get("background_revalidate", {}) or {}).get("placeholder", True)),
        )
