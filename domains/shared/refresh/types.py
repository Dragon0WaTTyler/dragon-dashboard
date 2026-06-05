from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class RefreshAction:
    key: str
    enabled: bool = False
    placeholder: bool = False

    def to_dict(self):
        return {
            "key": self.key,
            "enabled": bool(self.enabled),
            "placeholder": bool(self.placeholder),
        }


@dataclass(frozen=True)
class StaleInfo:
    state: str
    label: str
    is_stale: bool
    age_seconds: Optional[int]
    last_refreshed_at: str = ""

    def to_dict(self):
        return {
            "state": self.state,
            "label": self.label,
            "is_stale": bool(self.is_stale),
            "age_seconds": self.age_seconds,
            "last_refreshed_at": self.last_refreshed_at,
        }


@dataclass(frozen=True)
class RefreshResult:
    ok: bool
    status: str
    last_refreshed_at: str = ""
    refresh_error: str = ""

    def to_dict(self):
        return {
            "ok": bool(self.ok),
            "status": self.status,
            "last_refreshed_at": self.last_refreshed_at,
            "refresh_error": self.refresh_error,
        }


@dataclass(frozen=True)
class RefreshState:
    last_refreshed_at: str
    last_refreshed_at_display: str
    refresh_status: str
    refresh_error: str
    stale: StaleInfo
    refresh_now: RefreshAction
    background_revalidate: RefreshAction

    def to_dict(self):
        return {
            "last_refreshed_at": self.last_refreshed_at,
            "last_refreshed_at_display": self.last_refreshed_at_display,
            "refresh_status": self.refresh_status,
            "refresh_error": self.refresh_error,
            "stale_state": self.stale.state,
            "stale_label": self.stale.label,
            "stale_age_seconds": self.stale.age_seconds,
            "is_stale": bool(self.stale.is_stale),
            "refresh_now": self.refresh_now.to_dict(),
            "background_revalidate": self.background_revalidate.to_dict(),
        }
