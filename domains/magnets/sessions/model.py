from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any


SESSION_STATES = {
    "created",
    "prepared",
    "handed_off",
    "failed",
    "expired",
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def normalize_session_state(value: Any) -> str:
    state = str(value or "").strip().lower()
    if state in SESSION_STATES:
        return state
    return "created"


@dataclass(slots=True)
class StreamSession:
    session_id: str
    movie_id: str
    source_fingerprint: str
    handoff_mode: str
    preferred_runtime: str
    session_state: str
    compatibility_snapshot: dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=utc_now_iso)
    updated_at: str = field(default_factory=utc_now_iso)
    runtime_intent: str = ""
    admission_policy: dict[str, Any] = field(default_factory=dict)
    movie_title: str = ""
    failure_reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["session_state"] = normalize_session_state(payload.get("session_state"))
        payload["compatibility_snapshot"] = dict(payload.get("compatibility_snapshot") or {})
        payload["admission_policy"] = dict(payload.get("admission_policy") or {})
        return payload

