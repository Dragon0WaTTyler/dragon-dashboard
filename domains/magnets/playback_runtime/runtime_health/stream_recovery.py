from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(slots=True)
class RecoveryDecision:
    action: str = "none"
    reason: str = ""
    should_retry: bool = False


def build_recovery_decision(
    session: Mapping[str, Any],
    peer_health: Mapping[str, Any],
    buffer_health: Mapping[str, Any],
    *,
    helper_running: bool,
) -> RecoveryDecision:
    recovery_attempts = int(session.get("recovery_attempts", 0) or 0)
    if recovery_attempts >= 2:
        return RecoveryDecision(action="none", reason="retry_budget_exhausted", should_retry=False)

    if not helper_running:
        return RecoveryDecision(action="restart_session", reason="helper_crashed", should_retry=True)
    if bool(peer_health.get("dead_peer_stall")):
        return RecoveryDecision(action="restart_session", reason="dead_peer_stall", should_retry=True)
    if bool(peer_health.get("stalled")) and bool(buffer_health.get("starvation_risk")):
        return RecoveryDecision(action="refresh_status", reason="temporary_starvation", should_retry=True)
    return RecoveryDecision(action="none", reason="", should_retry=False)
