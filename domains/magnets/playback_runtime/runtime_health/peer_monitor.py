from __future__ import annotations

from typing import Any, Mapping


def evaluate_peer_health(
    session: Mapping[str, Any],
    status: Mapping[str, Any],
    *,
    stagnant_polls: int = 0,
) -> dict[str, Any]:
    peers_connected = int(status.get("numPeers", session.get("peer_count", 0)) or 0)
    download_speed = float(status.get("downloadSpeed", session.get("download_speed", 0.0)) or 0.0)
    complete = bool(status.get("complete", session.get("complete")))

    disconnected = peers_connected <= 0
    stalled = not complete and stagnant_polls >= 3 and download_speed <= 0
    dead_peer_stall = stalled and disconnected

    return {
        "peers_connected": peers_connected,
        "download_speed": download_speed,
        "peer_state": "disconnected" if disconnected else "connected",
        "stalled": stalled,
        "dead_peer_stall": dead_peer_stall,
    }
