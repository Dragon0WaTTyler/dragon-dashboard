from __future__ import annotations

import os


DEFAULT_HEADERS = {
    "User-Agent": "Dragon/1.0",
    "Accept": "application/json",
}

DEFAULT_TRACKERS = [
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.stealth.si:80/announce",
    "udp://tracker.torrent.eu.org:451/announce",
    "udp://exodus.desync.com:6969/announce",
]

TORRENTIO_BASE_URL = os.getenv("DRAGON_TORRENTIO_BASE_URL", "https://torrentio.strem.fun")
YTS_API_URLS = [
    url.strip()
    for url in os.getenv(
        "DRAGON_YTS_API_URLS",
        "https://movies-api.accel.li/api/v2,https://yts.rs/api/v2",
    ).split(",")
    if url.strip()
]
