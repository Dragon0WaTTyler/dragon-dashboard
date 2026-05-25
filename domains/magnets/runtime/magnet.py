from __future__ import annotations

import re
from typing import Any
from urllib.parse import parse_qs, urlparse


INFO_HASH_PATTERN = re.compile(r"^[A-Za-z0-9]{32,40}$")


def parse_magnet_uri(value: Any) -> dict[str, Any]:
    text = str(value or "").strip()
    if not text:
        return {
            "raw": "",
            "is_magnet_uri": False,
            "is_valid": False,
            "info_hash": "",
            "display_name": "",
            "trackers": [],
        }

    parsed = urlparse(text)
    query = parse_qs(parsed.query or "", keep_blank_values=False)
    xt_values = [str(item or "").strip() for item in query.get("xt", []) if str(item or "").strip()]
    info_hash = ""
    for item in xt_values:
        if item.lower().startswith("urn:btih:"):
            info_hash = item.split(":")[-1].strip()
            break

    display_name_values = [str(item or "").strip() for item in query.get("dn", []) if str(item or "").strip()]
    trackers = [str(item or "").strip() for item in query.get("tr", []) if str(item or "").strip()]
    is_magnet_uri = parsed.scheme.lower() == "magnet"
    is_valid = bool(is_magnet_uri and INFO_HASH_PATTERN.match(info_hash))

    return {
        "raw": text,
        "is_magnet_uri": is_magnet_uri,
        "is_valid": is_valid,
        "info_hash": info_hash,
        "display_name": display_name_values[0] if display_name_values else "",
        "trackers": trackers,
    }


def is_valid_magnet_uri(value: Any) -> bool:
    return bool(parse_magnet_uri(value).get("is_valid"))
