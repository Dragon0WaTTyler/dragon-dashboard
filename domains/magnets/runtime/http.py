from __future__ import annotations

from typing import Any

import requests

from .config import DEFAULT_HEADERS


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    return session


def safe_json_get(session: requests.Session, url: str, *, params: dict[str, Any] | None = None, timeout: int = 15) -> dict[str, Any]:
    try:
        response = session.get(url, params=params, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}
