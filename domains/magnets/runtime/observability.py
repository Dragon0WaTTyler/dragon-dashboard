from __future__ import annotations

from typing import Any


def emit_event(tag: str, **fields: Any) -> None:
    parts = [tag]
    for key, value in fields.items():
        text = str(value if value is not None else "").strip()
        if not text:
            continue
        parts.append(f"{key}={text}")
    print(" ".join(parts).encode("ascii", errors="backslashreplace").decode("ascii"))

