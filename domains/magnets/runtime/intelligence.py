from __future__ import annotations

from typing import Any, Mapping


def build_release_pattern(context: Mapping[str, Any]) -> str:
    source_type = str(context.get("source_type") or "unknown").strip() or "unknown"
    resolution = str(context.get("resolution") or "unknown").strip() or "unknown"
    codec = str(context.get("codec") or "unknown").strip() or "unknown"
    return f"{source_type}|{resolution}|{codec}"


def top_ratio_items(
    stats: Mapping[str, Any],
    *,
    success_key: str,
    total_key: str = "total",
    minimum_total: int = 1,
    limit: int = 3,
    reverse: bool = True,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for key, payload in dict(stats or {}).items():
        if not isinstance(payload, Mapping):
            continue
        total = int(payload.get(total_key, 0) or 0)
        if total < minimum_total:
            continue
        success = int(payload.get(success_key, 0) or 0)
        ratio = success / total if total else 0.0
        items.append(
            {
                "key": str(key or "").strip(),
                "count": total,
                "success_count": success,
                "ratio": round(ratio, 4),
            }
        )
    items.sort(key=lambda item: (item["ratio"], item["count"], item["key"]), reverse=reverse)
    return items[:limit]


def failure_ratio_items(
    stats: Mapping[str, Any],
    *,
    failure_key: str,
    total_key: str = "total",
    minimum_total: int = 1,
    limit: int = 3,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for key, payload in dict(stats or {}).items():
        if not isinstance(payload, Mapping):
            continue
        total = int(payload.get(total_key, 0) or 0)
        if total < minimum_total:
            continue
        failures = int(payload.get(failure_key, 0) or 0)
        ratio = failures / total if total else 0.0
        items.append(
            {
                "key": str(key or "").strip(),
                "count": total,
                "failure_count": failures,
                "ratio": round(ratio, 4),
            }
        )
    items.sort(key=lambda item: (item["ratio"], item["count"], item["key"]), reverse=True)
    return items[:limit]


def counter_to_ranked_list(counter: Mapping[str, Any], *, limit: int = 5) -> list[dict[str, Any]]:
    items = [
        {"key": str(key or "").strip(), "count": int(value or 0)}
        for key, value in dict(counter or {}).items()
        if str(key or "").strip()
    ]
    items.sort(key=lambda item: (item["count"], item["key"]), reverse=True)
    return items[:limit]
