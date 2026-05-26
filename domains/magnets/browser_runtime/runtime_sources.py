from __future__ import annotations

from typing import Any, Mapping

from ..runtime.magnet import parse_magnet_uri


def normalize_runtime_source(source: Mapping[str, Any] | None) -> dict[str, Any]:
    data = dict(source or {})
    magnet = str(data.get("magnet") or "").strip()
    magnet_meta = parse_magnet_uri(magnet)
    resolution = str(data.get("resolution") or data.get("quality_label") or "").strip().lower()
    codec = str(data.get("codec") or data.get("codec_label") or "").strip().lower()
    source_type = str(data.get("source_type") or "").strip().upper()
    provider = str(data.get("provider") or data.get("source") or "").strip().lower()
    size_gb = _float_value(data.get("size_gb"))
    errors: list[str] = []
    warnings: list[str] = []

    if not magnet_meta.get("is_valid"):
        errors.append("invalid_magnet")
    if not resolution:
        warnings.append("unknown_resolution")
    if not codec:
        warnings.append("unknown_codec")
    if size_gb <= 0:
        warnings.append("unknown_size")

    descriptor = {
        "source_fingerprint": str(data.get("source_fingerprint") or "").strip(),
        "title": str(data.get("title") or "").strip(),
        "provider": provider,
        "resolution": resolution,
        "codec": codec,
        "source_type": source_type,
        "size_gb": size_gb,
        "magnet": magnet,
        "magnet_valid": bool(magnet_meta.get("is_valid")),
        "likely_streamable": bool(data.get("likely_streamable", True)),
        "runtime_safe_metadata": {
            "seeders": _int_value(data.get("seeders")),
            "hdr": bool(data.get("hdr")),
            "dolby_vision": bool(data.get("dolby_vision")),
            "quality_label": str(data.get("quality_label") or "").strip(),
        },
        "transport_descriptor": {
            "transport_kind": "magnet_uri",
            "tracker_count": len(list(magnet_meta.get("trackers") or [])),
            "display_name": str(magnet_meta.get("display_name") or "").strip(),
        },
        "source_valid": not errors,
        "errors": errors,
        "warnings": warnings,
    }
    return descriptor


def _float_value(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _int_value(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0
