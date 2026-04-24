#!/usr/bin/env python
from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import app as dragon_app  # noqa: E402


def safe_print(message: str) -> None:
    text = str(message)
    try:
        print(text)
    except UnicodeEncodeError:
        stream = getattr(sys, "stdout", None)
        if stream is None:
            return
        encoding = getattr(stream, "encoding", None) or "utf-8"
        stream.write(text.encode(encoding, errors="replace").decode(encoding, errors="replace") + "\n")


def format_source_line(result: dict) -> str:
    name = str(result.get("name", "Unknown Source") or "Unknown Source").strip()
    status = str(result.get("status", "") or "").strip().lower() or "unknown"
    raw_count = int(result.get("count", 0) or 0)
    normalized = int(result.get("normalized", 0) or 0)
    imported = int(result.get("imported", 0) or 0)
    duplicates = int(result.get("already_existing", 0) or 0)
    missing_key = int(result.get("missing_key", 0) or 0)
    reason = str(result.get("reason", "") or "").strip()
    error = str(result.get("error", "") or "").strip()
    parts = [
        f"{name}",
        f"status={status}",
        f"fetched={raw_count}",
        f"normalized={normalized}",
        f"imported={imported}",
        f"duplicates={duplicates}",
    ]
    if missing_key:
        parts.append(f"missing_key={missing_key}")
    if reason:
        parts.append(f"reason={reason}")
    if error:
        parts.append(f"error={error}")
    return " | ".join(parts)


def build_summary(result: dict) -> dict:
    source_results = list(result.get("source_results", []) or [])
    fetched_total = sum(int(item.get("count", 0) or 0) for item in source_results)
    normalized_total = sum(int(item.get("normalized", 0) or 0) for item in source_results)
    imported_total = int(result.get("imported_total", 0) or 0)
    duplicate_total = sum(int(item.get("already_existing", 0) or 0) for item in source_results)
    missing_key_total = sum(int(item.get("missing_key", 0) or 0) for item in source_results)
    failed_sources = [item for item in source_results if str(item.get("status", "")).strip().lower() == "error"]
    return {
        "fetched_total": fetched_total,
        "normalized_total": normalized_total,
        "imported_total": imported_total,
        "duplicate_total": duplicate_total,
        "missing_key_total": missing_key_total,
        "failed_sources": failed_sources,
        "source_results": source_results,
        "active_source_count": int(result.get("active_source_count", 0) or 0),
        "last_sync_message": str(result.get("last_sync_message", "") or "").strip(),
    }


def run_sync(source_id: str = "") -> int:
    try:
        result = dragon_app.sync_reading_sources(source_id=source_id)
    except Exception as exc:
        safe_print(f"Reading sync crashed: {exc}")
        return 1

    summary = build_summary(result)
    safe_print("Reading RSS sync completed.")
    safe_print(f"Active sources: {summary['active_source_count']}")
    safe_print(f"Fetched count: {summary['fetched_total']}")
    safe_print(f"Normalized count: {summary['normalized_total']}")
    safe_print(f"Imported/new count: {summary['imported_total']}")
    safe_print(f"Skipped/duplicate count: {summary['duplicate_total']}")
    safe_print(f"Skipped/missing key count: {summary['missing_key_total']}")
    if summary["last_sync_message"]:
        safe_print(f"Summary: {summary['last_sync_message']}")

    if summary["source_results"]:
        safe_print("Sources:")
        for item in summary["source_results"]:
            safe_print(f"- {format_source_line(item)}")

    if summary["failed_sources"]:
        safe_print("Failed sources:")
        for item in summary["failed_sources"]:
            safe_print(f"- {item.get('name', 'Unknown Source')}: {item.get('error') or item.get('reason') or 'Unknown error'}")

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync Dragon reading RSS sources into reading_data.json.")
    parser.add_argument("--source-id", default="", help="Optional specific reading source id to sync.")
    args = parser.parse_args()
    return run_sync(source_id=str(args.source_id or "").strip())


if __name__ == "__main__":
    raise SystemExit(main())
