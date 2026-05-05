#!/usr/bin/env python
from __future__ import annotations

import argparse
import signal
import sys
import time
import traceback
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
        "source_count": int(result.get("source_count", 0) or 0),
        "fetched_total": fetched_total,
        "normalized_total": normalized_total,
        "imported_total": imported_total,
        "duplicate_total": duplicate_total,
        "missing_key_total": missing_key_total,
        "failed_source_count": len(failed_sources),
        "failed_sources": failed_sources,
        "source_results": source_results,
        "active_source_count": int(result.get("active_source_count", 0) or 0),
        "last_sync_message": str(result.get("last_sync_message", "") or "").strip(),
        "retention_summary": result.get("retention_summary", {}) if isinstance(result.get("retention_summary", {}), dict) else {},
    }


def _install_signal_handlers() -> None:
    def _handle_signal(signum, _frame):
        raise KeyboardInterrupt(f"Reading sync interrupted by signal {signum}.")

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)


def run_sync(source_id: str = "") -> int:
    started_at = time.monotonic()
    safe_print(f"Reading RSS sync started | source_id={source_id or 'all'}")
    try:
        result = dragon_app.sync_reading_sources(source_id=source_id)
    except KeyboardInterrupt as exc:
        safe_print(str(exc) or "Reading sync cancelled.")
        return 130
    except Exception as exc:
        safe_print(f"Reading sync crashed: {type(exc).__name__}: {exc}")
        safe_print(traceback.format_exc().rstrip())
        return 1

    summary = build_summary(result)
    elapsed = time.monotonic() - started_at
    safe_print(
        "Reading RSS sync completed | "
        f"elapsed={elapsed:.1f}s | "
        f"sources={summary['source_count']} | "
        f"active_sources={summary['active_source_count']} | "
        f"failed_sources={summary['failed_source_count']}"
    )
    safe_print(f"Reading sync result: {summary['imported_total']} imported entries")
    safe_print(f"Active sources: {summary['active_source_count']}")
    safe_print(f"Total sources tracked: {summary['source_count']}")
    safe_print(f"Fetched count: {summary['fetched_total']}")
    safe_print(f"Normalized count: {summary['normalized_total']}")
    safe_print(f"Imported/new count: {summary['imported_total']}")
    safe_print(f"Skipped/duplicate count: {summary['duplicate_total']}")
    safe_print(f"Skipped/missing key count: {summary['missing_key_total']}")
    safe_print(f"Failed sources: {summary['failed_source_count']}")
    if summary["last_sync_message"]:
        safe_print(f"Summary: {summary['last_sync_message']}")
    retention_summary = summary.get("retention_summary", {}) or {}
    if retention_summary:
        safe_print(
            "Retention: "
            f"cap={retention_summary.get('cap', 100)} | "
            f"archived={retention_summary.get('archived_total', 0)}"
        )
        category_summary = retention_summary.get("category_summary", {}) or {}
        if category_summary:
            for category, info in sorted(category_summary.items()):
                safe_print(
                    f"- {category}: active={int((info or {}).get('active_count', 0) or 0)} "
                    f"unprotected={int((info or {}).get('unprotected_count', 0) or 0)} "
                    f"protected={int((info or {}).get('protected_count', 0) or 0)} "
                    f"archived={int((info or {}).get('archived_count', 0) or 0)}"
                )

    if summary["source_results"]:
        safe_print("Sources:")
        for item in summary["source_results"]:
            safe_print(f"- {format_source_line(item)}")

    if summary["failed_sources"]:
        safe_print("Failed sources:")
        for item in summary["failed_sources"]:
            safe_print(f"- {item.get('name', 'Unknown Source')}: {item.get('error') or item.get('reason') or 'Unknown error'}")

    safe_print(f"Reading RSS sync finished | elapsed={elapsed:.1f}s")
    return 0


def main() -> int:
    _install_signal_handlers()
    parser = argparse.ArgumentParser(description="Sync Dragon reading RSS sources into reading_data.json.")
    parser.add_argument("--source-id", default="", help="Optional specific reading source id to sync.")
    args = parser.parse_args()
    try:
        return run_sync(source_id=str(args.source_id or "").strip())
    except KeyboardInterrupt as exc:
        safe_print(str(exc) or "Reading sync cancelled.")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
