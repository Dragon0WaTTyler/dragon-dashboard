#!/usr/bin/env python
from __future__ import annotations

import argparse
import copy
import json
import sys
from collections import Counter
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


def build_counts(entries: list[dict]) -> tuple[Counter, Counter, Counter]:
    active_total = Counter()
    active_protected = Counter()
    active_unprotected = Counter()
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        category = dragon_app.reading_entry_retention_category(entry)
        if not category:
            continue
        if dragon_app.normalize_reading_status(entry.get("status", "")) == "archived":
            continue
        active_total[category] += 1
        if dragon_app.reading_entry_is_retention_protected(entry):
            active_protected[category] += 1
        else:
            active_unprotected[category] += 1
    return active_total, active_protected, active_unprotected


def format_counter(label: str, counter: Counter) -> str:
    ordered = {category: int(counter.get(category, 0)) for category in sorted(dragon_app.READING_RETENTION_CATEGORIES)}
    return f"{label}: {ordered}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify Dragon Reading retention results without modifying the source file.")
    parser.add_argument(
        "--path",
        default=str(PROJECT_ROOT / "reading_data.json"),
        help="Path to a reading_data.json file or temp copy to inspect in memory.",
    )
    args = parser.parse_args()

    data_path = Path(args.path).expanduser().resolve()
    with data_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    normalized, _ = dragon_app.normalize_reading_data(payload)
    before_total, before_protected, before_unprotected = build_counts(list(normalized.get("entries", []) or []))
    retained, retention_summary = dragon_app.apply_reading_retention_policy(copy.deepcopy(normalized))
    after_total, after_protected, after_unprotected = build_counts(list(retained.get("entries", []) or []))

    safe_print(f"Verification path: {data_path}")
    safe_print("Retention verification runs in memory only. Source file was not modified.")
    safe_print(format_counter("Before active total by category", before_total))
    safe_print(format_counter("Before active protected by category", before_protected))
    safe_print(format_counter("Before active unprotected by category", before_unprotected))
    safe_print(format_counter("After active total by category", after_total))
    safe_print(format_counter("After active protected by category", after_protected))
    safe_print(format_counter("After active unprotected by category", after_unprotected))
    safe_print(f"archived_by_retention: {int(retention_summary.get('archived_total', 0) or 0)}")

    news_unprotected = int(after_unprotected.get("news", 0))
    if news_unprotected > dragon_app.READING_RETENTION_CAP:
        safe_print(
            f"Verification FAILED: news active unprotected is {news_unprotected}, cap is {dragon_app.READING_RETENTION_CAP}."
        )
        return 1

    safe_print(
        f"Verification passed: news active unprotected is {news_unprotected} and protected news remains {int(after_protected.get('news', 0))}."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
