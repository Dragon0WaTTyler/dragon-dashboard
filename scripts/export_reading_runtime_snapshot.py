#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import app as dragon_app  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Export a lightweight Articles runtime snapshot.")
    parser.add_argument("--input", dest="input_path", default=str(PROJECT_ROOT / "reading_data.json"))
    parser.add_argument("--output", dest="output_path", default=str(PROJECT_ROOT / "cache" / "reading_runtime_snapshot.json"))
    args = parser.parse_args()

    input_path = Path(args.input_path).expanduser().resolve()
    output_path = Path(args.output_path).expanduser().resolve()

    payload = json.loads(input_path.read_text(encoding="utf-8"))
    lightweight_payload, stats = dragon_app.build_lightweight_articles_snapshot(payload)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(lightweight_payload, indent=2, ensure_ascii=False), encoding="utf-8")

    print(
        "Exported Articles runtime snapshot | "
        f"entries={int(stats.get('entries_count', 0) or 0)} | "
        f"sources={int(stats.get('sources_count', 0) or 0)} | "
        f"entries_with_content_before_strip={int(stats.get('entries_with_content_before_strip', 0) or 0)} | "
        f"output={output_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
