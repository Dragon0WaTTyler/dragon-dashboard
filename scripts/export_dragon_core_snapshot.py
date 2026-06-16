#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from domains.api.v1 import DRAGON_CORE_SNAPSHOT_PATH, export_dragon_core_snapshot  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Export the Dragon Core semantic snapshot.")
    parser.add_argument("--output", dest="output_path", default=str(DRAGON_CORE_SNAPSHOT_PATH))
    args = parser.parse_args(argv)

    summary = export_dragon_core_snapshot(Path(args.output_path).expanduser().resolve())
    print(f"path={summary['output_path']}")
    print(f"books_count={summary['books_count']}")
    print(f"articles_count={summary['articles_count']}")
    print(f"movies_count={summary['movies_count']}")
    print(f"youtube_sections_count={summary['youtube_sections_count']}")
    print(f"youtube_videos_count={summary['youtube_videos_count']}")
    print(f"partial={summary['snapshot']['status']['partial']}")
    print(f"warnings={summary['warnings']}")
    print(f"sources={json.dumps(summary['snapshot']['status'].get('sources', {}), ensure_ascii=False, sort_keys=True)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
