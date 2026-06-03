from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import app as dragon_app


def main():
    parser = argparse.ArgumentParser(description="Print a PocketTube coverage audit from local cache data.")
    parser.add_argument("--scope", default="", help="Optional PocketTube group/section scope.")
    args = parser.parse_args()
    report = dragon_app.YOUTUBE_FRESHNESS_SERVICE.build_pockettube_coverage_report(
        scope=args.scope,
        cache_data=dragon_app.load_cache_data(),
    )
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
