from __future__ import annotations

import argparse
import json
import sys
import traceback
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import app as dragon_app


def main():
    parser = argparse.ArgumentParser(description="Sync the PocketTube/YouTube freshness snapshot.")
    parser.add_argument("--scope", default="", help="Optional PocketTube section/group scope.")
    args = parser.parse_args()
    scope = str(args.scope or "").strip()

    try:
        snapshot = dragon_app.YOUTUBE_FRESHNESS_SERVICE.sync_snapshot(scope=scope)
        report = {
            "ok": True,
            "status": "completed",
            "scope": scope,
            "snapshot_version": snapshot.get("version", ""),
            "generated_at": snapshot.get("generated_at", ""),
            "synced_at": snapshot.get("synced_at", ""),
            "group_count": len(snapshot.get("groups", {}) or {}),
            "channel_count": len(snapshot.get("channels", {}) or {}),
            "warnings": list(snapshot.get("warnings", []) or []),
            "errors": list(snapshot.get("errors", []) or []),
        }
        print(json.dumps(report, indent=2, ensure_ascii=False))
        return 0
    except Exception as exc:
        try:
            dragon_app.YOUTUBE_FRESHNESS_SERVICE.save_sync_status({
                "status": "failed",
                "requested_at": dragon_app.current_timestamp(),
                "started_at": dragon_app.current_timestamp(),
                "completed_at": dragon_app.current_timestamp(),
                "last_error": f"{type(exc).__name__}: {exc}",
                "scope": scope,
                "source": "workflow",
                "updated_at": dragon_app.current_timestamp(),
            })
        except Exception:
            pass
        error_payload = {
            "ok": False,
            "status": "failed",
            "scope": scope,
            "error": f"{type(exc).__name__}: {exc}",
            "traceback": traceback.format_exc(),
        }
        print(json.dumps(error_payload, indent=2, ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
