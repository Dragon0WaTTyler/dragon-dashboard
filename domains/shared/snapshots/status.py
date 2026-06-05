from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

from domains.shared.refresh import sanitize_freshness_error


@dataclass(frozen=True)
class SnapshotStatus:
    domain: str
    snapshot_name: str
    local_path: str
    exists: bool
    size_bytes: int
    modified_at: str
    fingerprint: str
    source_of_truth: str
    sync_enabled: bool
    sync_status: str
    last_sync_at: str
    last_error_safe: str
    backup_available: bool
    backup_count: int
    restore_available: bool
    freshness_state: str
    next_action: str
    display_label: str
    display_message: str

    def to_dict(self):
        return {
            "domain": self.domain,
            "snapshot_name": self.snapshot_name,
            "local_path": self.local_path,
            "exists": bool(self.exists),
            "size_bytes": int(self.size_bytes),
            "modified_at": self.modified_at,
            "fingerprint": self.fingerprint,
            "source_of_truth": self.source_of_truth,
            "sync_enabled": bool(self.sync_enabled),
            "sync_status": self.sync_status,
            "last_sync_at": self.last_sync_at,
            "last_error_safe": self.last_error_safe,
            "backup_available": bool(self.backup_available),
            "backup_count": int(self.backup_count),
            "restore_available": bool(self.restore_available),
            "freshness_state": self.freshness_state,
            "next_action": self.next_action,
            "display_label": self.display_label,
            "display_message": self.display_message,
        }


def _isoformat_mtime(path: Path) -> str:
    try:
        stat_result = path.stat()
    except OSError:
        return ""
    try:
        return datetime.fromtimestamp(stat_result.st_mtime, tz=timezone.utc).astimezone().isoformat()
    except Exception:
        return ""


def _snapshot_fingerprint(path: Path) -> str:
    try:
        stat_result = path.stat()
    except OSError:
        return ""
    return f"{int(stat_result.st_mtime_ns)}:{int(stat_result.st_size)}"


def _count_backups(backups_dir: Path, snapshot_name: str) -> int:
    if not backups_dir.exists():
        return 0
    pattern = f"{Path(snapshot_name).stem.replace('_', '-')}-*.json"
    try:
        return len([path for path in backups_dir.glob(pattern) if path.is_file()])
    except OSError:
        return 0


def build_snapshot_status(
    *,
    domain: str,
    snapshot_path,
    backups_dir=None,
    source_of_truth="local_snapshot",
    sync_enabled=False,
    sync_status="idle",
    last_sync_at="",
    last_error="",
    freshness_state="unknown",
    format_timestamp_label: Optional[Callable[[str, str], str]] = None,
):
    snapshot = Path(snapshot_path).expanduser()
    exists = bool(snapshot.exists() and snapshot.is_file())
    size_bytes = int(snapshot.stat().st_size) if exists else 0
    modified_at = _isoformat_mtime(snapshot) if exists else ""
    fingerprint = _snapshot_fingerprint(snapshot) if exists else ""
    normalized_backups_dir = Path(backups_dir).expanduser() if backups_dir else snapshot.parent
    backup_count = _count_backups(normalized_backups_dir, snapshot.name)
    backup_available = backup_count > 0
    restore_available = backup_available
    normalized_sync_enabled = bool(sync_enabled)
    normalized_sync_status = str(sync_status or "").strip().lower() or ("disabled" if not normalized_sync_enabled else "idle")
    normalized_last_sync_at = str(last_sync_at or "").strip()
    normalized_last_error_safe = sanitize_freshness_error(last_error)
    normalized_freshness_state = str(freshness_state or "").strip().lower() or "unknown"

    if normalized_sync_status == "failed" or normalized_last_error_safe:
        normalized_sync_status = "failed"
        next_action = "sync"
        display_label = "Sync failed"
        display_message = "Snapshot sync needs attention."
    elif not exists:
        next_action = "pull_latest" if normalized_sync_enabled else "none"
        display_label = "Snapshot missing"
        display_message = "Local snapshot is missing."
    elif not normalized_sync_enabled:
        normalized_sync_status = "disabled"
        next_action = "none"
        display_label = "Snapshot local only"
        display_message = "Local snapshot is available. Remote sync is disabled."
    elif normalized_freshness_state in {"stale", "failed", "unknown"}:
        next_action = "pull_latest"
        display_label = "Snapshot available"
        display_message = "Local snapshot is available but may need an update."
    else:
        next_action = "none"
        display_label = "Snapshot available"
        display_message = "Local snapshot is available."

    if exists and modified_at and callable(format_timestamp_label):
        modified_display = str(format_timestamp_label(modified_at, "") or "").strip()
        if modified_display:
            display_message = f"{display_message} Last modified {modified_display}."
    if backup_count:
        display_message = f"{display_message} {backup_count} backup{'s' if backup_count != 1 else ''} available."

    return SnapshotStatus(
        domain=str(domain or "").strip(),
        snapshot_name=snapshot.name,
        local_path=str(snapshot),
        exists=exists,
        size_bytes=size_bytes,
        modified_at=modified_at,
        fingerprint=fingerprint,
        source_of_truth=str(source_of_truth or "").strip() or "local_snapshot",
        sync_enabled=normalized_sync_enabled,
        sync_status=normalized_sync_status,
        last_sync_at=normalized_last_sync_at,
        last_error_safe=normalized_last_error_safe,
        backup_available=backup_available,
        backup_count=backup_count,
        restore_available=restore_available,
        freshness_state=normalized_freshness_state,
        next_action=next_action,
        display_label=display_label,
        display_message=display_message,
    )
