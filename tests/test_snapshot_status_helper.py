import os
import tempfile
import unittest
from pathlib import Path

from domains.shared.snapshots import build_snapshot_status


class SnapshotStatusHelperTests(unittest.TestCase):
    def test_missing_file_returns_safe_snapshot_status(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            snapshot_path = Path(temp_dir) / "reading_data.json"
            backups_dir = Path(temp_dir) / "backups"

            status = build_snapshot_status(
                domain="reading",
                snapshot_path=snapshot_path,
                backups_dir=backups_dir,
                sync_enabled=True,
                sync_status="idle",
            ).to_dict()

            self.assertFalse(status["exists"])
            self.assertEqual(status["size_bytes"], 0)
            self.assertEqual(status["display_label"], "Snapshot missing")

    def test_existing_file_returns_metadata(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            snapshot_path = Path(temp_dir) / "reading_data.json"
            snapshot_path.write_text('{"ok": true}', encoding="utf-8")

            status = build_snapshot_status(
                domain="reading",
                snapshot_path=snapshot_path,
                backups_dir=Path(temp_dir) / "backups",
                sync_enabled=True,
                sync_status="ready",
            ).to_dict()

            self.assertTrue(status["exists"])
            self.assertGreater(status["size_bytes"], 0)
            self.assertTrue(status["modified_at"])
            self.assertTrue(status["fingerprint"])

    def test_fingerprint_changes_with_mtime_or_size(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            snapshot_path = Path(temp_dir) / "reading_data.json"
            snapshot_path.write_text("{}", encoding="utf-8")
            first = build_snapshot_status(domain="reading", snapshot_path=snapshot_path).to_dict()["fingerprint"]
            snapshot_path.write_text('{"bigger": true}', encoding="utf-8")
            os.utime(snapshot_path, None)
            second = build_snapshot_status(domain="reading", snapshot_path=snapshot_path).to_dict()["fingerprint"]

            self.assertNotEqual(first, second)

    def test_backup_count_and_restore_flags(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            snapshot_path = Path(temp_dir) / "reading_data.json"
            snapshot_path.write_text("{}", encoding="utf-8")
            backups_dir = Path(temp_dir) / "reading"
            backups_dir.mkdir(parents=True, exist_ok=True)
            (backups_dir / "reading-data-20260605-000000-save.json").write_text("{}", encoding="utf-8")
            (backups_dir / "reading-data-20260605-000001-save.json").write_text("{}", encoding="utf-8")

            status = build_snapshot_status(
                domain="reading",
                snapshot_path=snapshot_path,
                backups_dir=backups_dir,
            ).to_dict()

            self.assertEqual(status["backup_count"], 2)
            self.assertTrue(status["backup_available"])
            self.assertTrue(status["restore_available"])

    def test_failed_sync_sanitizes_error(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            snapshot_path = Path(temp_dir) / "reading_data.json"
            snapshot_path.write_text("{}", encoding="utf-8")

            status = build_snapshot_status(
                domain="reading",
                snapshot_path=snapshot_path,
                sync_enabled=True,
                sync_status="failed",
                last_error="ProxyError token=abc Traceback at C:\\secret\\reading.py",
            ).to_dict()

            self.assertEqual(status["sync_status"], "failed")
            self.assertEqual(status["last_error_safe"], "Refresh failed. Try again later.")

    def test_disabled_sync_state(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            snapshot_path = Path(temp_dir) / "reading_data.json"
            snapshot_path.write_text("{}", encoding="utf-8")

            status = build_snapshot_status(
                domain="reading",
                snapshot_path=snapshot_path,
                sync_enabled=False,
            ).to_dict()

            self.assertFalse(status["sync_enabled"])
            self.assertEqual(status["sync_status"], "disabled")
            self.assertEqual(status["next_action"], "none")


if __name__ == "__main__":
    unittest.main()
