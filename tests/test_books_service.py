import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from app import _entries_cache_result, _update_entries_runtime_cache
from domains.reading.services.books_service import BooksService
from dragon.state import BooksRuntimeState, TimedEntriesCache


def _make_runtime():
    runtime = BooksRuntimeState()
    runtime.books_entries = TimedEntriesCache()
    runtime.quotes_entries = TimedEntriesCache()
    return runtime


def _build_service(*, temp_dir, snapshot_payload, notion_books_database_id="books-db", live_pages=None, live_exception=None):
    snapshot_path = Path(temp_dir) / "books_snapshot.json"
    snapshot_path.write_text("{}", encoding="utf-8")

    runtime = _make_runtime()
    load_books_snapshot = Mock(return_value=snapshot_payload)
    save_books_snapshot = Mock()
    fetch_all_notion_database_pages = Mock(return_value=list(live_pages or []))
    if live_exception is not None:
        fetch_all_notion_database_pages.side_effect = live_exception

    live_entry = {
        "id": "live-book",
        "title": "Live Book",
        "status": "reading",
    }

    service = BooksService(
        books_runtime=runtime,
        time_module=SimpleNamespace(time=lambda: 1000.0),
        books_runtime_ttl_seconds=600,
        books_snapshot_ttl_seconds=21600,
        books_snapshot_path=snapshot_path,
        notion_books_database_id=notion_books_database_id,
        fetch_all_notion_database_pages=fetch_all_notion_database_pages,
        notion_book_page_to_entry=Mock(return_value=live_entry),
        normalize_book_status=lambda value: str(value or "").strip().lower(),
        books_status_label=lambda value: str(value or "").strip().title(),
        load_books_snapshot=load_books_snapshot,
        save_books_snapshot=save_books_snapshot,
        snapshot_age_seconds=lambda _updated_at: 0,
        update_entries_runtime_cache=_update_entries_runtime_cache,
        log_entries_cache_event=Mock(),
        schedule_entries_cache_refresh=Mock(return_value=True),
        entries_cache_result=_entries_cache_result,
    )
    service._load_books_snapshot_mock = load_books_snapshot
    service._save_books_snapshot_mock = save_books_snapshot
    service._fetch_all_notion_database_pages_mock = fetch_all_notion_database_pages
    service._live_entry = live_entry
    return service


class BooksServiceTests(unittest.TestCase):
    def test_valid_snapshot_is_used_normally(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service = _build_service(
                temp_dir=temp_dir,
                snapshot_payload={
                    "updated_at": "2026-06-07T00:00:00+01:00",
                    "entries": [{"id": "snapshot-book", "title": "Snapshot Book"}],
                    "error": "",
                },
            )

            result = service.fetch_books_entries()

        self.assertEqual(result["entries"], [{"id": "snapshot-book", "title": "Snapshot Book"}])
        self.assertEqual(result["error"], "")
        service._fetch_all_notion_database_pages_mock.assert_not_called()
        service.schedule_entries_cache_refresh.assert_not_called()

    def test_empty_snapshot_with_configured_notion_db_fetches_live_entries(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service = _build_service(
                temp_dir=temp_dir,
                snapshot_payload={
                    "updated_at": "2026-06-07T00:00:00+01:00",
                    "entries": [],
                    "error": "",
                },
                live_pages=[{"id": "live-page"}],
            )

            result = service.fetch_books_entries()

        self.assertEqual(result["entries"], [service._live_entry])
        self.assertEqual(result["error"], "")
        service._fetch_all_notion_database_pages_mock.assert_called_once_with(database_id="books-db")
        service._save_books_snapshot_mock.assert_called_once_with(service.books_snapshot_path, [service._live_entry], error="")

    def test_error_snapshot_with_configured_notion_db_fetches_live_entries(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service = _build_service(
                temp_dir=temp_dir,
                snapshot_payload={
                    "updated_at": "2026-06-07T00:00:00+01:00",
                    "entries": [{"id": "stale-book", "title": "Stale Book"}],
                    "error": "Snapshot error",
                },
                live_pages=[{"id": "live-page"}],
            )

            result = service.fetch_books_entries()

        self.assertEqual(result["entries"], [service._live_entry])
        self.assertEqual(result["error"], "")
        service._fetch_all_notion_database_pages_mock.assert_called_once_with(database_id="books-db")
        service._save_books_snapshot_mock.assert_called_once_with(service.books_snapshot_path, [service._live_entry], error="")

    def test_empty_or_error_snapshot_falls_back_when_live_fetch_fails(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service = _build_service(
                temp_dir=temp_dir,
                snapshot_payload={
                    "updated_at": "2026-06-07T00:00:00+01:00",
                    "entries": [{"id": "snapshot-book", "title": "Snapshot Book"}],
                    "error": "Snapshot error",
                },
                live_exception=RuntimeError("Notion unavailable"),
            )

            result = service.fetch_books_entries()

        self.assertEqual(result["entries"], [{"id": "snapshot-book", "title": "Snapshot Book"}])
        self.assertIn("Could not load Books from Notion", result["error"])
        self.assertIn("Notion unavailable", result["error"])
        service._fetch_all_notion_database_pages_mock.assert_called_once_with(database_id="books-db")
        service._save_books_snapshot_mock.assert_not_called()

    def test_no_notion_db_configured_preserves_snapshot_behavior(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service = _build_service(
                temp_dir=temp_dir,
                snapshot_payload={
                    "updated_at": "2026-06-07T00:00:00+01:00",
                    "entries": [],
                    "error": "Snapshot error",
                },
                notion_books_database_id="",
            )

            result = service.fetch_books_entries()

        self.assertEqual(result["entries"], [])
        self.assertEqual(result["error"], "Snapshot error")
        service._fetch_all_notion_database_pages_mock.assert_not_called()
        service.schedule_entries_cache_refresh.assert_not_called()


if __name__ == "__main__":
    unittest.main()
