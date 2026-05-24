class BooksService:
    def __init__(
        self,
        *,
        books_runtime,
        time_module,
        books_runtime_ttl_seconds,
        books_snapshot_ttl_seconds,
        books_snapshot_path,
        notion_books_database_id,
        fetch_all_notion_database_pages,
        notion_book_page_to_entry,
        normalize_book_status,
        books_status_label,
        load_books_snapshot,
        save_books_snapshot,
        snapshot_age_seconds,
        update_entries_runtime_cache,
        log_entries_cache_event,
        schedule_entries_cache_refresh,
        entries_cache_result,
    ):
        self.books_runtime = books_runtime
        self.time_module = time_module
        self.books_runtime_ttl_seconds = books_runtime_ttl_seconds
        self.books_snapshot_ttl_seconds = books_snapshot_ttl_seconds
        self.books_snapshot_path = books_snapshot_path
        self.notion_books_database_id = notion_books_database_id
        self.fetch_all_notion_database_pages = fetch_all_notion_database_pages
        self.notion_book_page_to_entry = notion_book_page_to_entry
        self.normalize_book_status = normalize_book_status
        self.books_status_label = books_status_label
        self.load_books_snapshot = load_books_snapshot
        self.save_books_snapshot = save_books_snapshot
        self.snapshot_age_seconds = snapshot_age_seconds
        self.update_entries_runtime_cache = update_entries_runtime_cache
        self.log_entries_cache_event = log_entries_cache_event
        self.schedule_entries_cache_refresh = schedule_entries_cache_refresh
        self.entries_cache_result = entries_cache_result

    def fetch_books_entries(self, force_refresh=False):
        cached_entries = self.books_runtime.books_entries.entries
        cached_updated_at = float(self.books_runtime.books_entries.updated_at or 0)
        runtime_age_seconds = (self.time_module.time() - cached_updated_at) if cached_entries is not None and cached_updated_at else None
        if not force_refresh and cached_entries is not None and runtime_age_seconds is not None and runtime_age_seconds < self.books_runtime_ttl_seconds:
            self.log_entries_cache_event("books", cache_hit="runtime", snapshot_age=int(runtime_age_seconds), refresh_reason="fresh_runtime")
            return self.entries_cache_result(self.books_runtime.books_entries)

        snapshot = self.load_books_snapshot(self.books_snapshot_path)
        snapshot_entries = snapshot.get("entries", [])
        snapshot_error = snapshot.get("error", "")
        snapshot_available = bool(snapshot.get("updated_at")) or self.books_snapshot_path.exists()
        snapshot_age = self.snapshot_age_seconds(snapshot.get("updated_at", ""))
        snapshot_stale = snapshot_age is None or snapshot_age >= self.books_snapshot_ttl_seconds

        if not force_refresh and snapshot_available:
            result = self.update_entries_runtime_cache(
                self.books_runtime.books_entries,
                snapshot_entries,
                error=snapshot_error,
                snapshot_loaded=True,
            )
            self.log_entries_cache_event(
                "books",
                cache_hit="snapshot",
                snapshot_age=int(snapshot_age or 0),
                refresh_reason="stale_snapshot" if snapshot_stale else "fresh_snapshot",
            )
            if snapshot_stale:
                self.schedule_entries_cache_refresh(
                    "books",
                    self.books_runtime.books_entries,
                    self.fetch_books_entries,
                    reason="snapshot_stale",
                )
            return result

        if not force_refresh and cached_entries is not None:
            self.log_entries_cache_event("books", cache_hit="runtime_stale", refresh_reason="snapshot_missing")
            self.schedule_entries_cache_refresh(
                "books",
                self.books_runtime.books_entries,
                self.fetch_books_entries,
                reason="runtime_stale",
            )
            return self.entries_cache_result(self.books_runtime.books_entries)

        database_id = str(self.notion_books_database_id or "").strip()
        if not database_id:
            result = {"entries": [], "error": "Set NOTION_BOOKS_DATABASE_ID to enable Books."}
            self.save_books_snapshot(self.books_snapshot_path, [], error=result["error"])
            return self.update_entries_runtime_cache(self.books_runtime.books_entries, [], error=result["error"])
        try:
            pages = self.fetch_all_notion_database_pages(database_id=database_id)
        except Exception as exc:
            error_message = f"Could not load Books from Notion: {exc}"
            fallback_entries = snapshot_entries if snapshot_available else (cached_entries or [])
            if fallback_entries or snapshot_available:
                self.log_entries_cache_event("books", cache_hit="fallback", refresh_reason="notion_error")
                return self.update_entries_runtime_cache(self.books_runtime.books_entries, fallback_entries, error=error_message)
            return self.update_entries_runtime_cache(self.books_runtime.books_entries, [], error=error_message)

        entries = []
        for page in pages:
            entry = self.notion_book_page_to_entry(page)
            if entry:
                entries.append(entry)

        entries.sort(
            key=lambda item: (
                1 if item.get("pinned") else 0,
                item.get("date_finished") or item.get("created_time") or "",
                item.get("last_edited_time") or "",
                item.get("title", "").lower(),
            ),
            reverse=True,
        )
        self.save_books_snapshot(self.books_snapshot_path, entries, error="")
        self.log_entries_cache_event("books", cache_miss="live", refresh_reason="notion_fetch")
        return self.update_entries_runtime_cache(self.books_runtime.books_entries, entries, error="")

    def filter_books_entries(self, entries, search_text="", status_filter="all"):
        normalized_search = str(search_text or "").strip().lower()
        status_text = str(status_filter or "").strip().lower()
        normalized_status = self.normalize_book_status(status_text) if status_text and status_text != "all" else ""
        filtered = list(entries or [])
        if normalized_status:
            filtered = [entry for entry in filtered if entry.get("status") == normalized_status]
        if normalized_search:
            filtered = [
                entry for entry in filtered
                if normalized_search in str(entry.get("title") or "").lower()
                or normalized_search in str(entry.get("authors_display") or "").lower()
                or normalized_search in str(entry.get("decision") or "").lower()
                or normalized_search in str(entry.get("history") or "").lower()
                or normalized_search in str(entry.get("content") or "").lower()
                or any(normalized_search in str(tag or "").lower() for tag in entry.get("tags", []) or [])
            ]
        return filtered

    def build_books_view(self, request_args):
        fetched = self.fetch_books_entries()
        entries = list(fetched.get("entries", []) or [])
        raw_search = str(request_args.get("search", "") or "").strip()
        raw_status = str(request_args.get("status", "all") or "all").strip().lower() or "all"
        filtered = self.filter_books_entries(entries, search_text=raw_search, status_filter=raw_status)
        statuses = ["all"]
        for entry in entries:
            status_value = str(entry.get("status") or "").strip().lower()
            if status_value and status_value not in statuses:
                statuses.append(status_value)
        if len(statuses) == 1:
            statuses.extend(["reading", "finished", "want to read"])
        status_labels = {status: self.books_status_label(status) for status in statuses if status != "all"}
        return {
            "entries": filtered,
            "total": len(filtered),
            "all_entries_count": len(entries),
            "error_message": str(fetched.get("error") or "").strip(),
            "current_filters": {
                "search": raw_search,
                "status": raw_status,
            },
            "status_options": statuses,
            "status_option_labels": status_labels,
        }
