class QuotesService:
    def __init__(
        self,
        *,
        books_runtime,
        time_module,
        books_runtime_ttl_seconds,
        books_snapshot_ttl_seconds,
        book_quotes_snapshot_path,
        notion_book_quotes_database_id,
        compact_notion_id,
        resolve_book_quotes_database,
        build_book_quotes_database_schema,
        validate_book_quotes_database_schema,
        fetch_all_notion_database_pages,
        notion_book_quote_page_to_entry,
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
        self.book_quotes_snapshot_path = book_quotes_snapshot_path
        self.notion_book_quotes_database_id = notion_book_quotes_database_id
        self.compact_notion_id = compact_notion_id
        self.resolve_book_quotes_database = resolve_book_quotes_database
        self.build_book_quotes_database_schema = build_book_quotes_database_schema
        self.validate_book_quotes_database_schema = validate_book_quotes_database_schema
        self.fetch_all_notion_database_pages = fetch_all_notion_database_pages
        self.notion_book_quote_page_to_entry = notion_book_quote_page_to_entry
        self.load_books_snapshot = load_books_snapshot
        self.save_books_snapshot = save_books_snapshot
        self.snapshot_age_seconds = snapshot_age_seconds
        self.update_entries_runtime_cache = update_entries_runtime_cache
        self.log_entries_cache_event = log_entries_cache_event
        self.schedule_entries_cache_refresh = schedule_entries_cache_refresh
        self.entries_cache_result = entries_cache_result

    def fetch_book_quotes_entries(self, force_refresh=False):
        cached_entries = self.books_runtime.quotes_entries.entries
        cached_updated_at = float(self.books_runtime.quotes_entries.updated_at or 0)
        runtime_age_seconds = (self.time_module.time() - cached_updated_at) if cached_entries is not None and cached_updated_at else None
        if not force_refresh and cached_entries is not None and runtime_age_seconds is not None and runtime_age_seconds < self.books_runtime_ttl_seconds:
            self.log_entries_cache_event("quotes", cache_hit="runtime", snapshot_age=int(runtime_age_seconds), refresh_reason="fresh_runtime")
            return self.entries_cache_result(self.books_runtime.quotes_entries)

        snapshot = self.load_books_snapshot(self.book_quotes_snapshot_path)
        snapshot_entries = snapshot.get("entries", [])
        snapshot_error = snapshot.get("error", "")
        snapshot_available = bool(snapshot.get("updated_at")) or self.book_quotes_snapshot_path.exists()
        snapshot_age = self.snapshot_age_seconds(snapshot.get("updated_at", ""))
        snapshot_stale = snapshot_age is None or snapshot_age >= self.books_snapshot_ttl_seconds

        if not force_refresh and snapshot_available:
            result = self.update_entries_runtime_cache(
                self.books_runtime.quotes_entries,
                snapshot_entries,
                error=snapshot_error,
                snapshot_loaded=True,
            )
            self.log_entries_cache_event(
                "quotes",
                cache_hit="snapshot",
                snapshot_age=int(snapshot_age or 0),
                refresh_reason="stale_snapshot" if snapshot_stale else "fresh_snapshot",
            )
            if snapshot_stale:
                self.schedule_entries_cache_refresh(
                    "quotes",
                    self.books_runtime.quotes_entries,
                    self.fetch_book_quotes_entries,
                    reason="snapshot_stale",
                )
            return result

        if not force_refresh and cached_entries is not None:
            self.log_entries_cache_event("quotes", cache_hit="runtime_stale", refresh_reason="snapshot_missing")
            self.schedule_entries_cache_refresh(
                "quotes",
                self.books_runtime.quotes_entries,
                self.fetch_book_quotes_entries,
                reason="runtime_stale",
            )
            return self.entries_cache_result(self.books_runtime.quotes_entries)

        database_id = str(self.notion_book_quotes_database_id or "").strip()
        if not database_id:
            result = {"entries": [], "error": "Set NOTION_BOOK_QUOTES_DATABASE_ID to enable Book Quotes."}
            self.save_books_snapshot(self.book_quotes_snapshot_path, [], error=result["error"])
            return self.update_entries_runtime_cache(self.books_runtime.quotes_entries, [], error=result["error"])

        try:
            database_payload = self.resolve_book_quotes_database(database_id=database_id)
            schema = self.build_book_quotes_database_schema(database_payload)
            blockers = self.validate_book_quotes_database_schema(schema)
            if blockers:
                result = {"entries": [], "error": "; ".join(blockers)}
                self.save_books_snapshot(self.book_quotes_snapshot_path, [], error=result["error"])
                return self.update_entries_runtime_cache(self.books_runtime.quotes_entries, [], error=result["error"])
            pages = self.fetch_all_notion_database_pages(database_id=schema.get("database_id", database_id))
        except Exception as exc:
            error_message = f"Could not load Book Quotes from Notion: {exc}"
            fallback_entries = snapshot_entries if snapshot_available else (cached_entries or [])
            if fallback_entries or snapshot_available:
                self.log_entries_cache_event("quotes", cache_hit="fallback", refresh_reason="notion_error")
                return self.update_entries_runtime_cache(self.books_runtime.quotes_entries, fallback_entries, error=error_message)
            return self.update_entries_runtime_cache(self.books_runtime.quotes_entries, [], error=error_message)

        entries = []
        for page in pages:
            entry = self.notion_book_quote_page_to_entry(page, schema)
            if entry:
                entries.append(entry)

        entries.sort(
            key=lambda item: (
                0 if item.get("favorite") else 1,
                item.get("page_sort_value", 10**9),
                item.get("created_time") or "",
                item.get("quote", "").lower(),
            )
        )
        self.save_books_snapshot(self.book_quotes_snapshot_path, entries, error="")
        self.log_entries_cache_event("quotes", cache_miss="live", refresh_reason="notion_fetch")
        return self.update_entries_runtime_cache(self.books_runtime.quotes_entries, entries, error="")

    def fetch_book_quotes_for_entry(self, book_page_id, force_refresh=False):
        book_id = self.compact_notion_id(book_page_id)
        if not book_id:
            return {"entries": [], "error": ""}
        fetched = self.fetch_book_quotes_entries(force_refresh=force_refresh)
        entries = [
            entry for entry in list(fetched.get("entries", []) or [])
            if book_id in list(entry.get("book_relation_ids", []) or [])
        ]
        return {
            "entries": entries,
            "error": str(fetched.get("error") or ""),
        }
