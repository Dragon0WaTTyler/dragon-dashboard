class ReadingRuntimeService:
    """GET-path retained read shaping and lightweight article/list runtime context."""

    def __init__(
        self,
        *,
        app_logger,
        load_reading_data_cached,
        default_reading_data,
        reading_data_path,
        normalize_reading_source,
        normalize_reading_url,
        absolutize_reading_url,
        reading_hash_key,
        reading_runtime_projection_service,
        normalize_reading_list_entry,
        parse_timestamp,
        normalize_reading_category,
        normalize_reading_status,
        reading_visible_topic_label,
        reading_short_text_direction,
        reading_title_direction,
        reading_entry_matches_filters,
        reading_entry_sort_key,
        reading_category_label,
        format_timestamp_label,
        reading_statuses,
        reading_categories,
        reading_list_default_limit,
        reading_list_limit_max,
        reading_list_limit_step,
        datetime_module,
        monotonic,
    ):
        self.app_logger = app_logger
        self.load_reading_data_cached = load_reading_data_cached
        self.default_reading_data = default_reading_data
        self.reading_data_path = reading_data_path
        self.normalize_reading_source = normalize_reading_source
        self.normalize_reading_url = normalize_reading_url
        self.absolutize_reading_url = absolutize_reading_url
        self.reading_hash_key = reading_hash_key
        self.reading_runtime_projection_service = reading_runtime_projection_service
        self.normalize_reading_list_entry = normalize_reading_list_entry
        self.parse_timestamp = parse_timestamp
        self.normalize_reading_category = normalize_reading_category
        self.normalize_reading_status = normalize_reading_status
        self.reading_visible_topic_label = reading_visible_topic_label
        self.reading_short_text_direction = reading_short_text_direction
        self.reading_title_direction = reading_title_direction
        self.reading_entry_matches_filters = reading_entry_matches_filters
        self.reading_entry_sort_key = reading_entry_sort_key
        self.reading_category_label = reading_category_label
        self.format_timestamp_label = format_timestamp_label
        self.reading_statuses = reading_statuses
        self.reading_categories = reading_categories
        self.reading_list_default_limit = reading_list_default_limit
        self.reading_list_limit_max = reading_list_limit_max
        self.reading_list_limit_step = reading_list_limit_step
        self.datetime_module = datetime_module
        self.monotonic = monotonic
        self._read_model_comparison_logged = set()

    def reading_filter_query_params(self, filters=None):
        filters = filters if isinstance(filters, dict) else {}
        query = {}
        source = str(filters.get("source", "All Sources") or "All Sources").strip()
        status = str(filters.get("status", "All Status") or "All Status").strip()
        category = str(filters.get("category", "All Categories") or "All Categories").strip()
        search = str(filters.get("search", "") or "").strip()
        limit = filters.get("limit", "")
        fresh = filters.get("fresh", False)
        if source and source != "All Sources":
            query["source"] = source
        if status and status != "All Status":
            query["status"] = status
        if category and category != "All Categories":
            query["category"] = category
        if search:
            query["search"] = search
        if fresh:
            query["fresh"] = "1"
        try:
            normalized_limit = int(limit or 0)
        except (TypeError, ValueError):
            normalized_limit = 0
        if normalized_limit and normalized_limit != self.reading_list_default_limit:
            query["limit"] = normalized_limit
        return query

    def _snapshot_freshness(self):
        try:
            stat_result = self.reading_data_path.stat()
        except OSError:
            return {
                "snapshot_path": str(self.reading_data_path),
                "snapshot_updated_at": "",
                "snapshot_updated_display": "Missing",
                "snapshot_age_seconds": None,
                "snapshot_freshness_state": "missing",
                "snapshot_freshness_label": "Snapshot missing",
            }
        updated_at = self.datetime_module.fromtimestamp(stat_result.st_mtime).astimezone()
        now = self.datetime_module.now().astimezone()
        age_seconds = max(0, int((now - updated_at).total_seconds()))
        if age_seconds <= 6 * 60 * 60:
            state = "fresh"
            label = "Fresh snapshot"
        elif age_seconds <= 24 * 60 * 60:
            state = "aging"
            label = "Snapshot aging"
        else:
            state = "stale"
            label = "Stale snapshot"
        return {
            "snapshot_path": str(self.reading_data_path),
            "snapshot_updated_at": updated_at.isoformat(),
            "snapshot_updated_display": self.format_timestamp_label(updated_at.isoformat(), default="Unknown"),
            "snapshot_age_seconds": age_seconds,
            "snapshot_freshness_state": state,
            "snapshot_freshness_label": label,
        }

    def _load_cached_data_for_get(self):
        started_at = self.monotonic()
        data = self.load_reading_data_cached()
        if not isinstance(data, dict):
            data = self.default_reading_data()
        self.app_logger.info(
            "reading_get_data load elapsed_ms=%.1f entries=%s sources=%s cache_first=1 retention_on_get=0",
            (self.monotonic() - started_at) * 1000,
            len((data or {}).get("entries", []) or []),
            len((data or {}).get("sources", []) or []),
        )
        return data

    def _build_normalized_entries(self, data, source_lookup=None, source_category_lookup=None, context_label="list"):
        started_at = self.monotonic()
        raw_entries = list(data.get("entries", []) or [])
        normalized_entries = [
            self.normalize_reading_list_entry(
                entry,
                index,
                source_lookup=source_lookup or {},
                source_category_lookup=source_category_lookup or {},
            )
            for index, entry in enumerate(raw_entries)
        ]
        self.app_logger.info(
            "reading_normalize entries elapsed_ms=%.1f context=%s entries=%s include_content=0 include_body_image_scan=0",
            (self.monotonic() - started_at) * 1000,
            context_label,
            len(normalized_entries),
        )
        return normalized_entries

    def _build_projection(self, data, context_label="list"):
        return self.reading_runtime_projection_service.build_projection(data, context_label=context_label)

    def _log_read_model_comparison(self, data, source_lookup=None, source_category_lookup=None, context_label="list", lightweight_entries=None):
        if context_label in self._read_model_comparison_logged:
            return
        self._read_model_comparison_logged.add(context_label)
        started_at = self.monotonic()
        normalized_entries = self._build_normalized_entries(
            data,
            source_lookup=source_lookup,
            source_category_lookup=source_category_lookup,
            context_label=f"{context_label}:legacy_compare",
        )
        legacy_elapsed_ms = (self.monotonic() - started_at) * 1000
        lightweight_entries = lightweight_entries if isinstance(lightweight_entries, list) else []
        avg_lightweight_fields = (sum(len(entry) for entry in lightweight_entries) / len(lightweight_entries)) if lightweight_entries else 0.0
        avg_legacy_fields = (sum(len(entry) for entry in normalized_entries) / len(normalized_entries)) if normalized_entries else 0.0
        self.app_logger.info(
            "reading_read_model compare context=%s entries=%s lightweight_fields_avg=%.1f legacy_fields_avg=%.1f legacy_elapsed_ms=%.1f comparison_mode=shadow_once",
            context_label,
            len(lightweight_entries),
            avg_lightweight_fields,
            avg_legacy_fields,
            legacy_elapsed_ms,
        )

    def _entry_import_timestamp(self, entry):
        return (
            self.parse_timestamp(entry.get("imported_at", ""))
            or self.parse_timestamp(entry.get("added_at", ""))
            or self.parse_timestamp(entry.get("published_at", ""))
        )

    def _mark_fresh_import_entries(self, entries, last_sync_timestamp=None, context_label="list"):
        started_at = self.monotonic()
        fresh_count = 0
        for entry in entries:
            entry_import_timestamp = self._entry_import_timestamp(entry)
            is_fresh_import = bool(
                last_sync_timestamp
                and entry_import_timestamp
                and entry_import_timestamp.timestamp() >= last_sync_timestamp.timestamp()
            )
            entry["is_fresh_import"] = is_fresh_import
            if is_fresh_import:
                fresh_count += 1
        self.app_logger.info(
            "reading_read_model freshness elapsed_ms=%.1f context=%s entries=%s fresh=%s",
            (self.monotonic() - started_at) * 1000,
            context_label,
            len(entries),
            fresh_count,
        )
        return fresh_count

    def _parse_view_filters(self, request_args):
        raw_category = str(request_args.get("category", "All Categories") or "All Categories").strip()
        raw_status = str(request_args.get("status", "All Status") or "All Status").strip().lower()
        raw_search = str(request_args.get("search", "") or "").strip()
        try:
            requested_limit = int(request_args.get("limit", self.reading_list_default_limit) or self.reading_list_default_limit)
        except (TypeError, ValueError):
            requested_limit = self.reading_list_default_limit
        requested_limit = max(1, min(requested_limit, self.reading_list_limit_max))
        return {
            "selected_source": str(request_args.get("source", "All Sources") or "All Sources").strip(),
            "selected_category": "All Categories" if raw_category.lower() == "all categories" else self.normalize_reading_category(raw_category),
            "selected_status": "All Status" if raw_status == "all status" else self.normalize_reading_status(raw_status),
            "raw_search": raw_search,
            "search": raw_search.lower(),
            "fresh_only": str(request_args.get("fresh", "") or "").strip().lower() in {"1", "true", "yes", "on"},
            "requested_limit": requested_limit,
        }

    def _filter_entries(self, entries, filters, last_sync_timestamp=None, context_label="list"):
        started_at = self.monotonic()
        filtered = [
            entry for entry in entries
            if self.reading_entry_matches_filters(
                entry,
                source=filters.get("selected_source", "All Sources"),
                status=filters.get("selected_status", "All Status"),
                category=filters.get("selected_category", "All Categories"),
                search=filters.get("search", ""),
            )
        ]
        fresh_entries = [entry for entry in filtered if entry.get("is_fresh_import")]
        fresh_entries.sort(key=self.reading_entry_sort_key, reverse=True)
        if filters.get("fresh_only") and last_sync_timestamp:
            filtered = fresh_entries[:]
        filtered.sort(key=self.reading_entry_sort_key, reverse=True)
        self.app_logger.info(
            "reading_filter slice elapsed_ms=%.1f context=%s entries_in=%s filtered=%s fresh_entries=%s fresh_only=%s search=%s",
            (self.monotonic() - started_at) * 1000,
            context_label,
            len(entries),
            len(filtered),
            len(fresh_entries),
            bool(filters.get("fresh_only")),
            bool(filters.get("raw_search")),
        )
        return filtered, fresh_entries

    def build_reading_view(self, request_args):
        started_at = self.monotonic()
        data = self._load_cached_data_for_get()
        snapshot_freshness = self._snapshot_freshness()
        projection = self._build_projection(data, context_label="list")
        sources = [dict(source) for source in projection.sources]
        source_lookup = dict(projection.source_lookup)
        source_category_lookup = dict(projection.source_category_lookup)
        entries = [dict(entry) for entry in projection.lightweight_entries]
        self._log_read_model_comparison(
            data,
            source_lookup=source_lookup,
            source_category_lookup=source_category_lookup,
            context_label="list",
            lightweight_entries=entries,
        )
        last_sync_timestamp = self.parse_timestamp(str(data.get("last_sync_at", "") or "").strip())
        self._mark_fresh_import_entries(entries, last_sync_timestamp=last_sync_timestamp, context_label="list")
        active_entries = [entry for entry in entries if entry.get("status") != "archived"]
        source_entry_count = {}
        for entry in active_entries:
            key = entry.get("source_id") or entry.get("source", "")
            source_entry_count[key] = source_entry_count.get(key, 0) + 1
        extra_sources = []
        seen_filter_ids = {source["id"] for source in sources}
        for entry in entries:
            source_id = entry.get("source_id") or ""
            source_name = entry.get("source", "") or "Unknown Source"
            if source_id and source_id not in seen_filter_ids:
                extra_sources.append({
                    "id": source_id,
                    "name": source_name,
                    "url": "",
                    "topic": entry.get("topic", ""),
                    "topic_display": self.reading_visible_topic_label(entry.get("topic", ""), entry.get("category", "news")),
                    "category": entry.get("category", "news"),
                    "active": False,
                    "added_at": "",
                    "updated_at": "",
                    "last_synced_at": "",
                    "last_sync_count": 0,
                    "last_sync_status": "",
                })
                seen_filter_ids.add(source_id)
        source_filters = [{"id": "All Sources", "name": "All Sources"}] + sources + extra_sources
        filters = self._parse_view_filters(request_args)
        selected_source = filters["selected_source"]
        selected_category = filters["selected_category"]
        selected_status = filters["selected_status"]
        raw_search = filters["raw_search"]
        fresh_only = filters["fresh_only"]
        requested_limit = filters["requested_limit"]
        summary = {
            "total": len(active_entries),
            "unread": len([entry for entry in active_entries if entry.get("status") == "unread"]),
            "reading": len([entry for entry in active_entries if entry.get("status") == "reading"]),
            "starred": len([entry for entry in active_entries if entry.get("starred")]),
        }
        last_sync_at = str(data.get("last_sync_at", "") or "").strip()
        filtered, fresh_entries = self._filter_entries(entries, filters, last_sync_timestamp=last_sync_timestamp, context_label="list")
        fresh_count = len(fresh_entries)
        total_matching = len(filtered)
        displayed_entries = filtered[:requested_limit]
        has_more = total_matching > len(displayed_entries)
        next_limit = min(requested_limit + self.reading_list_limit_step, self.reading_list_limit_max)
        showing_archived = selected_status == "archived"
        total_elapsed_ms = (self.monotonic() - started_at) * 1000
        self.app_logger.info(
            "reading_view build elapsed_ms=%.1f entries_total=%s entries_filtered=%s entries_displayed=%s sources=%s fresh_only=%s search=%s snapshot_state=%s lightweight_read_model=1 full_entry_rebuild=0",
            total_elapsed_ms,
            len(entries),
            total_matching,
            len(displayed_entries),
            len(sources),
            bool(fresh_only),
            bool(raw_search),
            snapshot_freshness.get("snapshot_freshness_state", ""),
        )
        view = {
            "entries": displayed_entries,
            "sources": source_filters,
            "source_options": source_filters,
            "reading_sources": sources,
            "status_options": [("All Status", "All Status")] + [(status, status.title()) for status in self.reading_statuses],
            "category_options": [("All Categories", "All Categories")] + [(category, self.reading_category_label(category)) for category in self.reading_categories],
            "current_filters": {
                "source": selected_source,
                "status": selected_status,
                "category": selected_category,
                "search": raw_search,
            },
            "filter_query": self.reading_filter_query_params({
                "source": selected_source,
                "status": selected_status,
                "category": selected_category,
                "search": raw_search,
                "limit": requested_limit,
                "fresh": fresh_only,
            }),
            "show_more_query": self.reading_filter_query_params({
                "source": selected_source,
                "status": selected_status,
                "category": selected_category,
                "search": raw_search,
                "limit": next_limit,
                "fresh": fresh_only,
            }),
            "summary": summary,
            "source_count": len(sources),
            "active_source_count": len([source for source in sources if source.get("active", True) and source.get("url")]),
            "source_entry_count": source_entry_count,
            "total_filtered": len(displayed_entries),
            "total_matching": total_matching,
            "render_limit": requested_limit,
            "render_limit_default": self.reading_list_default_limit,
            "render_limit_max": self.reading_list_limit_max,
            "has_more_entries": has_more,
            "next_limit": next_limit,
            "showing_archived": showing_archived,
            "fresh_only": fresh_only,
            "fresh_count": fresh_count,
            "fresh_label": "New since last sync" if fresh_count else "Up to date",
            "last_sync_at": last_sync_at,
            "last_sync_at_display": self.format_timestamp_label(last_sync_at, default="Never"),
            "last_sync_count": int(data.get("last_sync_count", 0) or 0),
            "last_sync_sources": int(data.get("last_sync_sources", 0) or 0),
            "last_sync_message": str(data.get("last_sync_message", "") or "").strip(),
        }
        view.update(snapshot_freshness)
        return view

    def build_reading_article_context(self, entry_id, request_args):
        started_at = self.monotonic()
        data = self._load_cached_data_for_get()
        projection = self._build_projection(data, context_label="article")
        entries = [dict(entry) for entry in projection.lightweight_entries]
        self._log_read_model_comparison(
            data,
            source_lookup=dict(projection.source_lookup),
            source_category_lookup=dict(projection.source_category_lookup),
            context_label="article",
            lightweight_entries=entries,
        )
        filters = self._parse_view_filters(request_args)
        last_sync_timestamp = self.parse_timestamp(str(data.get("last_sync_at", "") or "").strip())
        self._mark_fresh_import_entries(entries, last_sync_timestamp=last_sync_timestamp, context_label="article")
        displayed_entries, _fresh_entries = self._filter_entries(entries, filters, last_sync_timestamp=last_sync_timestamp, context_label="article")
        displayed_entries = displayed_entries[:filters["requested_limit"]]
        normalized_entry_id = str(entry_id or "").strip()
        current_index = next((index for index, item in enumerate(displayed_entries) if item.get("id") == normalized_entry_id), -1)
        preferred_entry = displayed_entries[current_index] if current_index >= 0 else next(
            (item for item in entries if item.get("id") == normalized_entry_id),
            None,
        )
        total_elapsed_ms = (self.monotonic() - started_at) * 1000
        self.app_logger.info(
            "reading_article_context build elapsed_ms=%.1f entry_id=%s entries_total=%s entries_displayed=%s current_index=%s lightweight_read_model=1 retention_on_get=0",
            total_elapsed_ms,
            normalized_entry_id,
            len(entries),
            len(displayed_entries),
            current_index,
        )
        return {
            "entries": displayed_entries,
            "preferred_entry": preferred_entry,
            "current_index": current_index,
            "prev_entry": displayed_entries[current_index - 1] if current_index > 0 else None,
            "next_entry": displayed_entries[current_index + 1] if current_index >= 0 and current_index < len(displayed_entries) - 1 else None,
            "filter_query": self.reading_filter_query_params({
                "source": filters["selected_source"],
                "status": filters["selected_status"],
                "category": filters["selected_category"],
                "search": filters["raw_search"],
                "limit": filters["requested_limit"],
                "fresh": filters["fresh_only"],
            }),
        }
