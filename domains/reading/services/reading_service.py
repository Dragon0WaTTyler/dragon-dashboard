import copy


class ReadingService:
    def __init__(
        self,
        *,
        load_reading_data_cached,
        default_reading_data,
        apply_reading_retention_policy,
        normalize_reading_source,
        normalize_reading_url,
        normalize_reading_list_entry,
        parse_timestamp,
        normalize_reading_category,
        normalize_reading_status,
        reading_visible_topic_label,
        reading_entry_matches_filters,
        reading_entry_sort_key,
        reading_category_label,
        format_timestamp_label,
        reading_statuses,
        reading_categories,
        reading_list_default_limit,
        reading_list_limit_max,
        reading_list_limit_step,
    ):
        self.load_reading_data_cached = load_reading_data_cached
        self.default_reading_data = default_reading_data
        self.apply_reading_retention_policy = apply_reading_retention_policy
        self.normalize_reading_source = normalize_reading_source
        self.normalize_reading_url = normalize_reading_url
        self.normalize_reading_list_entry = normalize_reading_list_entry
        self.parse_timestamp = parse_timestamp
        self.normalize_reading_category = normalize_reading_category
        self.normalize_reading_status = normalize_reading_status
        self.reading_visible_topic_label = reading_visible_topic_label
        self.reading_entry_matches_filters = reading_entry_matches_filters
        self.reading_entry_sort_key = reading_entry_sort_key
        self.reading_category_label = reading_category_label
        self.format_timestamp_label = format_timestamp_label
        self.reading_statuses = reading_statuses
        self.reading_categories = reading_categories
        self.reading_list_default_limit = reading_list_default_limit
        self.reading_list_limit_max = reading_list_limit_max
        self.reading_list_limit_step = reading_list_limit_step

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

    def build_reading_view(self, request_args):
        data = self.load_reading_data_cached()
        if not isinstance(data, dict):
            data = self.default_reading_data()
        retained_data, _ = self.apply_reading_retention_policy(copy.deepcopy(data))
        if isinstance(retained_data, dict):
            data = retained_data
        sources = [self.normalize_reading_source(source, index) for index, source in enumerate(data.get("sources", []))]
        source_lookup = {source["name"].lower(): source["id"] for source in sources if source.get("name")}
        source_lookup.update({
            self.normalize_reading_url(source.get("url", "")).lower(): source["id"]
            for source in sources
            if source.get("url")
        })
        source_lookup.update({source["id"]: source["id"] for source in sources})
        source_category_lookup = {source["id"]: source.get("category", "news") for source in sources}
        source_category_lookup.update({source["name"].lower(): source.get("category", "news") for source in sources if source.get("name")})
        raw_entries = list(data.get("entries", []) or [])
        entries = [
            self.normalize_reading_list_entry(entry, index, source_lookup=source_lookup, source_category_lookup=source_category_lookup)
            for index, entry in enumerate(raw_entries)
        ]
        last_sync_timestamp = self.parse_timestamp(str(data.get("last_sync_at", "") or "").strip())
        for entry in entries:
            entry_import_timestamp = self.parse_timestamp(entry.get("imported_at", "")) or self.parse_timestamp(entry.get("added_at", "")) or self.parse_timestamp(entry.get("published_at", ""))
            entry["is_fresh_import"] = bool(
                last_sync_timestamp
                and entry_import_timestamp
                and entry_import_timestamp.timestamp() >= last_sync_timestamp.timestamp()
            )
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
        selected_source = str(request_args.get("source", "All Sources") or "All Sources").strip()
        raw_category = str(request_args.get("category", "All Categories") or "All Categories").strip()
        selected_category = "All Categories" if raw_category.lower() == "all categories" else self.normalize_reading_category(raw_category)
        raw_status = str(request_args.get("status", "All Status") or "All Status").strip().lower()
        selected_status = "All Status" if raw_status == "all status" else self.normalize_reading_status(raw_status)
        raw_search = str(request_args.get("search", "") or "").strip()
        search = raw_search.lower()
        fresh_only = str(request_args.get("fresh", "") or "").strip().lower() in {"1", "true", "yes", "on"}
        try:
            requested_limit = int(request_args.get("limit", self.reading_list_default_limit) or self.reading_list_default_limit)
        except (TypeError, ValueError):
            requested_limit = self.reading_list_default_limit
        requested_limit = max(1, min(requested_limit, self.reading_list_limit_max))
        summary = {
            "total": len(active_entries),
            "unread": len([entry for entry in active_entries if entry.get("status") == "unread"]),
            "reading": len([entry for entry in active_entries if entry.get("status") == "reading"]),
            "starred": len([entry for entry in active_entries if entry.get("starred")]),
        }
        last_sync_at = str(data.get("last_sync_at", "") or "").strip()
        filtered = [
            entry for entry in entries
            if self.reading_entry_matches_filters(
                entry,
                source=selected_source,
                status=selected_status,
                category=selected_category,
                search=search,
            )
        ]
        fresh_entries = [entry for entry in filtered if entry.get("is_fresh_import")]
        fresh_entries.sort(key=self.reading_entry_sort_key, reverse=True)
        if fresh_only and last_sync_timestamp:
            filtered = fresh_entries[:]
        filtered.sort(key=self.reading_entry_sort_key, reverse=True)
        fresh_count = len(fresh_entries)
        total_matching = len(filtered)
        displayed_entries = filtered[:requested_limit]
        has_more = total_matching > len(displayed_entries)
        next_limit = min(requested_limit + self.reading_list_limit_step, self.reading_list_limit_max)
        showing_archived = selected_status == "archived"
        return {
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
