import copy


class ReadingSyncService:
    def __init__(
        self,
        *,
        load_reading_data,
        save_reading_data,
        default_reading_data,
        fetch_reading_feed,
        normalize_reading_entry,
        normalize_reading_category,
        normalize_reading_status,
        reading_entry_dedupe_keys,
        reading_entry_content_score,
        reading_source_sync_reason,
        reading_sync_should_preserve_snapshot,
        reading_sync_bbc_backfill_candidate,
        reading_sync_entry_age_timestamp,
        reading_sync_extraction_priority,
        reading_is_bbc_host,
        normalize_reading_url,
        reading_sync_entry_content_text_length,
        reading_sync_entry_has_weak_content_html,
        reading_entry_needs_content_upgrade,
        reading_sync_extract_retry_allowed,
        extract_reading_article_page,
        reading_merge_extraction_snapshot,
        reading_entry_sort_key,
        reading_hash_key,
        format_timestamp_label,
        current_timestamp,
        reading_log_text,
        monotonic,
        datetime_module,
        traceback_module,
        urllib_parse,
        reading_article_fulltext_prepare_record,
        reading_article_fulltext_save,
        dragon_reading_sync_extract_full_content,
        dragon_reading_sync_extract_max_articles,
        dragon_reading_sync_extract_timeout_seconds,
        dragon_reading_sync_extract_failure_retry_hours,
        dragon_reading_sync_extract_slow_log_limit,
        dragon_reading_sync_backfill_bbc,
        dragon_reading_sync_bbc_backfill_max,
        reading_github_sync_online_message,
        app_logger,
    ):
        self.load_reading_data = load_reading_data
        self.save_reading_data = save_reading_data
        self.default_reading_data = default_reading_data
        self.fetch_reading_feed = fetch_reading_feed
        self.normalize_reading_entry = normalize_reading_entry
        self.normalize_reading_category = normalize_reading_category
        self.normalize_reading_status = normalize_reading_status
        self.reading_entry_dedupe_keys = reading_entry_dedupe_keys
        self.reading_entry_content_score = reading_entry_content_score
        self.reading_source_sync_reason = reading_source_sync_reason
        self.reading_sync_should_preserve_snapshot = reading_sync_should_preserve_snapshot
        self.reading_sync_bbc_backfill_candidate = reading_sync_bbc_backfill_candidate
        self.reading_sync_entry_age_timestamp = reading_sync_entry_age_timestamp
        self.reading_sync_extraction_priority = reading_sync_extraction_priority
        self.reading_is_bbc_host = reading_is_bbc_host
        self.normalize_reading_url = normalize_reading_url
        self.reading_sync_entry_content_text_length = reading_sync_entry_content_text_length
        self.reading_sync_entry_has_weak_content_html = reading_sync_entry_has_weak_content_html
        self.reading_entry_needs_content_upgrade = reading_entry_needs_content_upgrade
        self.reading_sync_extract_retry_allowed = reading_sync_extract_retry_allowed
        self.extract_reading_article_page = extract_reading_article_page
        self.reading_merge_extraction_snapshot = reading_merge_extraction_snapshot
        self.reading_entry_sort_key = reading_entry_sort_key
        self.reading_hash_key = reading_hash_key
        self.format_timestamp_label = format_timestamp_label
        self.current_timestamp = current_timestamp
        self.reading_log_text = reading_log_text
        self.monotonic = monotonic
        self.datetime_module = datetime_module
        self.traceback_module = traceback_module
        self.urllib_parse = urllib_parse
        self.reading_article_fulltext_prepare_record = reading_article_fulltext_prepare_record
        self.reading_article_fulltext_save = reading_article_fulltext_save
        self.dragon_reading_sync_extract_full_content = dragon_reading_sync_extract_full_content
        self.dragon_reading_sync_extract_max_articles = dragon_reading_sync_extract_max_articles
        self.dragon_reading_sync_extract_timeout_seconds = dragon_reading_sync_extract_timeout_seconds
        self.dragon_reading_sync_extract_failure_retry_hours = dragon_reading_sync_extract_failure_retry_hours
        self.dragon_reading_sync_extract_slow_log_limit = dragon_reading_sync_extract_slow_log_limit
        self.dragon_reading_sync_backfill_bbc = dragon_reading_sync_backfill_bbc
        self.dragon_reading_sync_bbc_backfill_max = dragon_reading_sync_bbc_backfill_max
        self.reading_github_sync_online_message = reading_github_sync_online_message
        self.app_logger = app_logger

    def _existing_content_keys(self):
        return (
            "image_url",
            "lead_image_url",
            "lead_image_kind",
            "author",
            "author_image_url",
            "excerpt",
            "content_html",
            "content_text",
            "content_score",
            "extraction_status",
            "extraction_error",
            "content_cached_at",
        )

    def _strip_stored_entry_content(self, entries):
        stripped_entries = []
        for entry in list(entries or []):
            if not isinstance(entry, dict):
                continue
            cleaned = dict(entry)
            cleaned.pop("content_html", None)
            cleaned.pop("content_text", None)
            stripped_entries.append(cleaned)
        return stripped_entries

    def _normalize_fetch_attempts(self, fetch_result):
        fetch_attempts = []
        for attempt in list(fetch_result.get("attempts", []) or []):
            if not isinstance(attempt, dict):
                continue
            normalized_attempt = {
                "attempt": int(attempt.get("attempt", 0) or 0),
                "feed_url": str(attempt.get("feed_url", "") or "").strip(),
                "final_url": str(attempt.get("final_url", "") or "").strip(),
                "status_code": int(attempt.get("status_code", 0) or 0),
                "content_type": str(attempt.get("content_type", "") or "").strip(),
                "elapsed_ms": int(attempt.get("elapsed_ms", 0) or 0),
                "request_profile": str(attempt.get("request_profile", "") or "").strip(),
                "selected_headers_profile": str(attempt.get("selected_headers_profile", "") or "").strip(),
                "error": str(attempt.get("error", "") or "").strip(),
            }
            if any(normalized_attempt.values()):
                fetch_attempts.append(normalized_attempt)
        return fetch_attempts

    def _merge_import_into_entries(self, entries, existing_by_key, source, imported):
        merge_started_at = self.monotonic()
        dedupe_keys = self.reading_entry_dedupe_keys({
            **imported,
            "source_id": source.get("id", ""),
        })
        if not dedupe_keys:
            self.app_logger.info(
                "reading_sync merge elapsed_ms=%.1f source=%s imported=0 duplicate=0 missing_key=1 dedupe_keys=0",
                (self.monotonic() - merge_started_at) * 1000,
                self.reading_log_text(source.get("name", "Unknown Source")),
            )
            return {"missing_key": True, "imported": False, "index": None}

        existing_index = next((existing_by_key[key] for key in dedupe_keys if key in existing_by_key), None)
        import_seen_before = existing_index is not None
        import_added_at = self.current_timestamp()
        normalized_import = self.normalize_reading_entry({
            **imported,
            "source": source.get("name", "Unknown Source"),
            "source_id": source.get("id", ""),
            "added_at": import_added_at,
            "imported_at": import_added_at,
            "status": "unread",
            "starred": False,
            "origin": "rss",
            "category": self.normalize_reading_category(imported.get("category", "") or source.get("category", "")),
        }, len(entries))

        if import_seen_before:
            existing = dict(entries[existing_index])
            existing_content_snapshot = {
                key: existing.get(key, "")
                for key in self._existing_content_keys()
                if existing.get(key)
            }
            existing_score = self.reading_entry_content_score(existing)
            import_score = self.reading_entry_content_score(normalized_import)
            preserve_existing_content = bool(existing_content_snapshot) and existing_score >= import_score
            preserved = {
                "id": existing.get("id") or normalized_import["id"],
                "added_at": existing.get("added_at") or normalized_import["added_at"],
                "imported_at": existing.get("imported_at") or existing.get("added_at") or normalized_import["imported_at"],
                "status": self.normalize_reading_status(existing.get("status")),
                "starred": bool(existing.get("starred", False)),
            }
            existing.update(normalized_import)
            existing.update(preserved)
            if preserve_existing_content:
                existing.update(existing_content_snapshot)
            else:
                cached_content = {
                    key: existing.get(key, "")
                    for key in self._existing_content_keys()
                    if existing.get(key) and not normalized_import.get(key)
                }
                existing.update(cached_content)
            if not existing.get("topic"):
                existing["topic"] = normalized_import.get("topic", "")
            if not existing.get("category"):
                existing["category"] = normalized_import.get("category", "")
            if not existing.get("published_at"):
                existing["published_at"] = normalized_import.get("published_at", "")
            existing["published_display"] = self.format_timestamp_label(existing.get("published_at", ""), default="")
            entries[existing_index] = existing
            for dedupe_key in self.reading_entry_dedupe_keys(existing):
                existing_by_key[dedupe_key] = existing_index
            self.app_logger.info(
                "reading_sync merge elapsed_ms=%.1f source=%s imported=0 duplicate=1 missing_key=0 dedupe_keys=%s preserve_existing_content=%s",
                (self.monotonic() - merge_started_at) * 1000,
                self.reading_log_text(source.get("name", "Unknown Source")),
                len(dedupe_keys),
                bool(preserve_existing_content),
            )
            return {"missing_key": False, "imported": False, "index": existing_index}

        primary_key = sorted(dedupe_keys)[0]
        normalized_import["id"] = normalized_import.get("id") or f"reading-{self.reading_hash_key(primary_key)}"
        entries.append(normalized_import)
        new_index = len(entries) - 1
        for dedupe_key in self.reading_entry_dedupe_keys(normalized_import):
            existing_by_key[dedupe_key] = new_index
        self.app_logger.info(
            "reading_sync merge elapsed_ms=%.1f source=%s imported=1 duplicate=0 missing_key=0 dedupe_keys=%s",
            (self.monotonic() - merge_started_at) * 1000,
            self.reading_log_text(source.get("name", "Unknown Source")),
            len(dedupe_keys),
        )
        return {"missing_key": False, "imported": True, "index": new_index}

    def _sync_single_source(self, source, entries, existing_by_key, now):
        fetch_result = self.fetch_reading_feed(source)
        imported_items = list(fetch_result.get("items", []) or [])
        raw_count = int(fetch_result.get("raw_count", 0) or 0)
        normalized_count = int(fetch_result.get("normalized_count", len(imported_items)) or len(imported_items))
        fetch_ok = bool(fetch_result.get("ok", False))
        fetch_error = str(fetch_result.get("error", "") or "").strip()
        fetch_kind = str(fetch_result.get("feed_kind", "") or "").strip()
        fetch_status_code = int(fetch_result.get("status_code", 0) or 0)
        fetch_content_type = str(fetch_result.get("content_type", "") or "").strip()
        fetch_resolved_url = str(fetch_result.get("resolved_url", "") or "").strip()
        fetch_final_url = str(fetch_result.get("final_url", "") or fetch_resolved_url or "").strip()
        fetch_successful_url = str(fetch_result.get("successful_url", "") or "").strip()
        fetch_active = bool(fetch_result.get("active", source.get("active", True)))
        fetch_request_profile = str(fetch_result.get("request_profile", source.get("request_profile", "default")) or "default").strip() or "default"
        fetch_selected_headers_profile = str(fetch_result.get("selected_headers_profile", "") or "").strip()
        fetch_retry_count = int(fetch_result.get("retry_count", 0) or 0)
        fetch_timeout_reason = str(fetch_result.get("timeout_reason", "") or "").strip()
        fetch_bozo = str(fetch_result.get("feedparser_bozo", "") or "").strip()
        fetch_bozo_exception = str(fetch_result.get("feedparser_bozo_exception", "") or "").strip()
        fetch_feedparser_entry_count = int(fetch_result.get("feedparser_entry_count", 0) or 0)
        fetch_source_fallback_used = bool(fetch_result.get("source_fallback_used", False))
        fetch_source_url = str(fetch_result.get("feed_url", source.get("url", "")) or source.get("url", "")).strip()
        fetch_tried_urls = [
            str(url or "").strip()
            for url in list(fetch_result.get("tried_urls", []) or [])
            if str(url or "").strip()
        ]
        fetch_attempts = self._normalize_fetch_attempts(fetch_result)

        source_imported = 0
        source_skipped_existing = 0
        source_skipped_missing_key = 0
        extraction_candidate_indexes = []

        for imported in imported_items:
            merge_result = self._merge_import_into_entries(entries, existing_by_key, source, imported)
            if merge_result["missing_key"]:
                source_skipped_missing_key += 1
                continue
            if merge_result["imported"]:
                source_imported += 1
            else:
                source_skipped_existing += 1
            if merge_result["index"] is not None:
                extraction_candidate_indexes.append(merge_result["index"])

        source["last_synced_at"] = now
        source["last_sync_count"] = raw_count
        source["last_sync_raw_count"] = raw_count
        source["last_sync_normalized_count"] = normalized_count
        source["last_sync_imported_count"] = source_imported
        source["last_sync_already_had_count"] = source_skipped_existing
        source["last_sync_missing_key_count"] = source_skipped_missing_key
        source["last_sync_zero_import_streak"] = int(source.get("last_sync_zero_import_streak", 0) or 0)
        source["last_sync_status_code"] = fetch_status_code
        source["last_sync_content_type"] = fetch_content_type
        source["last_sync_feed_kind"] = fetch_kind
        source["last_sync_request_profile"] = fetch_request_profile
        source["last_sync_selected_headers_profile"] = fetch_selected_headers_profile
        source["last_sync_resolved_url"] = fetch_resolved_url
        source["last_sync_final_url"] = fetch_final_url
        source["last_sync_successful_url"] = fetch_successful_url
        source["last_successful_url"] = fetch_successful_url or str(source.get("last_successful_url", "") or "").strip()
        source["successful_url"] = fetch_successful_url or str(source.get("successful_url", "") or "").strip()
        source["last_sync_tried_urls"] = fetch_tried_urls
        source["last_sync_attempts"] = fetch_attempts
        source["last_sync_retry_count"] = fetch_retry_count
        source["last_sync_timeout_reason"] = fetch_timeout_reason
        source["last_sync_feedparser_bozo"] = fetch_bozo
        source["last_sync_feedparser_bozo_exception"] = fetch_bozo_exception
        source["last_sync_feedparser_entry_count"] = fetch_feedparser_entry_count
        source["last_sync_source_fallback_used"] = fetch_source_fallback_used
        source["last_sync_status"] = "ok" if fetch_ok else ("blocked_source" if fetch_status_code == 403 else "error")
        source["last_sync_error"] = fetch_error if not fetch_ok else ""
        source["last_sync_reason"] = self.reading_source_sync_reason(source)

        if fetch_ok:
            source["last_sync_message"] = (
                f"Fetched {raw_count} item(s), normalized {normalized_count}, "
                f"imported {source_imported}, already had {source_skipped_existing}."
            )
        else:
            blocked_note = ""
            if fetch_status_code == 403:
                blocked_note = "This source is blocking automated fetches from the sync environment."
            failure_prefix = blocked_note or f"Fetch failed ({fetch_kind or 'feed'}{f' {fetch_status_code}' if fetch_status_code else ''})"
            source["last_sync_message"] = f"{failure_prefix}: {fetch_error}" if fetch_error else failure_prefix
        if fetch_ok and source_skipped_missing_key:
            source["last_sync_message"] += f" Skipped {source_skipped_missing_key} item(s) with no stable URL or id."
        if fetch_ok and raw_count == 0:
            source["last_sync_message"] = "Fetched 0 items from feed."
        if fetch_ok and fetch_source_fallback_used and fetch_source_url:
            source["last_sync_message"] += f" Fallback URL used: {fetch_source_url}."
        if fetch_timeout_reason:
            source["last_sync_message"] += f" Timeout note: {fetch_timeout_reason}."
        if fetch_ok and raw_count > 0 and source_imported == 0:
            source["last_sync_zero_import_streak"] = int(source.get("last_sync_zero_import_streak", 0) or 0) + 1
        elif fetch_ok and raw_count == 0:
            source["last_sync_zero_import_streak"] = 0
        elif source_imported > 0:
            source["last_sync_zero_import_streak"] = 0
        source["last_sync_reason"] = self.reading_source_sync_reason(source)
        source["updated_at"] = now

        source_result = {
            "name": source.get("name", "Unknown Source"),
            "active": fetch_active,
            "request_profile": fetch_request_profile,
            "selected_headers_profile": fetch_selected_headers_profile,
            "count": raw_count,
            "normalized": normalized_count,
            "imported": source_imported,
            "already_existing": source_skipped_existing,
            "missing_key": source_skipped_missing_key,
            "status": "ok" if fetch_ok else ("blocked_source" if fetch_status_code == 403 else "error"),
            "reason": source.get("last_sync_reason", ""),
            "feed_kind": fetch_kind,
            "status_code": fetch_status_code,
            "content_type": fetch_content_type,
            "resolved_url": fetch_resolved_url,
            "final_url": fetch_final_url,
            "successful_url": fetch_successful_url,
            "retry_count": fetch_retry_count,
            "timeout_reason": fetch_timeout_reason,
            "feedparser_bozo": fetch_bozo,
            "feedparser_bozo_exception": fetch_bozo_exception,
            "feedparser_entry_count": fetch_feedparser_entry_count,
            "source_fallback_used": fetch_source_fallback_used,
            "tried_urls": fetch_tried_urls,
            "attempts": fetch_attempts,
            "error": fetch_error,
        }
        return {
            "fetch_ok": fetch_ok,
            "fetch_status_code": fetch_status_code,
            "fetch_content_type": fetch_content_type,
            "fetch_retry_count": fetch_retry_count,
            "fetch_timeout_reason": fetch_timeout_reason,
            "fetch_bozo": fetch_bozo,
            "fetch_source_url": fetch_source_url,
            "fetch_resolved_url": fetch_resolved_url,
            "raw_count": raw_count,
            "normalized_count": normalized_count,
            "source_imported": source_imported,
            "source_skipped_existing": source_skipped_existing,
            "source_skipped_missing_key": source_skipped_missing_key,
            "source_result": source_result,
            "extraction_candidate_indexes": extraction_candidate_indexes,
        }

    def _source_failure_result(self, source, exc, now):
        source["last_synced_at"] = now
        source["last_sync_count"] = 0
        source["last_sync_raw_count"] = 0
        source["last_sync_normalized_count"] = 0
        source["last_sync_imported_count"] = 0
        source["last_sync_already_had_count"] = 0
        source["last_sync_missing_key_count"] = 0
        source["last_sync_zero_import_streak"] = int(source.get("last_sync_zero_import_streak", 0) or 0) + 1
        source["last_sync_status_code"] = 0
        source["last_sync_content_type"] = ""
        source["last_sync_feed_kind"] = "error"
        source["last_sync_resolved_url"] = ""
        source["last_sync_final_url"] = ""
        source["last_sync_successful_url"] = ""
        source["last_sync_retry_count"] = 0
        source["last_sync_timeout_reason"] = ""
        source["last_sync_feedparser_bozo"] = ""
        source["last_sync_feedparser_bozo_exception"] = ""
        source["last_sync_feedparser_entry_count"] = 0
        source["last_sync_source_fallback_used"] = False
        source["last_sync_tried_urls"] = []
        source["last_sync_attempts"] = []
        source["last_sync_status"] = "error"
        source["last_sync_error"] = str(exc)
        source["last_sync_message"] = f"Fetch failed: {exc}"
        source["last_sync_reason"] = self.reading_source_sync_reason(source)
        source["updated_at"] = now
        return {
            "name": source.get("name", "Unknown Source"),
            "count": 0,
            "normalized": 0,
            "imported": 0,
            "active": bool(source.get("active", True)),
            "request_profile": str(source.get("request_profile", "default") or "default").strip() or "default",
            "selected_headers_profile": "",
            "status": "error",
            "reason": source.get("last_sync_reason", ""),
            "status_code": 0,
            "content_type": "",
            "resolved_url": "",
            "final_url": "",
            "successful_url": "",
            "retry_count": 0,
            "timeout_reason": "",
            "feedparser_bozo": "",
            "feedparser_bozo_exception": "",
            "feedparser_entry_count": 0,
            "source_fallback_used": False,
            "tried_urls": [],
            "attempts": [],
            "error": str(exc),
        }

    def _collect_bbc_backfill_candidates(self, entries, extraction_summary):
        bbc_backfill_candidate_indexes = []
        if self.dragon_reading_sync_extract_full_content and self.dragon_reading_sync_backfill_bbc and self.dragon_reading_sync_bbc_backfill_max > 0:
            for candidate_index, candidate_entry in enumerate(entries):
                if not self.reading_sync_bbc_backfill_candidate(candidate_entry):
                    continue
                bbc_backfill_candidate_indexes.append(candidate_index)
            bbc_backfill_candidate_indexes.sort(
                key=lambda candidate_index: (
                    self.reading_sync_entry_age_timestamp(entries[candidate_index]).timestamp()
                    if self.reading_sync_entry_age_timestamp(entries[candidate_index]) else float("inf"),
                    candidate_index,
                )
            )
            extraction_summary["bbc_backfill_candidates"] = len(bbc_backfill_candidate_indexes)
            print(
                "[reading-sync] BBC backfill candidates | "
                f"enabled=1 | "
                f"bbc_backfill_candidates={len(bbc_backfill_candidate_indexes)} | "
                f"max={self.dragon_reading_sync_bbc_backfill_max}"
            )
        elif self.dragon_reading_sync_extract_full_content:
            print(
                "[reading-sync] BBC backfill candidates | "
                f"enabled={int(self.dragon_reading_sync_backfill_bbc)} | "
                f"bbc_backfill_candidates=0 | "
                f"max={self.dragon_reading_sync_bbc_backfill_max}"
            )
        return bbc_backfill_candidate_indexes

    def _run_extraction_phase(self, entries, extraction_candidate_indexes, extraction_summary):
        extraction_phase_started_at = self.monotonic()
        bbc_backfill_candidate_indexes = self._collect_bbc_backfill_candidates(entries, extraction_summary)
        extract_full_content = bool(self.dragon_reading_sync_extract_full_content)
        extract_max_articles = int(self.dragon_reading_sync_extract_max_articles or 0)
        extract_timeout_seconds = int(self.dragon_reading_sync_extract_timeout_seconds or 12)
        extract_failure_retry_hours = int(self.dragon_reading_sync_extract_failure_retry_hours or 24)
        extract_slow_log_limit = int(self.dragon_reading_sync_extract_slow_log_limit or 5)

        if not (extract_full_content and extract_max_articles > 0 and (extraction_candidate_indexes or bbc_backfill_candidate_indexes)):
            if extract_full_content and extract_max_articles > 0:
                print("[reading-sync] enrich | no candidate entries needed content extraction")
            return

        now_dt = self.datetime_module.now().astimezone()
        candidate_records = []
        for candidate_index in extraction_candidate_indexes:
            entry = entries[candidate_index] if 0 <= candidate_index < len(entries) else {}
            if self.dragon_reading_sync_backfill_bbc and self.reading_sync_bbc_backfill_candidate(entry):
                continue
            priority = self.reading_sync_extraction_priority(candidate_index, entry)
            candidate_records.append({
                "index": candidate_index,
                "entry": entry,
                "kind": "primary",
                "priority": priority,
                "sort_key": (1,) + tuple(-part for part in priority),
                "age_timestamp": self.reading_sync_entry_age_timestamp(entry),
            })
        for candidate_index in bbc_backfill_candidate_indexes:
            entry = entries[candidate_index] if 0 <= candidate_index < len(entries) else {}
            text_length, _ = self.reading_sync_entry_content_text_length(entry)
            age_timestamp = self.reading_sync_entry_age_timestamp(entry)
            age_sort_value = age_timestamp.timestamp() if age_timestamp else float("inf")
            status_rank = 0 if str(entry.get("extraction_status", "") or "").strip().lower() == "feed" else 1
            candidate_records.append({
                "index": candidate_index,
                "entry": entry,
                "kind": "bbc_backfill",
                "priority": (
                    10000
                    + (3200 if str(entry.get("extraction_status", "") or "").strip().lower() == "feed" else 2600)
                    + (
                        1800
                        if self.reading_is_bbc_host(
                            self.urllib_parse.urlsplit(
                                str(self.normalize_reading_url(entry.get("original_url") or entry.get("url"))) or ""
                            ).netloc.lower()
                        ) else 0
                    )
                    + max(0, 1200 - text_length)
                    + (1500 if self.reading_sync_entry_has_weak_content_html(entry) else 0)
                ),
                "age_timestamp": age_timestamp,
                "sort_key": (
                    0,
                    status_rank,
                    age_sort_value,
                    text_length,
                    candidate_index,
                ),
            })
        candidate_records.sort(key=lambda item: item.get("sort_key", (2, float("inf"), 0, 0, 0)))

        seen_candidate_indexes = set()
        for candidate in candidate_records:
            candidate_index = int(candidate.get("index", -1) or -1)
            candidate_kind = str(candidate.get("kind", "primary") or "primary")
            if candidate_index in seen_candidate_indexes:
                continue
            seen_candidate_indexes.add(candidate_index)
            if candidate_index < 0 or candidate_index >= len(entries):
                continue
            entry = self.normalize_reading_entry(entries[candidate_index], candidate_index)
            article_url = self.normalize_reading_url(entry.get("original_url") or entry.get("url"))
            if not article_url:
                continue
            if candidate_kind == "primary" and extraction_summary["attempted"] >= extract_max_articles:
                continue
            if candidate_kind == "bbc_backfill" and extraction_summary["bbc_backfill_attempted"] >= self.dragon_reading_sync_bbc_backfill_max:
                continue
            if not self.reading_entry_needs_content_upgrade(entry):
                if candidate_kind == "bbc_backfill":
                    continue
                extraction_summary["skipped_cached"] += 1
                continue
            if not self.reading_sync_extract_retry_allowed(entry, retry_after_hours=extract_failure_retry_hours, now_dt=now_dt):
                if candidate_kind == "bbc_backfill":
                    extraction_summary["bbc_backfill_skipped_recent_failure"] += 1
                else:
                    extraction_summary["skipped_recent_failure"] += 1
                continue
            extraction_started_at = self.monotonic()
            if candidate_kind == "bbc_backfill":
                extraction_summary["bbc_backfill_attempted"] += 1
            else:
                extraction_summary["attempted"] += 1
            extraction = self.extract_reading_article_page(article_url, timeout_seconds=extract_timeout_seconds)
            extraction_elapsed = self.monotonic() - extraction_started_at
            merged_entry = self.normalize_reading_entry(
                self.reading_merge_extraction_snapshot(entry, extraction),
                candidate_index,
            )
            extraction_status = str(extraction.get("status", "") or "").strip().lower()
            if extraction_status in {"ok", "partial", "weak_partial"} and (
                str(extraction.get("content_text", "") or "").strip()
                or str(extraction.get("content_html", "") or "").strip()
            ):
                cache_record = self.reading_article_fulltext_prepare_record(entry, extraction, article_url)
                if str(cache_record.get("content_text", "") or "").strip():
                    self.reading_article_fulltext_save(article_url, cache_record)
            entries[candidate_index] = merged_entry
            if extraction_status in {"ok", "partial", "weak_partial"} and (
                self.reading_entry_content_score(merged_entry) >= self.reading_entry_content_score(entry)
                or merged_entry.get("content_html")
                or merged_entry.get("content_text")
                or merged_entry.get("image_url")
            ):
                if candidate_kind == "bbc_backfill":
                    extraction_summary["bbc_backfill_enriched"] += 1
                else:
                    extraction_summary["enriched"] += 1
            elif extraction_status == "failed":
                if candidate_kind == "bbc_backfill":
                    extraction_summary["bbc_backfill_failed"] += 1
                else:
                    extraction_summary["failed"] += 1
            else:
                if candidate_kind == "bbc_backfill":
                    extraction_summary["bbc_backfill_enriched"] += 1
                else:
                    extraction_summary["enriched"] += 1
            extraction_summary["slowest"].append({
                "elapsed": extraction_elapsed,
                "source": entry.get("source", ""),
                "url": article_url,
                "status": extraction_status or "unknown",
                "error": str(extraction.get("error", "") or "").strip(),
                "content_text_length": len(str(extraction.get("content_text", "") or "")),
                "selector": str(extraction.get("extraction_selector", "") or "").strip(),
                "paragraph_count": int(extraction.get("extraction_paragraph_count", 0) or 0),
                "extracted_text_length": int(extraction.get("extraction_text_length", 0) or 0),
                "kind": candidate_kind,
            })
            print(
                "[reading-sync] enrich | "
                f"kind={candidate_kind} | "
                f"source={self.reading_log_text(entry.get('source', 'Unknown Source'))} | "
                f"status={extraction_status or 'unknown'} | "
                f"elapsed={extraction_elapsed:.1f}s | "
                f"content_text_length={len(str(extraction.get('content_text', '') or ''))} | "
                f"selector={str(extraction.get('extraction_selector', '') or '').strip() or 'unknown'} | "
                f"paragraph_count={int(extraction.get('extraction_paragraph_count', 0) or 0)} | "
                f"error={str(extraction.get('error', '') or '').strip() or 'none'} | "
                f"url={article_url}"
            )

        extraction_summary["slowest"] = sorted(
            extraction_summary["slowest"],
            key=lambda item: float(item.get("elapsed", 0.0) or 0.0),
            reverse=True,
        )[:extract_slow_log_limit]
        self.app_logger.info(
            "reading_sync extraction_phase elapsed_ms=%.1f candidate_indexes=%s attempted=%s enriched=%s failed=%s skipped_cached=%s skipped_recent_failure=%s bbc_backfill_attempted=%s",
            (self.monotonic() - extraction_phase_started_at) * 1000,
            len(extraction_candidate_indexes),
            int(extraction_summary.get("attempted", 0) or 0),
            int(extraction_summary.get("enriched", 0) or 0),
            int(extraction_summary.get("failed", 0) or 0),
            int(extraction_summary.get("skipped_cached", 0) or 0),
            int(extraction_summary.get("skipped_recent_failure", 0) or 0),
            int(extraction_summary.get("bbc_backfill_attempted", 0) or 0),
        )

    def sync_reading_sources(self, source_id=""):
        sync_started_at = self.monotonic()
        copy_started_at = self.monotonic()
        loaded_data = self.load_reading_data()
        data = copy.deepcopy(loaded_data)
        copy_elapsed_ms = (self.monotonic() - copy_started_at) * 1000
        source_id = str(source_id or "").strip()
        extract_full_content = bool(self.dragon_reading_sync_extract_full_content)
        extract_max_articles = int(self.dragon_reading_sync_extract_max_articles or 0)
        extract_timeout_seconds = int(self.dragon_reading_sync_extract_timeout_seconds or 12)
        extract_failure_retry_hours = int(self.dragon_reading_sync_extract_failure_retry_hours or 24)
        extract_slow_log_limit = int(self.dragon_reading_sync_extract_slow_log_limit or 5)
        target_sources = []
        for source in data.get("sources", []):
            if not isinstance(source, dict):
                continue
            if source_id and source.get("id") != source_id:
                continue
            if not source.get("active", True):
                continue
            if not str(source.get("url", "") or "").strip():
                continue
            target_sources.append(source)
        total_sources = len(data.get("sources", []) or [])
        print(
            "[reading-sync] start | "
            f"source_id={source_id or 'all'} | "
            f"tracked_sources={total_sources} | "
            f"active_sources={len(target_sources)} | "
            f"extract_full_content={int(extract_full_content)} | "
            f"extract_max_articles={extract_max_articles} | "
            f"extract_timeout={extract_timeout_seconds}s"
        )
        if not target_sources:
            print("[reading-sync] no active sources matched this sync run")

        entries = list(data.get("entries", []))
        self.app_logger.info(
            "reading_sync dataset_loaded elapsed_ms=%.1f copy_ms=%.1f source_id=%s entries=%s sources=%s dataset_copy=1",
            (self.monotonic() - sync_started_at) * 1000,
            copy_elapsed_ms,
            source_id or "all",
            len(entries),
            len(data.get("sources", []) or []),
        )
        existing_by_key = {}
        for index, entry in enumerate(entries):
            for dedupe_key in self.reading_entry_dedupe_keys(entry):
                existing_by_key[dedupe_key] = index

        imported_total = 0
        source_results = []
        zero_import_reasons = {}
        extraction_candidate_indexes = []
        extraction_summary = {
            "enabled": extract_full_content,
            "max_articles": extract_max_articles,
            "timeout_seconds": extract_timeout_seconds,
            "failure_retry_hours": extract_failure_retry_hours,
            "attempted": 0,
            "skipped_cached": 0,
            "skipped_recent_failure": 0,
            "enriched": 0,
            "failed": 0,
            "slowest": [],
            "bbc_backfill_enabled": self.dragon_reading_sync_backfill_bbc,
            "bbc_backfill_max": self.dragon_reading_sync_bbc_backfill_max,
            "bbc_backfill_candidates": 0,
            "bbc_backfill_attempted": 0,
            "bbc_backfill_enriched": 0,
            "bbc_backfill_failed": 0,
            "bbc_backfill_skipped_recent_failure": 0,
        }
        now = self.current_timestamp()

        for position, source in enumerate(target_sources, start=1):
            source_name = str(source.get("name", "Unknown Source") or "Unknown Source").strip() or "Unknown Source"
            safe_source_name = self.reading_log_text(source_name)
            source_started_at = self.monotonic()
            print(
                "[reading-sync] source start | "
                f"{position}/{len(target_sources)} | "
                f"name={safe_source_name}"
            )
            try:
                sync_result = self._sync_single_source(source, entries, existing_by_key, now)
                imported_total += sync_result["source_imported"]
                extraction_candidate_indexes.extend(sync_result["extraction_candidate_indexes"])
                if sync_result["fetch_ok"] and sync_result["source_imported"] == 0:
                    reason = str(source.get("last_sync_reason", "") or "").strip() or "No new items"
                    zero_import_reasons[reason] = zero_import_reasons.get(reason, 0) + 1
                source_results.append(sync_result["source_result"])
                source_elapsed = self.monotonic() - source_started_at
                print(
                    "[reading-sync] source done | "
                    f"{position}/{len(target_sources)} | "
                    f"name={safe_source_name} | "
                    f"elapsed={source_elapsed:.1f}s | "
                    f"fetched={sync_result['raw_count']} | "
                    f"normalized={sync_result['normalized_count']} | "
                    f"imported={sync_result['source_imported']} | "
                    f"duplicates={sync_result['source_skipped_existing']} | "
                    f"missing_key={sync_result['source_skipped_missing_key']} | "
                    f"status={'ok' if sync_result['fetch_ok'] else 'error'} | "
                    f"status_code={sync_result['fetch_status_code'] or 0} | "
                    f"resolved_url={sync_result['fetch_resolved_url'] or sync_result['fetch_source_url'] or source.get('url', '')} | "
                    f"content_type={sync_result['fetch_content_type'] or 'unknown'} | "
                    f"retry_count={sync_result['fetch_retry_count']} | "
                    f"timeout_reason={sync_result['fetch_timeout_reason'] or 'none'} | "
                    f"bozo={sync_result['fetch_bozo'] or '0'}"
                )
            except Exception as exc:
                source_elapsed = self.monotonic() - source_started_at
                source_results.append(self._source_failure_result(source, exc, now))
                print(
                    "[reading-sync] source failed | "
                    f"{position}/{len(target_sources)} | "
                    f"name={safe_source_name} | "
                    f"elapsed={source_elapsed:.1f}s | "
                    f"error={self.reading_log_text(exc)}"
                )
                self.traceback_module.print_exc()

        self._run_extraction_phase(entries, extraction_candidate_indexes, extraction_summary)

        entries = self._strip_stored_entry_content(entries)
        entries.sort(key=self.reading_entry_sort_key, reverse=True)
        data["entries"] = entries
        data["last_sync_at"] = now if target_sources else data.get("last_sync_at", "")
        data["last_sync_count"] = imported_total
        data["last_sync_sources"] = len(target_sources)
        if not target_sources:
            data["last_sync_message"] = "No active sources were available for sync"
        elif imported_total:
            data["last_sync_message"] = f"Imported {imported_total} new items from {len(target_sources)} active source(s)"
        else:
            reason_text = ", ".join(
                f"{count} {reason.lower()}"
                for reason, count in sorted(zero_import_reasons.items(), key=lambda item: (-item[1], item[0].lower()))
            )
            data["last_sync_message"] = f"0 new items from {len(target_sources)} active source(s)"
            if reason_text:
                data["last_sync_message"] += f": {reason_text}"
        if extract_full_content and extract_max_articles > 0:
            data["last_sync_message"] += (
                f" | extraction attempted {extraction_summary['attempted']}, "
                f"enriched {extraction_summary['enriched']}, "
                f"failed {extraction_summary['failed']}, "
                f"skipped cached {extraction_summary['skipped_cached']}, "
                f"skipped recent failures {extraction_summary['skipped_recent_failure']}"
            )
            if self.dragon_reading_sync_backfill_bbc:
                data["last_sync_message"] += (
                    f" | BBC backfill candidates {extraction_summary['bbc_backfill_candidates']}, "
                    f"attempted {extraction_summary['bbc_backfill_attempted']}, "
                    f"enriched {extraction_summary['bbc_backfill_enriched']}, "
                    f"failed {extraction_summary['bbc_backfill_failed']}, "
                    f"skipped recent failures {extraction_summary['bbc_backfill_skipped_recent_failure']}"
                )

        preserve_snapshot = self.reading_sync_should_preserve_snapshot(source_results, len(target_sources), imported_total=imported_total)
        if preserve_snapshot:
            print(
                "[reading-sync] preserve snapshot | "
                f"active_sources={len(target_sources)} | "
                f"imported_total={imported_total} | "
                "reason=proxy_or_403_failures"
            )
            saved_data = self.load_reading_data()
            saved_data = copy.deepcopy(saved_data) if isinstance(saved_data, dict) else self.default_reading_data()
            saved_data["last_sync_message"] = (
                "Proxy/403 failures from all active sources; keeping the existing snapshot unchanged. "
                f"{self.reading_github_sync_online_message}"
            )
            self.app_logger.warning(
                "reading_sync preserve_snapshot active_sources=%s imported_total=%s stale_snapshot_resurrection_risk=1 save_skipped=1",
                len(target_sources),
                imported_total,
            )
        else:
            # Ownership note: sync still owns the full data rewrite, retention pass, and final save in one step.
            saved_data = self.save_reading_data(data, apply_retention=True, retention_reason="sync")

        total_elapsed = self.monotonic() - sync_started_at
        failed_source_count = sum(
            1 for item in source_results if str(item.get("status", "")).strip().lower() in {"error", "blocked_source"}
        )
        print(
            "[reading-sync] finish | "
            f"active_sources={len(target_sources)} | "
            f"imported_total={imported_total} | "
            f"failed_sources={failed_source_count} | "
            f"elapsed={total_elapsed:.1f}s"
        )
        if extract_full_content and extract_max_articles > 0:
            print(
                "[reading-sync] extraction summary | "
                f"attempted={extraction_summary['attempted']} | "
                f"skipped_cached={extraction_summary['skipped_cached']} | "
                f"skipped_recent_failure={extraction_summary['skipped_recent_failure']} | "
                f"enriched={extraction_summary['enriched']} | "
                f"failed={extraction_summary['failed']}"
            )
            if self.dragon_reading_sync_backfill_bbc:
                print(
                    "[reading-sync] BBC backfill summary | "
                    f"bbc_backfill_enabled={int(self.dragon_reading_sync_backfill_bbc)} | "
                    f"bbc_backfill_candidates={extraction_summary['bbc_backfill_candidates']} | "
                    f"bbc_backfill_attempted={extraction_summary['bbc_backfill_attempted']} | "
                    f"bbc_backfill_enriched={extraction_summary['bbc_backfill_enriched']} | "
                    f"bbc_backfill_failed={extraction_summary['bbc_backfill_failed']} | "
                    f"bbc_backfill_skipped_recent_failure={extraction_summary['bbc_backfill_skipped_recent_failure']}"
                )
            for item in extraction_summary.get("slowest", []) or []:
                print(
                    "[reading-sync] slow extraction | "
                    f"elapsed={float(item.get('elapsed', 0.0) or 0.0):.1f}s | "
                    f"source={self.reading_log_text(item.get('source', 'Unknown Source'))} | "
                    f"status={item.get('status', 'unknown')} | "
                    f"content_text_length={int(item.get('content_text_length', 0) or 0)} | "
                    f"selector={item.get('selector', 'unknown')} | "
                    f"paragraph_count={int(item.get('paragraph_count', 0) or 0)} | "
                    f"kind={item.get('kind', 'primary')} | "
                    f"url={item.get('url', '')}"
                )

        return {
            "imported_total": imported_total,
            "source_results": source_results,
            "zero_import_reasons": zero_import_reasons,
            "source_count": len(data.get("sources", [])),
            "active_source_count": len(target_sources),
            "last_sync_at": saved_data.get("last_sync_at", ""),
            "last_sync_message": saved_data.get("last_sync_message", ""),
            "retention_summary": saved_data.get("retention_summary", {}),
            "extraction_summary": extraction_summary,
        }
