import json


class ReadingSnapshotAccess:
    def __init__(
        self,
        *,
        app_logger,
        reading_runtime,
        reading_data_path,
        base_dir,
        temp_file_factory,
        path_class,
        requests_module,
        reading_http_session,
        reading_snapshot_url,
        reading_snapshot_pull_enabled,
        validate_snapshot_payload,
        normalize_reading_data,
        build_lightweight_snapshot,
        load_reading_sources_registry,
        apply_reading_sources_registry_overrides,
        backup_reading_data_file,
        rotate_webhook_backup,
        clear_reading_data_cache,
        reading_data_cache_fingerprint,
        reading_format_mtime,
        monotonic,
    ):
        self.app_logger = app_logger
        self.reading_runtime = reading_runtime
        self.reading_data_path = reading_data_path
        self.base_dir = base_dir
        self.temp_file_factory = temp_file_factory
        self.path_class = path_class
        self.requests_module = requests_module
        self.reading_http_session = reading_http_session
        self.reading_snapshot_url = reading_snapshot_url
        self.reading_snapshot_pull_enabled = reading_snapshot_pull_enabled
        self.validate_snapshot_payload = validate_snapshot_payload
        self.normalize_reading_data = normalize_reading_data
        self.build_lightweight_snapshot = build_lightweight_snapshot
        self.load_reading_sources_registry = load_reading_sources_registry
        self.apply_reading_sources_registry_overrides = apply_reading_sources_registry_overrides
        self.backup_reading_data_file = backup_reading_data_file
        self.rotate_webhook_backup = rotate_webhook_backup
        self.clear_reading_data_cache = clear_reading_data_cache
        self.reading_data_cache_fingerprint = reading_data_cache_fingerprint
        self.reading_format_mtime = reading_format_mtime
        self.monotonic = monotonic

    def _load_local_payload(self):
        if not self.reading_data_path.exists():
            return {}
        try:
            return json.loads(self.reading_data_path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _source_match_keys(self, source):
        item = source if isinstance(source, dict) else {}
        name = str(item.get("name", "") or "").strip().lower()
        url = str(item.get("url", "") or "").strip().lower()
        return name, url

    def _merge_sources_keep_local_first(self, local_sources, remote_sources):
        merged_sources = []
        local_sources = [dict(source) for source in (local_sources or []) if isinstance(source, dict)]
        remote_sources = [dict(source) for source in (remote_sources or []) if isinstance(source, dict)]

        for local_source in local_sources:
            merged_sources.append(local_source)

        local_by_name = {}
        local_by_url = {}
        for local_source in local_sources:
            name_key, url_key = self._source_match_keys(local_source)
            if name_key:
                local_by_name[name_key] = local_source
            if url_key:
                local_by_url[url_key] = local_source

        remote_source_match_map = {
            "ids": {},
            "names": {},
            "urls": {},
        }
        for remote_source in remote_sources:
            remote_source_id = str(remote_source.get("id", "") or remote_source.get("source_id", "") or "").strip()
            name_key, url_key = self._source_match_keys(remote_source)
            matched_local = None
            if url_key and url_key in local_by_url:
                matched_local = local_by_url[url_key]
            elif name_key and name_key in local_by_name:
                matched_local = local_by_name[name_key]

            if matched_local is not None:
                if remote_source_id and matched_local.get("id"):
                    remote_source_match_map["ids"][remote_source_id] = matched_local.get("id")
                if name_key and matched_local.get("id"):
                    remote_source_match_map["names"][name_key] = matched_local.get("id")
                if url_key and matched_local.get("id"):
                    remote_source_match_map["urls"][url_key] = matched_local.get("id")
                continue

            merged_sources.append(remote_source)
            if name_key:
                local_by_name[name_key] = remote_source
            if url_key:
                local_by_url[url_key] = remote_source
            if remote_source_id and remote_source.get("id"):
                remote_source_match_map["ids"][remote_source_id] = remote_source.get("id")
            if name_key and remote_source.get("id"):
                remote_source_match_map["names"][name_key] = remote_source.get("id")
            if url_key and remote_source.get("id"):
                remote_source_match_map["urls"][url_key] = remote_source.get("id")

        return merged_sources, remote_source_match_map

    def _remap_remote_entries_to_merged_sources(self, entries, merged_sources, remote_source_match_map):
        merged_sources = [dict(source) for source in (merged_sources or []) if isinstance(source, dict)]
        remote_source_match_map = remote_source_match_map if isinstance(remote_source_match_map, dict) else {}
        remote_id_map = remote_source_match_map.get("ids", {}) if isinstance(remote_source_match_map.get("ids", {}), dict) else {}
        remote_name_map = remote_source_match_map.get("names", {}) if isinstance(remote_source_match_map.get("names", {}), dict) else {}
        remote_url_map = remote_source_match_map.get("urls", {}) if isinstance(remote_source_match_map.get("urls", {}), dict) else {}
        merged_by_name = {}
        merged_by_url = {}
        merged_by_id = {}
        for merged_source in merged_sources:
            name_key, url_key = self._source_match_keys(merged_source)
            if name_key:
                merged_by_name[name_key] = merged_source
            if url_key:
                merged_by_url[url_key] = merged_source
            if merged_source.get("id"):
                merged_by_id[merged_source.get("id")] = merged_source

        remapped_entries = []
        for entry in list(entries or []):
            if not isinstance(entry, dict):
                remapped_entries.append(entry)
                continue
            updated_entry = dict(entry)
            source_id = str(updated_entry.get("source_id", "") or "").strip()
            source_name = str(updated_entry.get("source", "") or "").strip().lower()
            feed_url = str(updated_entry.get("feed_url", "") or "").strip().lower()

            target_source = None
            if source_id and source_id in remote_id_map:
                updated_entry["source_id"] = remote_id_map[source_id]
                target_source = merged_by_id.get(updated_entry["source_id"])
            elif feed_url and feed_url in remote_url_map:
                updated_entry["source_id"] = remote_url_map[feed_url]
                target_source = merged_by_id.get(updated_entry["source_id"])
            elif source_name and source_name in remote_name_map:
                updated_entry["source_id"] = remote_name_map[source_name]
                target_source = merged_by_id.get(updated_entry["source_id"])
            elif feed_url and feed_url in merged_by_url:
                target_source = merged_by_url[feed_url]
                if target_source.get("id"):
                    updated_entry["source_id"] = target_source.get("id")
            elif source_name and source_name in merged_by_name:
                target_source = merged_by_name[source_name]
                if target_source.get("id"):
                    updated_entry["source_id"] = target_source.get("id")

            if target_source is not None and target_source.get("name"):
                updated_entry["source"] = target_source.get("name")
            remapped_entries.append(updated_entry)
        return remapped_entries

    def pull_latest_articles_snapshot(self):
        refresh_started_at = self.monotonic()
        snapshot_url = str(self.reading_snapshot_url or "").strip()
        if not self.reading_snapshot_pull_enabled:
            raise RuntimeError("Remote Articles snapshot pull is disabled.")
        if not snapshot_url:
            raise RuntimeError("Remote Articles snapshot URL is not configured.")
        with self.reading_runtime.github_refresh_lock:
            old_mtime = self.reading_format_mtime(self.reading_data_path)
            local_payload = self._load_local_payload()
            normalized_local_payload, _ = self.normalize_reading_data(local_payload)
            local_sources = list(normalized_local_payload.get("sources", []) or [])
            local_source_count = len(local_sources)
            self.app_logger.info("reading_snapshot_download started url=%s", snapshot_url)

            try:
                with self.reading_http_session.get(
                    snapshot_url,
                    timeout=30,
                    stream=True,
                ) as response:
                    response.raise_for_status()
                    downloaded_bytes = 0
                    temp_path = None
                    try:
                        with self.temp_file_factory(
                            mode="wb",
                            delete=False,
                            dir=str(self.base_dir),
                            prefix="reading_data.",
                            suffix=".download.tmp",
                        ) as temp_file:
                            temp_path = self.path_class(temp_file.name)
                            for chunk in response.iter_content(chunk_size=1024 * 1024):
                                if not chunk:
                                    continue
                                temp_file.write(chunk)
                                downloaded_bytes += len(chunk)
                        self.app_logger.info("reading_snapshot_download downloaded bytes=%s", downloaded_bytes)

                        try:
                            raw_payload = json.loads(temp_path.read_text(encoding="utf-8"))
                        except Exception as exc:
                            self.app_logger.warning("reading_snapshot_validation failed reason=invalid_json error=%s", exc)
                            raise RuntimeError(f"Downloaded Reading snapshot is not valid JSON: {exc}") from exc

                        is_valid, validation_error = self.validate_snapshot_payload(raw_payload, downloaded_bytes)
                        if not is_valid:
                            self.app_logger.warning("reading_snapshot_validation failed reason=%s", validation_error)
                            raise RuntimeError(validation_error)

                        normalized_payload, _ = self.normalize_reading_data(raw_payload)
                        remote_sources = list(normalized_payload.get("sources", []) or [])
                        remote_source_count = len(remote_sources)
                        merged_sources, remote_source_match_map = self._merge_sources_keep_local_first(local_sources, remote_sources)
                        registry_sources = self.load_reading_sources_registry() if callable(self.load_reading_sources_registry) else []
                        if registry_sources and callable(self.apply_reading_sources_registry_overrides):
                            merged_sources, _registry_changed = self.apply_reading_sources_registry_overrides(merged_sources, registry_sources)
                        final_payload = dict(normalized_payload)
                        final_payload["sources"] = merged_sources
                        final_payload["entries"] = self._remap_remote_entries_to_merged_sources(
                            normalized_payload.get("entries", []) or [],
                            merged_sources,
                            remote_source_match_map,
                        )
                        lightweight_payload, snapshot_stats = self.build_lightweight_snapshot(final_payload)
                        temp_path.write_text(json.dumps(lightweight_payload, indent=2, ensure_ascii=False), encoding="utf-8")
                        written_bytes = temp_path.stat().st_size
                        self.app_logger.info(
                            "reading_snapshot_validation ok entries=%s sources=%s local_sources=%s remote_sources=%s written_bytes=%s stripped_entries=%s",
                            len(lightweight_payload.get("entries", []) or []),
                            len(lightweight_payload.get("sources", []) or []),
                            local_source_count,
                            remote_source_count,
                            written_bytes,
                            int(snapshot_stats.get("entries_with_content_before_strip", 0) or 0),
                        )

                        backup_path = ""
                        try:
                            backup_path = self.backup_reading_data_file("remote-pull")
                        except Exception as exc:
                            self.app_logger.warning("reading_snapshot_backup failed error=%s", exc)
                        try:
                            self.rotate_webhook_backup()
                        except OSError as exc:
                            self.app_logger.warning("reading_snapshot_backup failed error=%s", exc)

                        temp_path.replace(self.reading_data_path)
                        self.app_logger.info(
                            "reading_snapshot_atomic_replace done path=%s backup=%s",
                            self.reading_data_path,
                            backup_path or "none",
                        )

                        self.clear_reading_data_cache()
                        with self.reading_runtime.data_cache_lock:
                            self.reading_runtime.data_cache["fingerprint"] = self.reading_data_cache_fingerprint()
                            self.reading_runtime.data_cache["data"] = lightweight_payload
                        self.app_logger.info(
                            "reading_snapshot_cache hydrated entries=%s sources=%s",
                            len(lightweight_payload.get("entries", []) or []),
                            len(lightweight_payload.get("sources", []) or []),
                        )

                        new_mtime = self.reading_format_mtime(self.reading_data_path)
                        entries_count = len(lightweight_payload.get("entries", []) or [])
                        sources_count = len(lightweight_payload.get("sources", []) or [])
                        updated_at = str(lightweight_payload.get("snapshot_updated_at", "") or lightweight_payload.get("last_sync_at", "") or "").strip()
                        with_content_count = int(snapshot_stats.get("entries_with_content_after_strip", 0) or 0)
                        merged_source_count = sources_count
                    finally:
                        if temp_path is not None and temp_path.exists():
                            try:
                                temp_path.unlink()
                            except Exception:
                                pass
            except self.requests_module.RequestException as exc:
                self.app_logger.warning("reading_snapshot_download failed error=%s", exc)
                raise RuntimeError(f"Download failed: {exc}") from exc
        self.app_logger.info(
            "reading_snapshot_download finished elapsed_ms=%.1f old_mtime=%s new_mtime=%s bytes=%s entries=%s sources=%s",
            (self.monotonic() - refresh_started_at) * 1000,
            old_mtime,
            new_mtime,
            downloaded_bytes,
            entries_count,
            sources_count,
        )

        return {
            "ok": True,
            "status": "updated",
            "old_mtime": old_mtime,
            "new_mtime": new_mtime,
            "downloaded_bytes": downloaded_bytes,
            "written_bytes": written_bytes,
            "entries_count": entries_count,
            "entry_count": entries_count,
            "sources_count": sources_count,
            "source_count": sources_count,
            "remote_source_count": remote_source_count,
            "local_source_count": local_source_count,
            "merged_source_count": merged_source_count,
            "updated_at": updated_at,
            "with_content_count": with_content_count,
            "backup_path": backup_path,
        }

    def refresh_deployed_reading_snapshot_from_github(self):
        return self.pull_latest_articles_snapshot()

    def reading_refresh_snapshot_worker(self, trigger_label="github"):
        try:
            result = self.pull_latest_articles_snapshot()
            self.app_logger.info(
                "reading_snapshot_refresh success trigger=%s entries=%s sources=%s",
                trigger_label,
                result.get("entries_count", 0),
                result.get("sources_count", 0),
            )
        except Exception as exc:
            self.app_logger.warning("reading_snapshot_refresh failed trigger=%s error=%s", trigger_label, exc)
