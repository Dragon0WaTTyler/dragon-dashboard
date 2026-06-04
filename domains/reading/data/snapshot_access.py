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
        self.backup_reading_data_file = backup_reading_data_file
        self.rotate_webhook_backup = rotate_webhook_backup
        self.clear_reading_data_cache = clear_reading_data_cache
        self.reading_data_cache_fingerprint = reading_data_cache_fingerprint
        self.reading_format_mtime = reading_format_mtime
        self.monotonic = monotonic

    def pull_latest_articles_snapshot(self):
        refresh_started_at = self.monotonic()
        snapshot_url = str(self.reading_snapshot_url or "").strip()
        if not self.reading_snapshot_pull_enabled:
            raise RuntimeError("Remote Articles snapshot pull is disabled.")
        if not snapshot_url:
            raise RuntimeError("Remote Articles snapshot URL is not configured.")
        with self.reading_runtime.github_refresh_lock:
            old_mtime = self.reading_format_mtime(self.reading_data_path)
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
                        lightweight_payload, snapshot_stats = self.build_lightweight_snapshot(normalized_payload)
                        temp_path.write_text(json.dumps(lightweight_payload, indent=2, ensure_ascii=False), encoding="utf-8")
                        written_bytes = temp_path.stat().st_size
                        self.app_logger.info(
                            "reading_snapshot_validation ok entries=%s sources=%s written_bytes=%s stripped_entries=%s",
                            len(lightweight_payload.get("entries", []) or []),
                            len(lightweight_payload.get("sources", []) or []),
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
