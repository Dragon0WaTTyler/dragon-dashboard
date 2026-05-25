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
        validate_snapshot_payload,
        normalize_reading_data,
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
        self.validate_snapshot_payload = validate_snapshot_payload
        self.normalize_reading_data = normalize_reading_data
        self.rotate_webhook_backup = rotate_webhook_backup
        self.clear_reading_data_cache = clear_reading_data_cache
        self.reading_data_cache_fingerprint = reading_data_cache_fingerprint
        self.reading_format_mtime = reading_format_mtime
        self.monotonic = monotonic

    def refresh_deployed_reading_snapshot_from_github(self):
        refresh_started_at = self.monotonic()
        with self.reading_runtime.github_refresh_lock:
            old_mtime = self.reading_format_mtime(self.reading_data_path)
            self.app_logger.info("reading_snapshot_download started url=%s", self.reading_snapshot_url)

            try:
                with self.reading_http_session.get(
                    self.reading_snapshot_url,
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
                        temp_path.write_text(json.dumps(normalized_payload, indent=2, ensure_ascii=False), encoding="utf-8")
                        self.app_logger.info(
                            "reading_snapshot_validation ok entries=%s sources=%s",
                            len(normalized_payload.get("entries", []) or []),
                            len(normalized_payload.get("sources", []) or []),
                        )

                        backup_path = ""
                        try:
                            backup_path = self.rotate_webhook_backup()
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
                            self.reading_runtime.data_cache["data"] = normalized_payload
                        self.app_logger.info(
                            "reading_snapshot_cache hydrated entries=%s sources=%s",
                            len(normalized_payload.get("entries", []) or []),
                            len(normalized_payload.get("sources", []) or []),
                        )

                        new_mtime = self.reading_format_mtime(self.reading_data_path)
                        entries_count = len(normalized_payload.get("entries", []) or [])
                        sources_count = len(normalized_payload.get("sources", []) or [])
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
            "entries_count": entries_count,
            "sources_count": sources_count,
        }

    def reading_refresh_snapshot_worker(self, trigger_label="github"):
        try:
            result = self.refresh_deployed_reading_snapshot_from_github()
            self.app_logger.info(
                "reading_snapshot_refresh success trigger=%s entries=%s sources=%s",
                trigger_label,
                result.get("entries_count", 0),
                result.get("sources_count", 0),
            )
        except Exception as exc:
            self.app_logger.warning("reading_snapshot_refresh failed trigger=%s error=%s", trigger_label, exc)
