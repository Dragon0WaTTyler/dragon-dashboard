class ReadingCacheAccess:
    def __init__(
        self,
        *,
        default_reading_data,
        normalize_reading_source,
        normalize_reading_entry,
        normalize_reading_url,
        strip_reading_demo_entries,
        backup_reading_data_file,
        apply_reading_retention_policy,
        load_reading_backup_payload,
        load_json_file,
        save_json_file,
        reading_data_path,
        reading_runtime,
        reading_retention_cap,
        reading_map_news_english_feed_url,
        reading_morocco_world_news_name,
        reading_data_cache_fingerprint,
    ):
        self.default_reading_data = default_reading_data
        self.normalize_reading_source = normalize_reading_source
        self.normalize_reading_entry = normalize_reading_entry
        self.normalize_reading_url = normalize_reading_url
        self.strip_reading_demo_entries = strip_reading_demo_entries
        self.backup_reading_data_file = backup_reading_data_file
        self.apply_reading_retention_policy = apply_reading_retention_policy
        self.load_reading_backup_payload = load_reading_backup_payload
        self.load_json_file = load_json_file
        self.save_json_file = save_json_file
        self.reading_data_path = reading_data_path
        self.reading_runtime = reading_runtime
        self.reading_retention_cap = reading_retention_cap
        self.reading_map_news_english_feed_url = reading_map_news_english_feed_url
        self.reading_morocco_world_news_name = reading_morocco_world_news_name
        self.reading_data_cache_fingerprint = reading_data_cache_fingerprint

    def normalize_reading_data(self, payload):
        data = payload if isinstance(payload, dict) else self.default_reading_data()
        data.setdefault("version", 1)
        data.setdefault("sources", [])
        data.setdefault("entries", [])
        changed = not isinstance(payload, dict)
        normalized_sources = []
        source_lookup = {}
        source_category_lookup = {}
        source_name_lookup = {}
        for index, source in enumerate(data.get("sources", []) or []):
            normalized_source = self.normalize_reading_source(source, index)
            if not isinstance(source, dict) or source != normalized_source:
                changed = True
            normalized_sources.append(normalized_source)
            source_lookup[normalized_source["name"].lower()] = normalized_source["id"]
            source_lookup[normalized_source["id"]] = normalized_source["id"]
            if normalized_source.get("url"):
                normalized_url = self.normalize_reading_url(normalized_source["url"])
                source_lookup[normalized_url.lower()] = normalized_source["id"]
                if normalized_url == self.reading_map_news_english_feed_url:
                    source_lookup[self.reading_morocco_world_news_name.lower()] = normalized_source["id"]
            source_category_lookup[normalized_source["id"]] = normalized_source.get("category", "news")
            source_category_lookup[normalized_source["name"].lower()] = normalized_source.get("category", "news")
            source_name_lookup[normalized_source["id"]] = normalized_source["name"]
        source_lookup["__source_name_lookup__"] = source_name_lookup
        normalized_entries = []
        for index, entry in enumerate(data.get("entries", []) or []):
            normalized_entry = self.normalize_reading_entry(
                entry,
                index,
                source_lookup=source_lookup,
                source_category_lookup=source_category_lookup,
            )
            if not isinstance(entry, dict) or entry != normalized_entry:
                changed = True
            normalized_entries.append(normalized_entry)
        data["sources"] = normalized_sources
        data["entries"] = normalized_entries
        if self.strip_reading_demo_entries(data):
            changed = True
        return data, changed

    def _load_reading_data_uncached(self):
        data = self.load_json_file(self.reading_data_path, None)
        read_failed = self.reading_data_path.exists() and data is None
        if read_failed:
            backup_payload = self.load_reading_backup_payload()
            if backup_payload is not None:
                data = backup_payload
        normalized, changed = self.normalize_reading_data(data)
        if changed and not read_failed:
            self.backup_reading_data_file("normalize")
            self.save_json_file(self.reading_data_path, normalized)
        return normalized

    def load_reading_data(self):
        return self._load_reading_data_uncached()

    def load_reading_data_cached(self):
        fingerprint = self.reading_data_cache_fingerprint()
        with self.reading_runtime.data_cache_lock:
            cached_fingerprint = self.reading_runtime.data_cache.get("fingerprint")
            cached_data = self.reading_runtime.data_cache.get("data")
            if fingerprint and cached_fingerprint == fingerprint and cached_data is not None:
                return cached_data
        data = self._load_reading_data_uncached()
        with self.reading_runtime.data_cache_lock:
            self.reading_runtime.data_cache["fingerprint"] = self.reading_data_cache_fingerprint()
            self.reading_runtime.data_cache["data"] = data
        return data

    def clear_reading_data_cache(self):
        with self.reading_runtime.data_cache_lock:
            self.reading_runtime.data_cache["fingerprint"] = None
            self.reading_runtime.data_cache["data"] = None

    def save_reading_data(self, data, apply_retention=False, retention_reason="save"):
        normalized, _ = self.normalize_reading_data(data)
        retention_summary = {
            "changed": False,
            "archived_total": 0,
            "cap": self.reading_retention_cap,
            "category_summary": {},
        }
        if apply_retention:
            normalized, retention_summary = self.apply_reading_retention_policy(normalized)
            normalized["retention_last_run_reason"] = str(retention_reason or "save").strip() or "save"
            normalized["retention_summary"] = retention_summary
        self.backup_reading_data_file(retention_reason if apply_retention else "save")
        self.save_json_file(self.reading_data_path, normalized)
        return normalized
