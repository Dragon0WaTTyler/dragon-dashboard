from dataclasses import dataclass
from types import MappingProxyType


@dataclass(frozen=True)
class ReadingRuntimeProjection:
    """Owned read-only projection for source context and lightweight entry rows."""

    sources: tuple
    source_lookup: MappingProxyType
    source_category_lookup: MappingProxyType
    lightweight_entries: tuple


class ReadingRuntimeProjectionService:
    """Owns normalized source context and lightweight read projections."""

    def __init__(
        self,
        *,
        app_logger,
        normalize_reading_source,
        normalize_reading_url,
        absolutize_reading_url,
        reading_hash_key,
        normalize_reading_category,
        normalize_reading_status,
        reading_visible_topic_label,
        reading_short_text_direction,
        reading_title_direction,
        format_timestamp_label,
        monotonic,
    ):
        self.app_logger = app_logger
        self.normalize_reading_source = normalize_reading_source
        self.normalize_reading_url = normalize_reading_url
        self.absolutize_reading_url = absolutize_reading_url
        self.reading_hash_key = reading_hash_key
        self.normalize_reading_category = normalize_reading_category
        self.normalize_reading_status = normalize_reading_status
        self.reading_visible_topic_label = reading_visible_topic_label
        self.reading_short_text_direction = reading_short_text_direction
        self.reading_title_direction = reading_title_direction
        self.format_timestamp_label = format_timestamp_label
        self.monotonic = monotonic

    def build_source_context(self, data, context_label="runtime"):
        started_at = self.monotonic()
        sources = [self.normalize_reading_source(source, index) for index, source in enumerate(data.get("sources", []) or [])]
        source_lookup = {source["name"].lower(): source["id"] for source in sources if source.get("name")}
        source_lookup.update({
            self.normalize_reading_url(source.get("url", "")).lower(): source["id"]
            for source in sources
            if source.get("url")
        })
        source_lookup.update({source["id"]: source["id"] for source in sources})
        source_lookup["__source_name_lookup__"] = {
            source["id"]: source["name"]
            for source in sources
            if source.get("id") and source.get("name")
        }
        source_category_lookup = {source["id"]: source.get("category", "news") for source in sources}
        source_category_lookup.update({
            source["name"].lower(): source.get("category", "news")
            for source in sources
            if source.get("name")
        })
        self.app_logger.info(
            "reading_projection sources elapsed_ms=%.1f context=%s sources=%s source_lookup_keys=%s",
            (self.monotonic() - started_at) * 1000,
            context_label,
            len(sources),
            len(source_lookup),
        )
        return {
            "sources": sources,
            "source_lookup": source_lookup,
            "source_category_lookup": source_category_lookup,
        }

    def _build_lightweight_entry(self, entry, index=0, source_lookup=None, source_category_lookup=None):
        item = entry if isinstance(entry, dict) else {}
        published_at = str(item.get("published_at", "") or "").strip()
        added_at = str(item.get("added_at", "") or "").strip()
        source_name = str(item.get("source", "") or "Unknown Source").strip()
        source_id = str(item.get("source_id", "") or "").strip()
        source_lookup = source_lookup if isinstance(source_lookup, dict) else {}
        source_category_lookup = source_category_lookup if isinstance(source_category_lookup, dict) else {}
        if not source_id:
            source_id = source_lookup.get(source_name.lower(), "")
        url = self.normalize_reading_url(item.get("url", ""))
        original_url = self.normalize_reading_url(item.get("original_url", "")) or url
        title = str(item.get("title", "") or "").strip() or "Untitled article"
        topic = str(item.get("topic", "") or "").strip()
        entry_seed = url or "|".join([source_name.lower(), title.lower(), published_at.lower()])
        entry_id = str(item.get("id", "") or item.get("entry_id", "") or "").strip()
        if not entry_id:
            entry_id = f"reading-{self.reading_hash_key(entry_seed or str(index))}"
        raw_category = str(
            source_category_lookup.get(source_id, "")
            or source_category_lookup.get(source_name.lower(), "")
            or item.get("category", "")
            or ""
        ).strip()
        category = self.normalize_reading_category(raw_category)
        excerpt = str(item.get("excerpt", "") or item.get("summary", "") or "").strip()
        status = self.normalize_reading_status(item.get("status", ""))
        saved = bool(item.get("saved", False) or item.get("starred", False))
        return {
            "id": entry_id,
            "source_id": source_id,
            "source": source_name,
            "source_dir": self.reading_short_text_direction(source_name),
            "title": title,
            "title_dir": self.reading_title_direction(title),
            "url": url,
            "original_url": original_url,
            "published_at": published_at,
            "published_display": str(item.get("published_display", "") or "").strip() or self.format_timestamp_label(published_at, default=""),
            "added_at": added_at,
            "added_display": str(item.get("added_display", "") or "").strip() or self.format_timestamp_label(added_at, default=""),
            "imported_at": str(item.get("imported_at", "") or added_at).strip(),
            "status": status,
            "read": status != "unread",
            "saved": saved,
            "starred": bool(item.get("starred", False)),
            "topic": topic,
            "topic_display": self.reading_visible_topic_label(topic, category),
            "category": category,
            "image_url": self.absolutize_reading_url(item.get("image_url", ""), original_url or url),
            "summary": excerpt,
            "excerpt": excerpt,
        }

    def build_lightweight_entries(self, data, source_lookup=None, source_category_lookup=None, context_label="runtime"):
        started_at = self.monotonic()
        raw_entries = list(data.get("entries", []) or [])
        lightweight_entries = [
            self._build_lightweight_entry(
                entry,
                index,
                source_lookup=source_lookup or {},
                source_category_lookup=source_category_lookup or {},
            )
            for index, entry in enumerate(raw_entries)
        ]
        avg_field_count = (sum(len(entry) for entry in lightweight_entries) / len(lightweight_entries)) if lightweight_entries else 0.0
        self.app_logger.info(
            "reading_projection entries elapsed_ms=%.1f context=%s entries=%s avg_fields=%.1f path=lightweight",
            (self.monotonic() - started_at) * 1000,
            context_label,
            len(lightweight_entries),
            avg_field_count,
        )
        return lightweight_entries

    def build_projection(self, data, context_label="runtime"):
        started_at = self.monotonic()
        source_context = self.build_source_context(data, context_label=context_label)
        lightweight_entries = self.build_lightweight_entries(
            data,
            source_lookup=source_context["source_lookup"],
            source_category_lookup=source_context["source_category_lookup"],
            context_label=context_label,
        )
        projection = ReadingRuntimeProjection(
            sources=tuple(source_context["sources"]),
            source_lookup=MappingProxyType(dict(source_context["source_lookup"])),
            source_category_lookup=MappingProxyType(dict(source_context["source_category_lookup"])),
            lightweight_entries=tuple(lightweight_entries),
        )
        self.app_logger.info(
            "reading_projection build elapsed_ms=%.1f context=%s sources=%s entries=%s owned_projection=1",
            (self.monotonic() - started_at) * 1000,
            context_label,
            len(projection.sources),
            len(projection.lightweight_entries),
        )
        return projection
