from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any


@dataclass
class TimedEntriesCache:
    entries: Any = None
    error: str = ""
    updated_at: float = 0.0
    refresh_lock: threading.Lock = field(default_factory=threading.Lock)
    refreshing: bool = False
    last_refresh_started_at: float = 0.0
    last_refresh_completed_at: float = 0.0
    last_snapshot_loaded_at: float = 0.0


@dataclass
class TimedBooksCache:
    books: Any = None
    updated_at: float = 0.0


@dataclass
class ReadingRuntimeState:
    sync_trigger_lock: threading.Lock = field(default_factory=threading.Lock)
    github_refresh_lock: threading.Lock = field(default_factory=threading.Lock)
    tts_generation_lock: threading.Lock = field(default_factory=threading.Lock)
    data_cache_lock: threading.Lock = field(default_factory=threading.Lock)
    data_cache: dict[str, Any] = field(default_factory=lambda: {"fingerprint": None, "data": None})


@dataclass
class BooksRuntimeState:
    cover_cache: dict[str, str] = field(default_factory=dict)
    books_entries: TimedEntriesCache = field(default_factory=TimedEntriesCache)
    quotes_import: TimedBooksCache = field(default_factory=TimedBooksCache)
    quotes_entries: TimedEntriesCache = field(default_factory=TimedEntriesCache)


@dataclass
class YtsRuntimeState:
    torrents_cache: dict[str, Any] = field(default_factory=dict)
    torrents_cache_loaded: bool = False


@dataclass
class YoutubeRuntimeState:
    duration_cache: dict[str, Any] = field(default_factory=dict)
    duration_cache_loaded: bool = False
    playlist_index: dict[str, Any] = field(default_factory=dict)
    channel_debug_index: dict[str, Any] = field(default_factory=dict)
    latest_uploads_index: dict[str, Any] = field(default_factory=dict)
    group_feed_videos_index: dict[str, Any] = field(default_factory=dict)
    section_pick_index: dict[str, Any] = field(default_factory=dict)
    section_feed_index: dict[str, Any] = field(default_factory=dict)


@dataclass
class TmdbRuntimeState:
    lookup_cache: dict[str, Any] = field(default_factory=dict)
    person_lookup_cache: dict[str, Any] = field(default_factory=dict)
    external_ids_cache: dict[str, Any] = field(default_factory=dict)
    country_name_cache: dict[str, str] | None = None


@dataclass
class DragonRuntimeState:
    reading: ReadingRuntimeState = field(default_factory=ReadingRuntimeState)
    books: BooksRuntimeState = field(default_factory=BooksRuntimeState)
    yts: YtsRuntimeState = field(default_factory=YtsRuntimeState)
    youtube: YoutubeRuntimeState = field(default_factory=YoutubeRuntimeState)
    tmdb: TmdbRuntimeState = field(default_factory=TmdbRuntimeState)


RUNTIME_STATE = DragonRuntimeState()
