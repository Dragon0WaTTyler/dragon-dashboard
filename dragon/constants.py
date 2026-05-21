from __future__ import annotations


READING_LOCAL_SYNC_DISABLED_MESSAGE = "Local RSS sync is disabled online. Use GitHub Actions sync."
READING_GITHUB_SYNC_ONLINE_MESSAGE = "Reading sync is handled by GitHub Actions online."
READING_MAP_NEWS_ENGLISH_FEED_URL = "https://www.mapnews.ma/en/rss.xml"
READING_MAP_NEWS_ENGLISH_NAME = "MAP News English"
READING_MOROCCO_WORLD_NEWS_NAME = "Morocco World News"

CACHE_BUCKETS = (
    "films",
    "youtube_playlists",
    "youtube_section_feeds",
    "youtube_channel_latest_uploads",
)

DEFAULT_RUNTIME_CACHE = {
    "initialized": False,
    "films": None,
    "library_films": {},
    "want_to_union_films": None,
    "youtube_playlists": {},
    "youtube_channel_debug": {},
    "youtube_channel_latest_uploads": {},
    "youtube_channel_group_feed_videos": {},
    "youtube_section_picks": {},
    "youtube_section_feeds": {},
    "refreshing": {},
}

