"""Service construction helpers for Dragon app wiring."""

from domains.reading import (
    ReadingCacheAccess,
    ReadingRecipeOfDayService,
    ReadingRssService,
    ReadingRuntimeProjectionService,
    ReadingRuntimeService,
    ReadingSnapshotAccess,
    ReadingSyncService,
)
from domains.shared.refresh import RefreshService
from domains.youtube.services import (
    YouTubeFreshnessService,
    YouTubePlaylistService,
    YouTubeRecommendationService,
    YouTubeVideoService,
)


def build_reading_runtime_projection_service(**kwargs):
    return ReadingRuntimeProjectionService(**kwargs)


def build_reading_cache_access(**kwargs):
    return ReadingCacheAccess(**kwargs)


def build_reading_rss_service(**kwargs):
    return ReadingRssService(**kwargs)


def build_reading_sync_service(**kwargs):
    return ReadingSyncService(**kwargs)


def build_refresh_service(**kwargs):
    return RefreshService(**kwargs)


def build_reading_runtime_service(**kwargs):
    return ReadingRuntimeService(**kwargs)


def build_reading_recipe_of_day_service(**kwargs):
    return ReadingRecipeOfDayService(**kwargs)


def build_reading_snapshot_access(**kwargs):
    return ReadingSnapshotAccess(**kwargs)


def build_youtube_recommendation_service(**kwargs):
    return YouTubeRecommendationService(**kwargs)


def build_youtube_playlist_service(**kwargs):
    return YouTubePlaylistService(**kwargs)


def build_youtube_freshness_service(**kwargs):
    return YouTubeFreshnessService(**kwargs)


def build_youtube_video_service(**kwargs):
    return YouTubeVideoService(**kwargs)
